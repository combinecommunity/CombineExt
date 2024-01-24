// swiftlint:disable all

//===----------------------------------------------------------------------===//
//
// This source file is part of the Swift.org open source project
//
// Copyright (c) 2014 - 2019 Apple Inc. and the Swift project authors
// Licensed under Apache License v2.0 with Runtime Library Exception
//
// See https://swift.org/LICENSE.txt for license information
// See https://swift.org/CONTRIBUTORS.txt for the list of Swift project authors
//
//===----------------------------------------------------------------------===//

// Only support 64bit
#if !(os(iOS) && (arch(i386) || arch(arm))) && canImport(Combine)
  import Combine
  import Foundation

  @available(OSX 10.15, iOS 13.0, tvOS 13.0, watchOS 6.0, *)
  extension Scheduler {
    /// Returns a publisher that repeatedly emits the scheduler's current time on the given
    /// interval.
    ///
    /// - Parameters:
    ///   - interval: The time interval on which to publish events. For example, a value of `0.5`
    ///     publishes an event approximately every half-second.
    ///   - tolerance: The allowed timing variance when emitting events. Defaults to `nil`, which
    ///     allows any variance.
    ///   - options: Scheduler options passed to the timer. Defaults to `nil`.
    /// - Returns: A publisher that repeatedly emits the current date on the given interval.
    internal func timerPublisher(
      every interval: SchedulerTimeType.Stride,
      tolerance: SchedulerTimeType.Stride? = nil,
      options: SchedulerOptions? = nil
    ) -> Publishers.Timer<Self> {
      Publishers.Timer(every: interval, tolerance: tolerance, scheduler: self, options: options)
    }
  }

  @available(macOS 10.15, iOS 13.0, tvOS 13.0, watchOS 6.0, *)
  extension Publishers {
    /// A publisher that emits a scheduler's current time on a repeating interval.
    ///
    /// This publisher is an alternative to Foundation's `Timer.publisher`, with its primary
    /// difference being that it allows you to use any scheduler for the timer, not just `RunLoop`.
    /// This is useful because the `RunLoop` scheduler is not testable in the sense that if you want
    /// to write tests against a publisher that makes use of `Timer.publisher` you must explicitly
    /// wait for time to pass in order to get emissions. This is likely to lead to fragile tests and
    /// greatly bloat the time your tests take to execute.
    ///
    /// It can be used much like Foundation's timer, except you specify a scheduler rather than a
    /// run loop:
    ///
    ///     Publishers.Timer(every: .seconds(1), scheduler: DispatchQueue.main)
    ///       .sink { print("Timer", $0) }
    ///
    /// But more importantly, you can use it with `TestScheduler` so that any Combine code you write
    /// involving timers becomes more testable. This shows how we can easily simulate the idea of
    /// moving time forward 1,000 seconds in a timer:
    ///
    ///     let scheduler = DispatchQueue.testScheduler
    ///     var output: [Int] = []
    ///
    ///     Publishers.Timer(every: 1, scheduler: scheduler)
    ///       .sink { _ in output.append(output.count) }
    ///       .store(in: &self.cancellables)
    ///
    ///     XCTAssertEqual(output, [])
    ///
    ///     scheduler.advance(by: 1)
    ///     XCTAssertEqual(output, [0])
    ///
    ///     scheduler.advance(by: 1)
    ///     XCTAssertEqual(output, [0, 1])
    ///
    ///     scheduler.advance(by: 1_000)
    ///     XCTAssertEqual(output, Array(0...1_001))
    ///
    internal final class Timer<S: Scheduler>: ConnectablePublisher {
      internal typealias Output = S.SchedulerTimeType
      internal typealias Failure = Never

      internal let interval: S.SchedulerTimeType.Stride
      internal let options: S.SchedulerOptions?
      internal let scheduler: S
      internal let tolerance: S.SchedulerTimeType.Stride?

      private lazy var routingSubscription: RoutingSubscription = {
        return RoutingSubscription(parent: self)
      }()

      // Stores if a `.connect()` happened before subscription, internally readable for tests
      internal var isConnected: Bool {
        return routingSubscription.isConnected
      }

      internal init(
        every interval: S.SchedulerTimeType.Stride,
        tolerance: S.SchedulerTimeType.Stride? = nil,
        scheduler: S,
        options: S.SchedulerOptions? = nil
      ) {
        self.interval = interval
        self.options = options
        self.scheduler = scheduler
        self.tolerance = tolerance
      }

      /// Adapter subscription to allow `Timer` to multiplex to multiple subscribers
      /// the values produced by a single `TimerPublisher.Inner`
      private class RoutingSubscription: Subscription, Subscriber, CustomStringConvertible,
        CustomReflectable, CustomPlaygroundDisplayConvertible
      {
        typealias Input = S.SchedulerTimeType
        typealias Failure = Never

        private typealias ErasedSubscriber = AnySubscriber<Output, Failure>

        private let lock: Lock

        // Inner is IUP due to init requirements
        private var inner: Inner<RoutingSubscription>!
        private var subscribers: [ErasedSubscriber] = []

        private var _lockedIsConnected = false
        var isConnected: Bool {
          get {
            lock.lock()
            defer { lock.unlock() }
            return _lockedIsConnected
          }

          set {
            lock.lock()
            let oldValue = _lockedIsConnected
            _lockedIsConnected = newValue

            // Inner will always be non-nil
            let inner = self.inner!
            lock.unlock()

            guard newValue, !oldValue else {
              return
            }
            inner.enqueue()
          }
        }

        var description: String { return "Timer" }
        var customMirror: Mirror { return inner.customMirror }
        var playgroundDescription: Any { return description }
        var combineIdentifier: CombineIdentifier { return inner.combineIdentifier }

        init(parent: Publishers.Timer<S>) {
          self.lock = Lock()
          self.inner = .init(parent, self)
        }

        deinit {
          lock.cleanupLock()
        }

        func addSubscriber<T: Subscriber>(_ sub: T)
        where
          T.Failure == Failure,
          T.Input == Output
        {
          lock.lock()
          subscribers.append(AnySubscriber(sub))
          lock.unlock()

          sub.receive(subscription: self)
        }

        func receive(subscription: Subscription) {
          lock.lock()
          let subscribers = self.subscribers
          lock.unlock()

          for sub in subscribers {
            sub.receive(subscription: subscription)
          }
        }

        func receive(_ value: Input) -> Subscribers.Demand {
          var resultingDemand: Subscribers.Demand = .max(0)
          lock.lock()
          let subscribers = self.subscribers
          let isConnected = _lockedIsConnected
          lock.unlock()

          guard isConnected else { return .none }

          for sub in subscribers {
            resultingDemand += sub.receive(value)
          }
          return resultingDemand
        }

        func receive(completion: Subscribers.Completion<Failure>) {
          lock.lock()
          let subscribers = self.subscribers
          lock.unlock()

          for sub in subscribers {
            sub.receive(completion: completion)
          }
        }

        func request(_ demand: Subscribers.Demand) {
          lock.lock()
          // Inner will always be non-nil
          let inner = self.inner!
          lock.unlock()

          inner.request(demand)
        }

        func cancel() {
          lock.lock()
          // Inner will always be non-nil
          let inner = self.inner!
          _lockedIsConnected = false
          self.subscribers = []
          lock.unlock()

          inner.cancel()
        }
      }

      internal func receive<T: Subscriber>(subscriber: T)
      where Failure == T.Failure, Output == T.Input {
        routingSubscription.addSubscriber(subscriber)
      }

      internal func connect() -> Cancellable {
        routingSubscription.isConnected = true
        return routingSubscription
      }

      private typealias Parent = Publishers.Timer
      private final class Inner<Downstream: Subscriber>: NSObject, Subscription, CustomReflectable,
        CustomPlaygroundDisplayConvertible
      where
        Downstream.Input == S.SchedulerTimeType,
        Downstream.Failure == Never
      {
        private var cancellable: Cancellable?
        private let lock: Lock
        private var downstream: Downstream?
        private var parent: Parent<S>?
        private var started: Bool
        private var demand: Subscribers.Demand

        override var description: String { return "Timer" }
        var customMirror: Mirror {
          lock.lock()
          defer { lock.unlock() }
          return Mirror(
            self,
            children: [
              "downstream": downstream as Any,
              "interval": parent?.interval as Any,
              "tolerance": parent?.tolerance as Any,
            ])
        }
        var playgroundDescription: Any { return description }

        init(_ parent: Parent<S>, _ downstream: Downstream) {
          self.lock = Lock()
          self.parent = parent
          self.downstream = downstream
          self.started = false
          self.demand = .max(0)
          super.init()
        }

        deinit {
          lock.cleanupLock()
        }

        func enqueue() {
          lock.lock()
          guard let parent = self.parent, !started else {
            lock.unlock()
            return
          }

          started = true
          lock.unlock()

          self.cancellable = parent.scheduler.schedule(
            after: parent.scheduler.now.advanced(by: parent.interval),
            interval: parent.interval,
            tolerance: parent.tolerance ?? .zero,
            options: parent.options
          ) {
            self.timerFired()
          }
        }

        func cancel() {
          lock.lock()
          guard let t = self.cancellable else {
            lock.unlock()
            return
          }

          // clear out all optionals
          downstream = nil
          parent = nil
          started = false
          demand = .max(0)
          lock.unlock()

          // cancel the timer
          t.cancel()
        }

        func request(_ n: Subscribers.Demand) {
          lock.lock()
          defer { lock.unlock() }
          guard parent != nil else {
            return
          }
          demand += n
        }

        @objc
        func timerFired() {
          lock.lock()
          guard let ds = downstream, let parent = self.parent else {
            lock.unlock()
            return
          }

          // This publisher drops events on the floor when there is no space in the subscriber
          guard demand > 0 else {
            lock.unlock()
            return
          }

          demand -= 1
          lock.unlock()

          let extra = ds.receive(parent.scheduler.now)
          guard extra > 0 else {
            return
          }

          lock.lock()
          demand += extra
          lock.unlock()
        }
      }
    }
  }

#endif /* !(os(iOS) && (arch(i386) || arch(arm))) */
