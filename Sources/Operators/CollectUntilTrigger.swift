//
//  CollectUntilTrigger.swift
//  CombineExt
//
//  Created by ferologics on 09/06/2021.
//  Copyright © 2021 Combine Community. All rights reserved.
//

#if canImport(Combine)
import Combine

@available(OSX 10.15, iOS 13.0, tvOS 13.0, watchOS 6.0, *)
public extension Publisher {
  func collect<Trigger:Publisher>(
    until trigger: Trigger
  ) -> Publishers.CollectUntilTrigger<Self, Trigger> where
  Trigger.Output == Void,
  Trigger.Failure == Never
  {
    Publishers.CollectUntilTrigger(
      upstream: self,
      trigger: trigger
    )
  }
}

// MARK: - Publisher

@available(OSX 10.15, iOS 13.0, tvOS 13.0, watchOS 6.0, *)
public extension Publishers {
  struct CollectUntilTrigger<
    Upstream: Publisher,
    Trigger: Publisher
  >: Publisher where
  Trigger.Output == Void,
  Trigger.Failure == Never
  {
    public typealias Output = [Upstream.Output]
    public typealias Failure = Upstream.Failure

    private let upstream: Upstream
    private let trigger: Trigger

    init(upstream: Upstream, trigger: Trigger) {
      self.upstream = upstream
      self.trigger = trigger
    }

    public func receive<S: Subscriber>(subscriber: S)
    where Failure == S.Failure, Output == S.Input
    {
      subscriber.receive(
        subscription: Subscription(
          upstream: upstream,
          downstream: subscriber,
          trigger: trigger
        )
      )
    }
  }
}

// MARK: - Subscription

@available(OSX 10.15, iOS 13.0, tvOS 13.0, watchOS 6.0, *)
private extension Publishers.CollectUntilTrigger {
  final class Subscription<
    Downstream: Subscriber
  >: Combine.Subscription where
  Downstream.Input == [Upstream.Output],
  Downstream.Failure == Upstream.Failure
  {
    private var sink: Sink<Downstream>?
    private var cancellable: Cancellable?

    init(
      upstream: Upstream,
      downstream: Downstream,
      trigger: Trigger
    ) {
      self.sink = Sink(
        upstream: upstream,
        downstream: downstream
      )

      cancellable = trigger.sink { [self] in
        _ = sink?.buffer.buffer(value: sink?.elements ?? [])
        _ = sink?.buffer.demand(.max(1))
        sink?.flush()
      }
    }

    func request(_ demand: Subscribers.Demand) {
      sink?.demand(demand)
    }

    func cancel() {
      sink = nil
      cancellable?.cancel()
      cancellable = nil
    }

    var description: String {
      return "CollectUntilTrigger.Subscription<\(Downstream.Input.self), \(Downstream.Failure.self)>"
    }
  }
}

// MARK: - Sink

@available(OSX 10.15, iOS 13.0, tvOS 13.0, watchOS 6.0, *)
private extension Publishers.CollectUntilTrigger {
  final class Sink<
    Downstream: Subscriber
  >: CombineExt.Sink<Upstream, Downstream> where
  Downstream.Input == [Upstream.Output],
  Downstream.Failure == Upstream.Failure
  {
    private let lock = NSRecursiveLock()
    var elements: [Upstream.Output] = []

    override func receive(_ input: Upstream.Output) -> Subscribers.Demand {
      lock.lock()
      defer { lock.unlock() }
      elements.append(input)
      return .none
    }

    func flush() {
      lock.lock()
      defer { lock.unlock() }
      elements = []
    }
  }
}

#endif

