//
//  ConcatMap.swift
//  CombineExt
//
//  Created by Daniel Peter on 22/11/2020.
//  Copyright © 2020 Combine Community. All rights reserved.
//

#if canImport(Combine)
import Combine

@available(OSX 10.15, iOS 13.0, tvOS 13.0, watchOS 6.0, *)
public extension Publisher {
    /// Transforms an output value into a new publisher, and flattens the stream of events from these multiple upstream publishers to appear as if they were coming from a single stream of events.
    ///
    /// Mapping to a new publisher will keep the subscription to the previous one alive until it completes and only then subscribe to the new one. This also means that all values sent by the new publisher are not forwarded as long as the previous one hasn't completed.
    ///
    /// - parameter transform: A transform to apply to each emitted value, from which you can return a new Publisher
    ///
    /// - returns: A publisher emitting the values of all emitted publishers in order.
    func concatMap<T, P>(
        _ transform: @escaping (Self.Output) -> P
    ) -> Publishers.ConcatMap<P, Self> where T == P.Output, P: Publisher, Self.Failure == P.Failure {
        return Publishers.ConcatMap(upstream: self, transform: transform)
    }
}

@available(OSX 10.15, iOS 13.0, tvOS 13.0, watchOS 6.0, *)
public extension Publishers {
    struct ConcatMap<NewPublisher, Upstream>: Publisher where NewPublisher: Publisher, Upstream: Publisher, NewPublisher.Failure == Upstream.Failure  {
        public typealias Transform = (Upstream.Output) -> NewPublisher
        public typealias Output = NewPublisher.Output
        public typealias Failure = Upstream.Failure

        public let upstream: Upstream
        public let transform: Transform

        public init(
            upstream: Upstream,
            transform: @escaping Transform
        ) {
            self.upstream = upstream
            self.transform = transform
        }

        public func receive<S: Subscriber>(subscriber: S)
        where Output == S.Input, Failure == S.Failure {
            subscriber.receive(
                subscription: Subscription(
                    upstream: upstream,
                    downstream: subscriber,
                    transform: transform
                )
            )
        }
    }
}

// MARK: - Subscription
@available(OSX 10.15, iOS 13.0, tvOS 13.0, watchOS 6.0, *)
private extension Publishers.ConcatMap {
    final class Subscription<Downstream: Subscriber>: Combine.Subscription where
        Downstream.Input == NewPublisher.Output,
        Downstream.Failure == Failure
    {
        private var sink: OuterSink<Downstream>?

        init(
            upstream: Upstream,
            downstream: Downstream,
            transform: @escaping Transform
        ) {
            self.sink = OuterSink(
                upstream: upstream,
                downstream: downstream,
                transform: transform
            )
        }

        func request(_ demand: Subscribers.Demand) {
            sink?.demand(demand)
        }

        func cancel() {
            sink = nil
        }
    }
}

// MARK: - Sink
@available(OSX 10.15, iOS 13.0, tvOS 13.0, watchOS 6.0, *)
private extension Publishers.ConcatMap {
    final class OuterSink<Downstream: Subscriber>: Subscriber where
        Downstream.Input == NewPublisher.Output,
        Downstream.Failure == Upstream.Failure
    {
        private let lock = NSRecursiveLock()

        private let downstream: Downstream
        private let transform: Transform

        private var upstreamSubscription: Combine.Subscription?
        private var innerSink: InnerSink<Downstream>?

        private var bufferedDemand: Subscribers.Demand = .none

        init(
            upstream: Upstream,
            downstream: Downstream,
            transform: @escaping Transform
        ) {
            self.downstream = downstream
            self.transform = transform

            upstream.subscribe(self)
        }

        func demand(_ demand: Subscribers.Demand) {
            lock.lock(); defer { lock.unlock() }
            if let innerSink = innerSink {
                innerSink.demand(demand)
            } else {
                bufferedDemand = demand
            }

            upstreamSubscription?.requestIfNeeded(.unlimited)
        }

        func receive(_ input: Upstream.Output) -> Subscribers.Demand {
            lock.lock(); defer { lock.unlock() }
            let transformedPublisher = transform(input)

            if let innerSink = innerSink {
                innerSink.enqueue(publisher: transformedPublisher)
            } else {
                innerSink = InnerSink(
                    outerSink: self,
                    upstream: transformedPublisher,
                    downstream: downstream
                )

                innerSink?.demand(bufferedDemand)
            }

            return .unlimited
        }

        func receive(subscription: Combine.Subscription) {
            lock.lock(); defer { lock.unlock() }
            upstreamSubscription = subscription
        }

        func receive(completion: Subscribers.Completion<Upstream.Failure>) {
            lock.lock(); defer { lock.unlock() }
            innerSink = nil
            downstream.receive(completion: completion)
            cancelUpstream()
        }

        func cancelUpstream() {
            lock.lock(); defer { lock.unlock() }
            upstreamSubscription.kill()
        }

        deinit { cancelUpstream() }
    }

    final class InnerSink<Downstream: Subscriber>: CombineExt.Sink<NewPublisher, Downstream> where
        Downstream.Input == NewPublisher.Output,
        Downstream.Failure == Upstream.Failure
    {
        private weak var outerSink: OuterSink<Downstream>?
        private let lock: NSRecursiveLock = NSRecursiveLock()

        private var hasActiveSubscription: Bool
        private var publisherQueue: [NewPublisher]

        init(
            outerSink: OuterSink<Downstream>,
            upstream: NewPublisher,
            downstream: Downstream
        ) {
            self.outerSink = outerSink
            self.hasActiveSubscription = false
            self.publisherQueue = []
            
            super.init(
                upstream: upstream,
                downstream: downstream
            )
        }

        func enqueue(publisher: NewPublisher) {
            lock.lock(); defer { lock.unlock() }
            if hasActiveSubscription {
                publisherQueue.append(publisher)
            } else {
                publisher.subscribe(self)
            }
        }

        override func receive(_ input: NewPublisher.Output) -> Subscribers.Demand {
            buffer.buffer(value: input)
        }

        override func receive(subscription: Combine.Subscription) {
            lock.lock(); defer { lock.unlock() }
            hasActiveSubscription = true

            super.receive(subscription: subscription)
            subscription.requestIfNeeded(buffer.remainingDemand)
        }

        override func receive(completion: Subscribers.Completion<Upstream.Failure>) {
            lock.lock(); defer { lock.unlock() }
            hasActiveSubscription = false
            
            switch completion {
            case .finished:
                if !publisherQueue.isEmpty {
                    publisherQueue.removeFirst().subscribe(self)
                }
            case let .failure(error):
                buffer.complete(completion: .failure(error))
                outerSink?.receive(completion: completion)
            }
        }
    }
}

@available(OSX 10.15, iOS 13.0, tvOS 13.0, watchOS 6.0, *)
extension Publishers.ConcatMap.Subscription: CustomStringConvertible {
    var description: String {
        "ConcatMap.Subscription<\(Downstream.Input.self), \(Downstream.Failure.self)>"
    }
}
#endif
