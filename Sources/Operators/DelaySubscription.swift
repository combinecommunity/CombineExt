//
//  DelaySubscription.swift
//  CombineExt
//
//  Created by Jack Stone on 06/03/2021.
//  Copyright © 2021 Combine Community. All rights reserved.
//

#if canImport(Combine)
import Combine

@available(OSX 10.15, iOS 13.0, tvOS 13.0, watchOS 6.0, *)
public extension Publisher {

    /// Time shifts the delivery of all output to the downstream receiver by delaying
    /// the time a subscriber starts receiving elements from its subscription.
    ///
    /// Note that delaying a subscription may result in skipped elements for "hot" publishers.
    /// However, this won't make a difference for "cold" publishers.
    ///
    /// - Parameter interval: The amount of delay time.
    /// - Parameter tolerance: The allowed tolerance in the firing of the delayed subscription.
    /// - Parameter scheduler: The scheduler to schedule the subscription delay on.
    /// - Parameter options: Any additional scheduler options.
    ///
    /// - Returns: A publisher with its subscription delayed.
    ///
    func delaySubscription<S: Scheduler>(for interval: S.SchedulerTimeType.Stride,
                                         tolerance: S.SchedulerTimeType.Stride? = nil,
                                         scheduler: S,
                                         options: S.SchedulerOptions? = nil) -> Publishers.DelaySubscription<Self, S> {
        return Publishers.DelaySubscription(upstream: self,
                                            interval: interval,
                                            tolerance: tolerance ?? scheduler.minimumTolerance,
                                            scheduler: scheduler,
                                            options: options)
    }
}

// MARK: - Publisher
@available(OSX 10.15, iOS 13.0, tvOS 13.0, watchOS 6.0, *)
public extension Publishers {

    /// A publisher that delays the upstream subscription.
    struct DelaySubscription<U: Publisher, S: Scheduler>: Publisher {

        public typealias Output = U.Output      // Upstream output
        public typealias Failure = U.Failure    // Upstream failure

        /// The publisher that this publisher receives signals from.
        public let upstream: U

        /// The amount of delay time.
        public let interval: S.SchedulerTimeType.Stride

        /// The allowed tolerance in the firing of the delayed subscription.
        public let tolerance: S.SchedulerTimeType.Stride

        /// The scheduler to run the subscription delay timer on.
        public let scheduler: S

        /// Any additional scheduler options.
        public let options: S.SchedulerOptions?

        init(upstream: U,
             interval: S.SchedulerTimeType.Stride,
             tolerance: S.SchedulerTimeType.Stride,
             scheduler: S,
             options: S.SchedulerOptions?) {
            self.upstream = upstream
            self.interval = interval
            self.tolerance = tolerance
            self.scheduler = scheduler
            self.options = options
        }

        public func receive<S>(subscriber: S) where S : Subscriber, Self.Failure == S.Failure, Self.Output == S.Input {
            self.upstream.subscribe(DelayedSubscription(publisher: self, downstream: subscriber))
        }
    }
}

// MARK: - Subscription
@available(OSX 10.15, iOS 13.0, tvOS 13.0, watchOS 6.0, *)
private extension Publishers.DelaySubscription {

    /// The delayed subscription where the scheduler advancing takes place.
    final class DelayedSubscription<D: Subscriber>: Subscriber where D.Input == Output, D.Failure == U.Failure {

        typealias Input = U.Output      // Upstream output
        typealias Failure = U.Failure   // Upstream failure

        private let interval: S.SchedulerTimeType.Stride
        private let tolerance: S.SchedulerTimeType.Stride
        private let scheduler: S
        private let options: S.SchedulerOptions?

        private let downstream: D

        init(publisher: Publishers.DelaySubscription<U, S>,
             downstream: D) {
            self.interval = publisher.interval
            self.tolerance = publisher.tolerance
            self.scheduler = publisher.scheduler
            self.options = publisher.options
            self.downstream = downstream
        }

        func receive(subscription: Subscription) {
            scheduler.schedule(after: scheduler.now.advanced(by: interval),
                               tolerance: tolerance,
                               options: options) { [weak self] in
                self?.downstream.receive(subscription: subscription)
            }
        }

        func receive(_ input: U.Output) -> Subscribers.Demand {
            return downstream.receive(input)
        }

        func receive(completion: Subscribers.Completion<U.Failure>) {
            downstream.receive(completion: completion)
        }
    }
}
#endif
