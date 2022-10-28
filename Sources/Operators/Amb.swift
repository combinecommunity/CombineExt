//
//  Amb.swift
//  CombineExt
//
//  Created by Shai Mishali on 29/03/2020.
//  Copyright © 2020 Combine Community. All rights reserved.
//

#if canImport(Combine)
import Combine

@available(OSX 10.15, iOS 13.0, tvOS 13.0, watchOS 6.0, *)
public extension Publisher {
    /// Returns a publisher which mirrors the first publisher to emit an event.
    ///
    /// - parameter other: The second publisher to "compete" against.
    ///
    /// - returns: A publisher which mirrors either `self` or `other`.
    func amb<Other: Publisher>(_ other: Other)
        -> Publishers.Amb<Self, Other> where Other.Output == Output, Other.Failure == Failure {
        Publishers.Amb(first: self, second: other)
    }

    /// Returns a publisher which mirrors the first publisher to emit an event.
    ///
    /// - parameter others: Other publishers to "compete" against.
    ///
    /// - returns: A publisher which mirrors the first publisher to emit an event.
    func amb<Other: Publisher>(with others: Other...)
        -> AnyPublisher<Self.Output, Self.Failure> where Other.Output == Output, Other.Failure == Failure {
        amb(with: others)
    }

    /// Returns a publisher which mirrors the first publisher to emit an event.
    ///
    /// - parameter others: Other publishers to "compete" against.
    ///
    /// - returns: A publisher which mirrors the first publisher to emit an event.
    func amb<Others: Collection>(with others: Others)
        -> AnyPublisher<Self.Output, Self.Failure>
        where Others.Element: Publisher, Others.Element.Output == Output, Others.Element.Failure == Failure {
            others.reduce(self.eraseToAnyPublisher()) { result, current in
                result.amb(current).eraseToAnyPublisher()
            }
    }
}

@available(OSX 10.15, iOS 13.0, tvOS 13.0, watchOS 6.0, *)
public extension Collection where Element: Publisher {
    /// Projects a collection of publishers onto one that has each “compete”. The publisher that wins out by emitting first will be mirrored.
    ///
    /// - Returns: A publisher which mirrors the first publisher to emit an event.
    func amb() -> AnyPublisher<Element.Output, Element.Failure> {
        switch count {
        case 0:
            return Empty().eraseToAnyPublisher()
        case 1:
            return self[startIndex]
                .amb(with: [Element]())
        default:
            let first = self[startIndex]
            let others = self[index(after: startIndex)...]

            return first
                .amb(with: others)
        }
    }
}

// MARK: - Publisher
@available(OSX 10.15, iOS 13.0, tvOS 13.0, watchOS 6.0, *)
public extension Publishers {
    struct Amb<First: Publisher, Second: Publisher>: Publisher where First.Output == Second.Output, First.Failure == Second.Failure {
        public func receive<S: Subscriber>(subscriber: S) where Failure == S.Failure, Output == S.Input {
            subscriber.receive(subscription: Subscription(first: first,
                                                          second: second,
                                                          downstream: subscriber))
        }

        public typealias Output = First.Output
        public typealias Failure = First.Failure

        private let first: First
        private let second: Second

        public init(first: First,
                    second: Second) {
            self.first = first
            self.second = second
        }
    }
}

// MARK: - Subscription
@available(OSX 10.15, iOS 13.0, tvOS 13.0, watchOS 6.0, *)
private extension Publishers.Amb {
    class Subscription<Downstream: Subscriber>: Combine.Subscription where Output == Downstream.Input, Failure == Downstream.Failure {
        private var firstSink: Sink<First, Downstream>?
        private var secondSink: Sink<Second, Downstream>?
        private var preDecisionDemand = Subscribers.Demand.none
        private var decision: Decision? {
            didSet {
                guard let decision = decision else { return }
                switch decision {
                case .first:
                    secondSink?.cancelUpstream()
                    secondSink = nil
                case .second:
                    firstSink?.cancelUpstream()
                    firstSink = nil
                }

                request(preDecisionDemand)
                preDecisionDemand = .none
            }
        }

        init(first: First,
             second: Second,
             downstream: Downstream) {
            self.firstSink = Sink(upstream: first,
                                  downstream: downstream) { [weak self] in
                                guard let self = self,
                                      self.decision == nil else { return }

                                self.decision = .first
                             }

            self.secondSink = Sink(upstream: second,
                                   downstream: downstream) { [weak self] in
                                guard let self = self,
                                      self.decision == nil else { return }

                                self.decision = .second
                              }
        }

        func request(_ demand: Subscribers.Demand) {
            guard decision != nil else {
                preDecisionDemand += demand
                return
            }

            firstSink?.demand(demand)
            secondSink?.demand(demand)
        }

        func cancel() {
            firstSink?.cancelUpstream()
            firstSink = nil
            secondSink?.cancelUpstream()
            secondSink = nil
        }
    }
}

@available(OSX 10.15, iOS 13.0, tvOS 13.0, watchOS 6.0, *)
private enum Decision {
    case first
    case second
}

// MARK: - Sink
@available(OSX 10.15, iOS 13.0, tvOS 13.0, watchOS 6.0, *)
private extension Publishers.Amb {
    class Sink<Upstream: Publisher, Downstream: Subscriber>: CombineExt.Sink<Upstream, Downstream> where Upstream.Output == Downstream.Input, Upstream.Failure == Downstream.Failure {
        private let emitted: () -> Void

        init(upstream: Upstream,
             downstream: Downstream,
             emitted: @escaping () -> Void) {
            self.emitted = emitted
            super.init(upstream: upstream,
                       downstream: downstream,
                       transformOutput: { $0 },
                       transformFailure: { $0 })
        }

        override func receive(subscription: Combine.Subscription) {
            super.receive(subscription: subscription)

            // We demand a single event from each upstreram publisher
            // so we can determine who wins the race
            subscription.request(.max(1))
        }

        override func receive(_ input: Upstream.Output) -> Subscribers.Demand {
            emitted()
            return buffer.buffer(value: input)
        }

        override func receive(completion: Subscribers.Completion<Downstream.Failure>) {
            emitted()
            buffer.complete(completion: completion)
        }
    }
}
#endif
