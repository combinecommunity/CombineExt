//
//  Dematerialize.swift
//  CombineExt
//
//  Created by Shai Mishali on 14/03/2020.
//  Copyright © 2020 Combine Community. All rights reserved.
//

#if canImport(Combine)
import Combine

@available(OSX 10.15, iOS 13.0, tvOS 13.0, watchOS 6.0, *)
public extension Publisher where Output: EventConvertible, Failure == Never {
    /// Converts any previously-materialized publisher into its original form
    ///
    /// - returns: A publisher dematerializing the materialized events
    func dematerialize() -> Publishers.Dematerialize<Self> {
        Publishers.Dematerialize(upstream: self)
    }
}

// MARK: - Publisher
@available(OSX 10.15, iOS 13.0, tvOS 13.0, watchOS 6.0, *)
public extension Publishers {
    /// A publisher which takes a materialized upstream publisher and converts
    /// the wrapped events back into their original form
    struct Dematerialize<Upstream: Publisher>: Publisher where Upstream.Output: EventConvertible {
        public typealias Output = Upstream.Output.Output
        public typealias Failure = Upstream.Output.Failure

        private let upstream: Upstream

        public init(upstream: Upstream) {
            self.upstream = upstream
        }

        public func receive<S: Subscriber>(subscriber: S) where Failure == S.Failure, Output == S.Input {
            subscriber.receive(subscription: Subscription(upstream: upstream, downstream: subscriber))
        }
    }
}

// MARK: - Subscription
@available(OSX 10.15, iOS 13.0, tvOS 13.0, watchOS 6.0, *)
private extension Publishers.Dematerialize {
    class Subscription<Downstream: Subscriber>: Combine.Subscription
    where Downstream.Input == Upstream.Output.Output, Downstream.Failure == Upstream.Output.Failure {
        private var sink: Sink<Downstream>?

        init(upstream: Upstream,
             downstream: Downstream) {
            self.sink = Sink(upstream: upstream,
                             downstream: downstream)
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
private extension Publishers.Dematerialize {
    class Sink<Downstream: Subscriber>: CombineExt.Sink<Upstream, Downstream>
    where Downstream.Input == Upstream.Output.Output, Downstream.Failure == Upstream.Output.Failure {
        override func receive(_ input: Upstream.Output) -> Subscribers.Demand {
            /// We have to override the default mechanism here to convert a
            /// materialized failure into an actual failure
            switch input.event {
            case .value(let value):
                return buffer.buffer(value: value)
            case .failure(let failure):
                buffer.complete(completion: .failure(failure))
                return .none
            case .finished:
                buffer.complete(completion: .finished)
                return .none
            }
        }
    }
}

@available(OSX 10.15, iOS 13.0, tvOS 13.0, watchOS 6.0, *)
extension Publishers.Dematerialize.Subscription: CustomStringConvertible {
    var description: String {
        return "Dematerialize.Subscription<\(Downstream.Input.self), \(Downstream.Failure.self)>"
    }
}
#endif
