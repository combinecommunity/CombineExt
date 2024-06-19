//
//  WithUnretained.swift
//  CombineExt
//
//  Created by Robert on 01/09/2021.
//  Copyright © 2020 Combine Community. All rights reserved.
//

#if canImport(Combine)
import Combine

@available(OSX 10.15, iOS 13.0, tvOS 13.0, watchOS 6.0, *)
public extension Publisher {
    /**
     Provides an unretained, safe to use (i.e. not implicitly unwrapped), reference to an object along with the events published by the publisher.
     
     In the case the provided object cannot be retained successfully, the publisher will complete.
     
     - parameter obj: The object to provide an unretained reference on.
     - parameter resultSelector: A function to combine the unretained referenced on `obj` and the value of the observable sequence.
     - returns: A publisher that contains the result of `resultSelector` being called with an unretained reference on `obj` and the values of the upstream.
     */
    func withUnretained<UnretainedObject: AnyObject, Output>(_ obj: UnretainedObject, resultSelector: @escaping (UnretainedObject, Self.Output) -> Output) -> Publishers.WithUnretained<UnretainedObject, Self, Output> {
        Publishers.WithUnretained(unretainedObject: obj, upstream: self, resultSelector: resultSelector)
    }

    /**
     Provides an unretained, safe to use (i.e. not implicitly unwrapped), reference to an object along with the events published by the publisher.
     
     In the case the provided object cannot be retained successfully, the publisher will complete.
     
     - parameter obj: The object to provide an unretained reference on.
     - returns: A publisher that publishes a sequence of tuples that contains both an unretained reference on `obj` and the values of the upstream.
     */
    func withUnretained<UnretainedObject: AnyObject>(_ obj: UnretainedObject) -> Publishers.WithUnretained<UnretainedObject, Self, (UnretainedObject, Output)> {
        Publishers.WithUnretained(unretainedObject: obj, upstream: self) { ($0, $1) }
    }

    /// Attaches a subscriber with closure-based behavior.
    ///
    /// Use ``Publisher/sink(unretainedObject:receiveCompletion:receiveValue:)`` to observe values received by the publisher and process them using a closure you specify.
    /// This method creates the subscriber and immediately requests an unlimited number of values, prior to returning the subscriber.
    /// The return value should be held, otherwise the stream will be canceled.
    ///
    /// - parameter obj: The object to provide an unretained reference on.
    /// - parameter receiveComplete: The closure to execute on completion.
    /// - parameter receiveValue: The closure to execute on receipt of a value.
    /// - Returns: A cancellable instance, which you use when you end assignment of the received value. Deallocation of the result will tear down the subscription stream.
    func sink<UnretainedObject: AnyObject>(unretainedObject obj: UnretainedObject, receiveCompletion: @escaping ((Subscribers.Completion<Self.Failure>) -> Void), receiveValue: @escaping ((UnretainedObject, Self.Output) -> Void)) -> AnyCancellable {
        withUnretained(obj)
            .sink(receiveCompletion: receiveCompletion, receiveValue: receiveValue)
    }
}

// MARK: - Publisher
@available(OSX 10.15, iOS 13.0, tvOS 13.0, watchOS 6.0, *)
public extension Publishers {
    struct WithUnretained<UnretainedObject: AnyObject, Upstream: Publisher, Output>: Publisher {
        public typealias Failure = Upstream.Failure

        private weak var unretainedObject: UnretainedObject?
        private let upstream: Upstream
        private let resultSelector: (UnretainedObject, Upstream.Output) -> Output

        public init(unretainedObject: UnretainedObject, upstream: Upstream, resultSelector: @escaping (UnretainedObject, Upstream.Output) -> Output) {
            self.unretainedObject = unretainedObject
            self.upstream = upstream
            self.resultSelector = resultSelector
        }

        public func receive<S: Combine.Subscriber>(subscriber: S) where Failure == S.Failure, Output == S.Input {
            upstream.subscribe(Subscriber(unretainedObject: unretainedObject, downstream: subscriber, resultSelector: resultSelector))
        }
    }
}

// MARK: - Subscriber
@available(OSX 10.15, iOS 13.0, tvOS 13.0, watchOS 6.0, *)
private extension Publishers.WithUnretained {
    class Subscriber<Downstream: Combine.Subscriber>: Combine.Subscriber where Downstream.Input == Output, Downstream.Failure == Failure {
        typealias Input = Upstream.Output
        typealias Failure = Downstream.Failure

        private weak var unretainedObject: UnretainedObject?
        private let downstream: Downstream
        private let resultSelector: (UnretainedObject, Input) -> Output

        init(unretainedObject: UnretainedObject?, downstream: Downstream, resultSelector: @escaping (UnretainedObject, Input) -> Output) {
            self.unretainedObject = unretainedObject
            self.downstream = downstream
            self.resultSelector = resultSelector
        }

        func receive(subscription: Subscription) {
            if unretainedObject == nil { return }
            downstream.receive(subscription: subscription)
        }

        func receive(_ input: Input) -> Subscribers.Demand {
            guard let unretainedObject = unretainedObject else { return .none }
            return downstream.receive(resultSelector(unretainedObject, input))
        }

        func receive(completion: Subscribers.Completion<Failure>) {
            if unretainedObject == nil {
                return downstream.receive(completion: .finished)
            }
            downstream.receive(completion: completion)
        }
    }
}
#endif
