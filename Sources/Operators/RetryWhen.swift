//
//  RetryWhen.swift
//  CombineExt
//
//  Created by Daniel Tartaglia on 3/21/20.
//

#if canImport(Combine)
import Combine

@available(OSX 10.15, iOS 13.0, tvOS 13.0, watchOS 6.0, *)
public extension Publisher {
    /// Repeats the source publisher on error when the notifier emits a next value. If the source publisher errors and the notifier completes, it will complete the source sequence.
    ///
    /// - Parameter notificationHandler: A handler that is passed a publisher of errors raised by the source publisher and returns a publisher that either continues, completes or errors. This behavior is then applied to the source publisher.
    /// - Returns: A publisher producing the elements of the given sequence repeatedly until it terminates successfully or is notified to error or complete.
    func retryWhen<RetryTrigger>(_ errorTrigger: @escaping (AnyPublisher<Self.Failure, Never>) -> RetryTrigger)
    -> Publishers.RetryWhen<Self, RetryTrigger, Output, Failure> where RetryTrigger: Publisher {
        .init(upstream: self, errorTrigger: errorTrigger)
    }
}

@available(OSX 10.15, iOS 13.0, tvOS 13.0, watchOS 6.0, *)
public extension Publishers {
    class RetryWhen<Upstream, RetryTrigger, Output, Failure>: Publisher where Upstream: Publisher, Upstream.Output == Output, Upstream.Failure == Failure, RetryTrigger: Publisher {
        typealias ErrorTrigger = (AnyPublisher<Upstream.Failure, Never>) -> RetryTrigger

        private let upstream: Upstream
        private let errorTrigger: ErrorTrigger

        init(upstream: Upstream, errorTrigger: @escaping ErrorTrigger) {
            self.upstream = upstream
            self.errorTrigger = errorTrigger
        }

        public func receive<S>(subscriber: S) where S: Subscriber, Failure == S.Failure, Output == S.Input {
            subscriber.receive(subscription: Subscription(upstream: upstream, downstream: subscriber, errorTrigger: errorTrigger))
        }
    }
}

@available(OSX 10.15, iOS 13.0, tvOS 13.0, watchOS 6.0, *)
extension Publishers.RetryWhen {
    class Subscription<Downstream>: Combine.Subscription where Downstream: Subscriber, Downstream.Input == Upstream.Output, Downstream.Failure == Upstream.Failure {
        private let upstream: Upstream
        private let downstream: Downstream
        private let errorSubject = CurrentValueSubject<Upstream.Failure?, Never>(nil)
        private var sink: Sink<Upstream, Downstream>?
        private var cancellable: AnyCancellable?

        init(
            upstream: Upstream,
            downstream: Downstream,
            errorTrigger: @escaping (AnyPublisher<Upstream.Failure, Never>) -> RetryTrigger
        ) {
            self.upstream = upstream
            self.downstream = downstream
            self.sink = Sink(
                upstream: upstream,
                downstream: downstream,
                transformOutput: { $0 },
                transformFailure: { [errorSubject] in
                    errorSubject.send($0)
                    return nil
                }
            )
            self.cancellable = errorTrigger(errorSubject.compactMap { $0 }.eraseToAnyPublisher())
                .sink(
                    receiveCompletion: { [sink] completion in
                        switch completion {
                        case .finished:
                            sink?.buffer.complete(completion: .finished)
                        case .failure(let error):
                            if let error = error as? Downstream.Failure {
                                sink?.buffer.complete(completion: .failure(error))
                            }
                        }
                    },
                    receiveValue: { [upstream, sink] _ in
                        guard let sink = sink else { return }
                        upstream.subscribe(sink)
                    }
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

@available(OSX 10.15, iOS 13.0, tvOS 13.0, watchOS 6.0, *)
extension Publishers.RetryWhen.Subscription: CustomStringConvertible {
    var description: String {
        return "RetryWhen.Subscription<\(Output.self), \(Failure.self)>"
    }
}
#endif
