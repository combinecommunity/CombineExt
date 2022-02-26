//
//  FromAsync.swift
//  CombineExt
//
//  Created by Thibault Wittemberg on 2021-06-15.
//  Copyright © 2021 Combine Community. All rights reserved.
//

#if canImport(Combine)
import Combine

@available(macOS 12.0, iOS 15.0, tvOS 15.0, watchOS 8.0, *)
public extension Publishers {
    /// Creates a Combine Publisher from an async function
    /// The Publisher emits a value and then completes when the async function returns its result.
    /// The task that supports the async function is canceled when the publisher's subscription is canceled.
    /// ```
    /// var value: Int {
    ///     get async {
    ///         3
    ///     }
    /// }
    ///
    /// Publishers
    ///   .fromAsync {
    ///        await value
    ///    }.sink {
    ///        print($0)
    ///     } receiveValue: {
    ///        print($0)
    ///    }
    ///
    ///    // will print:
    ///    // 3
    ///    // finished
    ///  ```
    /// - parameter priority: Optional value indicating the priority of the Task supporting the execution of the async function
    /// - Returns: The Combine Publisher wrapping the async function execution
    static func fromAsync<Output>(priority: TaskPriority? = nil,
                                  _ asyncFunction: @escaping () async -> Output) -> AnyPublisher<Output, Never> {
        AnyPublisher<Output, Never>.create { subscriber in
            let task = Task(priority: priority) {
                let result = await asyncFunction()
                subscriber.send(result)
                subscriber.send(completion: .finished)
            }

            return AnyCancellable {
                task.cancel()
            }
        }
    }

    /// Creates a Combine Publisher from a throwing async function
    /// The Publisher emits a value or fail according the the async function execution result.
    /// The task that supports the async function is canceled when the publisher's subscription is canceled.
    ///
    /// ```
    /// var value: Int {
    /// get async {
    ///        3
    ///    }
    /// }
    ///
    /// Publishers
    ///    .fromAsync {
    ///        await value
    ///    }.sink {
    ///        print($0)
    ///    } receiveValue: {
    ///        print($0)
    ///    }
    ///
    ///    // will print:
    ///    // 3
    ///    // finished
    /// ```
    ///
    /// Whenever the async function throws an error, the stream will  faile:
    ///
    /// ```
    /// struct MyError: Error, CustomStringConvertible {
    ///     var description: String {
    ///        "Async Error"
    ///    }
    /// }
    ///
    /// Publishers
    ///    .fromAsync { () async throws -> String in
    ///        throw MyError()
    ///    }.sink {
    ///        print($0)
    ///    } receiveValue: {
    ///        print($0)
    ///    }
    ///
    ///    // will print:
    ///    // failure(Async Error)
    ///```
    /// - parameter priority: Optional value indicating the priority of the Task supporting the execution of the async function
    /// - Returns: The Combine Publisher wrapping the async function execution
    static func fromThrowingAsync<Output>(priority: TaskPriority? = nil,
                                          _ asyncThrowingFunction: @escaping () async throws -> Output) -> AnyPublisher<Output, Error> {
        AnyPublisher<Output, Error>.create { subscriber in
            let task = Task(priority: priority) {
                do {
                    let result = try await asyncThrowingFunction()
                    subscriber.send(result)
                    subscriber.send(completion: .finished)
                } catch {
                    subscriber.send(completion: .failure(error))
                }
            }

            return AnyCancellable {
                task.cancel()
            }
        }
    }

    /// Creates a Combine Publisher from an async sequence.
    /// The Publisher emits values or fail according the the async sequence execution result.
    ///
    /// ```
    /// let sequence = [1, 2, 3].publisher.values
    ///
    /// Publishers
    ///    .fromAsyncSequence(sequence).sink {
    ///        print($0)
    ///    } receiveValue: {
    ///        print($0)
    ///    }
    ///
    ///    // will print:
    ///    // 1
    ///    // 2
    ///    // 3
    ///    // finished
    /// ```
    ///
    /// If the asyncSequence faild:
    ///
    /// ```
    /// struct MyError: Error, CustomStringConvertible {
    ///    var description: String {
    ///        "Async Error"
    ///    }
    /// }
    ///
    /// let sequence = AsyncThrowingStream(Int.self) { continuation in
    ///    continuation.yield(1)
    ///    continuation.yield(2)
    ///    continuation.finish(throwing: MockError(value: Int.random(in: 1...100)))
    /// }
    ///
    /// Publishers
    ///    .fromAsyncSequence(sequence).sink {
    ///        print($0)
    ///    } receiveValue: {
    ///        print($0)
    ///    }
    ///
    ///    // will print:
    ///    // 1
    ///    // 2
    ///    // failure(Async Error)
    ///```
    /// - parameter priority: Optional value indicating the priority of the Task supporting the async sequence execution
    /// - Returns: The Combine Publisher wrapping the async sequence iteration
    static func fromAsyncSequence<Output, AsyncSequenceType>(priority: TaskPriority? = nil,
                                                             _ asyncSequence: AsyncSequenceType) -> AnyPublisher<Output, Error>
    where AsyncSequenceType: AsyncSequence, AsyncSequenceType.Element == Output {
        AnyPublisher<Output, Error>.create { subscriber in
            let task = Task(priority: priority) {
                do {
                    for try await result in asyncSequence {
                        subscriber.send(result)
                    }
                    subscriber.send(completion: .finished)
                } catch {
                    subscriber.send(completion: .failure(error))
                }
            }

            return AnyCancellable {
                task.cancel()
            }
        }
    }
}
#endif
