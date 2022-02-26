//
//  CompactScan.swift
//  CombineExt
//
//  Created by Thibault Wittemberg on 04/09/2021.
//  Copyright © 2021 Combine Community. All rights reserved.
//

#if canImport(Combine)
import Combine

@available(OSX 10.15, iOS 13.0, tvOS 13.0, watchOS 6.0, *)
public extension Publisher {
    /// Transforms elements from the upstream publisher by providing the current
    /// element to a closure along with the last value returned by the closure.
    ///
    /// The ``nextPartialResult`` closure might return nil values. In that case the accumulator won't change until the next non-nil upstream publisher value.
    ///
    /// Use ``Publisher/compactScan(_:_:)`` to accumulate all previously-published values into a single
    /// value, which you then combine with each newly-published value.
    ///
    /// The following example logs a running total of all values received
    /// from the sequence publisher.
    ///
    ///     let range = (0...5)
    ///     let cancellable = range.publisher
    ///         .compactScan(0) {
    ///             guard $1.isMultiple(of: 2) else { return nil }
    ///             return $0 + $1
    ///         }
    ///         .sink { print ("\($0)", terminator: " ") }
    ///      // Prints: "0 2 6 ".
    ///
    /// - Parameters:
    ///   - initialResult: The previous result returned by the `nextPartialResult` closure.
    ///   - nextPartialResult: A closure that takes as its arguments the previous value returned by the closure and the next element emitted from the upstream publisher.
    /// - Returns: A publisher that transforms elements by applying a closure that receives its previous return value and the next element from the upstream publisher.
    func compactScan<T>(_ initialResult: T, _ nextPartialResult: @escaping (T, Output) -> T?) -> AnyPublisher<T, Failure> {
        self.scan((initialResult, initialResult)) { accumulator, value -> (T, T?) in
            let lastNonNilAccumulator = accumulator.0
            let newAccumulator = nextPartialResult(lastNonNilAccumulator, value)
            return (newAccumulator ?? lastNonNilAccumulator, newAccumulator)
        }
        .compactMap { $0.1 }
        .eraseToAnyPublisher()
    }

    /// Transforms elements from the upstream publisher by providing the current element to an error-throwing closure along with the last value returned by the closure.
    ///
    /// The ``nextPartialResult`` closure might return nil values. In that case the accumulator won't change until the next non-nil upstream publisher value.
    ///
    /// Use ``Publisher/tryCompactScan(_:_:)`` to accumulate all previously-published values into a single value, which you then combine with each newly-published value.
    /// If your accumulator closure throws an error, the publisher terminates with the error.
    ///
    /// In the example below, ``Publisher/tryCompactScan(_:_:)`` calls a division function on elements of a collection publisher. The resulting publisher publishes each result until the function encounters a `DivisionByZeroError`, which terminates the publisher.
    ///
    ///     struct DivisionByZeroError: Error {}
    ///
    ///     /// A function that throws a DivisionByZeroError if `current` provided by the TryScan publisher is zero.
    ///     func myThrowingFunction(_ lastValue: Int, _ currentValue: Int) throws -> Int? {
    ///         guard currentValue.isMultiple(of: 2) else { return nil }
    ///         guard currentValue != 0 else { throw DivisionByZeroError() }
    ///         return lastValue / currentValue
    ///      }
    ///
    ///     let numbers = [1, 2, 3, 4, 5, 0, 6, 7, 8, 9]
    ///     let cancellable = numbers.publisher
    ///         .tryCompactScan(10) { try myThrowingFunction($0, $1) }
    ///         .sink(
    ///             receiveCompletion: { print ("\($0)") },
    ///             receiveValue: { print ("\($0)", terminator: " ") }
    ///          )
    ///
    ///     // Prints: "6 2 failure(DivisionByZeroError())".
    ///
    /// If the closure throws an error, the publisher fails with the error.
    ///
    /// - Parameters:
    ///   - initialResult: The previous result returned by the `nextPartialResult` closure.
    ///   - nextPartialResult: An error-throwing closure that takes as its arguments the previous value returned by the closure and the next element emitted from the upstream publisher.
    /// - Returns: A publisher that transforms elements by applying a closure that receives its previous return value and the next element from the upstream publisher.
    func tryCompactScan<T>(_ initialResult: T, _ nextPartialResult: @escaping (T, Output) throws -> T?) -> AnyPublisher<T, Error> {
        self.tryScan((initialResult, initialResult)) { accumulator, value -> (T, T?) in
            let lastNonNilAccumulator = accumulator.0
            let newAccumulator = try nextPartialResult(lastNonNilAccumulator, value)
            return (newAccumulator ?? lastNonNilAccumulator, newAccumulator)
        }
        .compactMap { $0.1 }
        .eraseToAnyPublisher()
    }
}
#endif
