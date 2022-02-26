//
//  FromAsyncTests.swift
//  CombineExtTests
//
//  Created by Thibault Wittemberg on 2021-06-15.
//  Copyright © 2021 Combine Community. All rights reserved.
//

#if !os(watchOS)
import Combine
import CombineExt
import XCTest

struct MockError: Error, Equatable {
    let value: Int
}

@available(macOS 12.0, iOS 15.0, tvOS 15.0, watchOS 8.0, *)
final class FromAsyncTests: XCTestCase {
    func testFromAsync_publishes_value() {
        let exp = expectation(description: "fromAsync publishes the expected value when executing an async function")

        var asyncFunctionNumberOfExecutions = 0
        let expectedOutput = UUID().uuidString
        var receivedOutput: String?

        // Given: an async function
        // When: making the publisher from the function and subscribing to it
        let cancelable = Publishers
            .fromAsync {
                asyncFunctionNumberOfExecutions += 1
                return expectedOutput
            }
            .handleEvents(receiveCompletion: { _ in exp.fulfill() })
            .sink { receivedOutput = $0 }

        waitForExpectations(timeout: 1)

        // Then: The value from the async function is published and then the stream completes
        XCTAssertEqual(receivedOutput, expectedOutput)
        XCTAssertEqual(asyncFunctionNumberOfExecutions, 1)

        cancelable.cancel()
    }

    func testFromAsync_executes_asyncFunction_with_specified_priority_when_called_with_taskPriority() {
        let exp = expectation(description: "fromAsync uses the expected priority when executing an async function with a priority")

        var asyncFunctionNumberOfExecutions = 0
        var receivedQueue: String?

        // Given: an async function
        // When: making the publisher from the function and subscribing to it with a priority
        let cancelable = Publishers
            .fromAsync(priority: .userInitiated) { () async -> String in
                asyncFunctionNumberOfExecutions += 1
                receivedQueue = DispatchQueue.currentLabel
                return ""
            }
            .handleEvents(receiveCompletion: { _ in exp.fulfill() })
            .sink { _ in }

        waitForExpectations(timeout: 1)

        // Then: The async function is executed with the expected priority
        XCTAssertTrue(receivedQueue!.contains("user-initiated"))
        XCTAssertEqual(asyncFunctionNumberOfExecutions, 1)

        cancelable.cancel()
    }

    func testFromAsync_cancels_task_when_subscription_is_canceled() {
        let exp = expectation(description: "fromAsync cancels the task when the subscription is canceled")

        let semaphore = DispatchSemaphore(value: 0)

        var isTaskCanceled = false

        // Given: an async function that records the cancelation of its execution task
        // When: making the publisher from the function and subscribing to it
        let cancelable = Publishers
            .fromAsync { () async -> String in
                semaphore.wait()
                isTaskCanceled = Task.isCancelled
                exp.fulfill()
                return ""
            }
            .sink { _ in }

        // When: canceling the subscription
        cancelable.cancel()
        semaphore.signal()

        waitForExpectations(timeout: 1)

        // Then: the aync task has been canceled
        XCTAssertTrue(isTaskCanceled)
    }

    func testFromThrowingAsync_publishes_value_when_throwing_asyncFunction_does_not_throw() {
        let exp = expectation(description: "fromAsync publishes the expected value when executing an async function that does no throw")

        var asyncFunctionNumberOfExecutions = 0
        let expectedOutput = UUID().uuidString
        var receivedOutput: String?

        // Given: a throwing async function that returns a value
        // When: making the publisher from the function and subscribing to it
        let cancelable = Publishers
            .fromThrowingAsync {
                asyncFunctionNumberOfExecutions += 1
                return expectedOutput
            }
            .handleEvents(receiveCompletion: { _ in exp.fulfill() })
            .sink(receiveCompletion: { _ in }, receiveValue: { asyncOutput in receivedOutput = asyncOutput })

        waitForExpectations(timeout: 1)

        // Then: The value from the async function is published
        XCTAssertEqual(receivedOutput, expectedOutput)
        XCTAssertEqual(asyncFunctionNumberOfExecutions, 1)

        cancelable.cancel()
    }

    func testFromThrowingAsync_executes_asynFunction_with_specified_priority_when_called_with_taskPriority() {
        let exp = expectation(description: "fromAsync uses the expected priority when executing a throwable async function with a priority")

        var asyncFunctionNumberOfExecutions = 0
        var receivedQueue: String?

        // Given: a throwable async function
        // When: making the publisher from the function and subscribing to it with a priority
        let cancelable = Publishers
            .fromThrowingAsync(priority: .userInitiated) { () async throws -> String in
                asyncFunctionNumberOfExecutions += 1
                receivedQueue = DispatchQueue.currentLabel
                return ""
            }
            .handleEvents(receiveCompletion: { _ in exp.fulfill() })
            .sink(receiveCompletion: { _ in }, receiveValue: { _ in })

        waitForExpectations(timeout: 1)

        // Then: The async function is executed with the expected priority
        XCTAssertTrue(receivedQueue!.contains("user-initiated"))
        XCTAssertEqual(asyncFunctionNumberOfExecutions, 1)

        cancelable.cancel()
    }

    func testFromThrowingAsync_completesWithFailure_when_asyncFunction_throws() {
        let exp = expectation(description: "fromAsync completes with the expected failure when executing an async function that throws")

        var asyncFunctionNumberOfExecutions = 0
        let expectedError = MockError(value: Int.random(in: 0...100))
        var receivedError: Error?

        // Given: an async function that throws
        // When: making the publisher from the function and subscribing to it
        let cancelable = Publishers
            .fromThrowingAsync { () async throws -> String in
                asyncFunctionNumberOfExecutions += 1
                throw expectedError
            }
            .handleEvents(receiveCompletion: { _ in exp.fulfill() })
            .sink(receiveCompletion: { completion in
                if case let .failure(error) = completion {
                    receivedError = error
                }
            }, receiveValue: { _ in })

        waitForExpectations(timeout: 1)

        // Then: The error from the async function is catched in the completion
        XCTAssertEqual(receivedError as? MockError, expectedError)
        XCTAssertEqual(asyncFunctionNumberOfExecutions, 1)

        cancelable.cancel()
    }

    func testFromThrowingAsync_cancels_task_when_subscription_is_canceled() {
        let exp = expectation(description: "fromAsync cancels the task when the subscription is canceled")

        let semaphore = DispatchSemaphore(value: 0)

        var isTaskCanceled = false

        // Given: a throwing async function that records the cancelation of its execution task
        // When: making the publisher from the function and subscribing to it
        let cancelable = Publishers
            .fromThrowingAsync { () async throws -> String in
                semaphore.wait()
                isTaskCanceled = Task.isCancelled
                exp.fulfill()
                return ""
            }
            .sink(receiveCompletion: { _ in }, receiveValue: { _ in })

        // When: canceling the subscription
        cancelable.cancel()
        semaphore.signal()

        waitForExpectations(timeout: 1)

        // Then: the async task has been canceled
        XCTAssertTrue(isTaskCanceled)
    }

    func testFromAsyncSequence_publishes_the_values_from_a_non_throwing_asyncSequence() {
        let exp = expectation(description: "fromAsync publishes the expected values when executing an async sequence")

        let expectedValues = [1, 2, 3, 4, 5, 6, 7, 8, 9, 10]
        var receivedValues: [Int]?

        // Given: an async sequence
        let sut = expectedValues.publisher.values

        // When: making the publisher from the sequence and subscribing to it
        let cancelable = Publishers
            .fromAsyncSequence(sut)
            .collect()
            .sink { completion in
                exp.fulfill()
            } receiveValue: { output in
                receivedValues = output
            }

        waitForExpectations(timeout: 1)

        // Then: the publisher publishes the values from the sequence
        XCTAssertEqual(receivedValues, expectedValues)

        cancelable.cancel()
    }

    func testFromAsyncSequence_completes_with_failure_when_asyncSequence_throws() {
        let exp = expectation(description: "fromAsync completes with error when async sequence fails")

        let expectedError = MockError(value: Int.random(in: 1...100))
        var receivedError: Error?

        // Given: an async sequence that throws
        let sut = AsyncThrowingStream(Int.self) { continuation in
            continuation.finish(throwing: expectedError)
        }

        // When: making the publisher from the sequence and subscribing to it
        let cancelable = Publishers
            .fromAsyncSequence(sut)
            .sink { completion in
                if case let .failure(error) = completion {
                    receivedError = error
                }
                exp.fulfill()
            } receiveValue: { _ in }

        waitForExpectations(timeout: 1)

        // Then: The error from the async sequence is catched in the completion
        XCTAssertEqual(receivedError as? MockError, expectedError)

        cancelable.cancel()
    }

    func testFromAsyncSequence_cancels_task_when_subscription_is_canceled() {
        let exp = expectation(description: "fromAsync cancels the task when the subscription is canceled")

        class CancelRecorder {
            var isCanceled = false

            init(isCanceled: Bool) {
                self.isCanceled = isCanceled
            }

            func setCanceled() {
                self.isCanceled = true
            }
        }

        let cancelRecorder = CancelRecorder(isCanceled: false)

        // Given: An async sequence that records its cancelation
        // When: making the publisher from the sequence and subscribing to it
        let cancelable = Publishers
            .fromAsyncSequence(AsyncStream(Int.self) { continuation in
                continuation.onTermination = { @Sendable _ in
                    cancelRecorder.setCanceled()
                    exp.fulfill()
                }
            })
            .sink(receiveCompletion: { print($0) }, receiveValue: { print($0) })

        // When: canceling the subscription
        cancelable.cancel()

        waitForExpectations(timeout: 1)

        // Then: the async sequence has been canceled
        XCTAssertTrue(cancelRecorder.isCanceled)
    }
}

fileprivate extension DispatchQueue {
    class var currentLabel: String {
        return String(validatingUTF8: __dispatch_queue_get_label(nil))!
    }
}
#endif
