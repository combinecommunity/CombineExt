//
//  RetryWhenTests.swift
//  CombineExtTests
//
//  Created by Daniel Tartaglia on 8/28/21.
//

#if !os(watchOS)
import XCTest
import Combine
import CombineExt

@available(OSX 10.15, iOS 13.0, tvOS 13.0, watchOS 6.0, *)
class RetryWhenTests: XCTestCase {
    var subscription: AnyCancellable!

    func testPassthroughNextAndComplete() {
        let source = PassthroughSubject<Int, MyError>()

        var expectedOutput: Int?

        var completion: Subscribers.Completion<MyError>?

        subscription = source
            .retryWhen { error in
                error.filter { _ in false }
            }
            .sink(
                receiveCompletion: { completion = $0 },
                receiveValue: { expectedOutput = $0 }
            )

        source.send(2)
        source.send(completion: .finished)

        XCTAssertEqual(
            expectedOutput,
            2
        )
        XCTAssertEqual(completion, .finished)
    }

    func testSuccessfulRetry() {
        var times = 0
        var retriesCount = 0
        var expectedOutput: Int?

        var completion: Subscribers.Completion<RetryWhenTests.MyError>?

        subscription = Deferred(createPublisher: { () -> AnyPublisher<Int, MyError> in
            defer { times += 1 }
            if times == 0 {
                return Fail<Int, MyError>(error: MyError.someError).eraseToAnyPublisher()
            }
            else {
                return Just(5).setFailureType(to: MyError.self).eraseToAnyPublisher()
            }
        })
        .retryWhen { error in
            error
                .handleEvents(receiveOutput: { _ in retriesCount += 1})
                .map { _ in }
        }
        .sink(
            receiveCompletion: { completion = $0 },
            receiveValue: { expectedOutput = $0 }
        )

        XCTAssertEqual(
            expectedOutput,
            5
        )
        XCTAssertEqual(completion, .finished)
        XCTAssertEqual(times, 2)
        XCTAssertEqual(retriesCount, 1)
    }

    func testRetryFailure() {
        var expectedOutput: Int?

        var completion: Subscribers.Completion<RetryWhenTests.MyError>?

        subscription = Fail<Int, MyError>(error: MyError.someError)
            .retryWhen { error in
                error.tryMap { _ in throw MyError.retryError }
            }
            .sink(
                receiveCompletion: { completion = $0 },
                receiveValue: { expectedOutput = $0 }
            )

        XCTAssertEqual(
            expectedOutput,
            nil
        )
        XCTAssertEqual(completion, .failure(MyError.retryError))
    }

    func testRetryComplete() {
        var expectedOutput: Int?

        var completion: Subscribers.Completion<RetryWhenTests.MyError>?

        subscription = Fail<Int, MyError>(error: MyError.someError)
            .retryWhen { error in
                error.prefix(1)
            }
            .sink(
                receiveCompletion: { completion = $0 },
                receiveValue: { expectedOutput = $0 }
            )

        XCTAssertEqual(
            expectedOutput,
            nil
        )
        XCTAssertEqual(completion, .finished)
    }

    enum MyError: Swift.Error {
        case someError
        case retryError
    }
}
#endif
