//
//  ConcatMapTests.swift
//  CombineExtTests
//
//  Created by Daniel Peter on 22/11/2020.
//  Copyright © 2020 Combine Community. All rights reserved.
//

#if !os(watchOS)
import XCTest
import Combine
import CombineExt

@available(OSX 10.15, iOS 13.0, tvOS 13.0, watchOS 6.0, *)
final class ConcatMapTests: XCTestCase {
    private enum TestError: Swift.Error {
        case failure
    }
    private typealias P = PassthroughSubject<Int, TestError>
    private var cancellables: Set<AnyCancellable>!

    override func setUp() {
        super.setUp()
        cancellables = []
    }

    func test_publishes_values_in_order() {
        var receivedValues = [Int]()
        let expectedValues = [1, 2, 3]

        let firstPublisher = P()
        let secondPublisher = P()
        let thirdPublisher = P()

        let sut = PassthroughSubject<P, TestError>()

        sut.concatMap { $0 }
            .sink(
                receiveCompletion: { _ in },
                receiveValue: { value in receivedValues.append(value) }
            )
            .store(in: &cancellables)

        sut.send(firstPublisher)
        sut.send(secondPublisher)

        firstPublisher.send(1)
        firstPublisher.send(completion: .finished)

        secondPublisher.send(2)
        sut.send(thirdPublisher)
        secondPublisher.send(completion: .finished)

        thirdPublisher.send(3)

        XCTAssertEqual(expectedValues, receivedValues)
    }

    func test_ignores_values_of_subsequent_while_previous_hasNot_completed() {
        var receivedValues = [Int]()
        let expectedValues = [1, 3]

        let firstPublisher = P()
        let secondPublisher = P()

        let sut = PassthroughSubject<P, TestError>()

        sut.concatMap { $0 }
            .sink(
                receiveCompletion: { _ in },
                receiveValue: { value in receivedValues.append(value) }
            )
            .store(in: &cancellables)

        sut.send(firstPublisher)
        sut.send(secondPublisher)

        firstPublisher.send(1)
        secondPublisher.send(2)
        firstPublisher.send(completion: .finished)

        secondPublisher.send(3)
        secondPublisher.send(completion: .finished)

        XCTAssertEqual(expectedValues, receivedValues)
    }

    func test_publishes_values_of_subsequent_publisher_after_emptying_publisher_queue() {
        var receivedValues = [Int]()
        let expectedValues = [1, 2]

        let firstPublisher = P()
        let secondPublisher = P()

        let sut = PassthroughSubject<P, TestError>()

        sut.concatMap { $0 }
            .sink(
                receiveCompletion: { _ in },
                receiveValue: { value in receivedValues.append(value) }
            )
            .store(in: &cancellables)

        sut.send(firstPublisher)
        firstPublisher.send(1)
        firstPublisher.send(completion: .finished)

        sut.send(secondPublisher)
        secondPublisher.send(2)
        secondPublisher.send(completion: .finished)

        XCTAssertEqual(expectedValues, receivedValues)
    }

    func test_synchronous_completion() {
        var receivedValues = [Int]()
        let expectedValues = [1, 2]
        let firstPublisher = Just<Int>(1)
        let secondPublisher = Just<Int>(2)

        let sut = PassthroughSubject<Just<Int>, Never>()

        sut.concatMap { $0 }
            .sink { value in receivedValues.append(value) }
            .store(in: &cancellables)

        sut.send(firstPublisher)
        sut.send(secondPublisher)

        XCTAssertEqual(expectedValues, receivedValues)
    }

    func test_completes_when_upstream_completes() {
        var receivedCompletion: Subscribers.Completion<TestError>?

        let firstPublisher = P()
        let secondPublisher = P()

        let sut = PassthroughSubject<P, TestError>()

        sut.concatMap { $0 }
            .sink(
                receiveCompletion: { receivedCompletion = $0 },
                receiveValue: { _ in }
            )
            .store(in: &cancellables)

        sut.send(firstPublisher)
        sut.send(secondPublisher)
        firstPublisher.send(completion: .finished)
        XCTAssertNil(receivedCompletion)
        secondPublisher.send(completion: .finished)
        XCTAssertNil(receivedCompletion)
        sut.send(completion: .finished)
        XCTAssertNotNil(receivedCompletion)
    }

    func test_completes_with_failure_if_publisher_fails() {
        let expectedCompletion = Subscribers.Completion<TestError>.failure(.failure)
        var receivedCompletion: Subscribers.Completion<TestError>?

        let firstPublisher = P()
        let secondPublisher = P()

        let sut = PassthroughSubject<P, TestError>()

        sut.concatMap { $0 }
            .sink(
                receiveCompletion: { receivedCompletion = $0 },
                receiveValue: { _ in }
            )
            .store(in: &cancellables)

        sut.send(firstPublisher)
        sut.send(secondPublisher)
        firstPublisher.send(completion: .failure(.failure))
        XCTAssertEqual(receivedCompletion, expectedCompletion)
        secondPublisher.send(completion: .finished)
        XCTAssertEqual(receivedCompletion, expectedCompletion)
    }
}
#endif
