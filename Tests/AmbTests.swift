//
//  AmbTests.swift
//  CombineExtTests
//
//  Created by Shai Mishali on 29/03/2020.
//  Copyright © 2020 Combine Community. All rights reserved.
//

#if !os(watchOS)
import XCTest
import Combine
import CombineExt

@available(OSX 10.15, iOS 13.0, tvOS 13.0, watchOS 6.0, *)
class AmbTests: XCTestCase {
    var subscriptions = Set<AnyCancellable>()

    override func tearDown() {
        subscriptions = Set()
    }

    func testAmbValues() {
        let subject1 = PassthroughSubject<Int, Never>()
        let subject2 = PassthroughSubject<Int, Never>()
        var completion: Subscribers.Completion<Never>?
        var values = [Int]()

        subject1
            .amb(subject2)
            .sink(receiveCompletion: { completion = $0 },
                  receiveValue: { values.append($0) })
            .store(in: &subscriptions)

        subject2.send(1)
        subject2.send(2)
        subject1.send(6)
        subject1.send(6)
        subject1.send(6)
        subject2.send(8)

        XCTAssertEqual(values, [1, 2, 8])

        XCTAssertNil(completion)
        subject1.send(completion: .finished)
        XCTAssertNil(completion)
        subject2.send(completion: .finished)
        XCTAssertEqual(completion, .finished)
    }

    func testAmbCancelPreSubscription() {
        let ambPublisher: AnyCancellable?

        let subject1Cancelled = expectation(description: "first publisher cancelled")
        let subject1 = PassthroughSubject<Int, Error>()
        let subject1Publisher = subject1
            .handleEvents(receiveCancel: {
                subject1Cancelled.fulfill()
            })
            .eraseToAnyPublisher()

        let subject2Cancelled = expectation(description: "second publisher cancelled")
        let subject2 = PassthroughSubject<Int, Error>()
        let subject2Publisher = subject2
            .handleEvents(receiveCancel: {
                subject2Cancelled.fulfill()
            })
            .eraseToAnyPublisher()

        ambPublisher = Publishers.Amb(first: subject1Publisher, second: subject2Publisher)
            .sink(receiveCompletion: { _ in },
                  receiveValue: { _ in })

        // cancelling amb should cancel the inner publishers
        ambPublisher?.cancel()

        waitForExpectations(timeout: 0.01)
    }

    func testAmbCancelPostSubscription() {
        let subject1 = PassthroughSubject<Int, Error>()
        var subject1cancelCounter = 0
        let subject1Publisher = subject1
            .handleEvents(receiveCancel: {
                subject1cancelCounter += 1
            })
            .eraseToAnyPublisher()

        let subject2 = PassthroughSubject<Int, Error>()
        var subject2cancelCounter = 0
        let subject2Publisher = subject2
            .handleEvents(receiveCancel: {
                subject2cancelCounter += 1
            })
            .eraseToAnyPublisher()

        Publishers.Amb(first: subject1Publisher, second: subject2Publisher)
            .sink(receiveCompletion: { _ in },
                  receiveValue: { _ in })
            .store(in: &subscriptions)

        // subject1 wins the race, so 2 has to be cancelled
        subject1.send(1)

        // At dealloc both publishes are cancelled, so we cannot use expectations here and count the cancel events instead
        XCTAssertEqual(subject1cancelCounter, 0)
        XCTAssertEqual(subject2cancelCounter, 1)
    }

    func testAmbLimitedPreDemand() {
        let subject1 = PassthroughSubject<Int, Never>()
        let subject2 = PassthroughSubject<Int, Never>()
        var values = [Int]()

        let subscriber = AnySubscriber<Int, Never>(
            receiveSubscription: { subscription in
                subscription.request(.max(2))

                subject2.send(3)
                subject1.send(1)
                subject1.send(0)
                subject2.send(0)
                subject2.send(7)
                subject2.send(11)
                subject1.send(12)
                subject2.send(14)
            },
            receiveValue: { value in
                values.append(value)
                return .none
            },
            receiveCompletion: { _ in })

        subject1
            .amb(subject2)
            .subscribe(subscriber)

        // We expect only the first two values emitted by subject2
        // since this is the demand that should be accumulated before
        // a decision was made on who is the winning publisher
        XCTAssertEqual(values, [3, 0])
    }

    func testAmbEmptyAndNever() {
        let subject1 = Empty<Int, Never>(completeImmediately: false).append(0)
        let subject2 = Empty<Int, Never>(completeImmediately: true).append(1)
        var completion: Subscribers.Completion<Never>?
        var values = [Int]()

        subject1
            .amb(subject2)
            .sink(receiveCompletion: { completion = $0 },
                  receiveValue: { values.append($0) })
            .store(in: &subscriptions)

        XCTAssertEqual(values, [1])
        XCTAssertEqual(completion, .finished)
    }

    func testAmbEmptyAndEmpty() {
        let subject1 = Empty<Int, Never>(completeImmediately: true)
        let subject2 = Empty<Int, Never>(completeImmediately: true)
        var completion: Subscribers.Completion<Never>?
        var values = [Int]()

        Publishers
            .Amb(first: subject1, second: subject2)
            .sink(receiveCompletion: { completion = $0 },
                  receiveValue: { values.append($0) })
            .store(in: &subscriptions)

        XCTAssertEqual(completion, .finished)
    }

    func testAmbNeverAndNever() {
        let subject1 = Empty<Int, Never>(completeImmediately: false)
        let subject2 = Empty<Int, Never>(completeImmediately: false)
        var completion: Subscribers.Completion<Never>?
        var values = [Int]()

        Publishers
            .Amb(first: subject1, second: subject2)
            .sink(receiveCompletion: { completion = $0 },
                  receiveValue: { values.append($0) })
            .store(in: &subscriptions)

        XCTAssertNil(completion)
    }

    func testAmbVariadicValues() {
        let subject1 = PassthroughSubject<Int, Never>()
        let subject2 = PassthroughSubject<Int, Never>()
        let subject3 = PassthroughSubject<Int, Never>()
        let subject4 = PassthroughSubject<Int, Never>()

        var completionPair: Subscribers.Completion<Never>?
        var completionThree: Subscribers.Completion<Never>?
        var completionFour: Subscribers.Completion<Never>?

        var valuesPair = [Int]()
        var valuesThree = [Int]()
        var valuesFour = [Int]()

        subject4
            .amb(with: subject2, subject3, subject1)
            .sink(receiveCompletion: { completionFour = $0 },
                  receiveValue: { valuesFour.append($0) })
            .store(in: &subscriptions)

        subject1
            .amb(with: subject2, subject3)
            .sink(receiveCompletion: { completionThree = $0 },
                  receiveValue: { valuesThree.append($0) })
            .store(in: &subscriptions)

        subject1
            .amb(with: subject2)
            .sink(receiveCompletion: { completionPair = $0 },
                  receiveValue: { valuesPair.append($0) })
            .store(in: &subscriptions)

        subject4.send(3)
        subject2.send(1)
        subject2.send(2)
        subject3.send(5)
        subject1.send(6)
        subject3.send(2)
        subject1.send(6)
        subject4.send(2)
        subject1.send(6)
        subject4.send(7)
        subject2.send(8)

        XCTAssertEqual(valuesFour, [3, 2, 7])
        XCTAssertEqual(valuesThree, [1, 2, 8])
        XCTAssertEqual(valuesPair, [1, 2, 8])

        XCTAssertNil(completionFour)
        subject1.send(completion: .finished)
        XCTAssertNil(completionFour)
        subject2.send(completion: .finished)
        XCTAssertEqual(completionPair, .finished)
        XCTAssertNil(completionFour)
        subject3.send(completion: .finished)
        XCTAssertEqual(completionThree, .finished)
        XCTAssertNil(completionFour)
        subject4.send(completion: .finished)
        XCTAssertEqual(completionFour, .finished)
    }

    func testAmbCollectionNone() {
        var results = [Int]()
        var completion: Subscribers.Completion<Never>?

        [AnyPublisher<Int, Never>]()
            .amb()
            .sink(receiveCompletion: { completion = $0 },
                  receiveValue: { results.append($0) })
            .store(in: &subscriptions)

        XCTAssertTrue(results.isEmpty)
        XCTAssertEqual(.finished, completion)
    }

    func testAmbCollectionOne() {
        let subject1 = PassthroughSubject<Int, Never>()

        var results = [Int]()
        var completion: Subscribers.Completion<Never>?

        [subject1]
            .amb()
            .sink(receiveCompletion: { completion = $0 },
                  receiveValue: { results.append($0) })
            .store(in: &subscriptions)

        subject1.send(1)
        subject1.send(completion: .finished)

        XCTAssertEqual([1], results)
        XCTAssertEqual(.finished, completion)
    }

    func testAmbCollectionMany() {
        let subject1 = PassthroughSubject<Int, Never>()
        let subject2 = PassthroughSubject<Int, Never>()
        let subject3 = PassthroughSubject<Int, Never>()

        var results = [Int]()
        var completion: Subscribers.Completion<Never>?

        [subject1, subject2, subject3]
            .amb()
            .sink(receiveCompletion: { completion = $0 },
                  receiveValue: { results.append($0) })
            .store(in: &subscriptions)

        subject1.send(1)
        subject2.send(2)
        subject3.send(3)
        subject1.send(4)

        subject1.send(completion: .finished)

        XCTAssertEqual([1, 4], results)
        XCTAssertEqual(.finished, completion)
    }
}
#endif
