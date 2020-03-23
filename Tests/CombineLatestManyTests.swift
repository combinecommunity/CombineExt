//
//  CombineLatestManyTests.swift
//  CombineExtTests
//
//  Created by Jasdev Singh on 3/22/20.
//

import Combine
import CombineExt
import XCTest

final class CombineLatestManyTests: XCTestCase {
    private var subscription: AnyCancellable!

    private enum CombineLatestManyTestError: Error {
        case anError
    }

    func testCollectionCombineLatestWithFinishedEvent() {
        let first = PassthroughSubject<Int, Never>()
        let second = PassthroughSubject<Int, Never>()
        let third = PassthroughSubject<Int, Never>()
        let fourth = PassthroughSubject<Int, Never>()

        var completed = false
        var results = [[Int]]()

        subscription = [first, second, third, fourth]
            .combineLatest()
            .sink(receiveCompletion: { _ in completed = true },
                  receiveValue: { results.append($0) })

        first.send(1)
        second.send(2)

        XCTAssertTrue(results.isEmpty)
        XCTAssertFalse(completed)

        third.send(3)
        fourth.send(4)

        XCTAssertEqual(results, [[1, 2, 3, 4]])
        XCTAssertFalse(completed)

        first.send(1)

        XCTAssertEqual(results, [[1, 2, 3, 4], [1, 2, 3, 4]])
        XCTAssertFalse(completed)

        fourth.send(4)

        XCTAssertEqual(results, [[1, 2, 3, 4], [1, 2, 3, 4], [1, 2, 3, 4]])
        XCTAssertFalse(completed)

        first.send(completion: .finished)

        XCTAssertEqual(results, [[1, 2, 3, 4], [1, 2, 3, 4], [1, 2, 3, 4]])
        XCTAssertFalse(completed)

        [second, third, fourth].forEach {
            $0.send(completion: .finished)
        }

        XCTAssertTrue(completed)
    }

    func testCollectionCombineLatestWithNoEvents() {
        let first = PassthroughSubject<Int, Never>()
        let second = PassthroughSubject<Int, Never>()

        var completed = false
        var results = [[Int]]()

        subscription = [first, second]
            .combineLatest()
            .sink(receiveCompletion: { _ in completed = true },
                  receiveValue: { results.append($0) })


        XCTAssertTrue(results.isEmpty)
        XCTAssertFalse(completed)
    }

    func testCollectionCombineLatestWithErrorEvent() {
        let first = PassthroughSubject<Int, CombineLatestManyTestError>()
        let second = PassthroughSubject<Int, CombineLatestManyTestError>()

        var completion: Subscribers.Completion<CombineLatestManyTestError>?
        var results = [[Int]]()

        subscription = [first, second]
            .combineLatest()
            .sink(receiveCompletion: { completion = $0 },
                  receiveValue: { results.append($0) })

        first.send(1)
        second.send(2)

        XCTAssertEqual(results, [[1, 2]])
        XCTAssertNil(completion)

        second.send(completion: .failure(.anError))

        XCTAssertEqual(completion, .failure(.anError))
    }

    func testCollectionCombineLatestWithASinglePublisher() {
        let first = PassthroughSubject<Int, Never>()

        var completed = false
        var results = [[Int]]()

        subscription = [first]
            .combineLatest()
            .sink(receiveCompletion: { _ in completed = true },
                  receiveValue: { results.append($0) })

        first.send(1)

        XCTAssertEqual(results, [[1]])
        XCTAssertFalse(completed)

        first.send(completion: .finished)

        XCTAssertTrue(completed)
    }

    func testCollectionCombineLatestWithNoPublishers() {
        var completed = false
        var results = [[Int]]()

        subscription = [AnyPublisher<Int, Never>]()
            .combineLatest()
            .sink(receiveCompletion: { _ in completed = true },
                  receiveValue: { results.append($0) })

        XCTAssertTrue(results.isEmpty)
        XCTAssertTrue(completed)
    }

    func testMethodCombineLatestWithFinishedEvent() {
        let first = PassthroughSubject<Int, Never>()
        let second = PassthroughSubject<Int, Never>()

        var completed = false
        var results = [[Int]]()

        subscription = first.combineLatest(with: [second])
            .sink(receiveCompletion: { _ in completed = true },
                  receiveValue: { results.append($0) })

        first.send(1)
        second.send(2)

        XCTAssertEqual(results, [[1, 2]])
        XCTAssertFalse(completed)

        second.send(2)
        second.send(2)

        XCTAssertEqual(results, [[1, 2], [1, 2], [1, 2]])
        XCTAssertFalse(completed)

        first.send(completion: .finished)

        XCTAssertFalse(completed)

        second.send(completion: .finished)

        XCTAssertTrue(completed)
    }
}
