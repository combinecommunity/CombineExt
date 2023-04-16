//
//  PassthroughRelayTests.swift
//  CombineExtTests
//
//  Created by Shai Mishali on 15/03/2020.
//  Copyright © 2020 Combine Community. All rights reserved.
//

#if !os(watchOS)
import XCTest
import Combine
import CombineExt

@available(OSX 10.15, iOS 13.0, tvOS 13.0, watchOS 6.0, *)
class PassthroughRelayTests: XCTestCase {
    private var relay: PassthroughRelay<String>?
    private var values = [String]()
    private var subscriptions = Set<AnyCancellable>()

    override func setUp() {
        relay = PassthroughRelay<String>()
        subscriptions = .init()
        values = []
    }

    func testFinishesOnDeinit() {
        var completed = false
        relay?
            .sink(receiveCompletion: { _ in completed = true },
                  receiveValue: { _ in })
            .store(in: &subscriptions)

        XCTAssertFalse(completed)
        relay = nil
        XCTAssertTrue(completed)
    }

    func testNoReplay() {
        relay?.accept("these")
        relay?.accept("values")
        relay?.accept("shouldnt")
        relay?.accept("be")
        relay?.accept("forwaded")

        relay?
            .sink(receiveValue: { self.values.append($0) })
            .store(in: &subscriptions)

        XCTAssertEqual(values, [])

        relay?.accept("yo")
        XCTAssertEqual(values, ["yo"])

        relay?.accept("sup")
        XCTAssertEqual(values, ["yo", "sup"])

        var secondInitial: String?
        _ = relay?.sink(receiveValue: { secondInitial = $0 })
        XCTAssertNil(secondInitial)
    }

    func testVoidAccept() {
        let voidRelay = PassthroughRelay<Void>()
        var count = 0

        voidRelay
            .sink(receiveValue: { count += 1 })
            .store(in: &subscriptions)

        voidRelay.accept()
        voidRelay.accept()
        voidRelay.accept()
        voidRelay.accept()
        voidRelay.accept()

        XCTAssertEqual(count, 5)
    }

    func testSubscribePublisher() {
        var completed = false
        relay?
            .sink(receiveCompletion: { _ in completed = true },
                  receiveValue: { self.values.append($0) })
            .store(in: &subscriptions)

        ["1", "2", "3"]
            .publisher
            .subscribe(relay!)
            .store(in: &subscriptions)

        XCTAssertFalse(completed)
        XCTAssertEqual(values, ["1", "2", "3"])

        relay!.accept("4")

        XCTAssertFalse(completed)
        XCTAssertEqual(values, ["1", "2", "3", "4"])
    }

    func testSubscribeRelay_Passthroughs() {
        var completed = false

        let input = PassthroughRelay<String>()
        let output = PassthroughRelay<String>()

        input
            .subscribe(output)
            .store(in: &subscriptions)
        output
            .sink(receiveCompletion: { _ in completed = true },
                  receiveValue: { self.values.append($0) })
            .store(in: &subscriptions)

        input.accept("1")
        input.accept("2")
        input.accept("3")

        XCTAssertFalse(completed)
        XCTAssertEqual(values, ["1", "2", "3"])
    }

    func testSubscribeRelay_CurrentValueToPassthrough() {
        var completed = false

        let input = CurrentValueRelay<String>("initial")
        let output = PassthroughRelay<String>()

        input
            .subscribe(output)
            .store(in: &subscriptions)
        output
            .sink(receiveCompletion: { _ in completed = true },
                  receiveValue: { self.values.append($0) })
            .store(in: &subscriptions)

        input.accept("1")
        input.accept("2")
        input.accept("3")

        XCTAssertFalse(completed)
        XCTAssertEqual(values, ["1", "2", "3"])
    }
}
#endif
