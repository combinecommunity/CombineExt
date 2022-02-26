//
//  CompactScanTests.swift
//  CombineExtTests
//
//  Created by Thibault Wittemberg on 04/09/2021.
//  Copyright © 2021 Combine Community. All rights reserved.
//

#if !os(watchOS)
import XCTest
import Combine
import CombineExt

@available(OSX 10.15, iOS 13.0, tvOS 13.0, watchOS 6.0, *)
final class CompactScanTests: XCTestCase {
    func testCompactScan_drops_nil_values() {
        let expectedValues = [0, 2, 6]
        var receivedValues = [Int]()

        // Given: a stream of integers from 0 to 5
        let sut = (0...5).publisher

        // When: using a compactScan operator using a closure that returns nil when the value from the upstream publisher is odd
        let cancellable = sut
            .compactScan(0) {
                guard $1.isMultiple(of: 2) else { return nil }
                return $0 + $1
            }
            .assertNoFailure()
            .sink { receivedValues.append($0) }

        // Then: the nil results have been discarded
        XCTAssertEqual(receivedValues, expectedValues)

        cancellable.cancel()
    }

    func testTryCompactScan_drops_nil_values() {
        let expectedValues = [0, 2, 6]
        var receivedValues = [Int]()

        // Given: a stream of integers from 0 to 5
        let sut = (0...5).publisher

        // When: using a tryCompactScan operator using a closure that returns nil when the value from the upstream publisher is odd
        let cancellable = sut
            .tryCompactScan(0) {
                guard $1.isMultiple(of: 2) else { return nil }
                return $0 + $1
            }
            .assertNoFailure()
            .sink { receivedValues.append($0) }

        // Then: the nil results have been discarded
        XCTAssertEqual(receivedValues, expectedValues)

        cancellable.cancel()
    }

    func testTryCompactScan_drops_nil_values_and_throws_error() {
        struct DivisionByZeroError: Error, Equatable {}

        let expectedValues = [6, 2]
        var receivedValues = [Int]()

        let expectedError = DivisionByZeroError()
        var receivedCompletion: Subscribers.Completion<Error>?

        // Given: a sequence a integers containing a 0
        let sut = [1, 2, 3, 4, 5, 0, 6, 7, 8, 9].publisher

        // When: using a tryCompactScan operator using a closure that returns nil when the value from the upstream publisher is odd
        // and throws when the value is 0
        let cancellable = sut
            .tryCompactScan(10) {
                guard $1.isMultiple(of: 2) else { return nil }
                guard $1 != 0 else { throw expectedError }
                return ($0 + $1) / $1
            }
            .sink {
                receivedCompletion = $0
            } receiveValue: {
                receivedValues.append($0)
            }

        cancellable.cancel()

        // Then: the nil results have been discarded
        XCTAssertEqual(receivedValues, expectedValues)

        // Then: the thrown error provoqued a failure
        switch receivedCompletion {
        case let .failure(receivedError): XCTAssertEqual(receivedError as? DivisionByZeroError, expectedError)
        default: XCTFail()
        }
    }
}
#endif
