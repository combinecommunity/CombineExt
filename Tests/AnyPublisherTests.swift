//
//  OptionalTests.swift
//  CombineExt
//
//  Created by Jasdev Singh on 11/05/2020.
//  Copyright © 2020 Combine Community. All rights reserved.
//

#if !os(watchOS)
import Combine
import CombineExt
import XCTest

@available(OSX 10.15, iOS 13.0, tvOS 13.0, watchOS 6.0, *)
final class AnyPublisherTests: XCTestCase {
    private var subscription: AnyCancellable!

    func testJustInitialization() {
        var results = [Int]()
        var completion: Subscribers.Completion<Never>?

        subscription = AnyPublisher.just(1)
            
            .sink(receiveCompletion: { completion = $0 },
                  receiveValue: { results.append($0) })

        XCTAssertEqual([1], results)
        XCTAssertEqual(.finished, completion)
    }
    
    func testEmptyInitialization() {
        var results = [Int]()
        var completion: Subscribers.Completion<Never>?

        subscription = AnyPublisher<Int, Never>.empty()
            .sink(receiveCompletion: { completion = $0 }, receiveValue: { results.append($0) })

        XCTAssertEqual([], results)
        XCTAssertEqual(.finished, completion)
    }
    
    func testFailInitialization() {
        enum CustomError: Swift.Error {
            case someError
        }
        var results = [Int]()
        var completion: Subscribers.Completion<CustomError>?

        subscription = AnyPublisher<Int, CustomError>.fail(error: .someError)
            .sink(receiveCompletion: { completion = $0 }, receiveValue: { results.append($0) })

        XCTAssertEqual([], results)
        XCTAssertEqual(.failure(.someError), completion)
    }

}
#endif
