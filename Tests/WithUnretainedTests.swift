//
//  WithUnretainedTests.swift
//  CombineExtTests
//
//  Created by Robert on 02/09/2021.
//

#if !os(watchOS)
import XCTest
import Foundation
import Combine
import CombineExt

@available(OSX 10.15, iOS 13.0, tvOS 13.0, watchOS 6.0, *)
final class WithUnretainedTests: XCTestCase {
    fileprivate var testClass: TestClass!
    var subscription: AnyCancellable?
    var values: [String] = []
    
    enum WithUnretainedTestsError: Swift.Error {
        case someError
    }
    
    override func setUp() {
        super.setUp()
        
        testClass = TestClass()
        values = []
    }
    
    override func tearDown() {
        subscription?.cancel()
        subscription = nil
    }
    
    func testObjectAttached() {
        let testClassId = testClass.id
        var completed = false
        
        let correctValues = [
            "\(testClassId), 1",
            "\(testClassId), 2",
            "\(testClassId), 3",
            "\(testClassId), 5",
            "\(testClassId), 8"
        ]
        
        let inputArr = [1, 2, 3, 5, 8]
        
        subscription = Publishers.Sequence<[Int], WithUnretainedTestsError>(sequence: inputArr)
            .withUnretained(self.testClass)
            .map { "\($0.id), \($1)" }
            .sink(receiveCompletion: { _ in completed = true },
                  receiveValue: { self.values.append($0) })

        XCTAssertEqual(values, correctValues)
        XCTAssertTrue(completed)
    }
    
    func testObjectDeallocatesWithEmptyPublisher() {
        subscription = Empty<Int, WithUnretainedTestsError>()
            .withUnretained(self.testClass)
            .sink(receiveCompletion: { _ in }, receiveValue: { _ in })
        
        // Confirm the object can be deallocated
        XCTAssertTrue(testClass != nil)
        testClass = nil
        XCTAssertTrue(testClass == nil)
    }
    
    func testObjectDeallocates() {
        let inputArr = [1, 2, 3, 5, 8]
        
        subscription = Publishers.Sequence<[Int], WithUnretainedTestsError>(sequence: inputArr)
            .withUnretained(self.testClass)
            .sink(receiveCompletion: { _ in }, receiveValue: { _ in })
        
        // Confirm the object can be deallocated
        XCTAssertTrue(testClass != nil)
        testClass = nil
        XCTAssertTrue(testClass == nil)
    }
    
    func testObjectDeallocatesSequenceCompletes() {
        let testClassId = testClass.id
        var completed = false
        
        let correctValues = [
            "\(testClassId), 1",
            "\(testClassId), 2",
            "\(testClassId), 3"
        ]

        let inputArr = [1, 2, 3]
        subscription = Publishers.Sequence<[Int], WithUnretainedTestsError>(sequence: inputArr)
            .withUnretained(self.testClass)
            .handleEvents(receiveOutput: { _, value in
                // Release the object in the middle of the sequence
                // to confirm it properly terminates the sequence
                if value == 3 {
                    self.testClass = nil
                }
            })
            .map { "\($0.id), \($1)" }
            .sink(receiveCompletion: { _ in completed = true },
                  receiveValue: { self.values.append($0) })
        
        XCTAssertEqual(values, correctValues)
        XCTAssertTrue(completed)
    }
    
    func testResultsSelector() {
        let testClassId = testClass.id
        var completed = false
        
        let inputArr = [(1, "a"), (2, "b"), (3, "c"), (5, "d"), (8, "e")]
        
        let correctValues = [
            "\(testClassId), 1, a",
            "\(testClassId), 2, b",
            "\(testClassId), 3, c",
            "\(testClassId), 5, d",
            "\(testClassId), 8, e"
        ]

        subscription = Publishers.Sequence<[(Int, String)], WithUnretainedTestsError>(sequence: inputArr)
                .withUnretained(self.testClass) { ($0, $1.0, $1.1) }
                .map { "\($0.id), \($1), \($2)" }
            .sink(receiveCompletion: { _ in completed = true },
                  receiveValue: { self.values.append($0) })

        XCTAssertEqual(values, correctValues)
        XCTAssertTrue(completed)
    }
}

private class TestClass {
    let id: String = UUID().uuidString
}
#endif
