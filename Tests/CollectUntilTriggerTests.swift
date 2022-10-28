//
//  CollectUntilTriggerTests.swift
//  CombineExtTests
//
//  Created by ferologics on 09/06/2021.
//  Copyright © 2020 Combine Community. All rights reserved.
//

#if !os(watchOS)
import XCTest
import Combine

@available(OSX 10.15, iOS 13.0, tvOS 13.0, watchOS 6.0, *)
class CollectUntilTriggerTests: XCTestCase {
    var subscription: AnyCancellable!

    func test() {
        // Given
        let elements = [1,2,3,4,5]
        var receivedElements = [Int]()
        let elementsPublisher = PassthroughSubject<Int, Never>()
        let trigger = PassthroughSubject<Void, Never>()

        // When
        subscription = elementsPublisher
            .collect(until: trigger)
            .sink { receivedElements = $0 }

        for x in elements {
            elementsPublisher.send(x)
        }

        // Then
        XCTAssertTrue(receivedElements.isEmpty)
        trigger.send(())
        XCTAssertEqual(elements.count, receivedElements.count)
        for (a, b) in zip(elements, receivedElements) {
            XCTAssertEqual(a, b)
        }
    }
}
#endif
