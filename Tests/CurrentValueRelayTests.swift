//
//  CurrentValueRelayTests.swift
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
class CurrentValueRelayTests: XCTestCase {
    private var relay: CurrentValueRelay<String>?
    private var values = [String]()
    private var subscriptions = Set<AnyCancellable>()

    override func setUp() {
        relay = CurrentValueRelay<String>("initial")
        subscriptions = .init()
        values = []
    }

    func testValueGetter() {
        XCTAssertEqual(relay?.value, "initial")
        relay?.accept("second")
        XCTAssertEqual(relay?.value, "second")
        relay?.accept("third")
        XCTAssertEqual(relay?.value, "third")
    }

    func testFinishesOnDeinit() {
        var completed = false

        relay?
            .sink(receiveCompletion: { _ in completed = true },
                  receiveValue: { _ in })
            .store(in: &subscriptions)

        XCTAssertEqual(relay?.value, "initial")

        XCTAssertFalse(completed)
        relay = nil
        XCTAssertTrue(completed)
    }

    func testReplaysCurrentValue() {
        relay?
            .sink(receiveValue: { self.values.append($0) })
            .store(in: &subscriptions)

        XCTAssertEqual(values, ["initial"])

        relay?.accept("yo")
        XCTAssertEqual(values, ["initial", "yo"])

        var secondInitial: String?
        _ = relay?.sink(receiveValue: { secondInitial = $0 })
        XCTAssertEqual(secondInitial, "yo")
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
        XCTAssertEqual(values, ["initial", "1", "2", "3"])

        relay!.accept("4")

        XCTAssertFalse(completed)
        XCTAssertEqual(values, ["initial", "1", "2", "3", "4"])
    }

    func testSubscribeRelay_CurrentValues() {
        var completed = false

        let input = CurrentValueRelay<String>("initial")
        let output = CurrentValueRelay<String>("initial")

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
        XCTAssertEqual(values, ["initial", "1", "2", "3"])
    }

    func testSubscribeRelay_PassthroughToCurrentValue() {
        var completed = false

        let input = PassthroughRelay<String>()
        let output = CurrentValueRelay<String>("initial")

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
        XCTAssertEqual(values, ["initial", "1", "2", "3"])
    }
    
    // There was a race condition which caused the value of a relay
    // to leak. Details of the race condition are in this PR:
    //
    // https://github.com/CombineCommunity/CombineExt/pull/137
    //
    // The easiest way to reproduce the race condition is
    // to initialize `cancellables` before `relay`.
    // The first two tests confirm the value of the relay is
    // released regardless of when cancellables is initialized.
    //
    // The last two tests check the scenario where a relay is
    // chained with a withLatestFrom operator. This leads
    // to two objects being leaked if cancellables is initialized
    // before the relays.
    final class StoredObject {
        static var storedObjectReleased = false
        
        let value = 10
        
        init() {
            Self.storedObjectReleased = false
        }

        deinit {
            Self.storedObjectReleased = true
        }
    }

    final class StoredObject2 {
        static var storedObjectReleased = false
        
        let value = 20
        
        init() {
            Self.storedObjectReleased = false
        }
        
        deinit {
            Self.storedObjectReleased = true
        }
    }
    
    func testStoredObjectIsDeallocatedWhenRelayIsDeallocatedAndDeclaredAfterCancellables() {
        final class ContainerClass {
            static var receivedCompletion = false
            static var receivedCancel = false

            // Cancellables comes before the relay.
            var cancellables = Set<AnyCancellable>()
            let relay = CurrentValueRelay(StoredObject())
            
            init() {
                relay
                    .handleEvents(receiveCancel: {
                        Self.receivedCancel = true
                    })
                    .sink(
                        receiveCompletion: { _ in
                            Self.receivedCompletion = true
                        },
                        receiveValue: { _ in }
                    )
                    .store(in: &cancellables)
            }
        }
        
        var container: ContainerClass? = ContainerClass()
        
        XCTAssertFalse(ContainerClass.receivedCompletion)
        XCTAssertFalse(StoredObject.storedObjectReleased)
        container = nil
        XCTAssertTrue(StoredObject.storedObjectReleased)
        XCTAssertNil(container)
        
        // In this case the cancellables is deallocated before the relay.
        // The deinit method of AnyCancellable calls cancel for all subscriptions.
        // Completion will never be called for a canceled subscription.
        XCTAssertFalse(ContainerClass.receivedCompletion)
        XCTAssertTrue(ContainerClass.receivedCancel)
    }

    func testStoredObjectIsDeallocatedWhenRelayIsDeallocatedAndDeclaredBeforeCancellables() {
        final class ContainerClass {
            static var receivedCompletion = false
            static var receivedCancel = false

            // Cancellables comes after the relay.
            let relay = CurrentValueRelay(StoredObject())
            var cancellables = Set<AnyCancellable>()

            init() {
                relay
                    .handleEvents(receiveCancel: {
                        Self.receivedCancel = true
                    })
                    .sink(
                        receiveCompletion: { _ in
                            Self.receivedCompletion = true
                        },
                        receiveValue: { _ in }
                    )
                    .store(in: &cancellables)
            }
        }
        
        var container: ContainerClass? = ContainerClass()
        
        XCTAssertFalse(ContainerClass.receivedCompletion)
        XCTAssertFalse(StoredObject.storedObjectReleased)
        container = nil
        XCTAssertTrue(StoredObject.storedObjectReleased)
        XCTAssertNil(container)
        
        // In this case the cancellables is deinited after the CurrentValueRelay,
        // so completion will be called. Since the relay was completed, cancel will
        // not be called.
        XCTAssertTrue(ContainerClass.receivedCompletion)
        XCTAssertFalse(ContainerClass.receivedCancel)
    }
    
    func testBothStoredObjectsAreDeallocatedWhenRelayAndWithLatestFromOperatorAreDeallocatedAndDeclaredBeforeCancellables() {
        final class ContainerClass {
            static var receivedCompletion = false
            static var receivedCancel = false
            
            // Cancellables comes after the relay. In this case, there
            // is no leak.
            let relay = CurrentValueRelay(StoredObject())
            let relay2 = CurrentValueRelay(StoredObject2())
            var cancellables: Set<AnyCancellable>? = Set<AnyCancellable>()
            
            init() {
                relay
                    .withLatestFrom(relay2)
                    .handleEvents(receiveCancel: {
                        Self.receivedCancel = true
                    })
                    .sink(
                        receiveCompletion: { _ in
                            Self.receivedCompletion = true
                        },
                        receiveValue: { _ in }
                    )
                    .store(in: &cancellables!)
            }
        }
        
        var container: ContainerClass? = ContainerClass()
        
        XCTAssertFalse(ContainerClass.receivedCompletion)
        XCTAssertFalse(StoredObject.storedObjectReleased)
        XCTAssertFalse(StoredObject2.storedObjectReleased)
        // When the leak was fixed, the stream started crashing because cancel
        // was called twice on relay. A fix for the crash was added,
        // so setting the container to nil which deallocates cancellables
        // confirms there is no crash.
        container = nil
        XCTAssertTrue(StoredObject.storedObjectReleased)
        XCTAssertTrue(StoredObject2.storedObjectReleased)
        XCTAssertNil(container)
    }
    
    func testBothStoredObjectsAreDeallocatedWhenRelayAndWithLatestFromOperatorAreDeallocatedAndDeclaredAfterCancellables() {
        final class ContainerClass {
            static var receivedCompletion = false
            static var receivedCancel = false
            
            // Cancellables comes before the relay. In this case, the objects
            // for both relays leak.
            var cancellables: Set<AnyCancellable>? = Set<AnyCancellable>()
            let relay = CurrentValueRelay(StoredObject())
            let relay2 = CurrentValueRelay(StoredObject2())
            
            init() {
                relay
                    .withLatestFrom(relay2)
                    .handleEvents(receiveCancel: {
                        Self.receivedCancel = true
                    })
                    .sink(
                        receiveCompletion: { _ in
                            Self.receivedCompletion = true
                        },
                        receiveValue: { _ in }
                    )
                    .store(in: &cancellables!)
            }
        }
        
        var container: ContainerClass? = ContainerClass()
        
        XCTAssertFalse(ContainerClass.receivedCompletion)
        XCTAssertFalse(StoredObject.storedObjectReleased)
        XCTAssertFalse(StoredObject2.storedObjectReleased)
        // When the leak was fixed, the stream started crashing because cancel
        // was called twice on relay. A fix for the crash was added,
        // so setting the container to nil which deallocates cancellables
        // confirms there is no crash.
        container = nil
        XCTAssertTrue(StoredObject.storedObjectReleased)
        XCTAssertTrue(StoredObject2.storedObjectReleased)
        XCTAssertNil(container)
    }
}
#endif
