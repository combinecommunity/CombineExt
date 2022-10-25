//
//  DelaySubscriptionTests.swift
//  CombineExt
//
//  Created by Jack Stone on 06/03/2021.
//  Copyright © 2021 Combine Community. All rights reserved.
//

#if !os(watchOS)
import XCTest
import Combine
import CombineSchedulers
import CombineExt

@available(OSX 10.15, iOS 13.0, tvOS 13.0, watchOS 6.0, *)
final class DelaySubscriptionTests: XCTestCase {

    private var subscriptions = Set<AnyCancellable>()
    private var scheduler: TestScheduler<DispatchQueue.SchedulerTimeType, DispatchQueue.SchedulerOptions>!

    override func setUp() {
        super.setUp()
        subscriptions = Set<AnyCancellable>()
        scheduler = DispatchQueue.testScheduler
    }

    // MARK: - Timespan tests
    func testDelaySubscriptionDropsElementsWhileSubscriptionIsDelayed() {

        var output = [Int]()
        let subscriptionDelaySeconds = 10_000

        let subject = CurrentValueSubject<Int, Never>(0)

        subject
            .receive(on: scheduler)
            .delaySubscription(for: .seconds(subscriptionDelaySeconds), scheduler: scheduler)
            .sink { completion in
                XCTFail()
            } receiveValue: { value in
                output.append(value)
            }
            .store(in: &subscriptions)

        subject.send(1)
        subject.send(2)
        subject.send(3)
        subject.send(4)

        XCTAssertTrue(output.isEmpty)

        scheduler.advance(by: .seconds(subscriptionDelaySeconds))
        XCTAssertEqual(output, [4])
    }

    func testDelaySubscriptionTimeSpanSecondsSimple() {

        var output = [Int]()
        let subscriptionDelaySeconds = 100_000_000
        var delayRemaining = subscriptionDelaySeconds

        Just<Int>(1)
            .receive(on: scheduler)
            .delaySubscription(for: .seconds(subscriptionDelaySeconds), scheduler: scheduler)
            .sink { value in
                output.append(value)
            }
            .store(in: &subscriptions)

        XCTAssertTrue(output.isEmpty)

        scheduler.advance(by: .seconds(50_000_000))
        delayRemaining -= 50_000_000
        XCTAssertTrue(output.isEmpty)

        scheduler.advance(by: .seconds(49_999_999))
        delayRemaining -= 49_999_999
        XCTAssertTrue(output.isEmpty)

        scheduler.advance(by: .seconds(1))
        delayRemaining -= 1

        XCTAssertEqual(delayRemaining, 0)
        XCTAssertFalse(output.isEmpty)
        XCTAssertEqual(output, [1])
    }

    func testDelaySubscriptionTimeSpanMillisecondsSimple() {

        var output = [Int]()
        let subscriptionDelayMilliseconds = 100_000_000
        var delayRemaining = subscriptionDelayMilliseconds

        Just<Int>(1)
            .receive(on: scheduler)
            .delaySubscription(for: .milliseconds(subscriptionDelayMilliseconds), scheduler: scheduler)
            .sink { value in
                output.append(value)
            }
            .store(in: &subscriptions)

        XCTAssertTrue(output.isEmpty)

        scheduler.advance(by: .milliseconds(50_000_000))
        delayRemaining -= 50_000_000
        XCTAssertTrue(output.isEmpty)

        scheduler.advance(by: .milliseconds(49_999_999))
        delayRemaining -= 49_999_999
        XCTAssertTrue(output.isEmpty)

        scheduler.advance(by: .milliseconds(1))
        delayRemaining -= 1

        XCTAssertEqual(delayRemaining, 0)
        XCTAssertFalse(output.isEmpty)
        XCTAssertEqual(output, [1])
    }

    func testDelaySubscriptionTimeSpanMicrosecondsSimple() {

        var output = [Int]()
        let subscriptionDelayMicroseconds = 100_000_000
        var delayRemaining = subscriptionDelayMicroseconds

        Just<Int>(1)
            .receive(on: scheduler)
            .delaySubscription(for: .microseconds(subscriptionDelayMicroseconds), scheduler: scheduler)
            .sink { value in
                output.append(value)
            }
            .store(in: &subscriptions)

        XCTAssertTrue(output.isEmpty)

        scheduler.advance(by: .microseconds(50_000_000))
        delayRemaining -= 50_000_000
        XCTAssertTrue(output.isEmpty)

        scheduler.advance(by: .microseconds(49_999_999))
        delayRemaining -= 49_999_999
        XCTAssertTrue(output.isEmpty)

        scheduler.advance(by: .microseconds(1))
        delayRemaining -= 1

        XCTAssertEqual(delayRemaining, 0)
        XCTAssertFalse(output.isEmpty)
        XCTAssertEqual(output, [1])
    }

    func testDelaySubscriptionTimeSpanNanosecondsSimple() {

        var output = [Int]()
        let subscriptionDelayNanoseconds = 100_000_000
        var delayRemaining = subscriptionDelayNanoseconds

        Just<Int>(1)
            .receive(on: scheduler)
            .delaySubscription(for: .nanoseconds(subscriptionDelayNanoseconds), scheduler: scheduler)
            .sink { value in
                output.append(value)
            }
            .store(in: &subscriptions)

        XCTAssertTrue(output.isEmpty)

        scheduler.advance(by: .nanoseconds(50_000_000))
        delayRemaining -= 50_000_000
        XCTAssertTrue(output.isEmpty)

        scheduler.advance(by: .nanoseconds(49_999_999))
        delayRemaining -= 49_999_999
        XCTAssertTrue(output.isEmpty)

        scheduler.advance(by: .nanoseconds(1))
        delayRemaining -= 1

        XCTAssertEqual(delayRemaining, 0)
        XCTAssertFalse(output.isEmpty)
        XCTAssertEqual(output, [1])
    }

    func testDelaySubscriptionTimeSpanZeroSimple() {

        var output = [Int]()

        Just<Int>(1)
            .receive(on: scheduler)
            .delaySubscription(for: .zero, scheduler: scheduler)
            .sink { value in
                output.append(value)
            }
            .store(in: &subscriptions)

        XCTAssertTrue(output.isEmpty)

        scheduler.advance()
        XCTAssertFalse(output.isEmpty)
        XCTAssertEqual(output, [1])
    }

    // MARK: - Value propagation tests
    func testDelaySubscriptionTimeSpanCurrentValueSubject() {

        let subscriptionDelaySeconds = 10_000
        var output = [Int]()
        var hasFinished = false

        let subject = CurrentValueSubject<Int, Never>(-1)

        subject
            .receive(on: scheduler)
            .delaySubscription(for: .seconds(subscriptionDelaySeconds), scheduler: scheduler)
            .sink { completion in
                guard case .finished = completion else {
                    XCTFail()
                    return
                }

                hasFinished = true

            } receiveValue: { value in
                output.append(value)
            }
            .store(in: &subscriptions)

        XCTAssertTrue(output.isEmpty)

        subject.send(0)

        scheduler.advance(by: .seconds(subscriptionDelaySeconds))
        XCTAssertEqual(output, [0])
        XCTAssertFalse(hasFinished)

        subject.send(1)

        scheduler.advance(by: .seconds(subscriptionDelaySeconds))
        XCTAssertEqual(output, [0, 1])
        XCTAssertFalse(hasFinished)

        subject.send(2)
        subject.send(3)

        scheduler.advance(by: .seconds(subscriptionDelaySeconds))
        XCTAssertEqual(output, [0, 1, 2, 3])
        XCTAssertFalse(hasFinished)

        subject.send(4)
        subject.send(5)
        subject.send(6)

        scheduler.advance(by: .seconds(subscriptionDelaySeconds))
        XCTAssertEqual(output, [0, 1, 2, 3, 4, 5, 6])
        XCTAssertFalse(hasFinished)

        subject.send(completion: .finished)

        scheduler.advance()
        XCTAssertTrue(hasFinished)
    }

    func testDelaySubscriptionTimeSpanPassthroughSubject() {

        let subscriptionDelaySeconds = 10_000
        var output = [Int]()
        var hasFinished = false

        let subject = PassthroughSubject<Int, Never>()

        subject
            .receive(on: scheduler)
            .delaySubscription(for: .seconds(subscriptionDelaySeconds), scheduler: scheduler)
            .sink { completion in
                guard case .finished = completion else {
                    XCTFail()
                    return
                }

                hasFinished = true

            } receiveValue: { value in
                output.append(value)
            }
            .store(in: &subscriptions)

        XCTAssertTrue(output.isEmpty)

        scheduler.advance(by: .seconds(subscriptionDelaySeconds))
        XCTAssertEqual(output, [])
        XCTAssertFalse(hasFinished)

        subject.send(1)

        scheduler.advance(by: .seconds(subscriptionDelaySeconds))
        XCTAssertEqual(output, [1])
        XCTAssertFalse(hasFinished)

        subject.send(2)
        subject.send(3)

        scheduler.advance(by: .seconds(subscriptionDelaySeconds))
        XCTAssertEqual(output, [1, 2, 3])
        XCTAssertFalse(hasFinished)

        subject.send(4)
        subject.send(5)
        subject.send(6)

        scheduler.advance(by: .seconds(subscriptionDelaySeconds))
        XCTAssertEqual(output, [1, 2, 3, 4, 5, 6])
        XCTAssertFalse(hasFinished)

        subject.send(completion: .finished)

        scheduler.advance()
        XCTAssertTrue(hasFinished)
    }

    func testDelaySubscriptionTimeSpanSequence() {

        let subscriptionDelaySeconds = 10_000
        var output = [Int]()
        var hasFinished = false

        Array(0...100).publisher
            .receive(on: scheduler)
            .delaySubscription(for: .seconds(subscriptionDelaySeconds), scheduler: scheduler)
            .sink { completion in
                guard case .finished = completion else {
                    XCTFail()
                    return
                }

                hasFinished = true

            } receiveValue: { value in
                output.append(value)
            }
            .store(in: &subscriptions)

        XCTAssertTrue(output.isEmpty)
        XCTAssertFalse(hasFinished)

        scheduler.advance(by: .seconds(subscriptionDelaySeconds))
        XCTAssertFalse(output.isEmpty)
        XCTAssertEqual(output, Array(0...100))
        XCTAssertTrue(hasFinished)
    }

    // MARK: - Error tests
    func testDelaySubscriptionTimespanErrorPropogationBeforeDelay() {

        let subscriptionDelaySeconds = 10_000
        var output = [Int]()
        var testError: Error?

        Fail(error: TestError.generic)
            .setOutputType(to: Int.self)
            .receive(on: scheduler)
            .delaySubscription(for: .seconds(subscriptionDelaySeconds), scheduler: scheduler)
            .sink { completion in
                guard case .failure(let error) = completion else {
                    XCTFail()
                    return
                }

                testError = error

            } receiveValue: { value in
                output.append(value)
            }
            .store(in: &subscriptions)

        scheduler.advance()

        // Delay is bypassed for upstream failures
        XCTAssertTrue(output.isEmpty)
        XCTAssertNotNil(testError)
    }

    func testDelaySubscriptionTimespanErrorPropogationDuringDelay() {

        let subject = PassthroughSubject<Int, TestError>()

        var output = [Int]()
        let subscriptionDelaySeconds = 10_000
        var testError: Error?

        subject
            .receive(on: scheduler)
            .delaySubscription(for: .seconds(subscriptionDelaySeconds), scheduler: scheduler)
            .sink { completion in
                guard case .failure(let error) = completion else {
                    XCTFail()
                    return
                }

                testError = error

            } receiveValue: { value in
                output.append(value)
            }
            .store(in: &subscriptions)

        subject.send(42)
        subject.send(43)
        subject.send(completion: .failure(.generic))

        scheduler.advance(by: .seconds(subscriptionDelaySeconds))
        XCTAssertTrue(output.isEmpty) // As the error was emitted while subscription was being delayed, no elements were emitted.
        XCTAssertNotNil(testError)
    }

    func testDelaySubscriptionTimespanErrorPropogationAfterDelay() {

        let subject = PassthroughSubject<Int, TestError>()

        var output = [Int]()
        let subscriptionDelaySeconds = 10_000
        var testError: Error?

        subject
            .receive(on: scheduler)
            .delaySubscription(for: .seconds(subscriptionDelaySeconds), scheduler: scheduler)
            .sink { completion in
                guard case .failure(let error) = completion else {
                    XCTFail()
                    return
                }

                testError = error

            } receiveValue: { value in
                output.append(value)
            }
            .store(in: &subscriptions)

        subject.send(42)
        subject.send(43)

        scheduler.advance(by: .seconds(subscriptionDelaySeconds))
        XCTAssertTrue(output.isEmpty)
        XCTAssertNil(testError)

        subject.send(completion: .failure(.generic))
        scheduler.advance()
        XCTAssertNotNil(testError)
    }

    // MARK: - Completion tests
    func testDelaySubscriptionTimespanCompletedDuringDelay() {

        let subject = PassthroughSubject<Int, Never>()

        var output = [Int]()
        let subscriptionDelaySeconds = 10_000
        var hasFinished = false

        subject
            .receive(on: scheduler)
            .delaySubscription(for: .seconds(subscriptionDelaySeconds), scheduler: scheduler)
            .sink { completion in
                guard case .finished = completion else {
                    XCTFail()
                    return
                }

                hasFinished = true

            } receiveValue: { value in
                output.append(value)
            }
            .store(in: &subscriptions)

        subject.send(42)
        subject.send(43)
        subject.send(completion: .finished)

        scheduler.advance(by: .seconds(subscriptionDelaySeconds))
        XCTAssertTrue(output.isEmpty) // As the sequence was completed while subscription was being delayed, no elements were emitted.
        XCTAssertTrue(hasFinished)
    }

    func testDelaySubscriptionTimespanCompletedAfterDelay() {

        let subscriptionDelaySeconds = 10_000
        var hasFinished = false

        let subject = PassthroughSubject<Int, Never>()

        subject
            .receive(on: scheduler)
            .delaySubscription(for: .seconds(subscriptionDelaySeconds), scheduler: scheduler)
            .sink { completion in
                guard case .finished = completion else {
                    XCTFail()
                    return
                }

                hasFinished = true

            } receiveValue: { _ in }
            .store(in: &subscriptions)

        subject.send(42)

        scheduler.advance(by: .seconds(subscriptionDelaySeconds))
        XCTAssertFalse(hasFinished)

        subject.send(completion: .finished)
        scheduler.advance()
        XCTAssertTrue(hasFinished)
    }

    // MARK: - Empty tests
    func testDelaySubscriptionTimespanEmpty() {

        var output = [Int]()
        let subscriptionDelaySeconds = 10_000
        var hasFinished = false

        Empty()
            .setOutputType(to: Int.self)
            .setFailureType(to: Error.self)
            .receive(on: scheduler)
            .delaySubscription(for: .seconds(subscriptionDelaySeconds), scheduler: scheduler)
            .sink { completion in
                guard case .finished = completion else {
                    XCTFail()
                    return
                }

                hasFinished = true

            } receiveValue: { value in
                output.append(value)
            }
            .store(in: &subscriptions)

        scheduler.advance()
        XCTAssertTrue(output.isEmpty)
        XCTAssertTrue(hasFinished)
    }
}

// MARK: - Test helpers
@available(OSX 10.15, iOS 13.0, tvOS 13.0, watchOS 6.0, *)
private extension DelaySubscriptionTests {

    private enum TestError: Error {
        case generic
    }
}
#endif
