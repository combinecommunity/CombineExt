# CombineExt

<p align="center">
<img src="https://github.com/CombineCommunity/CombineExt/raw/master/Resources/logo.png" width="45%">
<br /><br />
<a href="https://actions-badge.atrox.dev/CombineCommunity/CombineExt/goto" target="_blank" alt="Build Status" title="Build Status"><img src="https://github.com/CombineCommunity/CombineExt/workflows/CombineExt/badge.svg?branch=master" alt="Build Status" title="Build Status"></a>
<a href="https://codecov.io/gh/CombineCommunity/CombineExt" target="_blank" alt="Code Coverage for CombineExt on codecov" title="Code Coverage for CombineExt on codecov"><img src="https://codecov.io/gh/CombineCommunity/CombineExt/branch/master/graph/badge.svg" alt="Code Coverage for CombineExt on codecov" title="Code Coverage for CombineExt on codecov"/></a>
<br />
<img src="https://img.shields.io/badge/platforms-iOS%2013%20%7C%20macOS 10.15%20%7C%20tvOS%2013%20%7C%20watchOS%206-333333.svg" />
<br />
<a href="https://cocoapods.org/pods/CombineExt" target="_blank"><img src="https://img.shields.io/cocoapods/v/CombineExt.svg?1" alt="CombineExt supports CocoaPods"></a>
<a href="https://github.com/apple/swift-package-manager" target="_blank"><img src="https://img.shields.io/badge/Swift%20Package%20Manager-compatible-brightgreen.svg" alt="CombineExt supports Swift Package Manager (SPM)"></a>
<a href="https://github.com/Carthage/Carthage" target="_blank"><img src="https://img.shields.io/badge/Carthage-compatible-4BC51D.svg?style=flat" alt="CombineExt supports Carthage"></a>
</p>

CombineExt provides a collection of operators, publishers and utilities for Combine, that are not provided by Apple themselves, but are common in other Reactive Frameworks and standards.

The original inspiration for many of these additions came from my journey investigating Combine after years of RxSwift and ReactiveX usage.

All operators, utilities and helpers respect Combine's publisher contract, including backpressure.

### Operators
* [withLatestFrom](#withLatestFrom)
* [flatMapLatest](#flatMapLatest)
* [assign](#assign)
* [amb](#amb)
* [materialize](#materialize)
* [values](#values)
* [failures](#failures)
* [dematerialize](#dematerialize)
* [partition](#partition)
* [zip(with:) and Collection.zip](#ZipMany)
* [combineLatest(with:) and Collection.combineLatest](#CombineLatestMany)
* [mapMany(_:)](#MapMany)
* [setOutputType(to:)](#setOutputType)
* [removeAllDuplicates and removeAllDuplicates(by:) ](#removeAllDuplicates)

### Publishers
* [AnyPublisher.create](#AnypublisherCreate)
* [CurrentValueRelay](#CurrentValueRelay)
* [PassthroughRelay](#PassthroughRelay)

### Utilities
* [Set.store(_:)](#SetStorage)

> **Note**: This is still a relatively early version of CombineExt, with much more to be desired. I gladly accept PRs, ideas, opinions, or improvements. Thank you! :)

## Installation

### CocoaPods

Add the following line to your **Podfile**:

```rb
pod 'CombineExt'
```

### Swift Package Manager

Add the following dependency to your **Package.swift** file:

```swift
.package(url: "https://github.com/CombineCommunity/CombineExt.git", from: "1.0.0")
```

### Carthage

Carthage support is offered as a prebuilt binary.

Add the following to your **Cartfile**:

```
github "CombineCommunity/CombineExt"
```

## Operators

This section outlines some of the custom operators CombineExt provides.

### withLatestFrom

Merges two publishers into a single publisher by combining each value from `self` with the _latest_ value from the second publisher, if any.

```swift
let taps = PassthroughSubject<Void, Never>()
let values = CurrentValueSubject<String, Never>("Hello")

taps
  .withLatestFrom(values)
  .sink(receiveValue: { print("withLatestFrom: \($0)") })

taps.send()
taps.send()
values.send("World!")
taps.send()
```

#### Output:

```none
withLatestFrom: Hello
withLatestFrom: Hello
withLatestFrom: World!
```

------

### flatMapLatest

Transforms an output value into a new publisher, and flattens the stream of events from these multiple upstream publishers to appear as if they were coming from a single stream of events.

Mapping to a new publisher will cancel the subscription to the previous one, keeping only a single subscription active along with its event emissions.

**Note**: `flatMapLatest` is a combination of `map` and `switchToLatest`.

```swift
let trigger = PassthroughSubject<Void, Never>()
trigger
    .flatMapLatest { performNetworkRequest() }

trigger.send()
trigger.send() // cancels previous request
trigger.send() // cancels previous request
```

------

### assign

CombineExt provides custom overloads of `assign(to:on:)` that let you bind a publisher to multiple keypath targets simultaneously.

```swift
var label1: UILabel
var label2: UILabel
var text: UITextField

["hey", "there", "friend"]
    .publisher
    .assign(to: \.text, on: label1,
            and: \.text, on: label2,
            and: \.text, on: text)
```

------

### amb

Amb takes multiple publishers and mirrors the first one to emit an event. You can think of it as a race of publishers, where the first one to emit passes its events, while the others are ignored.

The name `amb` comes from the [Reactive Extensions operator](http://reactivex.io/documentation/operators/amb.html), also known in RxJS as `race`.

```swift
let subject1 = PassthroughSubject<Int, Never>()
let subject2 = PassthroughSubject<Int, Never>()

subject1
  .amb(subject2)
  .sink(receiveCompletion: { print("amb: completed with \($0)") },
        receiveValue: { print("amb: \($0)") })

subject2.send(3) // Since this subject emit first, it becomes the active publisher
subject1.send(1)
subject2.send(6)
subject1.send(8)
subject1.send(7)

subject1.send(completion: .finished)
// Only when subject2 finishes, amb itself finishes as well, since it's the active publisher
subject2.send(completion: .finished)
```

#### Output:

```none
amb: 3
amb: 6
amb: completed with .finished
```

### materialize

Convert any publisher to a publisher of its events. Given a `Publisher<Output, MyError>`, this operator will return a `Publisher<Event<Output, MyError>, Never>`, which means your failure will actually be a regular value, which makes error handling much simpler in many use cases.

```swift
let values = PassthroughSubject<String, MyError>()
enum MyError: Swift.Error {
  case ohNo
}

values
  .materialize()
  .sink(receiveCompletion: { print("materialized: completed with \($0)") },
        receiveValue: { print("materialized: \($0)") })

values.send("Hello")
values.send("World")
values.send("What's up?")
values.send(completion: .failure(.ohNo))
```

#### Output:

```none
materialize: .value("Hello")
materialize: .value("World")
materialize: .value("What's up?")
materialize: .failure(.ohNo)
materialize: completed with .finished
```

------

### values

Given a materialized publisher, publish only the emitted upstream values, omitting failures. Given a `Publisher<Event<String, MyError>, Never>`, this operator will return a `Publisher<String, Never>`.

**Note**: This operator only works on publishers that were materialized with the `materialize()` operator.

```swift
let values = PassthroughSubject<String, MyError>()
enum MyError: Swift.Error {
  case ohNo
}

values
  .materialize()
  .values()
  .sink(receiveValue: { print("values: \($0)") })

values.send("Hello")
values.send("World")
values.send("What's up?")
values.send(completion: .failure(.ohNo))
```

#### Output:

```none
values: "Hello"
values: "World"
values: "What's up?"
```

------

### failures

Given a materialized publisher, publish only the emitted upstream failure, omitting values. Given a `Publisher<Event<String, MyError>, Never>`, this operator will return a `Publisher<MyError, Never>`.

**Note**: This operator only works on publishers that were materialized with the `materialize()` operator.

```swift
let values = PassthroughSubject<String, MyError>()
enum MyError: Swift.Error {
  case ohNo
}

values
  .materialize()
  .failures()
  .sink(receiveValue: { print("failures: \($0)") })

values.send("Hello")
values.send("World")
values.send("What's up?")
values.send(completion: .failure(.ohNo))
```

#### Output:

```none
failure: MyError.ohNo
```

------

### dematerialize

Converts a previously-materialized publisher into its original form. Given a `Publisher<Event<String, MyError>, Never>`, this operator will return a `Publisher<String, MyError>`

**Note**: This operator only works on publishers that were materialized with the `materialize()` operator.

------

### partition

Partition a publisher's values into two separate publishers of values that match, and don't match, the provided predicate.

```swift
let source = PassthroughSubject<Int, Never>()

let (even, odd) = source.partition { $0 % 2 == 0 }

even.sink(receiveValue: { print("even: \($0)") })
odd.sink(receiveValue: { print("odd: \($0)") })

source.send(1)
source.send(2)
source.send(3)
source.send(4)
source.send(5)
```

#### Output:

```none
odd: 1
even: 2
odd: 3
even: 4
odd: 5
```

------

### ZipMany

This repo includes two overloads on Combine’s `Publisher.zip` methods (which, at the time of writing only go up to arity three).

This lets you arbitrarily zip many publishers and receive an array of inner publisher outputs back.

```swift
let first = PassthroughSubject<Int, Never>()
let second = PassthroughSubject<Int, Never>()
let third = PassthroughSubject<Int, Never>()
let fourth = PassthroughSubject<Int, Never>()

subscription = first
  .zip(with: second, third, fourth)
  .map { $0.reduce(0, +) }
  .sink(receiveValue: { print("zipped: \($0)") })

first.send(1)
second.send(2)
third.send(3)
fourth.send(4)
```

You may also use `.zip()` directly on a collection of publishers with the same output and failure types, e.g.

```swift
[first, second, third, fourth]
  .zip()
  .map { $0.reduce(0, +) }
  .sink(receiveValue: { print("zipped: \($0)") })
```

#### Output:

```none
zipped: 10
```

### CombineLatestMany

This repo includes two overloads on Combine’s `Publisher.combineLatest` methods (which, at the time of writing only go up to arity three) and an `Collection.combineLatest` constrained extension.

This lets you arbitrarily combine many publishers and receive an array of inner publisher outputs back.

```swift
let first = PassthroughSubject<Bool, Never>()
let second = PassthroughSubject<Bool, Never>()
let third = PassthroughSubject<Bool, Never>()
let fourth = PassthroughSubject<Bool, Never>()

subscription = [first, second, third, fourth]
  .combineLatest()
  .sink(receiveValue: { print("combineLatest: \($0)") })

first.send(true)
second.send(true)
third.send(true)
fourth.send(true)

first.send(false)
```

#### Output:

```none
combineLatest: [true, true, true, true]
combineLatest: [false, true, true, true]
```

### MapMany

Projects each element of a publisher collection into a new publisher collection form.

```swift
let intArrayPublisher = PassthroughSubject<[Int], Never>()
    
intArrayPublisher
  .mapMany(String.init)
  .sink(receiveValue: { print($0) })
    
intArrayPublisher.send([10, 2, 2, 4, 3, 8])
```

#### Output:

```none
["10", "2", "2", "4", "3", "8"]
```

### setOutputType

`Publisher.setOutputType(to:)` is an analog to [`.setFailureType(to:)`](https://developer.apple.com/documentation/combine/publisher/3204753-setfailuretype) for when `Output` is constrained to `Never`. This is especially helpful when chaining operators after an [`.ignoreOutput()`](https://developer.apple.com/documentation/combine/publisher/3204714-ignoreoutput) call.

### removeAllDuplicates

`Publisher.removeAllDuplicates` and `.removeAllDuplicates(by:)` are stricter forms of Apple’s [`Publisher.removeDuplicates`](https://developer.apple.com/documentation/combine/publisher/3204745-removeduplicates) and [`.removeDuplicates(by:)`](https://developer.apple.com/documentation/combine/publisher/3204746-removeduplicates)—the operators de-duplicate across _all_ previous value events, instead of pairwise.

If your `Output` doesn‘t conform to `Hashable` or `Equatable`, you may instead use the comparator-based version of this operator to decide whether two elements are equal.

```swift
subscription = [1, 1, 2, 1, 3, 3, 4].publisher
  .removeAllDuplicates()
  .sink(receiveValue: { print("removeAllDuplicates: \($0)") })
```

```none
removeAllDuplicates: 1
removeAllDuplicates: 2
removeAllDuplicates: 3
removeAllDuplicates: 4
```

## Publishers

This section outlines some of the custom Combine publishers CombineExt provides

### AnyPublisher.create

A publisher which accepts a factory closure to which you can dynamically push value or completion events.

This lets you easily create custom publishers to wrap any non-publisher asynchronous work, while still respecting the downstream consumer's backpressure demand.

```swift
AnyPublisher<String, MyError>.create { subscriber in
  // Values
  subscriber(.value("Hello"))
  subscriber(.value("World!"))
  
  // Complete with error
  subscriber(.failure(MyError.someError))
  
  // Or, complete successfully
  subscriber(.finished)
}
```

You can also use an `AnyPublisher` initializer with the same signature:

```swift
AnyPublisher<String, MyError> { subscriber in 
    /// ...
}
```

------

### CurrentValueRelay

A `CurrentValueRelay` is identical to a `CurrentValueSubject` with two main differences:

* It only accepts values, but not completion events, which means it cannot fail.
* It only publishes a `.finished` event upon deallocation.

```swift
let relay = CurrentValueRelay<String>("well...")

relay.sink(receiveValue: { print($0) }) // replays current value, e.g. "well..."

relay.accept("values")
relay.accept("only")
relay.accept("provide")
relay.accept("great")
relay.accept("guarantees")
```

#### Output:

```none
well...
values
only
provide
great
guarantees
```

------

### PassthroughRelay

A `PassthroughRelay` is identical to a `PassthroughSubject` with two main differences:

* It only accepts values, but not completion events, which means it cannot fail.
* It only publishes a `.finished` event upon deallocation.

```swift
let relay = PassthroughRelay<String>()
relay.accept("well...")

relay.sink(receiveValue: { print($0) }) // does not replay past value(s)

relay.accept("values")
relay.accept("only")
relay.accept("provide")
relay.accept("great")
relay.accept("guarantees")
```

#### Output:

```none
values
only
provide
great
guarantees
```

## Utilities

### SetStorage

Two `Element == AnyCancellable`-constrained overloads of `Set.store(_:)` are provided for easier storage of cancellables into `Set`s.

The variadic version can save repeated `AnyCancellable.store(in:)` calls, e.g.

```swift
firstPublisher
    .sink( /* … */ )
    .store(in: &subscriptions)

secondPublisher
    .sink( /* … */ )
    .store(in: &subscriptions)

thirdPublisher
    .sink( /* … */ )
    .store(in: &subscriptions)
 ```

can be rewritten as

```swift
subscriptions.store(
    firstPublisher
        .sink( /* … */ ),
    secondPublisher
        .sink( /* … */ ),
    thirdPublisher
        .sink( /* … */ )
)
```

and for the `Sequence` variant:

```swift
let firstBatchOfCancellables = […]

/* … */

let secondBatchOfCancellables = […]

/* … */

let thirdBatchOfCancellables = […]

subscriptions.store(
    firstBatchOfCancellables +
    secondBatchOfCancellables +
    thirdBatchOfCancellables
)
```

## License

MIT, of course ;-) See the [LICENSE](LICENSE) file. 

The Apple logo and the Combine framework are property of Apple Inc.
