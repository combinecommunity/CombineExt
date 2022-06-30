//
//  AllSatisfy.swift
//  CombineExt
//
//  Created by Vitaly Sender on 2/8/21.
//  Copyright © 2021 Combine Community. All rights reserved.
//

#if canImport(Combine)
import Combine

@available(OSX 10.15, iOS 13.0, tvOS 13.0, watchOS 6.0, *)
public extension Publisher where Output: Collection {
  /// Returns a Boolean value indicating whether every element of a publisher `Collection` satisfies a given predicate.
  ///
  /// Example usages:
  ///
  ///    ```
  ///    let intArrayPublisher = PassthroughSubject<[Int], Never>()
  ///
  ///    intArrayPublisher
  ///      .allSatisfy { $0.isMultiple(of: 2) }
  ///      .sink { print("All multiples of 2? \($0)") }
  ///
  ///    intArrayPublisher.send([2, 4, 6, 8, 10])
  ///    intArrayPublisher.send([2, 4, 6, 9, 10])
  ///
  ///    // Output
  ///    All multiples of 2? true
  ///    All multiples of 2? false
  ///    ```
  ///
  ///    ```
  ///    let names = [Just("John"), Just("Jane"), Just("Jim"), Just("Jill"), Just("Joan")]
  ///
  ///    names
  ///      .combineLatest()
  ///      .allSatisfy { $0.count <= 4 }
  ///      .sink { print("All short names? \($0)") }
  ///
  ///    // Output
  ///    All short names? true
  ///    ```
  ///
  /// - parameter predicate: A closure that takes an element of the sequence as its argument and returns a Boolean value that indicates whether the passed element satisfies a condition.
  ///
  /// - returns: A publisher that represents whether all elements in the original publisher `Collection` satisfy `predicate`.
  ///
  func allSatisfy(_ predicate: @escaping (Output.Element) -> Bool) -> AnyPublisher<Bool, Failure> {
    map { $0.allSatisfy { predicate($0) } }
      .eraseToAnyPublisher()
  }
}
#endif
