//
//  AllSatisfyTests.swift
//  CombineExt
//
//  Created by Vitaly Sender on 3/8/21.
//

#if !os(watchOS)
import XCTest
import Combine

@available(OSX 10.15, iOS 13.0, tvOS 13.0, watchOS 6.0, *)
final class AllSatisfyTests: XCTestCase {
  var subscription: AnyCancellable!
  
  func testAllSatisfyWithEmptyArray() {
    let source = PassthroughSubject<[Int], Never>()
    var result = false
    
    subscription = source
      .allSatisfy { $0.isMultiple(of: 2) }
      .sink { result = $0 }
    
    XCTAssertFalse(result)
  }
  
  func testAllSatisfyWithSingleElement() {
    let source = PassthroughSubject<[Int], Never>()
    var result = false
    
    subscription = source
      .allSatisfy { $0.isMultiple(of: 2) }
      .sink { result = $0 }
    
    source.send([2])
    
    XCTAssertTrue(result)
  }
  
  func testAllSatisfyWithMultipleElementsFailing() {
    let source = PassthroughSubject<[Int], Never>()
    var result = false
    
    subscription = source
      .allSatisfy { $0.isMultiple(of: 2) }
      .sink { result = $0 }
    
    source.send([1, 4])
    
    XCTAssertFalse(result)
  }
  
  func testAllSatisfyWithMultipleElementsSucceeding() {
    let source = PassthroughSubject<[Int], Never>()
    var result = false
    
    subscription = source
      .allSatisfy { $0.isMultiple(of: 2) }
      .sink { result = $0 }
    
    source.send([2, 4, 10])
    
    XCTAssertTrue(result)
  }
}
#endif
