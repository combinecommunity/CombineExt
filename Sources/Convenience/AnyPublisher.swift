//
//  AnyPublisher.swift
//  CombineExt
//
//  Created by Stefano Mondino on 14/04/23.
//  Copyright © 2020 Combine Community. All rights reserved.
//

#if canImport(Combine)
import Combine

@available(OSX 10.15, iOS 13.0, tvOS 13.0, watchOS 6.0, *)
public extension AnyPublisher {
    /// A wrapper around a type erased Just
    static func just(_ value: Output) -> AnyPublisher<Output, Failure> {
        Just(value)
            .setFailureType(to: Failure.self)
            .eraseToAnyPublisher()
    }
    
    /// A wrapper around a type erased Empty
    static func empty() -> AnyPublisher<Output, Failure> {
        Empty()
            .setFailureType(to: Failure.self)
            .eraseToAnyPublisher()
    }
    
    /// A wrapper around a type erased Fail
    static func fail(error: Failure) -> AnyPublisher<Output, Failure> {
        Fail(error: error).eraseToAnyPublisher()
    }
}
#endif
