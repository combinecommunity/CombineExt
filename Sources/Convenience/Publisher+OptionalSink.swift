//
//  Publisher+OptionalSink.swift
//  CombineExt
//
//  Created by reiley kim on 2022/11/21.
//

import Foundation
import Combine

@available(OSX 10.15, iOS 13.0, tvOS 13.0, watchOS 6.0, *)
public extension Publisher {
    func sink(receiveCompletion: ((Subscribers.Completion<Self.Failure>) -> Void)? = nil,
                     receiveValue: ((Self.Output) -> Void)? = nil) -> AnyCancellable {
        let receiveValueClosure: ((Self.Output) -> Void) = receiveValue ?? { _ in }
        let receiveComletionClosure: ((Subscribers.Completion<Self.Failure>) -> Void) = receiveCompletion ?? { _ in }

        return sink(receiveCompletion: receiveComletionClosure, receiveValue: receiveValueClosure)
    }
}
