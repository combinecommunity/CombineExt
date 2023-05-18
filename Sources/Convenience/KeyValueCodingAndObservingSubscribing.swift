//
//  KeyValueCodingAndObservingSubscribing.swift
//  CombineExt
//
//  Created by Andrea Altea on 18/05/23.
//

#if canImport(Combine)
import Foundation
import Combine

@available(OSX 10.15, iOS 13.0, tvOS 13.0, watchOS 6.0, *)
protocol KeyValueCodingAndObservingSubscribing: AnyObject where Self : ObjectiveC.NSObject { }

@available(OSX 10.15, iOS 13.0, tvOS 13.0, watchOS 6.0, *)
extension KeyValueCodingAndObservingSubscribing {

    /// A method to subscribe a KeyValuePath to a Publisher.
    func subscribe<P: Publisher, Value>(_ keyPath: ReferenceWritableKeyPath<Self, Value>, to publisher: P) -> AnyCancellable where P.Output == Value, P.Failure == Never {
        subscribe(keyPath, to: publisher, on: RunLoop.main)
    }

    /// A method to subscribe a KeyValuePath to a Publisher.
    func subscribe<P: Publisher, S: Scheduler, Value>(_ keyPath: ReferenceWritableKeyPath<Self, Value>, to publisher: P, on scheduler: S) -> AnyCancellable where P.Output == Value, P.Failure == Never {
        
        publisher
            .receive(on: scheduler)
            .sink { [weak self] value in
                self?[keyPath: keyPath] = value
            }
    }
}

@available(OSX 10.15, iOS 13.0, tvOS 13.0, watchOS 6.0, *)
extension NSObject: KeyValueCodingAndObservingSubscribing { }

#endif
