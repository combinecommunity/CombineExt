Pod::Spec.new do |s|
    s.name         = "CombineExt"
    s.version      = "1.5.1"
    s.summary      = "Combine operators and helpers not provided by Apple, and inspired by other Reactive Frameworks"
    s.description  = <<-DESC
      A collection of operators for Combine adding capabilities and utilities not provided by Apple,
      but common ones found and known from other Reactive Frameworks
                     DESC
    s.homepage     = "https://github.com/CombineCommunity/CombineExt"
    s.license      = { :type => "MIT", :file => "LICENSE" }
    s.authors      = { "Combine Community" => "https://github.com/CombineCommunity", "Shai Mishali" => "freak4pc@gmail.com" }
  
    s.ios.deployment_target = '10.0'
    s.osx.deployment_target = '10.12'
    s.watchos.deployment_target = '3.0'
    s.tvos.deployment_target = '10.0'
  
    s.source       = { :git => "https://github.com/CombineCommunity/CombineExt.git", :tag => s.version }
    s.source_files = 'Sources/**/*.swift'
    s.frameworks   = ['Combine']
    s.swift_version = '5.5'
end
