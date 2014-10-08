# coding: utf-8
lib = File.expand_path('../lib', __FILE__)
$LOAD_PATH.unshift(lib) unless $LOAD_PATH.include?(lib)
require 'quasar/version'

Gem::Specification.new do |spec|
  spec.name          = "quasar"
  spec.version       = Quasar::VERSION
  spec.authors       = ["Ed Carrel"]
  spec.email         = ["edward@carrel.org"]
  spec.summary       = %q{Painless data uploading to Redshift via S3}
  spec.description   = %q{Quasar }
  spec.homepage      = ""
  spec.license       = "MIT"

  spec.platform      = Gem::Platform.local
  spec.files         = `git ls-files -z`.split("\x0")
  spec.test_files    = spec.files.grep(%r{^(test|spec|features)/})
  spec.require_paths = ["lib"]

  spec.add_runtime_dependency 'punchout'
  spec.add_runtime_dependency 'activesupport'
  spec.add_runtime_dependency 'schlepp-aws'
  spec.add_runtime_dependency 'schlepp-sink-fs'
  spec.add_runtime_dependency 'converge'


  spec.add_development_dependency 'awesome_print'
  spec.add_development_dependency 'test-unit'
  spec.add_development_dependency 'mocha' 
  spec.add_development_dependency 'simplecov'
  spec.add_development_dependency "bundler", "~> 1.6"
  spec.add_development_dependency "rake", "~> 10.0"
end
