# frozen_string_literal: true

require_relative "lib/pulsar/version"

Gem::Specification.new do |spec|
  spec.name = "pulsar-ruby-client"
  spec.version = Pulsar::VERSION
  spec.authors = ["Franky Lau"]
  spec.email = []

  spec.summary = "Pure Ruby Apache Pulsar client"
  spec.description = "A pure Ruby client for Apache Pulsar."
  spec.homepage = "https://github.com/frankylau/pulsar-ruby-client"
  spec.license = "Apache-2.0"
  spec.required_ruby_version = ">= 3.0"

  spec.metadata["homepage_uri"] = spec.homepage
  spec.metadata["source_code_uri"] = spec.homepage

  spec.files = Dir["lib/**/*.rb", "README.md", "LICENSE*"]
  spec.require_paths = ["lib"]

  spec.add_development_dependency "rake", "~> 13.0"
  spec.add_development_dependency "rspec", "~> 3.13"
end
