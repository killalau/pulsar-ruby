# frozen_string_literal: true

require_relative 'lib/pulsar/version'

Gem::Specification.new do |spec|
  spec.name = 'pulsar-ruby'
  spec.version = Pulsar::VERSION
  spec.authors = ['Franky Lau']
  spec.email = []

  spec.summary = 'Pure Ruby Apache Pulsar client'
  spec.description = 'A pure Ruby client for Apache Pulsar.'
  spec.homepage = 'https://github.com/killalau/pulsar-ruby'
  spec.license = 'Apache-2.0'
  spec.required_ruby_version = '>= 3.0'

  spec.metadata['homepage_uri'] = spec.homepage
  spec.metadata['source_code_uri'] = spec.homepage
  spec.metadata['rubygems_mfa_required'] = 'true'

  spec.files = Dir['lib/**/*.rb', 'proto/**/*.proto', 'README.md', 'LICENSE*']
  spec.require_paths = ['lib']

  spec.add_dependency 'google-protobuf', '~> 3.25'

  spec.add_development_dependency 'rake', '~> 13.0'
  spec.add_development_dependency 'rspec', '~> 3.13'
  spec.add_development_dependency 'rubocop', '~> 1.0'
  spec.add_development_dependency 'rubocop-rspec', '~> 3.0'
end
