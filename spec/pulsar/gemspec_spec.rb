# frozen_string_literal: true

RSpec.describe "gem specification" do
  it "packages the vendored Pulsar protobuf source" do
    spec = Gem::Specification.load("pulsar-ruby.gemspec")

    expect(spec.files).to include("proto/PulsarApi.proto")
  end

  it "includes development lint dependencies" do
    spec = Gem::Specification.load("pulsar-ruby.gemspec")

    dependency_names = spec.development_dependencies.map(&:name)
    expect(dependency_names).to include("rubocop", "rubocop-rspec")
  end
end
