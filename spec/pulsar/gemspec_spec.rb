# frozen_string_literal: true

RSpec.describe "gem specification" do
  it "packages the vendored Pulsar protobuf source" do
    spec = Gem::Specification.load("pulsar-ruby.gemspec")

    expect(spec.files).to include("proto/PulsarApi.proto")
  end
end
