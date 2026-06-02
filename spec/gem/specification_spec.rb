# frozen_string_literal: true

RSpec.describe Gem::Specification do
  it 'packages the vendored Pulsar protobuf source' do
    spec = described_class.load('pulsar-ruby.gemspec')

    expect(spec.files).to include('proto/PulsarApi.proto')
  end

  it 'keeps development dependencies in the Gemfile' do
    spec = described_class.load('pulsar-ruby.gemspec')

    expect(spec.development_dependencies).to be_empty
  end
end
