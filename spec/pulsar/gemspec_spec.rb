# frozen_string_literal: true

RSpec.describe 'gem specification' do
  it 'packages the vendored Pulsar protobuf source' do
    spec = Gem::Specification.load('pulsar-ruby.gemspec')

    expect(spec.files).to include('proto/PulsarApi.proto')
  end

  it 'keeps development dependencies in the Gemfile' do
    spec = Gem::Specification.load('pulsar-ruby.gemspec')

    expect(spec.development_dependencies).to be_empty
  end
end
