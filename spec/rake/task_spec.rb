# frozen_string_literal: true

require 'rake'

RSpec.describe Rake::Task do
  it 'defines proto:generate' do
    load File.expand_path('../../Rakefile', __dir__)

    expect(described_class.task_defined?('proto:generate')).to be(true)
  end

  it 'defines spec:integration' do
    load File.expand_path('../../Rakefile', __dir__)

    expect(described_class.task_defined?('spec:integration')).to be(true)
  end

  it 'defines smoke:local' do
    load File.expand_path('../../Rakefile', __dir__)

    expect(described_class.task_defined?('smoke:local')).to be(true)
  end
end
