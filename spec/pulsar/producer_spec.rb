# frozen_string_literal: true

RSpec.describe Pulsar::Producer do
  it 'raises an explicit unsupported feature error for broker operations' do
    producer = described_class.new(topic: 'persistent://public/default/test')

    expect { producer.send('hello') }.to raise_error(Pulsar::UnsupportedFeatureError)
  end

  it 'raises closed errors after close' do
    producer = described_class.new(topic: 'persistent://public/default/test')

    producer.close

    expect { producer.send('hello') }.to raise_error(Pulsar::ClosedError)
  end
end
