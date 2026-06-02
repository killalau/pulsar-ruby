# frozen_string_literal: true

RSpec.describe Pulsar::Consumer do
  it 'raises an explicit unsupported feature error for broker operations' do
    consumer = described_class.new(topic: 'persistent://public/default/test', subscription: 'ruby-sub')

    expect { consumer.receive }.to raise_error(Pulsar::UnsupportedFeatureError)
    expect { consumer.ack(Pulsar::MessageId.new(ledger_id: 1, entry_id: 2)) }
      .to raise_error(Pulsar::UnsupportedFeatureError)
  end

  it 'raises closed errors after close' do
    consumer = described_class.new(topic: 'persistent://public/default/test', subscription: 'ruby-sub')

    consumer.close

    expect { consumer.receive }.to raise_error(Pulsar::ClosedError)
  end
end
