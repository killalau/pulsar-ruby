# frozen_string_literal: true

RSpec.describe 'producer and consumer shells' do
  it 'raises an explicit unsupported feature error for broker operations' do
    producer = Pulsar::Producer.new(topic: 'persistent://public/default/test')
    consumer = Pulsar::Consumer.new(topic: 'persistent://public/default/test', subscription: 'ruby-sub')

    expect { producer.send('hello') }.to raise_error(Pulsar::UnsupportedFeatureError)
    expect { consumer.receive }.to raise_error(Pulsar::UnsupportedFeatureError)
    expect { consumer.ack(Pulsar::MessageId.new(ledger_id: 1, entry_id: 2)) }
      .to raise_error(Pulsar::UnsupportedFeatureError)
  end

  it 'raises closed errors after close' do
    producer = Pulsar::Producer.new(topic: 'persistent://public/default/test')
    consumer = Pulsar::Consumer.new(topic: 'persistent://public/default/test', subscription: 'ruby-sub')

    producer.close
    consumer.close

    expect { producer.send('hello') }.to raise_error(Pulsar::ClosedError)
    expect { consumer.receive }.to raise_error(Pulsar::ClosedError)
  end
end
