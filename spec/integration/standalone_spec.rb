# frozen_string_literal: true

RSpec.describe 'Pulsar standalone integration' do
  before do
    skip 'set PULSAR_INTEGRATION=1 to run broker integration specs' unless ENV['PULSAR_INTEGRATION'] == '1'
  end

  def unique_topic(name)
    "persistent://public/default/ruby-integration-#{name}-#{Time.now.to_i}-#{rand(1000)}"
  end

  it 'produces, receives, and acknowledges one message' do
    topic = unique_topic('single')

    Pulsar::Client.open('pulsar://127.0.0.1:6650', operation_timeout: 5, connection_timeout: 5) do |client|
      producer = client.producer(topic: topic)
      consumer = client.consumer(topic: topic, subscription: 'ruby-sub')

      message_id = producer.send('hello-pulsar', timeout: 5)
      message = consumer.receive(timeout: 5)
      consumer.ack(message)

      expect(message_id).to be_a(Pulsar::MessageId)
      expect(message.payload).to eq('hello-pulsar')
      expect(message.message_id).to eq(message_id)
    end
  end

  it 'produces and consumes multiple messages in order' do
    topic = unique_topic('multiple')
    payloads = Array.new(5) { |index| "message-#{index}" }

    Pulsar::Client.open('pulsar://127.0.0.1:6650', operation_timeout: 5, connection_timeout: 5) do |client|
      producer = client.producer(topic: topic)
      consumer = client.consumer(topic: topic, subscription: 'ruby-sub')

      message_ids = payloads.map { |payload| producer.send(payload, timeout: 5) }
      messages = payloads.map do
        consumer.receive(timeout: 5).tap { |message| consumer.ack(message) }
      end

      expect(messages.map(&:payload)).to eq(payloads)
      expect(messages.map(&:message_id)).to eq(message_ids)
      expect(message_ids.uniq.size).to eq(payloads.size)
    end
  end

  it 'round-trips message properties, key, and event time' do
    topic = unique_topic('metadata')
    event_time = (Time.now.to_f * 1000).to_i

    Pulsar::Client.open('pulsar://127.0.0.1:6650', operation_timeout: 5, connection_timeout: 5) do |client|
      producer = client.producer(topic: topic)
      consumer = client.consumer(topic: topic, subscription: 'ruby-sub')

      producer.send(
        'with-metadata',
        properties: { 'kind' => 'integration', 'source' => 'ruby' },
        key: 'order-1',
        event_time: event_time,
        timeout: 5
      )
      message = consumer.receive(timeout: 5)
      consumer.ack(message)

      expect(message.payload).to eq('with-metadata')
      expect(message.properties).to eq('kind' => 'integration', 'source' => 'ruby')
      expect(message.key).to eq('order-1')
      expect(message.event_time).to eq(event_time)
    end
  end

  it 'raises timeout when receiving from an empty topic' do
    topic = unique_topic('empty')

    Pulsar::Client.open('pulsar://127.0.0.1:6650', operation_timeout: 5, connection_timeout: 5) do |client|
      consumer = client.consumer(topic: topic, subscription: 'ruby-sub')

      expect { consumer.receive(timeout: 0.1) }.to raise_error(Pulsar::TimeoutError)
    end
  end

  it 'consumes messages from multiple producers on the same topic' do
    topic = unique_topic('multiple-producers')

    Pulsar::Client.open('pulsar://127.0.0.1:6650', operation_timeout: 5, connection_timeout: 5) do |client|
      producer_a = client.producer(topic: topic)
      producer_b = client.producer(topic: topic)
      consumer = client.consumer(topic: topic, subscription: 'ruby-sub')

      producer_a.send('from-a', timeout: 5)
      producer_b.send('from-b', timeout: 5)
      messages = 2.times.map do
        consumer.receive(timeout: 5).tap { |message| consumer.ack(message) }
      end

      expect(messages.map(&:payload)).to contain_exactly('from-a', 'from-b')
    end
  end

  it 'raises closed errors after closing producer and consumer' do
    topic = unique_topic('close')

    Pulsar::Client.open('pulsar://127.0.0.1:6650', operation_timeout: 5, connection_timeout: 5) do |client|
      producer = client.producer(topic: topic)
      consumer = client.consumer(topic: topic, subscription: 'ruby-sub')

      producer.close
      consumer.close

      expect { producer.send('closed', timeout: 5) }.to raise_error(Pulsar::ClosedError)
      expect { consumer.receive(timeout: 0.1) }.to raise_error(Pulsar::ClosedError)
      expect { consumer.ack(Pulsar::MessageId.new(ledger_id: 1, entry_id: 1)) }.to raise_error(Pulsar::ClosedError)
    end
  end

  it 'reattaches existing producer and consumer objects after connection replacement' do
    topic = unique_topic('reconnect')

    Pulsar::Client.open('pulsar://127.0.0.1:6650', operation_timeout: 5, connection_timeout: 5) do |client|
      producer = client.producer(topic: topic)
      consumer = client.consumer(topic: topic, subscription: 'ruby-sub')

      client.instance_variable_get(:@connection).close

      message_id = producer.send('after-reconnect', timeout: 5)
      message = consumer.receive(timeout: 5)
      consumer.ack(message)

      expect(message.payload).to eq('after-reconnect')
      expect(message.message_id).to eq(message_id)
    end
  end
end
