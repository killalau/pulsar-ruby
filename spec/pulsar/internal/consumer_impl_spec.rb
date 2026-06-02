# frozen_string_literal: true

RSpec.describe Pulsar::Internal::ConsumerImpl do
  class FakeConsumerConnection
    attr_reader :requests, :writes
    attr_writer :connected

    def initialize
      @request_id = 0
      @requests = []
      @writes = []
      @connected = true
    end

    def next_request_id
      @request_id += 1
    end

    def connected?
      @connected
    end

    def request(command, timeout:)
      @requests << [command, timeout]
      request_id = if command.type == :CLOSE_CONSUMER
                     command.close_consumer.request_id
                   else
                     command.subscribe.request_id
                   end
      Pulsar::Proto::BaseCommand.new(
        type: :SUCCESS,
        success: Pulsar::Proto::CommandSuccess.new(request_id: request_id)
      )
    end

    def write_command(command)
      @writes << command
    end

    def register_consumer(_consumer_id, _consumer)
      nil
    end

    def unregister_consumer(_consumer_id)
      nil
    end
  end

  it 'subscribes, receives a message, sends flow, and acks' do
    connection = FakeConsumerConnection.new
    consumer = described_class.create(
      connection: connection,
      topic: 'persistent://public/default/test',
      subscription: 'ruby-sub',
      consumer_id: 9,
      operation_timeout: 5,
      receiver_queue_size: 10
    )
    metadata = Pulsar::Proto::MessageMetadata.new(
      producer_name: 'producer',
      sequence_id: 1,
      publish_time: 123,
      properties: [Pulsar::Proto::KeyValue.new(key: 'kind', value: 'created')]
    )
    command = Pulsar::Proto::BaseCommand.new(
      type: :MESSAGE,
      message: Pulsar::Proto::CommandMessage.new(
        consumer_id: 9,
        message_id: Pulsar::Proto::MessageIdData.new(ledgerId: 10, entryId: 20, partition: -1, batch_index: -1)
      )
    )
    frame = Pulsar::Internal::FrameCodec.encode_message(command, metadata, 'hello')
    decoded = Pulsar::Internal::FrameCodec.decode_frame(frame)

    consumer.handle_message(decoded.command.message, decoded.headers_and_payload)
    message = consumer.receive(timeout: 1)
    consumer.ack(message)

    expect(consumer.topic).to eq('persistent://public/default/test')
    expect(consumer.subscription).to eq('ruby-sub')
    expect(message.payload).to eq('hello')
    expect(message.message_id).to eq(Pulsar::MessageId.new(ledger_id: 10, entry_id: 20))
    expect(message.properties).to eq('kind' => 'created')
    expect(connection.requests.first.first.type).to eq(:SUBSCRIBE)
    expect(connection.writes.map(&:type)).to include(:FLOW, :ACK)
    expect(connection.writes.select { |command| command.type == :FLOW }.map { |command| command.flow.messagePermits })
      .to eq([10, 1])
  end

  it 'closes broker-side consumers idempotently' do
    connection = FakeConsumerConnection.new
    consumer = described_class.create(
      connection: connection,
      topic: 'persistent://public/default/test',
      subscription: 'ruby-sub',
      consumer_id: 9,
      operation_timeout: 5,
      receiver_queue_size: 10
    )

    consumer.close
    consumer.close

    close_requests = connection.requests.select { |command, _timeout| command.type == :CLOSE_CONSUMER }
    expect(close_requests.size).to eq(1)
    expect(close_requests.first.first.close_consumer.consumer_id).to eq(9)
    expect(consumer).to be_closed
  end

  it 'rejects receive and ack after close' do
    connection = FakeConsumerConnection.new
    consumer = described_class.create(
      connection: connection,
      topic: 'persistent://public/default/test',
      subscription: 'ruby-sub',
      consumer_id: 9,
      operation_timeout: 5,
      receiver_queue_size: 10
    )

    consumer.close

    message_id = Pulsar::MessageId.new(ledger_id: 1, entry_id: 2)
    expect { consumer.receive(timeout: 1) }.to raise_error(Pulsar::ClosedError)
    expect { consumer.ack(message_id) }.to raise_error(Pulsar::ClosedError)
  end

  it 'wakes blocked receive calls when closed' do
    connection = FakeConsumerConnection.new
    consumer = described_class.create(
      connection: connection,
      topic: 'persistent://public/default/test',
      subscription: 'ruby-sub',
      consumer_id: 9,
      operation_timeout: 5,
      receiver_queue_size: 10
    )
    error = nil
    pending = Thread.new do
      consumer.receive(timeout: 5)
    rescue Pulsar::Error => e
      error = e
    end

    sleep 0.001 until pending.status == 'sleep'
    consumer.close
    pending.join

    expect(error).to be_a(Pulsar::ClosedError)
  end

  it 'reattaches to a replacement connection before acking' do
    first_connection = FakeConsumerConnection.new
    second_connection = FakeConsumerConnection.new
    connections = [first_connection, second_connection]
    consumer = described_class.create(
      connection_provider: -> { connections.first },
      topic: 'persistent://public/default/test',
      subscription: 'ruby-sub',
      consumer_id: 9,
      operation_timeout: 5,
      receiver_queue_size: 10
    )
    first_connection.connected = false
    connections.shift
    message_id = Pulsar::MessageId.new(ledger_id: 1, entry_id: 2)

    consumer.ack(message_id)

    expect(second_connection.requests.map { |command, _timeout| command.type }).to eq([:SUBSCRIBE])
    expect(second_connection.writes.map(&:type)).to eq(%i[FLOW ACK])
  end
end
