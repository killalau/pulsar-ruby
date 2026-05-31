# frozen_string_literal: true

RSpec.describe Pulsar::Internal::ConsumerImpl do
  class FakeConsumerConnection
    attr_reader :requests, :writes

    def initialize
      @request_id = 0
      @requests = []
      @writes = []
    end

    def next_request_id
      @request_id += 1
    end

    def request(command, timeout:)
      @requests << [command, timeout]
      Pulsar::Proto::BaseCommand.new(
        type: :SUCCESS,
        success: Pulsar::Proto::CommandSuccess.new(request_id: command.subscribe.request_id)
      )
    end

    def write_command(command)
      @writes << command
    end
  end

  it "subscribes, receives a message, sends flow, and acks" do
    connection = FakeConsumerConnection.new
    consumer = described_class.create(
      connection: connection,
      topic: "persistent://public/default/test",
      subscription: "ruby-sub",
      consumer_id: 9,
      operation_timeout: 5,
      receiver_queue_size: 10
    )
    metadata = Pulsar::Proto::MessageMetadata.new(
      producer_name: "producer",
      sequence_id: 1,
      publish_time: 123,
      properties: [Pulsar::Proto::KeyValue.new(key: "kind", value: "created")]
    )
    command = Pulsar::Proto::BaseCommand.new(
      type: :MESSAGE,
      message: Pulsar::Proto::CommandMessage.new(
        consumer_id: 9,
        message_id: Pulsar::Proto::MessageIdData.new(ledgerId: 10, entryId: 20, partition: -1, batch_index: -1)
      )
    )
    frame = Pulsar::Internal::FrameCodec.encode_message(command, metadata, "hello")
    decoded = Pulsar::Internal::FrameCodec.decode_frame(frame)

    consumer.handle_message(decoded.command.message, decoded.headers_and_payload)
    message = consumer.receive(timeout: 1)
    consumer.ack(message)

    expect(consumer.topic).to eq("persistent://public/default/test")
    expect(consumer.subscription).to eq("ruby-sub")
    expect(message.payload).to eq("hello")
    expect(message.message_id).to eq(Pulsar::MessageId.new(ledger_id: 10, entry_id: 20))
    expect(message.properties).to eq("kind" => "created")
    expect(connection.requests.first.first.type).to eq(:SUBSCRIBE)
    expect(connection.writes.map(&:type)).to include(:FLOW, :ACK)
  end
end
