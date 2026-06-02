# frozen_string_literal: true

RSpec.describe Pulsar::Internal::CommandFactory do
  it "builds producer creation commands" do
    command = described_class.producer(
      topic: "persistent://public/default/test",
      producer_id: 11,
      request_id: 22
    )

    expect(command.type).to eq(:PRODUCER)
    expect(command.producer.topic).to eq("persistent://public/default/test")
    expect(command.producer.producer_id).to eq(11)
    expect(command.producer.request_id).to eq(22)
  end

  it "builds unbatched send commands and message metadata" do
    command, metadata = described_class.send_message(
      producer_id: 11,
      sequence_id: 3,
      producer_name: "ruby-producer",
      properties: { "kind" => "created" },
      key: "order-1",
      event_time: 1234,
      publish_time: 5678
    )

    expect(command.type).to eq(:SEND)
    expect(command["send"].producer_id).to eq(11)
    expect(command["send"].sequence_id).to eq(3)
    expect(metadata.producer_name).to eq("ruby-producer")
    expect(metadata.sequence_id).to eq(3)
    expect(metadata.publish_time).to eq(5678)
    expect(metadata.partition_key).to eq("order-1")
    expect(metadata.event_time).to eq(1234)
    expect(metadata.properties.first.key).to eq("kind")
    expect(metadata.properties.first.value).to eq("created")
  end

  it "builds subscribe commands" do
    command = described_class.subscribe(
      topic: "persistent://public/default/test",
      subscription: "ruby-sub",
      consumer_id: 12,
      request_id: 34
    )

    expect(command.type).to eq(:SUBSCRIBE)
    expect(command.subscribe.topic).to eq("persistent://public/default/test")
    expect(command.subscribe.subscription).to eq("ruby-sub")
    expect(command.subscribe.consumer_id).to eq(12)
    expect(command.subscribe.request_id).to eq(34)
    expect(command.subscribe.subType).to eq(:Exclusive)
  end

  it "builds flow commands" do
    command = described_class.flow(consumer_id: 12, permits: 100)

    expect(command.type).to eq(:FLOW)
    expect(command.flow.consumer_id).to eq(12)
    expect(command.flow.messagePermits).to eq(100)
  end

  it "builds individual ack commands" do
    message_id = Pulsar::MessageId.new(ledger_id: 1, entry_id: 2, partition_index: -1, batch_index: -1)

    command = described_class.ack(consumer_id: 12, message_id: message_id)

    expect(command.type).to eq(:ACK)
    expect(command.ack.consumer_id).to eq(12)
    expect(command.ack.ack_type).to eq(:Individual)
    expect(command.ack.message_id.first.ledgerId).to eq(1)
    expect(command.ack.message_id.first.entryId).to eq(2)
  end

  it "builds lookup commands" do
    command = described_class.lookup(
      topic: "persistent://public/default/test",
      request_id: 99
    )

    expect(command.type).to eq(:LOOKUP)
    expect(command.lookupTopic.topic).to eq("persistent://public/default/test")
    expect(command.lookupTopic.request_id).to eq(99)
  end

  it "builds close producer commands" do
    command = described_class.close_producer(producer_id: 11, request_id: 22)

    expect(command.type).to eq(:CLOSE_PRODUCER)
    expect(command.close_producer.producer_id).to eq(11)
    expect(command.close_producer.request_id).to eq(22)
  end

  it "builds close consumer commands" do
    command = described_class.close_consumer(consumer_id: 11, request_id: 22)

    expect(command.type).to eq(:CLOSE_CONSUMER)
    expect(command.close_consumer.consumer_id).to eq(11)
    expect(command.close_consumer.request_id).to eq(22)
  end

  it "builds pong commands" do
    command = described_class.pong

    expect(command.type).to eq(:PONG)
    expect(command.pong).to be_a(Pulsar::Proto::CommandPong)
  end
end
