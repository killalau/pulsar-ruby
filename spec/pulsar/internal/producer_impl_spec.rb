# frozen_string_literal: true

RSpec.describe Pulsar::Internal::ProducerImpl do
  class FakeProducerConnection
    attr_reader :requests, :messages

    def initialize
      @request_id = 0
      @requests = []
      @messages = []
    end

    def next_request_id
      @request_id += 1
    end

    def request(command, timeout:)
      @requests << [command, timeout]
      Pulsar::Proto::BaseCommand.new(
        type: :PRODUCER_SUCCESS,
        producer_success: Pulsar::Proto::CommandProducerSuccess.new(
          request_id: command.producer.request_id,
          producer_name: "ruby-producer",
          schema_version: "".b
        )
      )
    end

    def send_message(command, metadata, payload, timeout:)
      @messages << [command, metadata, payload, timeout]
      Pulsar::Proto::BaseCommand.new(
        type: :SEND_RECEIPT,
        send_receipt: Pulsar::Proto::CommandSendReceipt.new(
          producer_id: command["send"].producer_id,
          sequence_id: command["send"].sequence_id,
          message_id: Pulsar::Proto::MessageIdData.new(ledgerId: 10, entryId: 20, partition: -1, batch_index: -1)
        )
      )
    end
  end

  it "creates a broker-side producer and sends one unbatched message" do
    connection = FakeProducerConnection.new
    producer = described_class.create(
      connection: connection,
      topic: "persistent://public/default/test",
      producer_id: 7,
      operation_timeout: 5
    )

    message_id = producer.send("hello", properties: { "kind" => "test" }, key: "k", event_time: 123, timeout: 2)

    expect(producer.topic).to eq("persistent://public/default/test")
    expect(producer.producer_name).to eq("ruby-producer")
    expect(connection.requests.first.first.type).to eq(:PRODUCER)
    expect(connection.messages.first.first.type).to eq(:SEND)
    expect(connection.messages.first[1].properties.first.key).to eq("kind")
    expect(connection.messages.first[2]).to eq("hello")
    expect(message_id).to eq(Pulsar::MessageId.new(ledger_id: 10, entry_id: 20))
  end
end
