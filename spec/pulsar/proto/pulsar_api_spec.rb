# frozen_string_literal: true

require "pulsar/proto/pulsar_api_pb"

RSpec.describe "Pulsar protobuf definitions" do
  it "loads generated message classes for protocol commands" do
    message_id = Pulsar::Proto::MessageIdData.new(
      ledgerId: 1,
      entryId: 2,
      partition: -1,
      batch_index: -1
    )

    command = Pulsar::Proto::BaseCommand.new(
      type: :CONNECT,
      connect: Pulsar::Proto::CommandConnect.new(
        client_version: "pulsar-ruby-test",
        protocol_version: 21
      )
    )

    expect(message_id.ledgerId).to eq(1)
    expect(command.type).to eq(:CONNECT)
    expect(command.connect.client_version).to eq("pulsar-ruby-test")
  end
end
