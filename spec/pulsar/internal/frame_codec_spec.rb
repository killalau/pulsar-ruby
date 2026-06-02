# frozen_string_literal: true

RSpec.describe Pulsar::Internal::FrameCodec do
  it "encodes and decodes command-only frames" do
    command = Pulsar::Proto::BaseCommand.new(type: :PING, ping: Pulsar::Proto::CommandPing.new)

    frame = described_class.encode_command(command)
    total_size = frame.byteslice(0, 4).unpack1("N")
    command_size = frame.byteslice(4, 4).unpack1("N")
    encoded_command = Pulsar::Proto::BaseCommand.encode(command)

    expect(total_size).to eq(4 + encoded_command.bytesize)
    expect(command_size).to eq(encoded_command.bytesize)
    expect(frame.byteslice(8, command_size)).to eq(encoded_command)

    decoded = described_class.decode_frame(frame)

    expect(decoded.command.type).to eq(:PING)
    expect(decoded.headers_and_payload).to eq("")
  end

  it "rejects frames shorter than the size prefix" do
    expect { described_class.decode_frame("\x00\x00".b) }
      .to raise_error(Pulsar::ProtocolError, /frame size prefix/)
  end

  it "rejects incomplete frames" do
    frame = [10].pack("N") + "abc"

    expect { described_class.decode_frame(frame) }
      .to raise_error(Pulsar::ProtocolError, /frame is incomplete/)
  end

  it "decodes trailing headers and payload bytes" do
    command = Pulsar::Proto::BaseCommand.new(type: :MESSAGE, message: Pulsar::Proto::CommandMessage.new(
      consumer_id: 1,
      message_id: Pulsar::Proto::MessageIdData.new(ledgerId: 2, entryId: 3)
    ))
    encoded_command = Pulsar::Proto::BaseCommand.encode(command)
    headers_and_payload = "metadata-and-payload".b
    frame = [4 + encoded_command.bytesize + headers_and_payload.bytesize, encoded_command.bytesize]
            .pack("NN") + encoded_command + headers_and_payload

    decoded = described_class.decode_frame(frame)

    expect(decoded.command.type).to eq(:MESSAGE)
    expect(decoded.headers_and_payload).to eq(headers_and_payload)
  end

  it "encodes and decodes message metadata plus payload frames" do
    command = Pulsar::Proto::BaseCommand.new(type: :SEND, send: Pulsar::Proto::CommandSend.new(
      producer_id: 1,
      sequence_id: 2
    ))
    metadata = Pulsar::Proto::MessageMetadata.new(
      producer_name: "ruby-producer",
      sequence_id: 2,
      publish_time: 123
    )
    payload = "hello".b

    frame = described_class.encode_message(command, metadata, payload)
    decoded = described_class.decode_frame(frame)
    message = described_class.decode_message_data(decoded.headers_and_payload)

    expect(decoded.command.type).to eq(:SEND)
    expect(message.metadata.producer_name).to eq("ruby-producer")
    expect(message.metadata.sequence_id).to eq(2)
    expect(message.payload).to eq(payload)
  end

  it "rejects message data with incomplete metadata" do
    headers_and_payload = [10].pack("N") + "short"

    expect { described_class.decode_message_data(headers_and_payload) }
      .to raise_error(Pulsar::ProtocolError, /metadata exceeds message data size/)
  end
end
