# frozen_string_literal: true

RSpec.describe Pulsar::Client do
  def read_frame(socket)
    size_prefix = socket.read(4)
    size = size_prefix.unpack1("N")
    size_prefix + socket.read(size)
  end

  def with_fake_broker
    server = TCPServer.new("127.0.0.1", 0)
    port = server.addr[1]
    thread = Thread.new do
      socket = server.accept
      yield socket
    ensure
      socket&.close
      server.close
    end

    [port, thread]
  end

  it "stores normalized client configuration" do
    client = described_class.new("pulsar://localhost:6650")

    expect(client.service_url).to eq("pulsar://localhost:6650")
    expect(client.operation_timeout).to eq(30)
    expect(client.connection_timeout).to eq(10)
    expect(client).not_to be_closed
  end

  it "closes idempotently" do
    client = described_class.new("pulsar://localhost:6650")

    expect(client.close).to be_nil
    expect(client.close).to be_nil
    expect(client).to be_closed
  end

  it "closes clients created through block form" do
    yielded = nil

    described_class.open("pulsar://localhost:6650") do |client|
      yielded = client
      expect(client).not_to be_closed
    end

    expect(yielded).to be_closed
  end

  it "rejects unsupported service URL schemes" do
    expect { described_class.new("http://localhost:6650") }
      .to raise_error(Pulsar::ConfigurationError, /pulsar:\/\//)
  end

  it "creates consumer shells" do
    client = described_class.new("pulsar://localhost:6650")

    consumer = client.consumer(topic: "persistent://public/default/test", subscription: "ruby-sub")

    expect(consumer.topic).to eq("persistent://public/default/test")
    expect(consumer.subscription).to eq("ruby-sub")
  end

  it "closes owned consumer shells" do
    client = described_class.new("pulsar://localhost:6650")
    consumer = client.consumer(topic: "persistent://public/default/test", subscription: "ruby-sub")

    client.close

    expect(consumer).to be_closed
  end

  it "rejects new resources after close" do
    client = described_class.new("pulsar://localhost:6650")
    client.close

    expect { client.producer(topic: "persistent://public/default/test") }
      .to raise_error(Pulsar::ClosedError)
  end

  it "creates a real producer and sends a message through the broker connection" do
    port, server_thread = with_fake_broker do |socket|
      read_frame(socket)
      connected = Pulsar::Proto::BaseCommand.new(
        type: :CONNECTED,
        connected: Pulsar::Proto::CommandConnected.new(server_version: "fake-broker", protocol_version: 21)
      )
      socket.write(Pulsar::Internal::FrameCodec.encode_command(connected))

      producer_command = Pulsar::Internal::FrameCodec.decode_frame(read_frame(socket)).command
      producer_success = Pulsar::Proto::BaseCommand.new(
        type: :PRODUCER_SUCCESS,
        producer_success: Pulsar::Proto::CommandProducerSuccess.new(
          request_id: producer_command.producer.request_id,
          producer_name: "ruby-producer",
          schema_version: "".b
        )
      )
      socket.write(Pulsar::Internal::FrameCodec.encode_command(producer_success))

      send_frame = Pulsar::Internal::FrameCodec.decode_frame(read_frame(socket))
      message = Pulsar::Internal::FrameCodec.decode_message_data(send_frame.headers_and_payload)
      expect(send_frame.command.type).to eq(:SEND)
      expect(message.payload).to eq("hello")

      receipt = Pulsar::Proto::BaseCommand.new(
        type: :SEND_RECEIPT,
        send_receipt: Pulsar::Proto::CommandSendReceipt.new(
          producer_id: send_frame.command["send"].producer_id,
          sequence_id: send_frame.command["send"].sequence_id,
          message_id: Pulsar::Proto::MessageIdData.new(ledgerId: 10, entryId: 20, partition: -1, batch_index: -1)
        )
      )
      socket.write(Pulsar::Internal::FrameCodec.encode_command(receipt))
      socket.read
    end
    client = described_class.new("pulsar://127.0.0.1:#{port}", operation_timeout: 1, connection_timeout: 1)

    producer = client.producer(topic: "persistent://public/default/test")
    message_id = producer.send("hello", timeout: 1)

    expect(message_id).to eq(Pulsar::MessageId.new(ledger_id: 10, entry_id: 20))

    client.close
    server_thread.join
  end
end
