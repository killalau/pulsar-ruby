# frozen_string_literal: true

require "socket"

RSpec.describe Pulsar::Internal::Connection do
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

  it "sends a connect command and accepts a connected response" do
    received_connect = nil
    port, server_thread = with_fake_broker do |socket|
      frame = read_frame(socket)
      received_connect = Pulsar::Internal::FrameCodec.decode_frame(frame).command
      connected = Pulsar::Proto::BaseCommand.new(
        type: :CONNECTED,
        connected: Pulsar::Proto::CommandConnected.new(
          server_version: "fake-broker",
          protocol_version: 21,
          max_message_size: 5_242_880
        )
      )
      socket.write(Pulsar::Internal::FrameCodec.encode_command(connected))
    end

    connection = described_class.connect(
      host: "127.0.0.1",
      port: port,
      connection_timeout: 1,
      operation_timeout: 1,
      client_version: "pulsar-ruby-test"
    )

    expect(connection).to be_connected
    expect(connection.server_version).to eq("fake-broker")
    expect(connection.protocol_version).to eq(21)
    expect(connection.max_message_size).to eq(5_242_880)
    expect(received_connect.type).to eq(:CONNECT)
    expect(received_connect.connect.client_version).to eq("pulsar-ruby-test")
    expect(received_connect.connect.protocol_version).to eq(21)

    connection.close
    server_thread.join
  end

  it "raises protocol errors for non-connected handshake responses" do
    port, server_thread = with_fake_broker do |socket|
      read_frame(socket)
      error = Pulsar::Proto::BaseCommand.new(
        type: :ERROR,
        error: Pulsar::Proto::CommandError.new(
          request_id: 0,
          error: :UnknownError,
          message: "nope"
        )
      )
      socket.write(Pulsar::Internal::FrameCodec.encode_command(error))
    end

    expect do
      described_class.connect(
        host: "127.0.0.1",
        port: port,
        connection_timeout: 1,
        operation_timeout: 1,
        client_version: "pulsar-ruby-test"
      )
    end.to raise_error(Pulsar::ProtocolError, /expected CONNECTED/)

    server_thread.join
  end

  it "allocates increasing request ids" do
    port, server_thread = with_fake_broker do |socket|
      read_frame(socket)
      connected = Pulsar::Proto::BaseCommand.new(
        type: :CONNECTED,
        connected: Pulsar::Proto::CommandConnected.new(server_version: "fake-broker", protocol_version: 21)
      )
      socket.write(Pulsar::Internal::FrameCodec.encode_command(connected))
      socket.read
    end
    connection = described_class.connect(
      host: "127.0.0.1",
      port: port,
      connection_timeout: 1,
      operation_timeout: 1,
      client_version: "pulsar-ruby-test"
    )

    expect(connection.next_request_id).to eq(1)
    expect(connection.next_request_id).to eq(2)

    connection.close
    server_thread.join
  end

  it "sends a request command and returns the broker response" do
    port, server_thread = with_fake_broker do |socket|
      read_frame(socket)
      connected = Pulsar::Proto::BaseCommand.new(
        type: :CONNECTED,
        connected: Pulsar::Proto::CommandConnected.new(server_version: "fake-broker", protocol_version: 21)
      )
      socket.write(Pulsar::Internal::FrameCodec.encode_command(connected))

      request = Pulsar::Internal::FrameCodec.decode_frame(read_frame(socket)).command
      expect(request.type).to eq(:PRODUCER)
      expect(request.producer.request_id).to eq(7)

      response = Pulsar::Proto::BaseCommand.new(
        type: :PRODUCER_SUCCESS,
        producer_success: Pulsar::Proto::CommandProducerSuccess.new(
          request_id: 7,
          producer_name: "ruby-producer",
          schema_version: "".b
        )
      )
      socket.write(Pulsar::Internal::FrameCodec.encode_command(response))
      socket.read
    end
    connection = described_class.connect(
      host: "127.0.0.1",
      port: port,
      connection_timeout: 1,
      operation_timeout: 1,
      client_version: "pulsar-ruby-test"
    )
    command = Pulsar::Proto::BaseCommand.new(
      type: :PRODUCER,
      producer: Pulsar::Proto::CommandProducer.new(
        topic: "persistent://public/default/test",
        producer_id: 1,
        request_id: 7
      )
    )

    response = connection.request(command, timeout: 1)

    expect(response.type).to eq(:PRODUCER_SUCCESS)
    expect(response.producer_success.request_id).to eq(7)
    expect(response.producer_success.producer_name).to eq("ruby-producer")

    connection.close
    server_thread.join
  end

  it "sends message frames and returns the broker response" do
    port, server_thread = with_fake_broker do |socket|
      read_frame(socket)
      connected = Pulsar::Proto::BaseCommand.new(
        type: :CONNECTED,
        connected: Pulsar::Proto::CommandConnected.new(server_version: "fake-broker", protocol_version: 21)
      )
      socket.write(Pulsar::Internal::FrameCodec.encode_command(connected))

      decoded = Pulsar::Internal::FrameCodec.decode_frame(read_frame(socket))
      message = Pulsar::Internal::FrameCodec.decode_message_data(decoded.headers_and_payload)
      expect(decoded.command.type).to eq(:SEND)
      expect(message.payload).to eq("hello")

      receipt = Pulsar::Proto::BaseCommand.new(
        type: :SEND_RECEIPT,
        send_receipt: Pulsar::Proto::CommandSendReceipt.new(
          producer_id: 1,
          sequence_id: 0,
          message_id: Pulsar::Proto::MessageIdData.new(ledgerId: 1, entryId: 2)
        )
      )
      socket.write(Pulsar::Internal::FrameCodec.encode_command(receipt))
      socket.read
    end
    connection = described_class.connect(
      host: "127.0.0.1",
      port: port,
      connection_timeout: 1,
      operation_timeout: 1,
      client_version: "pulsar-ruby-test"
    )
    command, metadata = Pulsar::Internal::CommandFactory.send_message(
      producer_id: 1,
      sequence_id: 0,
      producer_name: "ruby-producer",
      publish_time: 1
    )

    response = connection.send_message(command, metadata, "hello", timeout: 1)

    expect(response.type).to eq(:SEND_RECEIPT)

    connection.close
    server_thread.join
  end
end
