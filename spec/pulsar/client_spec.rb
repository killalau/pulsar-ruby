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

  def write_connected(socket)
    connected = Pulsar::Proto::BaseCommand.new(
      type: :CONNECTED,
      connected: Pulsar::Proto::CommandConnected.new(server_version: "fake-broker", protocol_version: 21)
    )
    socket.write(Pulsar::Internal::FrameCodec.encode_command(connected))
  end

  def write_lookup_response(socket, lookup_command, port)
    lookup_response = Pulsar::Proto::BaseCommand.new(
      type: :LOOKUP_RESPONSE,
      lookupTopicResponse: Pulsar::Proto::CommandLookupTopicResponse.new(
        request_id: lookup_command.lookupTopic.request_id,
        response: :Connect,
        brokerServiceUrl: "pulsar://127.0.0.1:#{port}"
      )
    )
    socket.write(Pulsar::Internal::FrameCodec.encode_command(lookup_response))
  end

  def write_producer_success(socket, producer_command)
    producer_success = Pulsar::Proto::BaseCommand.new(
      type: :PRODUCER_SUCCESS,
      producer_success: Pulsar::Proto::CommandProducerSuccess.new(
        request_id: producer_command.producer.request_id,
        producer_name: "ruby-producer",
        schema_version: "".b
      )
    )
    socket.write(Pulsar::Internal::FrameCodec.encode_command(producer_success))
  end

  def write_send_receipt(socket, send_frame)
    receipt = Pulsar::Proto::BaseCommand.new(
      type: :SEND_RECEIPT,
      send_receipt: Pulsar::Proto::CommandSendReceipt.new(
        producer_id: send_frame.command["send"].producer_id,
        sequence_id: send_frame.command["send"].sequence_id,
        message_id: Pulsar::Proto::MessageIdData.new(ledgerId: 10, entryId: 20, partition: -1, batch_index: -1)
      )
    )
    socket.write(Pulsar::Internal::FrameCodec.encode_command(receipt))
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

      lookup_command = Pulsar::Internal::FrameCodec.decode_frame(read_frame(socket)).command
      lookup_response = Pulsar::Proto::BaseCommand.new(
        type: :LOOKUP_RESPONSE,
        lookupTopicResponse: Pulsar::Proto::CommandLookupTopicResponse.new(
          request_id: lookup_command.lookupTopic.request_id,
          response: :Connect,
          brokerServiceUrl: "pulsar://127.0.0.1:#{port}"
        )
      )
      socket.write(Pulsar::Internal::FrameCodec.encode_command(lookup_response))

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

      close_command = Pulsar::Internal::FrameCodec.decode_frame(read_frame(socket)).command
      close_success = Pulsar::Proto::BaseCommand.new(
        type: :SUCCESS,
        success: Pulsar::Proto::CommandSuccess.new(request_id: close_command.close_producer.request_id)
      )
      socket.write(Pulsar::Internal::FrameCodec.encode_command(close_success))
      socket.read
    end
    client = described_class.new("pulsar://127.0.0.1:#{port}", operation_timeout: 1, connection_timeout: 1)

    producer = client.producer(topic: "persistent://public/default/test")
    message_id = producer.send("hello", timeout: 1)

    expect(message_id).to eq(Pulsar::MessageId.new(ledger_id: 10, entry_id: 20))

    client.close
    server_thread.join
  end

  it "reattaches an existing producer on the next send after connection loss" do
    messages = []
    server = TCPServer.new("127.0.0.1", 0)
    port = server.addr[1]
    server_thread = Thread.new do
      first_socket = server.accept
      read_frame(first_socket)
      write_connected(first_socket)
      lookup_command = Pulsar::Internal::FrameCodec.decode_frame(read_frame(first_socket)).command
      write_lookup_response(first_socket, lookup_command, port)
      producer_command = Pulsar::Internal::FrameCodec.decode_frame(read_frame(first_socket)).command
      write_producer_success(first_socket, producer_command)
      first_socket.close

      second_socket = server.accept
      read_frame(second_socket)
      write_connected(second_socket)
      producer_command = Pulsar::Internal::FrameCodec.decode_frame(read_frame(second_socket)).command
      write_producer_success(second_socket, producer_command)
      send_frame = Pulsar::Internal::FrameCodec.decode_frame(read_frame(second_socket))
      messages << Pulsar::Internal::FrameCodec.decode_message_data(send_frame.headers_and_payload).payload
      write_send_receipt(second_socket, send_frame)

      close_command = Pulsar::Internal::FrameCodec.decode_frame(read_frame(second_socket)).command
      close_success = Pulsar::Proto::BaseCommand.new(
        type: :SUCCESS,
        success: Pulsar::Proto::CommandSuccess.new(request_id: close_command.close_producer.request_id)
      )
      second_socket.write(Pulsar::Internal::FrameCodec.encode_command(close_success))
      second_socket.read
    ensure
      first_socket&.close
      second_socket&.close
      server.close
    end
    client = described_class.new("pulsar://127.0.0.1:#{port}", operation_timeout: 1, connection_timeout: 1)

    producer = client.producer(topic: "persistent://public/default/test")
    Timeout.timeout(1) { sleep 0.001 while client.instance_variable_get(:@connection).connected? }
    message_id = producer.send("after-reconnect", timeout: 1)

    expect(message_id).to eq(Pulsar::MessageId.new(ledger_id: 10, entry_id: 20))
    expect(messages).to eq(["after-reconnect"])

    client.close
    server_thread.join
  end

  it "creates a real consumer, receives a message, and acks it" do
    ack_command = nil
    port, server_thread = with_fake_broker do |socket|
      read_frame(socket)
      connected = Pulsar::Proto::BaseCommand.new(
        type: :CONNECTED,
        connected: Pulsar::Proto::CommandConnected.new(server_version: "fake-broker", protocol_version: 21)
      )
      socket.write(Pulsar::Internal::FrameCodec.encode_command(connected))

      lookup_command = Pulsar::Internal::FrameCodec.decode_frame(read_frame(socket)).command
      lookup_response = Pulsar::Proto::BaseCommand.new(
        type: :LOOKUP_RESPONSE,
        lookupTopicResponse: Pulsar::Proto::CommandLookupTopicResponse.new(
          request_id: lookup_command.lookupTopic.request_id,
          response: :Connect,
          brokerServiceUrl: "pulsar://127.0.0.1:#{port}"
        )
      )
      socket.write(Pulsar::Internal::FrameCodec.encode_command(lookup_response))

      subscribe_command = Pulsar::Internal::FrameCodec.decode_frame(read_frame(socket)).command
      success = Pulsar::Proto::BaseCommand.new(
        type: :SUCCESS,
        success: Pulsar::Proto::CommandSuccess.new(request_id: subscribe_command.subscribe.request_id)
      )
      socket.write(Pulsar::Internal::FrameCodec.encode_command(success))

      flow_command = Pulsar::Internal::FrameCodec.decode_frame(read_frame(socket)).command
      expect(flow_command.type).to eq(:FLOW)

      message_command = Pulsar::Proto::BaseCommand.new(
        type: :MESSAGE,
        message: Pulsar::Proto::CommandMessage.new(
          consumer_id: subscribe_command.subscribe.consumer_id,
          message_id: Pulsar::Proto::MessageIdData.new(ledgerId: 30, entryId: 40, partition: -1, batch_index: -1)
        )
      )
      metadata = Pulsar::Proto::MessageMetadata.new(
        producer_name: "fake-producer",
        sequence_id: 1,
        publish_time: 123
      )
      socket.write(Pulsar::Internal::FrameCodec.encode_message(message_command, metadata, "hello-consumer"))

      replenishment_flow_command = Pulsar::Internal::FrameCodec.decode_frame(read_frame(socket)).command
      expect(replenishment_flow_command.type).to eq(:FLOW)
      expect(replenishment_flow_command.flow.messagePermits).to eq(1)

      ack_command = Pulsar::Internal::FrameCodec.decode_frame(read_frame(socket)).command

      close_command = Pulsar::Internal::FrameCodec.decode_frame(read_frame(socket)).command
      close_success = Pulsar::Proto::BaseCommand.new(
        type: :SUCCESS,
        success: Pulsar::Proto::CommandSuccess.new(request_id: close_command.close_consumer.request_id)
      )
      socket.write(Pulsar::Internal::FrameCodec.encode_command(close_success))
      socket.read
    end
    client = described_class.new("pulsar://127.0.0.1:#{port}", operation_timeout: 1, connection_timeout: 1)

    consumer = client.consumer(topic: "persistent://public/default/test", subscription: "ruby-sub")
    message = consumer.receive(timeout: 1)
    consumer.ack(message)

    expect(message.payload).to eq("hello-consumer")
    expect(message.message_id).to eq(Pulsar::MessageId.new(ledger_id: 30, entry_id: 40))

    client.close
    server_thread.join
    expect(ack_command.type).to eq(:ACK)
  end
end
