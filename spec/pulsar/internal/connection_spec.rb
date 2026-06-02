# frozen_string_literal: true

require 'socket'

RSpec.describe Pulsar::Internal::Connection do
  def read_frame(socket)
    size_prefix = socket.read(4)
    size = size_prefix.unpack1('N')
    size_prefix + socket.read(size)
  end

  def with_fake_broker
    server = TCPServer.new('127.0.0.1', 0)
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

  it 'sends a connect command and accepts a connected response' do
    received_connect = nil
    port, server_thread = with_fake_broker do |socket|
      frame = read_frame(socket)
      received_connect = Pulsar::Internal::FrameCodec.decode_frame(frame).command
      connected = Pulsar::Proto::BaseCommand.new(
        type: :CONNECTED,
        connected: Pulsar::Proto::CommandConnected.new(
          server_version: 'fake-broker',
          protocol_version: 21,
          max_message_size: 5_242_880
        )
      )
      socket.write(Pulsar::Internal::FrameCodec.encode_command(connected))
      socket.read
    end

    connection = described_class.connect(
      host: '127.0.0.1',
      port: port,
      connection_timeout: 1,
      operation_timeout: 1,
      client_version: 'pulsar-ruby-test'
    )

    expect(connection).to be_connected
    expect(connection.server_version).to eq('fake-broker')
    expect(connection.protocol_version).to eq(21)
    expect(connection.max_message_size).to eq(5_242_880)
    expect(received_connect.type).to eq(:CONNECT)
    expect(received_connect.connect.client_version).to eq('pulsar-ruby-test')
    expect(received_connect.connect.protocol_version).to eq(21)

    connection.close
    server_thread.join
  end

  it 'raises protocol errors for non-connected handshake responses' do
    port, server_thread = with_fake_broker do |socket|
      read_frame(socket)
      error = Pulsar::Proto::BaseCommand.new(
        type: :ERROR,
        error: Pulsar::Proto::CommandError.new(
          request_id: 0,
          error: :UnknownError,
          message: 'nope'
        )
      )
      socket.write(Pulsar::Internal::FrameCodec.encode_command(error))
    end

    expect do
      described_class.connect(
        host: '127.0.0.1',
        port: port,
        connection_timeout: 1,
        operation_timeout: 1,
        client_version: 'pulsar-ruby-test'
      )
    end.to raise_error(Pulsar::ProtocolError, /expected CONNECTED/)

    server_thread.join
  end

  it 'allocates increasing request ids' do
    port, server_thread = with_fake_broker do |socket|
      read_frame(socket)
      connected = Pulsar::Proto::BaseCommand.new(
        type: :CONNECTED,
        connected: Pulsar::Proto::CommandConnected.new(server_version: 'fake-broker', protocol_version: 21)
      )
      socket.write(Pulsar::Internal::FrameCodec.encode_command(connected))
      socket.read
    end
    connection = described_class.connect(
      host: '127.0.0.1',
      port: port,
      connection_timeout: 1,
      operation_timeout: 1,
      client_version: 'pulsar-ruby-test'
    )

    expect(connection.next_request_id).to eq(1)
    expect(connection.next_request_id).to eq(2)

    connection.close
    server_thread.join
  end

  it 'stops the reader thread when closed' do
    port, server_thread = with_fake_broker do |socket|
      read_frame(socket)
      connected = Pulsar::Proto::BaseCommand.new(
        type: :CONNECTED,
        connected: Pulsar::Proto::CommandConnected.new(server_version: 'fake-broker', protocol_version: 21)
      )
      socket.write(Pulsar::Internal::FrameCodec.encode_command(connected))
      socket.read
    end
    connection = described_class.connect(
      host: '127.0.0.1',
      port: port,
      connection_timeout: 1,
      operation_timeout: 1,
      client_version: 'pulsar-ruby-test'
    )

    reader_thread = connection.instance_variable_get(:@reader_thread)
    expect(reader_thread).to be_alive

    connection.close
    server_thread.join

    expect(reader_thread).not_to be_alive
  end

  it 'sends a request command and returns the broker response' do
    port, server_thread = with_fake_broker do |socket|
      read_frame(socket)
      connected = Pulsar::Proto::BaseCommand.new(
        type: :CONNECTED,
        connected: Pulsar::Proto::CommandConnected.new(server_version: 'fake-broker', protocol_version: 21)
      )
      socket.write(Pulsar::Internal::FrameCodec.encode_command(connected))

      request = Pulsar::Internal::FrameCodec.decode_frame(read_frame(socket)).command
      expect(request.type).to eq(:PRODUCER)
      expect(request.producer.request_id).to eq(7)

      response = Pulsar::Proto::BaseCommand.new(
        type: :PRODUCER_SUCCESS,
        producer_success: Pulsar::Proto::CommandProducerSuccess.new(
          request_id: 7,
          producer_name: 'ruby-producer',
          schema_version: ''.b
        )
      )
      socket.write(Pulsar::Internal::FrameCodec.encode_command(response))
      socket.read
    end
    connection = described_class.connect(
      host: '127.0.0.1',
      port: port,
      connection_timeout: 1,
      operation_timeout: 1,
      client_version: 'pulsar-ruby-test'
    )
    command = Pulsar::Proto::BaseCommand.new(
      type: :PRODUCER,
      producer: Pulsar::Proto::CommandProducer.new(
        topic: 'persistent://public/default/test',
        producer_id: 1,
        request_id: 7
      )
    )

    response = connection.request(command, timeout: 1)

    expect(response.type).to eq(:PRODUCER_SUCCESS)
    expect(response.producer_success.request_id).to eq(7)
    expect(response.producer_success.producer_name).to eq('ruby-producer')

    connection.close
    server_thread.join
  end

  it 'maps command error responses to typed errors' do
    port, server_thread = with_fake_broker do |socket|
      read_frame(socket)
      connected = Pulsar::Proto::BaseCommand.new(
        type: :CONNECTED,
        connected: Pulsar::Proto::CommandConnected.new(server_version: 'fake-broker', protocol_version: 21)
      )
      socket.write(Pulsar::Internal::FrameCodec.encode_command(connected))

      request = Pulsar::Internal::FrameCodec.decode_frame(read_frame(socket)).command
      expect(request.type).to eq(:PRODUCER)

      error = Pulsar::Proto::BaseCommand.new(
        type: :ERROR,
        error: Pulsar::Proto::CommandError.new(
          request_id: request.producer.request_id,
          error: :ProducerBusy,
          message: 'producer already exists'
        )
      )
      socket.write(Pulsar::Internal::FrameCodec.encode_command(error))
      socket.read
    end
    connection = described_class.connect(
      host: '127.0.0.1',
      port: port,
      connection_timeout: 1,
      operation_timeout: 1,
      client_version: 'pulsar-ruby-test'
    )
    command = Pulsar::Proto::BaseCommand.new(
      type: :PRODUCER,
      producer: Pulsar::Proto::CommandProducer.new(
        topic: 'persistent://public/default/test',
        producer_id: 1,
        request_id: 7
      )
    )

    expect { connection.request(command, timeout: 1) }
      .to raise_error(Pulsar::ProducerBusyError, 'producer already exists')

    connection.close
    server_thread.join
  end

  it 'marks the connection disconnected and rejects pending requests when the broker closes the socket' do
    port, server_thread = with_fake_broker do |socket|
      read_frame(socket)
      connected = Pulsar::Proto::BaseCommand.new(
        type: :CONNECTED,
        connected: Pulsar::Proto::CommandConnected.new(server_version: 'fake-broker', protocol_version: 21)
      )
      socket.write(Pulsar::Internal::FrameCodec.encode_command(connected))

      request = Pulsar::Internal::FrameCodec.decode_frame(read_frame(socket)).command
      expect(request.type).to eq(:PRODUCER)
    end
    connection = described_class.connect(
      host: '127.0.0.1',
      port: port,
      connection_timeout: 1,
      operation_timeout: 1,
      client_version: 'pulsar-ruby-test'
    )
    command = Pulsar::Proto::BaseCommand.new(
      type: :PRODUCER,
      producer: Pulsar::Proto::CommandProducer.new(
        topic: 'persistent://public/default/test',
        producer_id: 1,
        request_id: 7
      )
    )

    expect { connection.request(command, timeout: 1) }
      .to raise_error(Pulsar::ConnectionError, /connection lost/)
    expect(connection).not_to be_connected

    connection.close
    server_thread.join
  end

  it 'rejects pending requests when closed' do
    request_seen = Queue.new
    port, server_thread = with_fake_broker do |socket|
      read_frame(socket)
      connected = Pulsar::Proto::BaseCommand.new(
        type: :CONNECTED,
        connected: Pulsar::Proto::CommandConnected.new(server_version: 'fake-broker', protocol_version: 21)
      )
      socket.write(Pulsar::Internal::FrameCodec.encode_command(connected))

      request = Pulsar::Internal::FrameCodec.decode_frame(read_frame(socket)).command
      expect(request.type).to eq(:PRODUCER)
      request_seen << true
      socket.read
    end
    connection = described_class.connect(
      host: '127.0.0.1',
      port: port,
      connection_timeout: 1,
      operation_timeout: 5,
      client_version: 'pulsar-ruby-test'
    )
    command = Pulsar::Proto::BaseCommand.new(
      type: :PRODUCER,
      producer: Pulsar::Proto::CommandProducer.new(
        topic: 'persistent://public/default/test',
        producer_id: 1,
        request_id: 7
      )
    )
    error = nil
    pending = Thread.new do
      connection.request(command, timeout: 5)
    rescue Pulsar::Error => e
      error = e
    end

    request_seen.pop
    connection.close
    pending.join
    server_thread.join

    expect(error).to be_a(Pulsar::ClosedError)
  end

  it 'sends message frames and returns the broker response' do
    port, server_thread = with_fake_broker do |socket|
      read_frame(socket)
      connected = Pulsar::Proto::BaseCommand.new(
        type: :CONNECTED,
        connected: Pulsar::Proto::CommandConnected.new(server_version: 'fake-broker', protocol_version: 21)
      )
      socket.write(Pulsar::Internal::FrameCodec.encode_command(connected))

      decoded = Pulsar::Internal::FrameCodec.decode_frame(read_frame(socket))
      message = Pulsar::Internal::FrameCodec.decode_message_data(decoded.headers_and_payload)
      expect(decoded.command.type).to eq(:SEND)
      expect(message.payload).to eq('hello')

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
      host: '127.0.0.1',
      port: port,
      connection_timeout: 1,
      operation_timeout: 1,
      client_version: 'pulsar-ruby-test'
    )
    command, metadata = Pulsar::Internal::CommandFactory.send_message(
      producer_id: 1,
      sequence_id: 0,
      producer_name: 'ruby-producer',
      publish_time: 1
    )

    response = connection.send_message(command, metadata, 'hello', timeout: 1)

    expect(response.type).to eq(:SEND_RECEIPT)

    connection.close
    server_thread.join
  end

  it 'rejects pending sends when closed' do
    send_seen = Queue.new
    port, server_thread = with_fake_broker do |socket|
      read_frame(socket)
      connected = Pulsar::Proto::BaseCommand.new(
        type: :CONNECTED,
        connected: Pulsar::Proto::CommandConnected.new(server_version: 'fake-broker', protocol_version: 21)
      )
      socket.write(Pulsar::Internal::FrameCodec.encode_command(connected))

      decoded = Pulsar::Internal::FrameCodec.decode_frame(read_frame(socket))
      expect(decoded.command.type).to eq(:SEND)
      send_seen << true
      socket.read
    end
    connection = described_class.connect(
      host: '127.0.0.1',
      port: port,
      connection_timeout: 1,
      operation_timeout: 5,
      client_version: 'pulsar-ruby-test'
    )
    command, metadata = Pulsar::Internal::CommandFactory.send_message(
      producer_id: 1,
      sequence_id: 0,
      producer_name: 'ruby-producer',
      publish_time: 1
    )
    error = nil
    pending = Thread.new do
      connection.send_message(command, metadata, 'hello', timeout: 5)
    rescue Pulsar::Error => e
      error = e
    end

    send_seen.pop
    connection.close
    pending.join
    server_thread.join

    expect(error).to be_a(Pulsar::ClosedError)
  end

  it 'maps send error responses to typed errors' do
    port, server_thread = with_fake_broker do |socket|
      read_frame(socket)
      connected = Pulsar::Proto::BaseCommand.new(
        type: :CONNECTED,
        connected: Pulsar::Proto::CommandConnected.new(server_version: 'fake-broker', protocol_version: 21)
      )
      socket.write(Pulsar::Internal::FrameCodec.encode_command(connected))

      decoded = Pulsar::Internal::FrameCodec.decode_frame(read_frame(socket))
      send_command = decoded.command['send']
      error = Pulsar::Proto::BaseCommand.new(
        type: :SEND_ERROR,
        send_error: Pulsar::Proto::CommandSendError.new(
          producer_id: send_command.producer_id,
          sequence_id: send_command.sequence_id,
          error: :AuthorizationError,
          message: 'not allowed'
        )
      )
      socket.write(Pulsar::Internal::FrameCodec.encode_command(error))
      socket.read
    end
    connection = described_class.connect(
      host: '127.0.0.1',
      port: port,
      connection_timeout: 1,
      operation_timeout: 1,
      client_version: 'pulsar-ruby-test'
    )
    command, metadata = Pulsar::Internal::CommandFactory.send_message(
      producer_id: 1,
      sequence_id: 0,
      producer_name: 'ruby-producer',
      publish_time: 1
    )

    expect { connection.send_message(command, metadata, 'hello', timeout: 1) }
      .to raise_error(Pulsar::AuthorizationError, 'not allowed')

    connection.close
    server_thread.join
  end

  it 'writes command-only frames without waiting for a response' do
    written_command = nil
    port, server_thread = with_fake_broker do |socket|
      read_frame(socket)
      connected = Pulsar::Proto::BaseCommand.new(
        type: :CONNECTED,
        connected: Pulsar::Proto::CommandConnected.new(server_version: 'fake-broker', protocol_version: 21)
      )
      socket.write(Pulsar::Internal::FrameCodec.encode_command(connected))

      written_command = Pulsar::Internal::FrameCodec.decode_frame(read_frame(socket)).command
      socket.read
    end
    connection = described_class.connect(
      host: '127.0.0.1',
      port: port,
      connection_timeout: 1,
      operation_timeout: 1,
      client_version: 'pulsar-ruby-test'
    )

    connection.write_command(Pulsar::Internal::CommandFactory.flow(consumer_id: 3, permits: 10))
    connection.close
    server_thread.join

    expect(written_command.type).to eq(:FLOW)
    expect(written_command.flow.consumer_id).to eq(3)
  end

  it 'responds to broker ping frames with pong' do
    pong_command = nil
    port, server_thread = with_fake_broker do |socket|
      read_frame(socket)
      connected = Pulsar::Proto::BaseCommand.new(
        type: :CONNECTED,
        connected: Pulsar::Proto::CommandConnected.new(server_version: 'fake-broker', protocol_version: 21)
      )
      socket.write(Pulsar::Internal::FrameCodec.encode_command(connected))

      ping = Pulsar::Proto::BaseCommand.new(type: :PING, ping: Pulsar::Proto::CommandPing.new)
      socket.write(Pulsar::Internal::FrameCodec.encode_command(ping))
      pong_command = Pulsar::Internal::FrameCodec.decode_frame(read_frame(socket)).command
      socket.read
    end
    connection = described_class.connect(
      host: '127.0.0.1',
      port: port,
      connection_timeout: 1,
      operation_timeout: 1,
      client_version: 'pulsar-ruby-test'
    )

    Timeout.timeout(1) do
      sleep 0.001 until pong_command
    end

    connection.close
    server_thread.join

    expect(pong_command.type).to eq(:PONG)
  end

  it 'routes broker-pushed message frames to registered consumers' do
    consumer = Struct.new(:handled, :received_command, :received_payload) do
      def handle_message(command_message, headers_and_payload)
        decoded = Pulsar::Internal::FrameCodec.decode_message_data(headers_and_payload)
        self.received_command = command_message
        self.received_payload = decoded.payload
        self.handled = true
      end
    end.new(false, nil, nil)

    port, server_thread = with_fake_broker do |socket|
      read_frame(socket)
      connected = Pulsar::Proto::BaseCommand.new(
        type: :CONNECTED,
        connected: Pulsar::Proto::CommandConnected.new(server_version: 'fake-broker', protocol_version: 21)
      )
      socket.write(Pulsar::Internal::FrameCodec.encode_command(connected))

      flow_command = Pulsar::Internal::FrameCodec.decode_frame(read_frame(socket)).command
      expect(flow_command.type).to eq(:FLOW)

      message_command = Pulsar::Proto::BaseCommand.new(
        type: :MESSAGE,
        message: Pulsar::Proto::CommandMessage.new(
          consumer_id: 3,
          message_id: Pulsar::Proto::MessageIdData.new(ledgerId: 1, entryId: 2)
        )
      )
      metadata = Pulsar::Proto::MessageMetadata.new(
        producer_name: 'fake-producer',
        sequence_id: 1,
        publish_time: 123
      )
      socket.write(Pulsar::Internal::FrameCodec.encode_message(message_command, metadata, 'hello'))
      socket.read
    end
    connection = described_class.connect(
      host: '127.0.0.1',
      port: port,
      connection_timeout: 1,
      operation_timeout: 1,
      client_version: 'pulsar-ruby-test'
    )

    connection.register_consumer(3, consumer)
    connection.write_command(Pulsar::Internal::CommandFactory.flow(consumer_id: 3, permits: 1))

    Timeout.timeout(1) do
      sleep 0.001 until consumer.handled
    end

    expect(consumer.received_command.consumer_id).to eq(3)
    expect(consumer.received_payload).to eq('hello')

    connection.close
    server_thread.join
  end
end
