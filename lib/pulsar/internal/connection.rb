# frozen_string_literal: true

module Pulsar
  module Internal
    class Connection
      PROTOCOL_VERSION = 21

      attr_reader :server_version, :protocol_version, :max_message_size

      def self.connect(host:, port:, connection_timeout:, operation_timeout:, client_version:)
        transport = TcpTransport.connect(host: host, port: port, connection_timeout: connection_timeout)
        new(
          transport: transport,
          operation_timeout: operation_timeout,
          client_version: client_version
        ).tap(&:connect)
      end

      def initialize(transport:, operation_timeout:, client_version:)
        @transport = transport
        @operation_timeout = operation_timeout
        @client_version = client_version
        @connected = false
        @closed = false
        @mutex = Mutex.new
        @request_id = 0
      end

      def connect
        write_connect_command
        command = read_command(timeout: @operation_timeout)
        unless command.type == :CONNECTED
          raise ProtocolError, "expected CONNECTED response, got #{command.type}"
        end

        @server_version = command.connected.server_version
        @protocol_version = command.connected.protocol_version
        @max_message_size = command.connected.max_message_size
        @connected = true
        self
      rescue Error
        close
        raise
      end

      def connected?
        @connected && !closed?
      end

      def close
        return nil if closed?

        @closed = true
        @connected = false
        @transport.close
        nil
      end

      def closed?
        @closed
      end

      def next_request_id
        @mutex.synchronize do
          @request_id += 1
        end
      end

      def request(command, timeout: @operation_timeout)
        ensure_connected!

        @mutex.synchronize do
          @transport.write(FrameCodec.encode_command(command))
          read_command(timeout: timeout)
        end
      end

      def send_message(command, metadata, payload, timeout: @operation_timeout)
        ensure_connected!

        @mutex.synchronize do
          @transport.write(FrameCodec.encode_message(command, metadata, payload))
          read_command(timeout: timeout)
        end
      end

      def write_command(command)
        ensure_connected!

        @mutex.synchronize do
          @transport.write(FrameCodec.encode_command(command))
        end
      end

      def read_frame(timeout: @operation_timeout)
        ensure_connected!

        @mutex.synchronize do
          read_decoded_frame(timeout: timeout)
        end
      end

      private

      def write_connect_command
        command = Proto::BaseCommand.new(
          type: :CONNECT,
          connect: Proto::CommandConnect.new(
            client_version: @client_version,
            protocol_version: PROTOCOL_VERSION
          )
        )
        @transport.write(FrameCodec.encode_command(command))
      end

      def read_command(timeout:)
        read_decoded_frame(timeout: timeout).command
      end

      def read_decoded_frame(timeout:)
        size_prefix = @transport.read_exact(4, timeout: timeout)
        size = size_prefix.unpack1("N")
        frame = size_prefix + @transport.read_exact(size, timeout: timeout)
        FrameCodec.decode_frame(frame)
      end

      def ensure_connected!
        raise ClosedError, "connection is closed" if closed?
        raise ConnectionError, "connection is not connected" unless connected?
      end
    end
  end
end
