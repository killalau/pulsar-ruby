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
        @state_mutex = Mutex.new
        @write_mutex = Mutex.new
        @request_id = 0
        @pending_requests = {}
        @pending_sends = {}
        @consumers = {}
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
        start_reader_thread
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
        reject_pending(ClosedError.new("connection is closed"))
        @reader_thread&.join unless Thread.current == @reader_thread
        nil
      end

      def closed?
        @closed
      end

      def next_request_id
        @state_mutex.synchronize do
          @request_id += 1
        end
      end

      def request(command, timeout: @operation_timeout)
        ensure_connected!
        promise = Promise.new
        request_id = request_id_for(command)
        add_pending_request(request_id, promise)

        begin
          write_frame(FrameCodec.encode_command(command))
          promise.wait(timeout: timeout)
        ensure
          remove_pending_request(request_id)
        end
      end

      def send_message(command, metadata, payload, timeout: @operation_timeout)
        ensure_connected!
        promise = Promise.new
        send_key = [command["send"].producer_id, command["send"].sequence_id]
        add_pending_send(send_key, promise)

        begin
          write_frame(FrameCodec.encode_message(command, metadata, payload))
          promise.wait(timeout: timeout)
        ensure
          remove_pending_send(send_key)
        end
      end

      def write_command(command)
        ensure_connected!

        write_frame(FrameCodec.encode_command(command))
      end

      def register_consumer(consumer_id, consumer)
        @state_mutex.synchronize { @consumers[consumer_id] = consumer }
        nil
      end

      def unregister_consumer(consumer_id)
        @state_mutex.synchronize { @consumers.delete(consumer_id) }
        nil
      end

      def read_frame(timeout: @operation_timeout)
        ensure_connected!

        read_decoded_frame(timeout: timeout)
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
        write_frame(FrameCodec.encode_command(command))
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

      def write_frame(frame)
        @write_mutex.synchronize { @transport.write(frame) }
      end

      def start_reader_thread
        @reader_thread = Thread.new { reader_loop }
      end

      def reader_loop
        loop do
          break if closed?

          route_frame(read_decoded_frame(timeout: @operation_timeout))
        end
      rescue ClosedError
        reject_pending(ClosedError.new("connection is closed")) unless closed?
      rescue ConnectionError => e
        if closed?
          reject_pending(ClosedError.new("connection is closed"))
        else
          fail_connection(ConnectionError.new("connection lost: #{e.message}"))
        end
      rescue Error => e
        reject_pending(e)
      end

      def route_frame(decoded)
        command = decoded.command

        case command.type
        when :MESSAGE
          consumer_for(command.message.consumer_id)&.handle_message(command.message, decoded.headers_and_payload)
        when :PING
          write_command(CommandFactory.pong)
        when :SEND_RECEIPT
          fulfill_pending_send(command.send_receipt.producer_id, command.send_receipt.sequence_id, command)
        when :SEND_ERROR
          reject_pending_send(
            command.send_error.producer_id,
            command.send_error.sequence_id,
            BrokerErrorMapper.from(command.send_error.error, command.send_error.message)
          )
        when :ERROR
          reject_pending_request(command.error.request_id, BrokerErrorMapper.from(command.error.error, command.error.message))
        else
          fulfill_pending_request(response_request_id(command), command)
        end
      end

      def add_pending_request(request_id, promise)
        @state_mutex.synchronize { @pending_requests[request_id] = promise }
      end

      def remove_pending_request(request_id)
        @state_mutex.synchronize { @pending_requests.delete(request_id) }
      end

      def fulfill_pending_request(request_id, command)
        promise = @state_mutex.synchronize { @pending_requests.delete(request_id) }
        promise&.fulfill(command)
      end

      def reject_pending_request(request_id, error)
        promise = @state_mutex.synchronize { @pending_requests.delete(request_id) }
        promise&.reject(error)
      end

      def add_pending_send(send_key, promise)
        @state_mutex.synchronize { @pending_sends[send_key] = promise }
      end

      def remove_pending_send(send_key)
        @state_mutex.synchronize { @pending_sends.delete(send_key) }
      end

      def fulfill_pending_send(producer_id, sequence_id, command)
        promise = @state_mutex.synchronize { @pending_sends.delete([producer_id, sequence_id]) }
        promise&.fulfill(command)
      end

      def reject_pending_send(producer_id, sequence_id, error)
        promise = @state_mutex.synchronize { @pending_sends.delete([producer_id, sequence_id]) }
        promise&.reject(error)
      end

      def consumer_for(consumer_id)
        @state_mutex.synchronize { @consumers[consumer_id] }
      end

      def reject_pending(error)
        requests, sends = @state_mutex.synchronize do
          [@pending_requests.values.tap { @pending_requests.clear },
           @pending_sends.values.tap { @pending_sends.clear }]
        end
        (requests + sends).each { |promise| promise.reject(error) }
      end

      def fail_connection(error)
        @state_mutex.synchronize { @connected = false }
        reject_pending(error)
      end

      def request_id_for(command)
        case command.type
        when :PRODUCER
          command.producer.request_id
        when :SUBSCRIBE
          command.subscribe.request_id
        when :LOOKUP
          command.lookupTopic.request_id
        when :CLOSE_PRODUCER
          command.close_producer.request_id
        when :CLOSE_CONSUMER
          command.close_consumer.request_id
        else
          raise ProtocolError, "command #{command.type} does not have a request id"
        end
      end

      def response_request_id(command)
        case command.type
        when :PRODUCER_SUCCESS
          command.producer_success.request_id
        when :SUCCESS
          command.success.request_id
        when :LOOKUP_RESPONSE
          command.lookupTopicResponse.request_id
        else
          raise ProtocolError, "unexpected broker command #{command.type}"
        end
      end
    end
  end
end
