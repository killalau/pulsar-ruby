# frozen_string_literal: true

module Pulsar
  module Internal
    class ProducerImpl
      attr_reader :topic, :producer_id, :producer_name

      def self.create(topic:, producer_id:, operation_timeout:, max_pending_messages: 1000,
                      connection: nil, connection_provider: nil)
        connection_provider ||= -> { connection }
        new(
          connection_provider: connection_provider,
          topic: topic,
          producer_id: producer_id,
          operation_timeout: operation_timeout,
          max_pending_messages: max_pending_messages
        ).tap(&:attach)
      end

      def initialize(connection_provider:, topic:, producer_id:, operation_timeout:, max_pending_messages:)
        @connection_provider = connection_provider
        @connection = nil
        @topic = topic
        @producer_id = producer_id
        @producer_name = nil
        @operation_timeout = operation_timeout
        @max_pending_messages = max_pending_messages
        @pending_sends = 0
        @pending_condition = ConditionVariable.new
        @sequence_id = -1
        @mutex = Mutex.new
        @closed = false
      end

      def send(payload, properties: {}, key: nil, event_time: nil, timeout: nil)
        raise ClosedError, 'producer is closed' if closed?

        send_timeout = timeout || @operation_timeout
        acquire_pending_send(timeout: send_timeout)

        begin
          attach unless attached?
          sequence_id = next_sequence_id
          command, metadata = CommandFactory.send_message(
            producer_id: producer_id,
            sequence_id: sequence_id,
            producer_name: producer_name,
            properties: properties,
            key: key,
            event_time: event_time,
            publish_time: current_time_millis
          )
          response = @connection.send_message(command, metadata, String(payload).b, timeout: send_timeout)

          unless response.type == :SEND_RECEIPT
            raise BrokerError, "send failed: #{response.type}"
          end

          message_id_from(response.send_receipt.message_id)
        ensure
          release_pending_send
        end
      end

      def close
        return nil if closed?

        if attached?
          request_id = @connection.next_request_id
          command = CommandFactory.close_producer(producer_id: producer_id, request_id: request_id)
          response = @connection.request(command, timeout: @operation_timeout)
          raise BrokerError, "producer close failed: #{response.type}" unless response.type == :SUCCESS
        end

        @closed = true
        nil
      end

      def closed?
        @closed
      end

      private

      def attach
        @connection = @connection_provider.call
        request_id = @connection.next_request_id
        command = CommandFactory.producer(topic: topic, producer_id: producer_id, request_id: request_id)
        response = @connection.request(command, timeout: @operation_timeout)

        unless response.type == :PRODUCER_SUCCESS
          raise BrokerError, "producer creation failed: #{response.type}"
        end

        @producer_name = response.producer_success.producer_name
        nil
      end

      def attached?
        @connection&.connected?
      end

      def next_sequence_id
        @mutex.synchronize do
          @sequence_id += 1
        end
      end

      def acquire_pending_send(timeout:)
        deadline = Process.clock_gettime(Process::CLOCK_MONOTONIC) + timeout
        @mutex.synchronize do
          loop do
            raise ClosedError, 'producer is closed' if @closed

            if @pending_sends < @max_pending_messages
              @pending_sends += 1
              return nil
            end

            remaining = deadline - Process.clock_gettime(Process::CLOCK_MONOTONIC)
            raise TimeoutError, 'operation timed out' if remaining <= 0

            @pending_condition.wait(@mutex, remaining)
          end
        end
      end

      def release_pending_send
        @mutex.synchronize do
          @pending_sends -= 1
          @pending_condition.broadcast
        end
      end

      def current_time_millis
        (Time.now.to_f * 1000).to_i
      end

      def message_id_from(data)
        MessageId.new(
          ledger_id: data.ledgerId,
          entry_id: data.entryId,
          partition_index: data.partition,
          batch_index: data.batch_index
        )
      end
    end
  end
end
