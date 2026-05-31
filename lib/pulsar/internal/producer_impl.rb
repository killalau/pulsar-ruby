# frozen_string_literal: true

module Pulsar
  module Internal
    class ProducerImpl
      attr_reader :topic, :producer_id, :producer_name

      def self.create(connection:, topic:, producer_id:, operation_timeout:)
        request_id = connection.next_request_id
        command = CommandFactory.producer(topic: topic, producer_id: producer_id, request_id: request_id)
        response = connection.request(command, timeout: operation_timeout)

        unless response.type == :PRODUCER_SUCCESS
          raise BrokerError, "producer creation failed: #{response.type}"
        end

        new(
          connection: connection,
          topic: topic,
          producer_id: producer_id,
          producer_name: response.producer_success.producer_name,
          operation_timeout: operation_timeout
        )
      end

      def initialize(connection:, topic:, producer_id:, producer_name:, operation_timeout:)
        @connection = connection
        @topic = topic
        @producer_id = producer_id
        @producer_name = producer_name
        @operation_timeout = operation_timeout
        @sequence_id = -1
        @mutex = Mutex.new
      end

      def send(payload, properties: {}, key: nil, event_time: nil, timeout: nil)
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
        response = @connection.send_message(command, metadata, String(payload).b, timeout: timeout || @operation_timeout)

        unless response.type == :SEND_RECEIPT
          raise BrokerError, "send failed: #{response.type}"
        end

        message_id_from(response.send_receipt.message_id)
      end

      def close
        nil
      end

      private

      def next_sequence_id
        @mutex.synchronize do
          @sequence_id += 1
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
