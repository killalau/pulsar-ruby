# frozen_string_literal: true

module Pulsar
  module Internal
    class ConsumerImpl
      attr_reader :topic, :subscription, :consumer_id

      def self.create(connection:, topic:, subscription:, consumer_id:, operation_timeout:, receiver_queue_size:)
        request_id = connection.next_request_id
        command = CommandFactory.subscribe(
          topic: topic,
          subscription: subscription,
          consumer_id: consumer_id,
          request_id: request_id
        )
        response = connection.request(command, timeout: operation_timeout)
        raise BrokerError, "subscribe failed: #{response.type}" unless response.type == :SUCCESS

        consumer = new(
          connection: connection,
          topic: topic,
          subscription: subscription,
          consumer_id: consumer_id,
          operation_timeout: operation_timeout,
          receiver_queue_size: receiver_queue_size
        )
        connection.register_consumer(consumer_id, consumer)
        consumer.tap { |created_consumer| created_consumer.flow(receiver_queue_size) }
      end

      def initialize(connection:, topic:, subscription:, consumer_id:, operation_timeout:, receiver_queue_size:)
        @connection = connection
        @topic = topic
        @subscription = subscription
        @consumer_id = consumer_id
        @operation_timeout = operation_timeout
        @receiver_queue = BoundedQueue.new(capacity: receiver_queue_size)
        @closed = false
      end

      def handle_message(command_message, headers_and_payload)
        raise ClosedError, "consumer is closed" if closed?

        decoded = FrameCodec.decode_message_data(headers_and_payload)
        @receiver_queue.push(
          Message.new(
            payload: decoded.payload,
            message_id: message_id_from(command_message.message_id),
            properties: decoded.metadata.properties.to_h { |property| [property.key, property.value] },
            key: decoded.metadata.partition_key,
            publish_time: decoded.metadata.publish_time,
            event_time: decoded.metadata.event_time
          ),
          timeout: @operation_timeout
        )
      end

      def receive(timeout: nil)
        raise ClosedError, "consumer is closed" if closed?

        @receiver_queue.pop(timeout: timeout || @operation_timeout).tap do
          flow(1)
        end
      end

      def ack(message_or_message_id)
        raise ClosedError, "consumer is closed" if closed?

        message_id = message_or_message_id.respond_to?(:message_id) ? message_or_message_id.message_id : message_or_message_id
        @connection.write_command(CommandFactory.ack(consumer_id: consumer_id, message_id: message_id))
        nil
      end

      def close
        return nil if closed?

        request_id = @connection.next_request_id
        command = CommandFactory.close_consumer(consumer_id: consumer_id, request_id: request_id)
        response = @connection.request(command, timeout: @operation_timeout)
        raise BrokerError, "consumer close failed: #{response.type}" unless response.type == :SUCCESS

        @connection.unregister_consumer(consumer_id)
        @receiver_queue.close
        @closed = true
        nil
      end

      def closed?
        @closed
      end

      def flow(permits)
        raise ClosedError, "consumer is closed" if closed?

        @connection.write_command(CommandFactory.flow(consumer_id: consumer_id, permits: permits))
      end

      private

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
