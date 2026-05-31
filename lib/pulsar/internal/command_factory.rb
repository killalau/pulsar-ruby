# frozen_string_literal: true

module Pulsar
  module Internal
    class CommandFactory
      def self.producer(topic:, producer_id:, request_id:)
        Proto::BaseCommand.new(
          type: :PRODUCER,
          producer: Proto::CommandProducer.new(
            topic: topic,
            producer_id: producer_id,
            request_id: request_id
          )
        )
      end

      def self.send_message(producer_id:, sequence_id:, producer_name:, properties: {}, key: nil,
                            event_time: nil, publish_time:)
        command = Proto::BaseCommand.new(
          type: :SEND,
          send: Proto::CommandSend.new(
            producer_id: producer_id,
            sequence_id: sequence_id
          )
        )
        metadata = Proto::MessageMetadata.new(
          producer_name: producer_name,
          sequence_id: sequence_id,
          publish_time: publish_time,
          properties: properties.map { |key_value, value| Proto::KeyValue.new(key: key_value.to_s, value: value.to_s) }
        )
        metadata.partition_key = key if key
        metadata.event_time = event_time if event_time

        [command, metadata]
      end

      def self.subscribe(topic:, subscription:, consumer_id:, request_id:, subscription_type: :Exclusive)
        Proto::BaseCommand.new(
          type: :SUBSCRIBE,
          subscribe: Proto::CommandSubscribe.new(
            topic: topic,
            subscription: subscription,
            subType: subscription_type,
            consumer_id: consumer_id,
            request_id: request_id
          )
        )
      end

      def self.flow(consumer_id:, permits:)
        Proto::BaseCommand.new(
          type: :FLOW,
          flow: Proto::CommandFlow.new(
            consumer_id: consumer_id,
            messagePermits: permits
          )
        )
      end

      def self.ack(consumer_id:, message_id:)
        Proto::BaseCommand.new(
          type: :ACK,
          ack: Proto::CommandAck.new(
            consumer_id: consumer_id,
            ack_type: :Individual,
            message_id: [
              Proto::MessageIdData.new(
                ledgerId: message_id.ledger_id,
                entryId: message_id.entry_id,
                partition: message_id.partition_index,
                batch_index: message_id.batch_index
              )
            ]
          )
        )
      end

      def self.lookup(topic:, request_id:)
        Proto::BaseCommand.new(
          type: :LOOKUP,
          lookupTopic: Proto::CommandLookupTopic.new(
            topic: topic,
            request_id: request_id
          )
        )
      end
    end
  end
end
