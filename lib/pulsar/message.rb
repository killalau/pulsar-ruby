# frozen_string_literal: true

module Pulsar
  class Message
    attr_reader :payload, :message_id, :properties, :key, :topic, :publish_time, :event_time

    def initialize(payload:, message_id:, properties: {}, key: nil, topic: nil, publish_time: nil, event_time: nil)
      raise ArgumentError, 'message_id must be a Pulsar::MessageId' unless message_id.is_a?(MessageId)

      @payload = String(payload).b.freeze
      @message_id = message_id
      @properties = properties.transform_keys(&:to_s).transform_values(&:to_s).freeze
      @key = key
      @topic = topic
      @publish_time = publish_time
      @event_time = event_time
      freeze
    end
  end
end
