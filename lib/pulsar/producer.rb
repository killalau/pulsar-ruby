# frozen_string_literal: true

module Pulsar
  # Public producer API for sending messages to one Pulsar topic.
  class Producer
    attr_reader :topic

    def initialize(topic:, impl: nil)
      @topic = String(topic)
      @impl = impl
      @closed = false
    end

    def send(payload, properties: {}, key: nil, event_time: nil, timeout: nil)
      ensure_open!
      raise UnsupportedFeatureError, 'producer send is not implemented yet' unless @impl

      @impl.send(payload, properties: properties, key: key, event_time: event_time, timeout: timeout)
    end

    def close
      return if closed?

      @impl&.close
      @closed = true
      nil
    end

    def closed?
      @closed
    end

    private

    def ensure_open!
      raise ClosedError, 'producer is closed' if closed?
    end
  end
end
