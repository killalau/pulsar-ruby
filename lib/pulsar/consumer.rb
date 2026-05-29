# frozen_string_literal: true

module Pulsar
  class Consumer
    attr_reader :topic, :subscription

    def initialize(topic:, subscription:, impl: nil)
      @topic = String(topic)
      @subscription = String(subscription)
      @impl = impl
      @closed = false
    end

    def receive(timeout: nil)
      ensure_open!
      raise UnsupportedFeatureError, "consumer receive is not implemented yet" unless @impl

      @impl.receive(timeout: timeout)
    end

    def ack(message_or_message_id)
      ensure_open!
      raise UnsupportedFeatureError, "consumer ack is not implemented yet" unless @impl

      @impl.ack(message_or_message_id)
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
      raise ClosedError, "consumer is closed" if closed?
    end
  end
end
