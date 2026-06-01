# frozen_string_literal: true

module Pulsar
  module Internal
    class BrokerErrorMapper
      ERROR_CLASSES = {
        AuthenticationError: AuthenticationError,
        AuthorizationError: AuthorizationError,
        TopicNotFound: TopicNotFoundError,
        ProducerBusy: ProducerBusyError,
        ProducerFenced: ProducerBusyError,
        ConsumerBusy: ConsumerBusyError
      }.freeze

      def self.from(server_error, message)
        error_class = ERROR_CLASSES.fetch(server_error, BrokerError)
        text = error_class == BrokerError ? "#{server_error}: #{message}" : message
        error_class.new(text)
      end
    end
  end
end
