# frozen_string_literal: true

module Pulsar
  module Internal
    class LookupService
      def initialize(connection:, operation_timeout:)
        @connection = connection
        @operation_timeout = operation_timeout
      end

      def lookup(topic)
        request_id = @connection.next_request_id
        response = @connection.request(
          CommandFactory.lookup(topic: topic, request_id: request_id),
          timeout: @operation_timeout
        )

        unless response.type == :LOOKUP_RESPONSE
          raise BrokerError, "lookup failed: #{response.type}"
        end

        lookup = response.lookupTopicResponse
        raise BrokerError, "lookup failed: #{lookup.message}" unless lookup.response == :Connect

        lookup.brokerServiceUrl
      end
    end
  end
end
