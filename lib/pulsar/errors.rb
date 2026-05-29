# frozen_string_literal: true

module Pulsar
  class Error < StandardError; end

  class TimeoutError < Error; end
  class ConnectionError < Error; end
  class ClosedError < Error; end
  class AuthenticationError < Error; end
  class AuthorizationError < Error; end
  class TopicNotFoundError < Error; end
  class ProducerBusyError < Error; end
  class ConsumerBusyError < Error; end
  class BrokerError < Error; end
  class ProtocolError < Error; end
  class ConfigurationError < Error; end
  class UnsupportedFeatureError < Error; end
end
