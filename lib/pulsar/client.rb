# frozen_string_literal: true

require "set"
require "uri"

module Pulsar
  class Client
    DEFAULT_OPERATION_TIMEOUT = 30
    DEFAULT_CONNECTION_TIMEOUT = 10

    attr_reader :service_url, :operation_timeout, :connection_timeout, :logger

    def self.open(service_url, **options)
      client = new(service_url, **options)
      return client unless block_given?

      begin
        yield client
      ensure
        client.close
      end
    end

    def initialize(service_url, operation_timeout: DEFAULT_OPERATION_TIMEOUT,
                   connection_timeout: DEFAULT_CONNECTION_TIMEOUT, logger: nil, **options)
      validate_unsupported_options!(options)

      @service_url = normalize_service_url(service_url)
      @operation_timeout = operation_timeout
      @connection_timeout = connection_timeout
      @logger = logger
      @producers = Set.new
      @consumers = Set.new
      @closed = false
    end

    def producer(topic:, **_options)
      ensure_open!

      Producer.new(topic: topic).tap { |producer| @producers.add(producer) }
    end

    def consumer(topic:, subscription:, **_options)
      ensure_open!

      Consumer.new(topic: topic, subscription: subscription).tap { |consumer| @consumers.add(consumer) }
    end

    def close
      return if closed?

      @closed = true
      close_all(@producers)
      close_all(@consumers)
      nil
    end

    def closed?
      @closed
    end

    private

    def close_all(resources)
      resources.each(&:close)
      resources.clear
    end

    def ensure_open!
      raise ClosedError, "client is closed" if closed?
    end

    def normalize_service_url(service_url)
      uri = URI(String(service_url))
      raise ConfigurationError, "service URL must use pulsar:// scheme" unless uri.scheme == "pulsar"
      raise ConfigurationError, "service URL must include a host" if uri.host.nil? || uri.host.empty?

      uri.to_s
    rescue URI::InvalidURIError => e
      raise ConfigurationError, "invalid service URL: #{e.message}"
    end

    def validate_unsupported_options!(options)
      unsupported = options.keys & %i[authentication tls]
      return if unsupported.empty?

      raise ConfigurationError, "unsupported MVP option(s): #{unsupported.join(", ")}"
    end
  end
end
