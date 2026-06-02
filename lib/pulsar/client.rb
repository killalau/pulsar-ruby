# frozen_string_literal: true

require 'set'
require 'uri'

module Pulsar
  # Entry point for creating producers and consumers against a Pulsar broker.
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
      @service_uri = URI(@service_url)
      @operation_timeout = operation_timeout
      @connection_timeout = connection_timeout
      @logger = logger
      @producers = Set.new
      @consumers = Set.new
      @closed = false
      @producer_id = 0
      @consumer_id = 0
    end

    def producer(topic:, max_pending_messages: 1000, **_options)
      ensure_open!
      lookup_topic(topic)

      impl = Internal::ProducerImpl.create(
        connection_provider: -> { connection },
        topic: topic,
        producer_id: next_producer_id,
        operation_timeout: operation_timeout,
        max_pending_messages: max_pending_messages
      )
      Producer.new(topic: topic, impl: impl).tap { |producer| @producers.add(producer) }
    end

    def consumer(topic:, subscription:, **_options)
      ensure_open!
      lookup_topic(topic)

      impl = Internal::ConsumerImpl.create(
        connection_provider: -> { connection },
        topic: topic,
        subscription: subscription,
        consumer_id: next_consumer_id,
        operation_timeout: operation_timeout,
        receiver_queue_size: 1000
      )
      Consumer.new(topic: topic, subscription: subscription, impl: impl).tap { |consumer| @consumers.add(consumer) }
    end

    def close
      return if closed?

      @closed = true
      close_all(@producers)
      close_all(@consumers)
      @connection&.close
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

    def connection
      @connection&.close if @connection && !@connection.connected?
      @connection = nil if @connection && !@connection.connected?
      @connection ||= Internal::Connection.connect(
        host: @service_uri.host,
        port: @service_uri.port || 6650,
        connection_timeout: connection_timeout,
        operation_timeout: operation_timeout,
        client_version: "pulsar-ruby/#{VERSION}"
      )
    end

    def lookup_topic(topic)
      Internal::LookupService.new(connection: connection, operation_timeout: operation_timeout).lookup(topic)
    end

    def next_producer_id
      @producer_id += 1
    end

    def next_consumer_id
      @consumer_id += 1
    end

    def ensure_open!
      raise ClosedError, 'client is closed' if closed?
    end

    def normalize_service_url(service_url)
      uri = URI(String(service_url))
      raise ConfigurationError, 'service URL must use pulsar:// scheme' unless uri.scheme == 'pulsar'
      raise ConfigurationError, 'service URL must include a host' if uri.host.nil? || uri.host.empty?

      uri.to_s
    rescue URI::InvalidURIError => e
      raise ConfigurationError, "invalid service URL: #{e.message}"
    end

    def validate_unsupported_options!(options)
      unsupported = options.keys & %i[authentication tls]
      return if unsupported.empty?

      raise ConfigurationError, "unsupported MVP option(s): #{unsupported.join(', ')}"
    end
  end
end
