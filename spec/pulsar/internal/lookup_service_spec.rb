# frozen_string_literal: true

RSpec.describe Pulsar::Internal::LookupService do
  class FakeLookupConnection
    attr_reader :requests

    def initialize(response)
      @response = response
      @request_id = 0
      @requests = []
    end

    def next_request_id
      @request_id += 1
    end

    def request(command, timeout:)
      @requests << [command, timeout]
      @response
    end
  end

  it 'returns broker service URLs for successful lookup responses' do
    response = Pulsar::Proto::BaseCommand.new(
      type: :LOOKUP_RESPONSE,
      lookupTopicResponse: Pulsar::Proto::CommandLookupTopicResponse.new(
        request_id: 1,
        response: :Connect,
        brokerServiceUrl: 'pulsar://127.0.0.1:6650'
      )
    )
    connection = FakeLookupConnection.new(response)
    service = described_class.new(connection: connection, operation_timeout: 5)

    broker_url = service.lookup('persistent://public/default/test')

    expect(broker_url).to eq('pulsar://127.0.0.1:6650')
    expect(connection.requests.first.first.type).to eq(:LOOKUP)
  end

  it 'raises broker errors for failed lookup responses' do
    response = Pulsar::Proto::BaseCommand.new(
      type: :LOOKUP_RESPONSE,
      lookupTopicResponse: Pulsar::Proto::CommandLookupTopicResponse.new(
        request_id: 1,
        response: :Failed,
        error: :ServiceNotReady,
        message: 'not ready'
      )
    )
    service = described_class.new(connection: FakeLookupConnection.new(response), operation_timeout: 5)

    expect { service.lookup('persistent://public/default/test') }
      .to raise_error(Pulsar::BrokerError, /not ready/)
  end
end
