# frozen_string_literal: true

RSpec.describe Pulsar::Internal::BrokerErrorMapper do
  it "maps selected server errors to typed Ruby errors" do
    expect(described_class.from(:AuthenticationError, "bad auth")).to be_a(Pulsar::AuthenticationError)
    expect(described_class.from(:AuthorizationError, "denied")).to be_a(Pulsar::AuthorizationError)
    expect(described_class.from(:TopicNotFound, "missing")).to be_a(Pulsar::TopicNotFoundError)
    expect(described_class.from(:ProducerBusy, "busy")).to be_a(Pulsar::ProducerBusyError)
    expect(described_class.from(:ProducerFenced, "fenced")).to be_a(Pulsar::ProducerBusyError)
    expect(described_class.from(:ConsumerBusy, "busy")).to be_a(Pulsar::ConsumerBusyError)
  end

  it "preserves broker error code and message for generic broker errors" do
    error = described_class.from(:MetadataError, "metadata failed")

    expect(error).to be_a(Pulsar::BrokerError)
    expect(error.message).to eq("MetadataError: metadata failed")
  end
end
