# frozen_string_literal: true

RSpec.describe Pulsar::Client do
  it "stores normalized client configuration" do
    client = described_class.new("pulsar://localhost:6650")

    expect(client.service_url).to eq("pulsar://localhost:6650")
    expect(client.operation_timeout).to eq(30)
    expect(client.connection_timeout).to eq(10)
    expect(client).not_to be_closed
  end

  it "closes idempotently" do
    client = described_class.new("pulsar://localhost:6650")

    expect(client.close).to be_nil
    expect(client.close).to be_nil
    expect(client).to be_closed
  end

  it "closes clients created through block form" do
    yielded = nil

    described_class.open("pulsar://localhost:6650") do |client|
      yielded = client
      expect(client).not_to be_closed
    end

    expect(yielded).to be_closed
  end

  it "rejects unsupported service URL schemes" do
    expect { described_class.new("http://localhost:6650") }
      .to raise_error(Pulsar::ConfigurationError, /pulsar:\/\//)
  end

  it "creates producer and consumer shells" do
    client = described_class.new("pulsar://localhost:6650")

    producer = client.producer(topic: "persistent://public/default/test")
    consumer = client.consumer(topic: "persistent://public/default/test", subscription: "ruby-sub")

    expect(producer.topic).to eq("persistent://public/default/test")
    expect(consumer.topic).to eq("persistent://public/default/test")
    expect(consumer.subscription).to eq("ruby-sub")
  end

  it "closes owned producer and consumer shells" do
    client = described_class.new("pulsar://localhost:6650")
    producer = client.producer(topic: "persistent://public/default/test")
    consumer = client.consumer(topic: "persistent://public/default/test", subscription: "ruby-sub")

    client.close

    expect(producer).to be_closed
    expect(consumer).to be_closed
  end

  it "rejects new resources after close" do
    client = described_class.new("pulsar://localhost:6650")
    client.close

    expect { client.producer(topic: "persistent://public/default/test") }
      .to raise_error(Pulsar::ClosedError)
  end
end
