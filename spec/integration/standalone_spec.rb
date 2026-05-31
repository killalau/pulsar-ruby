# frozen_string_literal: true

RSpec.describe "Pulsar standalone integration" do
  before do
    skip "set PULSAR_INTEGRATION=1 to run broker integration specs" unless ENV["PULSAR_INTEGRATION"] == "1"
  end

  it "produces, receives, and acknowledges one message" do
    topic = "persistent://public/default/ruby-integration-#{Time.now.to_i}-#{rand(1000)}"

    Pulsar::Client.open("pulsar://127.0.0.1:6650", operation_timeout: 5, connection_timeout: 5) do |client|
      producer = client.producer(topic: topic)
      consumer = client.consumer(topic: topic, subscription: "ruby-sub")

      message_id = producer.send("hello-pulsar", timeout: 5)
      message = consumer.receive(timeout: 5)
      consumer.ack(message)

      expect(message_id).to be_a(Pulsar::MessageId)
      expect(message.payload).to eq("hello-pulsar")
      expect(message.message_id).to eq(message_id)
    end
  end
end
