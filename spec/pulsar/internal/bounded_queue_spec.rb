# frozen_string_literal: true

RSpec.describe Pulsar::Internal::BoundedQueue do
  it "raises timeout when pushing to a full queue" do
    queue = described_class.new(capacity: 1)
    queue.push("first")

    expect { queue.push("second", timeout: 0.001) }
      .to raise_error(Pulsar::TimeoutError, "operation timed out")
  end

  it "raises closed errors after close" do
    queue = described_class.new(capacity: 1)

    queue.close

    expect { queue.push("message") }.to raise_error(Pulsar::ClosedError)
    expect { queue.pop(timeout: 0.001) }.to raise_error(Pulsar::ClosedError)
  end

  it "wakes a blocked push when closed" do
    queue = described_class.new(capacity: 1)
    queue.push("first")
    error = nil
    thread = Thread.new do
      queue.push("second")
    rescue StandardError => e
      error = e
    end

    sleep 0.01
    queue.close
    thread.join(0.1)

    expect(error).to be_a(Pulsar::ClosedError)
  ensure
    thread&.kill if thread&.alive?
  end
end
