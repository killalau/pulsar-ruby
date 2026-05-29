# frozen_string_literal: true

RSpec.describe Pulsar::Internal::Promise do
  it "waits for a fulfilled value" do
    promise = described_class.new

    Thread.new { promise.fulfill("ok") }.join

    expect(promise.wait(timeout: 0.1)).to eq("ok")
  end

  it "raises a rejected error" do
    promise = described_class.new

    promise.reject(Pulsar::ConnectionError.new("closed"))

    expect { promise.wait(timeout: 0.1) }
      .to raise_error(Pulsar::ConnectionError, "closed")
  end

  it "raises timeout when no value arrives before the deadline" do
    promise = described_class.new

    expect { promise.wait(timeout: 0.001) }
      .to raise_error(Pulsar::TimeoutError, "operation timed out")

    promise.fulfill("late")

    expect(promise.wait(timeout: 0.1)).to eq("late")
  end

  it "reports completion state" do
    promise = described_class.new

    expect(promise).not_to be_completed

    promise.fulfill("done")

    expect(promise).to be_completed
  end
end
