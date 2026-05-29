# frozen_string_literal: true

RSpec.describe Pulsar::MessageId do
  it "is comparable by its coordinate fields" do
    first = described_class.new(ledger_id: 1, entry_id: 2)
    second = described_class.new(ledger_id: 1, entry_id: 3)

    expect(first).to be < second
  end

  it "supports equality and hashing" do
    one = described_class.new(ledger_id: 1, entry_id: 2, partition_index: 0, batch_index: -1)
    two = described_class.new(ledger_id: 1, entry_id: 2, partition_index: 0, batch_index: -1)

    expect(one).to eq(two)
    expect([one, two].uniq).to eq([one])
  end
end
