# frozen_string_literal: true

module Pulsar
  # Comparable Pulsar message identifier.
  class MessageId
    include Comparable

    attr_reader :ledger_id, :entry_id, :partition_index, :batch_index

    def initialize(ledger_id:, entry_id:, partition_index: -1, batch_index: -1)
      @ledger_id = Integer(ledger_id)
      @entry_id = Integer(entry_id)
      @partition_index = Integer(partition_index)
      @batch_index = Integer(batch_index)
      freeze
    end

    def <=>(other)
      return nil unless other.is_a?(MessageId)

      to_a <=> other.to_a
    end

    def eql?(other)
      other.is_a?(MessageId) && to_a == other.to_a
    end

    def hash
      to_a.hash
    end

    def inspect
      "#<#{self.class.name} ledger_id=#{ledger_id} entry_id=#{entry_id} " \
        "partition_index=#{partition_index} batch_index=#{batch_index}>"
    end

    protected

    def to_a
      [ledger_id, entry_id, partition_index, batch_index]
    end
  end
end
