# frozen_string_literal: true

RSpec.describe Pulsar::Message do
  it 'stores immutable payload and metadata' do
    message_id = Pulsar::MessageId.new(ledger_id: 1, entry_id: 2)
    message = described_class.new(
      payload: 'hello',
      message_id: message_id,
      properties: { key: :value },
      topic: 'persistent://public/default/test'
    )

    expect(message.payload).to eq('hello')
    expect(message.payload.encoding).to eq(Encoding::ASCII_8BIT)
    expect(message.message_id).to eq(message_id)
    expect(message.properties).to eq('key' => 'value')
    expect(message.topic).to eq('persistent://public/default/test')
    expect(message).to be_frozen
  end

  it 'requires a message id' do
    expect { described_class.new(payload: 'hello', message_id: Object.new) }
      .to raise_error(ArgumentError, /message_id/)
  end
end
