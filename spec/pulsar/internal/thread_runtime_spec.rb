# frozen_string_literal: true

RSpec.describe Pulsar::Internal::ThreadRuntime do
  it 'creates internal promises' do
    runtime = described_class.new

    promise = runtime.promise
    promise.fulfill('ok')

    expect(promise.wait(timeout: 0.1)).to eq('ok')
  end

  it 'spawns tracked background work' do
    runtime = described_class.new
    promise = runtime.promise

    thread = runtime.spawn { promise.fulfill('worked') }

    expect(thread).to be_a(Thread)
    expect(promise.wait(timeout: 0.1)).to eq('worked')
  end

  it 'shuts down tracked background work' do
    runtime = described_class.new
    thread = runtime.spawn { sleep }

    runtime.shutdown

    expect(thread).not_to be_alive
  end

  it 'creates bounded queues' do
    runtime = described_class.new
    queue = runtime.queue(capacity: 1)

    queue.push('message')

    expect(queue.pop(timeout: 0.1)).to eq('message')
  end

  it 'closes created queues during shutdown' do
    runtime = described_class.new
    queue = runtime.queue(capacity: 1)

    runtime.shutdown

    expect { queue.push('message') }.to raise_error(Pulsar::ClosedError)
  end
end
