# frozen_string_literal: true

require 'socket'

RSpec.describe Pulsar::Internal::TcpTransport do
  def with_tcp_server
    server = TCPServer.new('127.0.0.1', 0)
    port = server.addr[1]
    thread = Thread.new do
      client = server.accept
      yield client
    ensure
      client&.close
      server.close
    end

    [port, thread]
  end

  it 'connects, writes bytes, and reads exact byte counts' do
    port, server_thread = with_tcp_server do |socket|
      expect(socket.read(4)).to eq('ping')
      socket.write('pong')
    end

    transport = described_class.connect(host: '127.0.0.1', port: port, connection_timeout: 1)
    transport.write('ping')

    expect(transport.read_exact(4, timeout: 1)).to eq('pong')

    transport.close
    server_thread.join
  end

  it 'closes idempotently and rejects operations after close' do
    port, server_thread = with_tcp_server { |socket| socket.read }
    transport = described_class.connect(host: '127.0.0.1', port: port, connection_timeout: 1)

    expect(transport.close).to be_nil
    expect(transport.close).to be_nil
    expect(transport).to be_closed
    expect { transport.write('ping') }.to raise_error(Pulsar::ClosedError)
    expect { transport.read_exact(1, timeout: 0.01) }.to raise_error(Pulsar::ClosedError)

    server_thread.join
  end

  it 'raises connection errors when the socket closes before enough bytes arrive' do
    port, server_thread = with_tcp_server do |socket|
      socket.write('ab')
    end
    transport = described_class.connect(host: '127.0.0.1', port: port, connection_timeout: 1)

    expect { transport.read_exact(4, timeout: 1) }
      .to raise_error(Pulsar::ConnectionError, /socket closed/)

    transport.close
    server_thread.join
  end
end
