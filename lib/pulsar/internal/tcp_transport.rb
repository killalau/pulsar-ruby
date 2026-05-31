# frozen_string_literal: true

require "socket"
require "timeout"

module Pulsar
  module Internal
    class TcpTransport
      def self.connect(host:, port:, connection_timeout:)
        socket = Socket.tcp(host, port, connect_timeout: connection_timeout)
        new(socket)
      rescue SystemCallError, SocketError, IOError => e
        raise ConnectionError, "failed to connect to #{host}:#{port}: #{e.message}"
      end

      def initialize(socket)
        @socket = socket
        @mutex = Mutex.new
        @closed = false
      end

      def write(bytes)
        ensure_open!
        @socket.write(String(bytes).b)
        nil
      rescue SystemCallError, IOError => e
        raise ConnectionError, "failed to write to socket: #{e.message}"
      end

      def read_exact(size, timeout:)
        ensure_open!

        Timeout.timeout(timeout, TimeoutError) do
          buffer = +""
          buffer.force_encoding(Encoding::BINARY)

          while buffer.bytesize < size
            chunk = @socket.read(size - buffer.bytesize)
            raise ConnectionError, "socket closed while reading" if chunk.nil?

            buffer << chunk
          end

          buffer
        end
      rescue TimeoutError
        raise TimeoutError, "operation timed out"
      rescue SystemCallError, IOError => e
        raise ConnectionError, "failed to read from socket: #{e.message}"
      end

      def close
        socket = @mutex.synchronize do
          return nil if @closed

          @closed = true
          @socket
        end

        socket.close unless socket.closed?
        nil
      end

      def closed?
        @mutex.synchronize { @closed }
      end

      private

      def ensure_open!
        raise ClosedError, "transport is closed" if closed?
      end
    end
  end
end
