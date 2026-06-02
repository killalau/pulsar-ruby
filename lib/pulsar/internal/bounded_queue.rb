# frozen_string_literal: true

module Pulsar
  module Internal
    class BoundedQueue
      def initialize(capacity:)
        raise ArgumentError, 'capacity must be positive' unless capacity.positive?

        @queue = SizedQueue.new(capacity)
        @mutex = Mutex.new
        @closed = false
      end

      def push(value, timeout: nil)
        ensure_open!

        if timeout
          push_with_timeout(value, timeout)
        else
          push_until_available(value)
        end

        nil
      end

      def pop(timeout:)
        ensure_open!

        deadline = Process.clock_gettime(Process::CLOCK_MONOTONIC) + timeout

        loop do
          ensure_open!
          return @queue.pop(true)
        rescue ThreadError
          remaining = deadline - Process.clock_gettime(Process::CLOCK_MONOTONIC)
          raise TimeoutError, 'operation timed out' if remaining <= 0

          sleep([remaining, 0.001].min)
        end
      end

      def close
        @mutex.synchronize { @closed = true }
        nil
      end

      private

      def push_with_timeout(value, timeout)
        deadline = Process.clock_gettime(Process::CLOCK_MONOTONIC) + timeout

        loop do
          ensure_open!
          return @queue.push(value, true)
        rescue ThreadError
          remaining = deadline - Process.clock_gettime(Process::CLOCK_MONOTONIC)
          raise TimeoutError, 'operation timed out' if remaining <= 0

          sleep([remaining, 0.001].min)
        end
      end

      def push_until_available(value)
        loop do
          ensure_open!
          return @queue.push(value, true)
        rescue ThreadError
          sleep 0.001
        end
      end

      def ensure_open!
        raise ClosedError, 'queue is closed' if @mutex.synchronize { @closed }
      end
    end
  end
end
