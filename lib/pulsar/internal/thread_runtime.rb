# frozen_string_literal: true

module Pulsar
  module Internal
    class ThreadRuntime
      def initialize
        @mutex = Mutex.new
        @threads = []
        @queues = []
        @shutdown = false
      end

      def promise
        Promise.new
      end

      def queue(capacity:)
        raise ClosedError, "runtime is shut down" if shutdown?

        BoundedQueue.new(capacity: capacity).tap do |queue|
          @mutex.synchronize { @queues << queue }
        end
      end

      def spawn(&block)
        raise ClosedError, "runtime is shut down" if shutdown?

        thread = Thread.new(&block)
        @mutex.synchronize { @threads << thread }
        thread
      end

      def shutdown
        threads, queues = @mutex.synchronize do
          @shutdown = true
          [@threads.dup, @queues.dup].tap do
            @threads.clear
            @queues.clear
          end
        end

        queues.each(&:close)
        threads.each(&:kill)
        threads.each { |thread| thread.join(0.1) }
        nil
      end

      def shutdown?
        @mutex.synchronize { @shutdown }
      end
    end
  end
end
