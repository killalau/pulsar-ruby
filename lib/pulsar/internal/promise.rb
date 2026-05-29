# frozen_string_literal: true

module Pulsar
  module Internal
    class Promise
      def initialize
        @mutex = Mutex.new
        @condition = ConditionVariable.new
        @completed = false
        @value = nil
        @error = nil
      end

      def fulfill(value)
        complete(value, nil)
      end

      def reject(error)
        complete(nil, error)
      end

      def wait(timeout:)
        @mutex.synchronize do
          @condition.wait(@mutex, timeout) unless @completed
          raise TimeoutError, "operation timed out" unless @completed
          raise @error if @error

          @value
        end
      end

      def completed?
        @mutex.synchronize { @completed }
      end

      private

      def complete(value, error)
        @mutex.synchronize do
          return if @completed

          @completed = true
          @value = value
          @error = error
          @condition.broadcast
        end

        nil
      end
    end
  end
end
