# frozen_string_literal: true

module Pulsar
  module Internal
    # Encodes and decodes Pulsar binary protocol frames.
    class FrameCodec
      DecodedFrame = Struct.new(:command, :headers_and_payload, keyword_init: true)
      DecodedMessageData = Struct.new(:metadata, :payload, keyword_init: true)

      def self.encode_command(command)
        encoded_command = Proto::BaseCommand.encode(command)
        [4 + encoded_command.bytesize, encoded_command.bytesize].pack('NN') + encoded_command
      end

      def self.encode_message(command, metadata, payload)
        encoded_command = Proto::BaseCommand.encode(command)
        encoded_metadata = Proto::MessageMetadata.encode(metadata)
        payload = String(payload).b
        total_size = 4 + encoded_command.bytesize + 4 + encoded_metadata.bytesize + payload.bytesize

        [total_size, encoded_command.bytesize].pack('NN') +
          encoded_command + [encoded_metadata.bytesize].pack('N') + encoded_metadata + payload
      end

      def self.decode_frame(frame)
        frame = String(frame).b
        raise ProtocolError, 'frame size prefix is incomplete' if frame.bytesize < 4

        total_size = frame.byteslice(0, 4).unpack1('N')
        raise ProtocolError, 'frame is incomplete' if frame.bytesize < 4 + total_size
        raise ProtocolError, 'command size prefix is incomplete' if total_size < 4

        command_size = frame.byteslice(4, 4).unpack1('N')
        raise ProtocolError, 'command exceeds frame size' if command_size > total_size - 4

        command_bytes = frame.byteslice(8, command_size)
        headers_and_payload = frame.byteslice(8 + command_size, total_size - 4 - command_size) || +''

        DecodedFrame.new(
          command: Proto::BaseCommand.decode(command_bytes),
          headers_and_payload: headers_and_payload.b
        )
      end

      def self.decode_message_data(headers_and_payload)
        headers_and_payload = String(headers_and_payload).b
        raise ProtocolError, 'metadata size prefix is incomplete' if headers_and_payload.bytesize < 4

        metadata_size = headers_and_payload.byteslice(0, 4).unpack1('N')
        raise ProtocolError, 'metadata exceeds message data size' if metadata_size > headers_and_payload.bytesize - 4

        metadata_bytes = headers_and_payload.byteslice(4, metadata_size)
        payload = headers_and_payload.byteslice(4 + metadata_size, headers_and_payload.bytesize - 4 - metadata_size) || +''

        DecodedMessageData.new(
          metadata: Proto::MessageMetadata.decode(metadata_bytes),
          payload: payload.b
        )
      end
    end
  end
end
