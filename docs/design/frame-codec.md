# Frame Codec

This document records the first implemented Pulsar binary frame codec behavior.

## Supported Frame Shapes

The MVP codec supports command-only frames:

```text
[TOTAL_SIZE][COMMAND_SIZE][COMMAND]
```

It also supports checksum-free message frames:

```text
[TOTAL_SIZE][COMMAND_SIZE][COMMAND][METADATA_SIZE][METADATA][PAYLOAD]
```

All size fields are 4-byte unsigned big-endian integers.

## Ruby API

Implemented internal API:

```ruby
Pulsar::Internal::FrameCodec.encode_command(command)
Pulsar::Internal::FrameCodec.decode_frame(frame)

Pulsar::Internal::FrameCodec.encode_message(command, metadata, payload)
Pulsar::Internal::FrameCodec.decode_message_data(headers_and_payload)
```

`decode_frame` returns a small internal value with:

- `command`: decoded `Pulsar::Proto::BaseCommand`.
- `headers_and_payload`: trailing bytes after the command.

`decode_message_data` returns a small internal value with:

- `metadata`: decoded `Pulsar::Proto::MessageMetadata`.
- `payload`: raw binary payload bytes.

## Current Limits

The codec currently does not implement:

- CRC32C checksum magic and validation.
- Compression.
- Batched single-message metadata.
- Chunked messages.
- Broker entry metadata.

These are deferred until the simple producer and consumer flows work against a
local standalone broker.

## Validation

The decoder raises `Pulsar::ProtocolError` for malformed frames, including:

- Missing frame size prefix.
- Incomplete frame bytes.
- Invalid command size.
- Incomplete metadata bytes.

The codec is intentionally independent from transport, connection, producer, and
consumer classes.
