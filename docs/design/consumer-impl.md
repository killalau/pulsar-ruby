# Consumer Implementation

This document records the first internal consumer implementation for the MVP.

## Implemented Behavior

`Pulsar::Internal::ConsumerImpl` currently supports:

- Creating a broker-side subscription with `CommandSubscribe`.
- Sending initial flow permits.
- Decoding incoming `CommandMessage` data into public `Pulsar::Message`.
- Receiving messages from an internal bounded queue.
- Sending individual ack commands.

The connection now also supports:

- `Connection#write_command(command)` for write-only commands such as flow and
  ack.

## Current Limits

This is still an internal implementation layer. Public `Client#consumer` is not
yet wired to a real broker connection or background reader.

Remaining consumer MVP work:

- Public client wiring.
- Background connection reader routing broker-pushed messages to consumers.
- Consumer close command.
- Receive queue replenishment policy.
- Redelivery and negative ack behavior.
- Integration test against real Pulsar standalone.
