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

Public `Client#consumer` is now wired to the internal connection and consumer
implementation. The current MVP receive path is synchronous: if the local queue
is empty, `consumer.receive` reads one broker-pushed frame from the connection,
decodes it, queues it, and returns the message.

Remaining consumer MVP work:

- Background connection reader routing broker-pushed messages to consumers.
- Consumer close command.
- Receive queue replenishment policy.
- Redelivery and negative ack behavior.
- Integration test against real Pulsar standalone.
