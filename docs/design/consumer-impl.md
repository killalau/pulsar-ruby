# Consumer Implementation

This document records the first internal consumer implementation for the MVP.

## Implemented Behavior

`Pulsar::Internal::ConsumerImpl` currently supports:

- Creating a broker-side subscription with `CommandSubscribe`.
- Sending initial flow permits.
- Decoding incoming `CommandMessage` data into public `Pulsar::Message`.
- Receiving messages from an internal bounded queue.
- Replenishing one flow permit when a message is received by application code.
- Sending individual ack commands.
- Closing broker-side consumers with `CommandCloseConsumer`.
- Rejecting receive, ack, flow, and pushed message handling after close.

The connection now also supports:

- `Connection#write_command(command)` for write-only commands such as flow and
  ack.
- Consumer registration and background routing of broker-pushed messages.

Public `Client#consumer` is now wired to the internal connection and consumer
implementation. Broker-pushed messages are now routed by the connection's
background reader into the consumer queue, and `consumer.receive` waits on that
queue.
