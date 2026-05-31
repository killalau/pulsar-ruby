# Producer Implementation

This document records the first internal producer implementation for the MVP.

## Implemented Behavior

`Pulsar::Internal::ProducerImpl` currently supports:

- Creating a broker-side producer with `CommandProducer`.
- Storing the broker-assigned producer name.
- Tracking producer sequence IDs.
- Sending one unbatched payload with message metadata.
- Mapping `CommandSendReceipt` into public `Pulsar::MessageId`.

The connection now also supports:

- `Connection#send_message(command, metadata, payload, timeout:)`

That method writes a checksum-free message frame and reads one synchronous broker
response.

## Current Limits

This is still an internal implementation layer. Public `Client#producer` is not
yet wired to a real broker connection.

Remaining producer MVP work:

- Public client wiring.
- Producer close command.
- Pending send limits.
- Send error mapping.
- Reconnect behavior.
- Integration test against real Pulsar standalone.
