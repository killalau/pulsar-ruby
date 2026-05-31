# Producer Implementation

This document records the first internal producer implementation for the MVP.

## Implemented Behavior

`Pulsar::Internal::ProducerImpl` currently supports:

- Creating a broker-side producer with `CommandProducer`.
- Storing the broker-assigned producer name.
- Tracking producer sequence IDs.
- Sending one unbatched payload with message metadata.
- Mapping `CommandSendReceipt` into public `Pulsar::MessageId`.
- Closing broker-side producers with `CommandCloseProducer`.
- Rejecting sends after close.
- Enforcing a bounded pending-send limit for concurrent send calls.

The connection now also supports:

- `Connection#send_message(command, metadata, payload, timeout:)`

That method writes a checksum-free message frame and reads one synchronous broker
response.

## Current Limits

Public `Client#producer` is now wired to the internal connection and producer
implementation. Against a broker that supports the simple connect, producer
creation, and send receipt sequence, `producer.send` returns a public
`Pulsar::MessageId`.

Remaining producer MVP work:

- Send error mapping.
- Reconnect behavior.
- Integration test against real Pulsar standalone.
