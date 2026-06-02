# Connection Handshake

This document records the first broker connection behavior implemented for the
Ruby Apache Pulsar client.

## Implemented Behavior

`Pulsar::Internal::Connection` currently supports:

- Opening a plaintext TCP transport.
- Sending `CommandConnect`.
- Reading `CommandConnected`.
- Capturing broker connection metadata:
  - server version
  - protocol version
  - max message size
- Allocating increasing request IDs.
- Sending request commands and resolving responses through a pending request
  map.
- Sending message frames and routing send receipts back to the waiting send.
- Mapping `CommandError` and `CommandSendError` responses to typed Ruby errors.
- Running a background reader thread after the connect handshake.
- Responding to broker `PING` frames with `PONG`.
- Registering consumers and routing broker-pushed messages by consumer ID.
- Marking the connection disconnected and rejecting pending work when the broker
  socket closes unexpectedly.
- Closing idempotently.

## Internal API

```ruby
connection = Pulsar::Internal::Connection.connect(
  host: "127.0.0.1",
  port: 6650,
  connection_timeout: 10,
  operation_timeout: 30,
  client_version: "pulsar-ruby"
)

request_id = connection.next_request_id
response = connection.request(command, timeout: 30)
connection.register_consumer(consumer_id, consumer)
connection.close
```

## Current Shape

The connection still performs the initial `CommandConnect` handshake
synchronously, then starts a background reader thread. Public request methods
write frames under a write mutex and wait on internal promises that the reader
fulfills when matching broker responses arrive.

Broker error mapping is centralized in `Pulsar::Internal::BrokerErrorMapper` so
future server error codes can be added without changing the reader loop.

The remaining MVP connection work is basic reconnect behavior. That should be
added with focused red-green tests and a documented retry policy.
