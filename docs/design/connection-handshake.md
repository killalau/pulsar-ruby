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
- Sending one command and reading one response synchronously.
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
connection.close
```

## Current Shape

The current request/response path is intentionally simple and synchronized. It
is enough to support the next small steps, such as command factory tests and
producer creation against a fake broker.

The final MVP connection still needs:

- A background reader thread.
- Pending request map keyed by request ID.
- Producer send receipt routing.
- Consumer message routing.
- Ping/pong handling.
- Broker error mapping.
- Connection loss behavior.

These should be added with focused red-green tests before public producer and
consumer methods are wired to real broker behavior.
