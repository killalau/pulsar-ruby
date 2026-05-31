# TCP Transport

This document records the first plaintext transport implementation for the Ruby
Apache Pulsar client.

## Purpose

`Pulsar::Internal::TcpTransport` is the lowest-level socket abstraction for the
MVP. It hides Ruby socket details from the connection and protocol layers.

The transport is intentionally small:

- Connect to a physical broker host and port.
- Write binary bytes.
- Read an exact byte count.
- Close idempotently.
- Map socket failures to typed Pulsar errors.

## Internal API

```ruby
transport = Pulsar::Internal::TcpTransport.connect(
  host: "127.0.0.1",
  port: 6650,
  connection_timeout: 10
)

transport.write(bytes)
bytes = transport.read_exact(4, timeout: 30)
transport.close
transport.closed?
```

## Error Mapping

- Connection setup failures raise `Pulsar::ConnectionError`.
- Socket read/write failures raise `Pulsar::ConnectionError`.
- Reading fewer bytes because the socket closes raises `Pulsar::ConnectionError`.
- Reads that exceed their timeout raise `Pulsar::TimeoutError`.
- Operations after close raise `Pulsar::ClosedError`.

## Current Limits

The MVP transport only supports plaintext TCP for `pulsar://`.

Deferred transport features:

- TLS via `OpenSSL::SSL::SSLSocket`.
- Authentication challenge handling.
- Proxy-aware connections.
- Fiber-scheduler-aware nonblocking transport.

Those features should be added behind the same transport boundary so public
client, producer, and consumer APIs do not need to change.
