# Reconnect Policy

This document records the MVP reconnect policy for the Ruby Apache Pulsar
client.

## MVP Policy

The MVP supports conservative automatic reconnect:

- A dropped socket marks the current connection disconnected.
- In-flight requests and sends fail with `Pulsar::ConnectionError`.
- Public `Producer` and `Consumer` objects remain open.
- The next operation asks the client for a connection.
- The client replaces the disconnected connection lazily.
- Producers and consumers reattach their broker-side state before continuing.

The MVP does not silently retry the operation that was already in flight when
the connection failed.

## Rationale

Reconnect and retry are related but separate concerns.

Reconnect restores transport and broker-side producer or consumer handles.
Retry decides whether a failed operation should be attempted again. Retrying a
send is not always safe because the broker may have received the message before
the client observed the connection failure.

Separating reconnect from retry gives the MVP a useful recovery path while
leaving room for a later retry policy with producer sequence tracking,
deduplication assumptions, backoff, and metrics.

## Implemented Shape

`Pulsar::Client` owns the replaceable connection. If the current connection is
not connected, the next connection request closes and discards it, then opens a
new connection to the service URL.

`Pulsar::Internal::ProducerImpl` and `Pulsar::Internal::ConsumerImpl` keep a
connection provider rather than treating a connection as permanent. Before
producer send, consumer receive, ack, flow, or close behavior that needs the
broker, the implementation checks whether it is attached to a live connection.
If not, it creates the broker-side producer or subscription again on the
replacement connection.

## Deferred Retry Work

Full retry support should add:

- Configurable retry and backoff policy.
- Per-send pending state that survives reconnect.
- Duplicate prevention strategy.
- Ack retry or ack grouping.
- Redelivery and dead-letter policy integration.
- Automatic retry integration tests for failed in-flight sends.
