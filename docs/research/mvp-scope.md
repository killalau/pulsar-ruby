# MVP Scope

This document tracks the proposed first implementation scope for the Ruby Apache
Pulsar client.

## Current Status

The MVP implementation supports the core non-partitioned producer/consumer path,
standalone integration testing, bounded queues, typed errors, lifecycle cleanup,
and conservative reconnect. Remaining work after MVP should focus on richer
retry semantics and deferred Pulsar features.

## Proposed MVP

The first Ruby client should support:

- Connect to a Pulsar broker with `pulsar://`.
- Complete the binary protocol connect handshake.
- Lookup the broker that owns a topic.
- Create a producer for a non-partitioned topic.
- Send single, unbatched byte/string messages.
- Receive send receipts and return message IDs.
- Create a consumer for a non-partitioned topic and subscription.
- Receive messages.
- Acknowledge messages.
- Maintain consumer flow permits.
- Enforce basic producer and consumer queue limits.
- Keep protocol command construction separate from socket IO.
- Keep lookup separate from producer and consumer implementation.
- Add an internal request/response correlation layer with request IDs and
  operation timeouts.
- Allow blocking send and receive calls to take timeout or cancellation options.
- Track explicit internal states for connections, producers, and consumers.
- Reconnect enough to recover from a dropped broker connection in simple cases.
- Run integration tests against a local Pulsar standalone broker.

## Deferred From MVP

- TLS.
- Authentication.
- Partitioned topics.
- Reader.
- Multi-topic consumers.
- Batching.
- Compression.
- Chunking.
- Schemas beyond raw bytes/string payloads.
- Negative ack and delayed reconsume.
- Dead letter and retry topics.
- Transactions.
- Interceptors.
- Admin API.
- Metrics and tracing.

## Ruby MVP Success Criteria

- A Ruby process can produce a message to a local Pulsar topic.
- A Ruby process can consume and acknowledge that message.
- Message IDs are represented as structured Ruby objects.
- The client does not grow unbounded memory under normal producer or consumer
  use because queue limits and flow permits are active.
- The protocol, connection, lookup, producer, and consumer layers are tested
  independently enough to support future features.
