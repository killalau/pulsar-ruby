# MVP Scope

This document tracks the proposed first implementation scope for the Ruby Apache
Pulsar client.

## Current Status

Initial MVP scope is based on the Java client baseline and should be refined
after comparing more official clients.

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
