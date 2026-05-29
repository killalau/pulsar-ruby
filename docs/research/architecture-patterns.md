# Architecture Patterns

This document captures architecture patterns found across official clients. It
starts with the Java baseline and will be refined as more clients are analyzed.

## Current Status

Java client baseline started. Cross-client patterns are provisional until at
least C++, Go, and DotPulsar are analyzed.

## Java Client Patterns

The Java client separates concerns into these layers:

- Public API interfaces: `PulsarClient`, `Producer`, `Consumer`, `Reader`,
  builders, messages, schemas, and exceptions.
- Builder and configuration layer: builder implementations populate internal
  configuration data objects.
- Client runtime: one central client implementation owns shared resources,
  lookup service, connection pool, and object factories.
- Lookup service: topic and partition metadata lookup are separate from
  producer and consumer logic.
- Transport layer: Netty channel initialization, connection pooling, protocol
  command handling, and pending request tracking.
- Domain runtimes: producer, consumer, reader, partitioned producer, and
  multi-topic consumer implementations.
- Support services: batching, ack grouping, unacked tracking, negative ack
  tracking, metrics, auth, schemas, and transactions.

## Candidate Ruby Architecture

Initial Ruby architecture should mirror the same separation, but with a smaller
surface:

- `Pulsar::Client`: service URL, configuration, lookup, and object factory.
- `Pulsar::Producer`: send messages and track send receipts.
- `Pulsar::Consumer`: receive messages, ack messages, and manage permits.
- `Pulsar::Message` and `Pulsar::MessageId`: payload, metadata, and identity.
- `Pulsar::Connection`: socket lifecycle, frame read/write, request tracking.
- `Pulsar::ConnectionPool`: reuse or create broker connections.
- `Pulsar::LookupService`: topic owner lookup and redirect handling.
- `Pulsar::Protocol`: protobuf command and frame encoding/decoding.
- `Pulsar::Errors`: client exception hierarchy.

## Early Design Rule

Do not put protocol framing, socket management, lookup, producer state, and
consumer state into one large object. The Java client shows these concerns
change independently and need separate tests.
