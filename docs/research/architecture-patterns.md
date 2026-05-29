# Architecture Patterns

This document captures architecture patterns found across official clients. It
starts with the Java baseline and will be refined as more clients are analyzed.

## Current Status

Java, C++, Go, and DotPulsar provide the main native architecture references.
Node.js and Python are binding layers over C/C++. Java Reactive Streams is an
adapter layer over Java.

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

## C++ Client Patterns

The C++ client repeats the Java separation while using language-specific
mechanics:

- Public API headers expose small value-like handles such as `Client`,
  `Producer`, `Consumer`, and `Reader`.
- Internal implementations live behind shared pointers such as `ClientImpl`,
  `ProducerImpl`, and `ConsumerImpl`.
- `ConnectionPool` owns broker connection reuse.
- `ClientConnection` owns socket lifecycle, protocol handshake, frame IO,
  pending request maps, producer/consumer registration, and reconnect
  notifications.
- `BinaryProtoLookupService` owns lookup, partition metadata, schema lookup, and
  redirect behavior.
- `Commands` owns protocol frame construction.
- `MemoryLimitController`, pending producer queues, and consumer receive queues
  make backpressure a runtime concern, not just configuration.
- A C wrapper lives on top of the C++ client rather than replacing the internal
  architecture.

## Strengthened Ruby Architecture Notes

After Java and C++, the minimum architecture for a native Ruby client should
include:

- Public handle objects that delegate to internal implementations.
- A dedicated protocol command/frame module.
- A connection object with request correlation and producer/consumer registries.
- A lookup service object, separate from client and connection.
- Queue-limit and permit accounting in the first consumer implementation.
- A producer pending-send queue with queue-full behavior.

## Go Client Patterns

The Go client adds an important intermediate layer:

- Public interfaces and options structs live in the `pulsar` package.
- Internal generated protobufs live under `pulsar/internal/pulsar_proto`.
- `RPCClient` owns request ID generation, request timeouts, and response
  correlation.
- `Connection` owns socket lifecycle, pending requests, write queues, producer
  listeners, consumer handlers, and keepalive.
- `LookupService` owns lookup and metadata behavior.
- Producer and consumer implementations compose partition-level producers and
  consumers.
- Consumer partition dispatchers use channels and permits to decouple broker IO
  from public `Receive`.

## Strengthened Ruby Architecture Notes

After Java, C++, and Go, add one more likely Ruby component:

- `Pulsar::RPC`: request ID generation, request timeout, request/response
  correlation, and no-wait command dispatch on top of `Pulsar::Connection`.

## DotPulsar Client Patterns

DotPulsar contributes these patterns:

- Public abstractions are small interfaces with internal implementations.
- Producers, consumers, readers, connections, and channels have explicit states.
- `ConnectionPool` combines lookup and connection reuse.
- `Connection` owns stream IO and typed command sending.
- `ChannelManager` maps producer/consumer IDs to channels and dispatches
  incoming broker data.
- `ProcessManager` and process classes respond to channel state changes and
  recreate channels after disconnects.
- Public methods use cancellation tokens, and runtime objects are async
  disposable.

## Strengthened Ruby Lifecycle Notes

Ruby should model lifecycle explicitly:

- Public objects should know whether they are open, connected, disconnected,
  closing, or closed.
- Reconnection should replace internal channels/connections without replacing the
  public producer or consumer object.
- Public errors should distinguish disposed/closed/faulted object state from
  broker/protocol errors.

## Binding And Adapter Patterns

Node.js and Python expose language-friendly APIs over the C/C++ client. Their
architecture mostly consists of:

- Option parsing and validation.
- Native object wrappers.
- Callback-to-promise or callback-to-future bridges.
- Language-level schemas/auth helpers.
- Platform-specific binary packaging.

Java Reactive Streams exposes a higher-level adapter over the Java client:

- Reactive sender/consumer/reader APIs.
- Producer caching.
- In-flight limits.
- Message pipelines.
- Reactor-based backpressure.

These patterns should be considered future layers for Ruby, not the MVP core.

## Architecture Conclusion For Ruby

The core Ruby client should be a pure Ruby native protocol implementation with
separate public handles, protocol encoding/decoding, connection, RPC, lookup,
producer, consumer, queues, permits, states, and errors. Binding or reactive
adapter layers can be added later if needed.
