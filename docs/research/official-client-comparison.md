# Official Client Comparison

This document captures per-client analysis for official Apache Pulsar clients.
Each client is studied with the shared template from
[Official Client Research Plan](official-client-research-plan.md), then compared
for common design ground that can guide the Ruby client.

## Status

- Java client: first-pass baseline complete.
- C++ client: pending.
- Go client: pending.
- C#/DotPulsar client: pending.
- Node.js client: pending.
- Python client: pending.
- Java Reactive Streams client: pending.
- Final cross-client comparison: pending.

## Client Analysis: Java Client

### Summary

The Java client is the most complete reference implementation. It provides a
stable public API around `PulsarClient`, `Producer`, `Consumer`, and `Reader`,
with both synchronous and asynchronous operations. Internally, it separates API
interfaces from implementation classes, uses Netty for transport, uses
`CompletableFuture` for async operations, and implements the Pulsar binary
protocol directly.

Key source areas:

- `.research/clients/pulsar/pulsar-client-api`
- `.research/clients/pulsar/pulsar-client`
- `.research/clients/pulsar/pulsar-common/src/main/proto/PulsarApi.proto`

### Repository And Maintenance Signals

The Java client lives in the main Apache Pulsar repository and appears to be the
primary reference client. It is split into API, implementation, admin, auth,
shaded, and packaging modules. This makes it the best source for behavior,
protocol expectations, feature breadth, and compatibility assumptions.

### Public API

The public API is builder-oriented:

- `PulsarClient.builder().serviceUrl(...).build()`
- `client.newProducer().topic(...).create()`
- `client.newConsumer().topic(...).subscriptionName(...).subscribe()`
- `client.newReader().topic(...).startMessageId(...).create()`

The main runtime objects are documented as thread-safe. Producer, consumer, and
reader APIs expose both blocking and async methods:

- Producer: `send`, `sendAsync`, `flush`, `flushAsync`, `newMessage`.
- Consumer: `receive`, `receiveAsync`, `batchReceive`, `acknowledge`,
  `negativeAcknowledge`, `reconsumeLater`, `unsubscribe`, `seek`.
- Reader: `readNext`, `readNextAsync`, `hasMessageAvailable`, `seek`, `close`.

The async API is based on `CompletableFuture`. The producer API explicitly warns
that async callbacks can run on internal network I/O threads, so callbacks must
remain lightweight or move to another executor.

### Supported Features

The Java API surface includes broad support for:

- Producers, consumers, readers, and table views.
- Sync and async operations.
- Schemas and typed messages.
- Partitioned topics.
- Multiple subscription types.
- Message listeners.
- Batch receive.
- Acknowledgment, negative acknowledgment, cumulative acknowledgment, and
  delayed reconsume.
- Batching, compression, chunking, and message routing.
- Dead letter and retry behavior.
- Transactions.
- Interceptors.
- Authentication, TLS, proxy settings, and failover configuration.
- Metrics and OpenTelemetry integration.
- Admin clients in separate modules.

### Missing Or Deferred Features

No obvious major client feature category is missing from the Java client. For
Ruby planning, the important lesson is not to match Java breadth immediately.
The Java client is a full target model, not an MVP baseline.

### Internal Architecture

Important implementation classes include:

- `PulsarClientImpl`: central client runtime and factory for producers,
  consumers, readers, lookup service, connections, and shared resources.
- `ClientBuilderImpl`, `ProducerBuilderImpl`, `ConsumerBuilderImpl`,
  `ReaderBuilderImpl`: mutable builder/configuration layer.
- `ClientConfigurationData`, `ProducerConfigurationData`,
  `ConsumerConfigurationData`, `ReaderConfigurationData`: internal config
  records.
- `ConnectionPool`, `ClientCnx`, `PulsarChannelInitializer`: connection and
  Netty pipeline layer.
- `BinaryProtoLookupService`: binary-protocol topic lookup and partition
  metadata lookup.
- `ProducerImpl`, `PartitionedProducerImpl`: producer runtime.
- `ConsumerImpl`, `MultiTopicsConsumerImpl`, `ZeroQueueConsumerImpl`: consumer
  runtime variants.
- `ReaderImpl`, `MultiTopicsReaderImpl`: reader runtime variants.
- `MessageImpl`, `MessageIdImpl`, `BatchMessageIdImpl`,
  `TopicMessageIdImpl`: message and message-id models.
- `AcknowledgmentsGroupingTracker`, `UnAckedMessageTracker`,
  `NegativeAcksTracker`: consumer acknowledgment and redelivery helpers.
- `BatchMessageContainerImpl` and related batch containers: batching support.

The repeated pattern is a public API interface, a builder, internal immutable or
mutable configuration data, and an implementation object that is attached to a
connection handler.

### Protocol And Transport

The Java client implements the Pulsar binary protocol over Netty. Shared command
definitions live in `PulsarApi.proto`.

Core protocol commands observed for the MVP path:

- `CommandConnect` / `CommandConnected`
- `CommandLookupTopic`
- `CommandPartitionedTopicMetadata`
- `CommandProducer` / `CommandProducerSuccess`
- `CommandSend` / `CommandSendReceipt` / `CommandSendError`
- `CommandSubscribe` / `CommandSuccess`
- `CommandMessage`
- `CommandAck` / `CommandAckResponse`
- `CommandFlow`

The client tracks request IDs for lookup and metadata operations. `ClientCnx`
keeps pending request maps and completes futures when responses arrive.

### Producer Flow

The high-level flow is:

1. Build a producer configuration through the public builder.
2. `PulsarClientImpl` resolves the topic and creates either a single-topic or
   partitioned producer.
3. Lookup identifies the broker that owns the topic.
4. The client opens or reuses a connection.
5. `ProducerImpl` sends a producer registration command.
6. Application sends become message metadata plus payload frames.
7. Broker send receipts complete pending send futures with `MessageId`.

Producer backpressure is visible in the public API through max pending messages
and queue-full behavior. `sendAsync` can fail when the queue is full unless the
producer is configured to block.

### Consumer Flow

The high-level flow is:

1. Build a consumer configuration with topic and subscription name.
2. `PulsarClientImpl` creates a consumer implementation.
3. Lookup identifies the broker that owns the topic.
4. The client opens or reuses a connection.
5. `ConsumerImpl` sends a subscribe command.
6. Broker pushes `CommandMessage` frames.
7. Consumer places messages into receive queues or completes pending async
   receive calls.
8. Application acknowledges messages with ack commands.
9. Flow permits are increased as messages are consumed.

The Java client treats flow permits as a central part of consumer backpressure.
The consumer has explicit logic for increasing permits after messages are
received, skipped, consumed, or released from the local queue.

### Reader Flow

Reader is a lower-level abstraction over a topic position. The public API reads
from a start `MessageId` and does not require user acknowledgments. Internally,
the reader implementation is closely related to consumer behavior, but exposed
as scanning rather than subscription processing.

### Connection, Lookup, And Reconnect Behavior

`ConnectionPool` and `ClientCnx` manage binary connections. `ClientCnx` handles
connection establishment, protocol negotiation, authentication continuation,
lookup responses, producer/consumer registration responses, messages, send
receipts, and connection closure.

When a connection closes, `ClientCnx` notifies attached producers, consumers,
transaction handlers, watchers, and sessions so they can reconnect. Producers
and consumers implement connection-handler interfaces for this lifecycle.

`BinaryProtoLookupService` performs topic lookup and partition metadata lookup
through the binary protocol. It handles lookup redirects and uses a pinned
executor for lookup work.

### Backpressure And Flow Control

Backpressure appears in both producer and consumer paths:

- Producer pending-message limits and queue-full behavior protect memory.
- Consumer receiver queue size and `CommandFlow` permits control broker push.
- Async receive calls are documented as needing sequential use to avoid
  application-created backlogs.
- Batch receive has explicit policies around size and wait time.

For Ruby, this means backpressure cannot be bolted on later. Even a minimal
consumer needs receiver queue and permit accounting.

### Error Handling

Public APIs throw or complete with `PulsarClientException` subclasses. Internally
server errors are mapped into typed client exceptions. Async operations use
futures, and connection/protocol operations complete futures exceptionally.

For Ruby, this suggests defining a small but intentional exception hierarchy
early, even if the first version maps fewer server error cases.

### Authentication And TLS

The Java API includes authentication abstractions, TLS settings, proxy protocol
settings, and failover configuration. Connection setup handles authentication
exchange and can continue mutual authentication before completing the connection
future.

Ruby MVP can defer full auth/TLS, but the connection abstraction should leave
space for pluggable authentication and TLS configuration.

### Batching, Compression, And Chunking

The Java client has dedicated batching containers, compression configuration,
chunk message IDs, and batch message IDs. These concerns affect message framing,
message IDs, send callbacks, and consumer decoding.

Ruby MVP should start with single unbatched messages, but should model
`MessageId` and message metadata carefully so batch and chunk support can be
added without changing the public API shape.

### Schema Support

Schemas are deeply integrated into the public API. `PulsarClient` creates typed
producers, consumers, and readers with `Schema<T>`, and message builders can
override schema.

Ruby MVP can default to bytes/string payloads, but should reserve an API path for
schema-aware encoding and decoding.

### Testing Strategy

The main repository contains extensive tests across client, broker, admin,
compatibility, and packaging modules. A later pass should inspect Java tests for
producer/consumer behavior, reconnect, lookup, batching, and ack edge cases.

### Packaging And Distribution

The Java client is built with Gradle and published as multiple artifacts:
public API, implementation, admin, auth modules, and shaded bundles. This split
is useful conceptually, but Ruby should initially prefer one gem with internal
namespaces until there is a reason to split packages.

### Lessons For The Ruby Client

- Start with the same conceptual API: client, producer, consumer, reader,
  message, and message ID.
- Provide blocking APIs first, but design internals around async connection and
  request completion.
- Keep protocol encoding/decoding separate from producer and consumer logic.
- Implement lookup as its own component.
- Implement connection state and reconnect as first-class behavior.
- Include consumer flow-control permits in the MVP.
- Define queue limits and backpressure behavior before implementing send loops.
- Keep auth, TLS, batching, compression, chunking, schemas, and transactions
  visible as future extension points, even if deferred.

### Open Questions

- Should Ruby expose async behavior through threads, fibers, or futures?
- Should the first Ruby MVP include reader, or only producer and consumer?
- What is the minimum useful exception hierarchy for early protocol errors?
- Should schema support start as bytes-only with later adapters, or should the
  public API include a schema slot from day one?
