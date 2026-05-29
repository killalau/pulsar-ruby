# Official Client Comparison

This document captures per-client analysis for official Apache Pulsar clients.
Each client is studied with the shared template from
[Official Client Research Plan](official-client-research-plan.md), then compared
for common design ground that can guide the Ruby client.

## Status

- Java client: first-pass baseline complete.
- C++ client: first-pass baseline complete.
- Go client: first-pass baseline complete.
- C#/DotPulsar client: first-pass baseline complete.
- Node.js client: first-pass baseline complete.
- Python client: first-pass baseline complete.
- Java Reactive Streams client: first-pass baseline complete.
- Final cross-client comparison: first-pass complete.

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

## Client Analysis: C++ Client

### Summary

The C++ client is a native binary-protocol implementation with a public C++ API,
an internal asynchronous runtime, and a C wrapper. It closely mirrors the Java
client concepts: client, producer, consumer, reader, configuration objects,
lookup service, connection pool, binary protocol commands, producer pending
queues, consumer receive queues, and flow permits.

Key source areas:

- `.research/clients/pulsar-client-cpp/include/pulsar`
- `.research/clients/pulsar-client-cpp/lib`
- `.research/clients/pulsar-client-cpp/proto/PulsarApi.proto`
- `.research/clients/pulsar-client-cpp/lib/c`

### Repository And Maintenance Signals

The standalone Apache C++ client repository is active enough to show recent
maintenance in the shallow clone. It has CMake build files, CI configuration,
examples, tests, packaging for common platforms, a C API wrapper, performance
tools, and test broker configuration. It is a strong reference for native
transport, memory/backpressure behavior, and packaging concerns.

### Public API

The public API is object and configuration based rather than builder based:

- `Client(serviceUrl, ClientConfiguration)`
- `Client::createProducer` / `createProducerAsync` / `createProducerV2`
- `Client::subscribe` / `subscribeAsync` / `subscribeV2`
- `Client::createReader` / async reader creation
- `Producer::send`, `sendAsync`, `flush`, `close`
- `Consumer::receive`, `receiveAsync`, `batchReceive`, `acknowledge`,
  `acknowledgeCumulative`, `negativeAcknowledge`, `unsubscribe`

The older API returns `Result` codes and passes output objects by reference. New
V2 APIs use `std::variant<Error, T>`, which carries both a result code and a
message. Async APIs are callback based.

### Supported Features

The C++ client supports the major core features:

- Producers, consumers, readers, table views, and multi-topic consumers.
- Sync and callback-based async APIs.
- Topic lookup and partition metadata lookup.
- Partitioned producers.
- Multiple subscription types, including key-shared support.
- Message listeners and receive queues.
- Acknowledgment, cumulative acknowledgment, negative acknowledgment, redelivery,
  seek, and batch receive.
- Batching, compression, chunking, schemas, and typed message helpers.
- Authentication, TLS, proxy-style logical/physical address handling, and
  service info providers for failover.
- Interceptors, crypto, dead letter behavior, stats, and memory limits.
- A C wrapper API for non-C++ language bindings.

### Missing Or Deferred Features

The C++ surface is broad, but the main research point is that native clients can
offer a smaller public API than Java while still implementing the same protocol
and reliability machinery internally. For Ruby, C++ suggests the MVP API can be
compact as long as internals keep connection, lookup, queueing, and permits
separate.

### Internal Architecture

Important implementation classes include:

- `ClientImpl`: central runtime, service info management, lookup service,
  connection pool, object creation, request ID generation, and memory limit.
- `ConnectionPool`: keyed connection reuse by logical and physical broker
  address.
- `ClientConnection`: Asio-based TCP/TLS connection, handshake, frame IO,
  command dispatch, request tracking, keepalive, and producer/consumer
  registration.
- `BinaryProtoLookupService`: topic lookup, partition metadata, namespace topic
  listing, schema lookup, and redirect handling.
- `Commands`: command/frame builder for connect, lookup, producer, subscribe,
  send, ack, flow, ping/pong, seek, and close commands.
- `ProducerImpl`: producer state, pending send queue, batching, timeout,
  resend-on-reconnect, memory accounting, and send receipts.
- `ConsumerImpl`: receive queue, pending receives, ack operations, negative ack,
  redelivery, batch receive, permit accounting, and message dispatch.
- `ReaderImpl`: reader abstraction built over similar connection and message
  behavior.
- `MemoryLimitController`: client-level memory pressure control.

### Protocol And Transport

The C++ client implements the same Pulsar binary protocol commands as the Java
client and carries its own `PulsarApi.proto`. Transport uses Boost.Asio or
standalone Asio, with optional TLS streams. `ClientConnection` handles TCP
connect, TLS handshake, Pulsar connect/auth handshake, command reads, incoming
message parsing, checksum verification, and write completion.

The `Commands` helper centralizes protocol frame construction. This is a strong
design signal for Ruby: protocol command construction should live in a dedicated
module, not inside producer/consumer classes.

### Producer Flow

The C++ producer flow mirrors Java:

1. `ClientImpl` creates producer configuration and resolves partition metadata.
2. Lookup and connection pool choose the broker connection.
3. `ProducerImpl` registers with a producer ID.
4. Sends enter a pending message queue or batching container.
5. `ClientConnection::sendMessage` writes frames.
6. Send receipts complete callbacks and remove pending operations.
7. Reconnect can resend pending messages when the producer is ready again.

Producer backpressure is explicit through pending-message limits,
`blockIfQueueFull`, memory-limit accounting, send timeouts, and queue-full
result codes.

### Consumer Flow

The C++ consumer flow also mirrors Java:

1. `ClientImpl` creates the consumer and resolves topic metadata.
2. `ConsumerImpl` subscribes through a broker connection.
3. Initial flow permits are sent with `Commands::newFlow`.
4. Incoming messages are either delivered to waiting async receives or stored in
   an `incomingMessages_` queue.
5. `receive` and `receiveAsync` drain that queue.
6. Ack operations send ack commands through the connection.
7. Permits are replenished when messages leave the local queue or are skipped.

The source comments repeat the Java warning that async receive calls should be
issued sequentially to avoid application-created receive backlogs.

### Reader Flow

The C++ client exposes reader as a first-class public object, with its own
configuration and implementation. Like Java, reader should be treated as a core
Pulsar concept but not necessarily required for the Ruby MVP.

### Connection, Lookup, And Reconnect Behavior

`ConnectionPool` separates logical broker address from physical address, which
matters for proxies and redirected clusters. `ClientConnection` owns connection
state and pending operations. On failure, producers and consumers are notified
and can reattach/resend as needed.

`BinaryProtoLookupService` performs binary lookups and partition metadata
requests, tracks lookup request IDs, and applies max redirect limits.

### Backpressure And Flow Control

C++ confirms that backpressure is a core design concern:

- Producer pending queue and memory limit prevent unlimited sends.
- `ResultProducerQueueIsFull` is a public API result.
- `MemoryLimitController` provides client-wide memory protection.
- Consumer receiver queue size limits local buffering.
- `CommandFlow` permits regulate broker delivery.
- Batch receive has message and byte thresholds.

For Ruby, queue limits, memory accounting hooks, and flow permits should be part
of the first internal design, even if the first memory controller is simple.

### Error Handling

C++ uses a `Result` enum for many public APIs, plus a newer `Error` struct and
`std::variant<Error, T>` for V2 APIs. The enum includes detailed cases for
lookup, connection, authentication, authorization, broker persistence, checksum,
queue full, topic not found, schema incompatibility, transaction errors, memory
limit, interruption, and disconnection.

Ruby should likely expose exceptions rather than result codes, but the C++ enum
is a useful source for the initial exception taxonomy.

### Authentication And TLS

C++ supports authentication, TLS, OAuth-related tests, Athenz auth, certificate
test fixtures, hostname verification scenarios, and service info providers.
Connection setup includes TCP, TLS, Pulsar connect, and auth response handling.

Ruby can defer auth/TLS from MVP, but the connection constructor should not make
plaintext-only assumptions impossible to unwind.

### Batching, Compression, And Chunking

The C++ producer has batching containers, batch timers, batch size/space checks,
and logic to create one or more send operations from batches. It also tracks
batch message IDs and chunk message IDs. This reinforces the Java lesson: Ruby
can defer batching/chunking, but message ID and metadata models should leave
room for them.

### Schema Support

The C++ API includes `Schema`, `TypedMessage`, and Protobuf native schema
support. It is less type-system-driven than Java but still treats schema as a
first-class concern.

### Testing Strategy

The C++ repository has unit/integration-style tests for client behavior,
producer behavior, consumer behavior, reader behavior, lookup, connection
failure, TLS/auth, batching, schemas, key-shared consumers, seek, stats, and C
bindings. It also includes example producers/consumers and performance clients.

### Packaging And Distribution

The C++ client uses CMake and includes packaging directories for Linux/macOS
package formats plus vcpkg support. The existence of a C API wrapper is
important for Ruby only as a possible future binding route, but native bindings
would add install complexity.

### Lessons For The Ruby Client

- A compact public API can still map to a full protocol implementation.
- Separate command builders from connection, producer, and consumer logic.
- Use explicit result/error taxonomy internally, even if Ruby raises exceptions.
- Design queue limits and producer queue-full behavior early.
- Consumer permits and receiver queues are non-negotiable for correctness.
- Keep logical and physical broker addresses distinct for future proxy support.
- Avoid a native C++ binding as the first path unless pure Ruby protocol work
  proves impractical.

### Open Questions

- Should Ruby implement a client-wide memory limit in the MVP or just producer
  and consumer queue limits?
- Should Ruby model both logical and physical broker addresses from day one?
- Is the C API useful as a fallback implementation strategy, or should it remain
  reference-only?

## Client Analysis: Go Client

### Summary

The Go client is a native binary-protocol implementation with a compact,
idiomatic public API and asynchronous internals built around goroutines,
channels, contexts, request IDs, connection handlers, and generated protobufs.
It reinforces the same shared client architecture found in Java and C++ while
showing a smaller public surface that still supports broad Pulsar features.

Key source areas:

- `.research/clients/pulsar-client-go/pulsar`
- `.research/clients/pulsar-client-go/pulsar/internal`
- `.research/clients/pulsar-client-go/pulsar/internal/pulsar_proto`
- `.research/clients/pulsar-client-go/pulsaradmin`

### Repository And Maintenance Signals

The standalone Apache Go repository has recent commits, a Go module, examples,
integration tests, CI, OAuth/auth modules, generated protocol code, admin
client code, performance tools, and Docker-based integration setups. It is a
strong source for idiomatic async runtime structure and public API simplicity.

### Public API

The public API uses options structs and interfaces:

- `pulsar.NewClient(ClientOptions)`
- `Client.CreateProducer(ProducerOptions)`
- `Client.Subscribe(ConsumerOptions)`
- `Client.CreateReader(ReaderOptions)`
- `Client.CreateTableView(TableViewOptions)`
- `Producer.Send(ctx, message)`
- `Producer.SendAsync(ctx, message, callback)`
- `Consumer.Receive(ctx)`
- `Consumer.Ack`, `AckID`, `AckCumulative`, `Nack`

The public API is blocking by default but accepts `context.Context`, which gives
callers cancellation and timeout control. Async send uses callbacks.

### Supported Features

The Go client supports:

- Producers, consumers, readers, table views, and admin client package.
- Sync APIs with contexts and callback-style async producer send.
- Topic lookup, partition metadata, partitioned producers and consumers.
- Multiple subscription types, regex/multi-topic consumers, and key-shared
  support.
- Message channels/listeners.
- Ack, ack lists, cumulative ack, ack with response, nack, nack backoff, ack
  grouping, batch index ack, retry and DLQ behavior.
- Batching, compression, chunking, encryption/decryption, schemas, and
  transactions.
- TLS, token auth, TLS auth, Basic, Athenz, OAuth2, listener names, lookup
  properties, and blue-green migration.
- Metrics, logging, memory limits, keepalive, idle connection cleanup, and
  connection pooling.

### Missing Or Deferred Features

The Go client is broad enough that no major core category appears absent from
the first pass. Its public API is smaller than Java's, which supports keeping
Ruby ergonomic without hiding essential runtime behavior.

### Internal Architecture

Important implementation areas include:

- `client_impl.go`: central client runtime, option normalization, lookup,
  producers, consumers, readers, transactions, metrics, and shutdown.
- `internal/rpc_client.go`: request IDs, producer/consumer IDs, request/response
  correlation, request timeout handling, and lookup service creation.
- `internal/connection.go`: TCP/TLS connect, Pulsar handshake, pending request
  map, write channel, listener registry, consumer handler registry, keepalive,
  and connection close handling.
- `internal/connection_pool.go`: pooled connections keyed by logical address,
  physical address, and connection suffix.
- `internal/lookup_service.go`: topic lookup, redirects, partition metadata,
  namespace topics, schema lookup, logical/physical broker address handling.
- `internal/commands.go`: protobuf base command construction.
- `producer_impl.go`, `producer_partition.go`: producer routing, partition
  producers, send timeout, queue limits, batching, chunking, and reconnect.
- `consumer_impl.go`, `consumer_partition.go`: consumer fan-in, partition
  consumers, queue channel, ack/nack, permits, redelivery, chunk handling, and
  dispatcher loop.

### Protocol And Transport

The Go client uses generated protobuf code from:

```text
.research/clients/pulsar-client-go/pulsar/internal/pulsar_proto/PulsarApi.proto
```

`internal/commands.go` builds `BaseCommand` messages. `internal/rpc_client.go`
wraps request/response behavior, and `internal/connection.go` owns the raw
connection lifecycle and command dispatch. This is the cleanest separation so
far between command construction, RPC/request correlation, connection IO, and
domain objects.

### Producer Flow

The high-level producer flow is:

1. `Client.CreateProducer` normalizes `ProducerOptions`.
2. Topic metadata determines single-partition or partitioned producer behavior.
3. Lookup resolves logical and physical broker addresses.
4. RPC client obtains a pooled connection.
5. Partition producer registers with the broker.
6. `Send` blocks on `SendAsync` style behavior and context.
7. `SendAsync` respects max pending messages and queue-full behavior.
8. Send receipts complete callbacks and message IDs.

### Consumer Flow

The Go consumer flow is:

1. `Client.Subscribe` normalizes `ConsumerOptions`.
2. Single-topic, multi-topic, regex, and partition consumers are composed.
3. A partition consumer registers as a connection consumer handler.
4. Dispatcher goroutines manage queue channels and initial permits.
5. `Receive(ctx)` waits on local channels and honors context cancellation.
6. Ack/nack operations are routed to partition consumers.
7. Flow permits are replenished as messages are dequeued or queues are drained.

Go makes the async nature visible internally without forcing async complexity on
the simplest public consumer API.

### Reader Flow

Reader is supported as a public client-created object and uses the same topic,
lookup, and receive foundations. Ruby can still defer reader, but Go confirms it
is a normal part of the official client surface.

### Connection, Lookup, And Reconnect Behavior

Go has an explicit `RPCClient` over a `ConnectionPool`. Connections track
logical and physical addresses, listeners, consumer handlers, pending requests,
write requests, keepalive, and close notifications. Lookup follows redirects up
to a max redirect count and respects proxy-through-service-url behavior.

Reconnection is handled at producer/consumer partition layers with configurable
backoff policies and max reconnect limits.

### Backpressure And Flow Control

Go provides several useful Ruby design signals:

- `MaxPendingMessages` and `DisableBlockIfQueueFull` define producer queue
  behavior.
- `MemoryLimitBytes` provides client-wide memory limiting.
- Consumer `ReceiverQueueSize` controls local prefetch.
- `EnableZeroQueueConsumer` is a special mode rather than the default.
- Partition consumers track permits atomically and send `CommandFlow`.
- Send/receive APIs use `context.Context` to bound blocking operations.

For Ruby, an optional timeout/cancellation parameter on blocking calls may be a
better fit than exposing futures immediately.

### Error Handling

The Go client uses normal Go `error` values, plus a `Result` enum and structured
`Error` type that exposes the result code. The result enum is very similar to
C++ and useful for Ruby exception categories.

### Authentication And TLS

Go has explicit auth providers for disabled, token, TLS, Basic, Athenz, and
OAuth2, plus TLS config fields and certificate tests. Auth and TLS are not MVP
requirements for Ruby, but Go shows that auth belongs behind provider objects,
not ad hoc connection flags.

### Batching, Compression, And Chunking

Go supports batching, configurable batch builders, compression types, and
chunking. It explicitly rejects enabling batching and chunking together. This is
a concrete compatibility rule to revisit when Ruby adds both features.

### Schema Support

Go has public `Schema` support and generated protobuf test fixtures. Like C++,
schemas are not as type-system-central as Java, but they remain first-class.

### Testing Strategy

The Go repo has unit tests for internal primitives, connection, commands, lookup,
memory limits, producers, consumers, readers, schemas, transactions, auth,
chunking, and integration tests with standalone, clustered, blue-green, TLS, and
token setups.

### Packaging And Distribution

Go is packaged as a Go module. It includes `pulsaradmin` as a package in the
same repository. For Ruby, this supports starting with one repository and one
gem while using internal namespaces to keep runtime and admin code separable.

### Lessons For The Ruby Client

- A blocking public API can sit on top of asynchronous internal request
  tracking.
- Context-style timeout/cancellation is valuable; Ruby should expose equivalent
  timeout options on blocking calls.
- Keep RPC/request correlation as its own layer between connection and domain
  objects.
- Use channels/queues internally to decouple socket IO from producer/consumer
  APIs.
- A client-wide memory limit is worth keeping in the design, even if MVP starts
  with per-queue limits.
- Logical and physical addresses are now confirmed by C++ and Go as important.

### Open Questions

- What is Ruby's equivalent of Go `context.Context` for send/receive timeout and
  cancellation?
- Should Ruby expose consumer message channels/enumerators in addition to
  `receive`?
- Should the Ruby MVP include a client-wide memory limit from the start?

## Client Analysis: C#/DotPulsar Client

### Summary

DotPulsar is an async-first C# client with a polished abstraction layer, explicit
state machines, internal channels/processes, cancellation tokens, and typed
exceptions. Compared with Java, C++, and Go, it appears intentionally narrower,
but its lifecycle and state design is one of the clearest references for a Ruby
client that wants a small public API without becoming vague internally.

Key source areas:

- `.research/clients/pulsar-dotpulsar/src/DotPulsar`
- `.research/clients/pulsar-dotpulsar/src/DotPulsar/Abstractions`
- `.research/clients/pulsar-dotpulsar/src/DotPulsar/Internal`
- `.research/clients/pulsar-dotpulsar/src/DotPulsar/Internal/PulsarApi.proto`

### Repository And Maintenance Signals

The Apache DotPulsar repository has recent maintenance, a NuGet-focused project
layout, CI workflows, documentation, design decision records, samples, tests,
compression benchmarks, generated protocol code, and a clear public/internal
split.

### Public API

The public API is builder and interface based:

- `PulsarClient.Builder()`
- `IPulsarClient.CreateProducer<T>(ProducerOptions<T>)`
- `IPulsarClient.CreateConsumer<T>(ConsumerOptions<T>)`
- `IPulsarClient.CreateReader<T>(ReaderOptions<T>)`
- `IProducer<T>.Send(...)` through `ISend<T>`
- `IProducer<T>.SendChannel`
- `IConsumer<T>.Receive(CancellationToken)`
- `IConsumer.Acknowledge`, `AcknowledgeCumulative`, `Unsubscribe`,
  `RedeliverUnacknowledgedMessages`

Operations use `Task`/`ValueTask` and `CancellationToken`. Runtime objects are
`IAsyncDisposable` and expose state through `IStateHolder<TState>`.

### Supported Features

DotPulsar supports:

- Producers, consumers, readers, and typed schemas.
- Async producer send and consumer receive.
- Send channels for producer workflows.
- Acknowledgment, cumulative acknowledgment, redelivery, seek, and last message
  ID.
- Partition handling through sub-producers/sub-consumers.
- Topic lookup and partition metadata.
- Flow permits through consumer channels.
- Compression, chunking pipeline, schemas, and message routing.
- Authentication, encryption policy, TLS-style secure connection policy, and
  proxy-through-service-url behavior.
- State change monitoring for client-created producers, consumers, and readers.

### Missing Or Deferred Features

DotPulsar appears narrower than Java and Go. First-pass source review did not
show a separate admin API package or the same breadth around transactions,
interceptors, DLQ/retry policy, or metrics controls. It is still valuable because
it demonstrates a maintainable smaller official client.

### Internal Architecture

Important implementation classes include:

- `PulsarClient`: creates producers, consumers, readers, tracks disposables, and
  owns the connection pool.
- `ConnectionPool`: performs topic lookup, follows redirects, resolves
  encrypted vs unencrypted broker URLs, handles proxy-through-service-url, and
  stores connections by physical/logical URL.
- `Connection`: serializes commands, sends frames, owns `ChannelManager`,
  ping/pong keepalive, auth continuation, connection state, and stream IO.
- `ChannelManager`: maps producer and consumer IDs to channels and routes
  incoming broker commands to the right channel.
- `Producer`, `SubProducer`, `ProducerChannel`, `ProducerProcess`: producer
  state, send dispatch, reconnect/channel replacement, and pending send limits.
- `Consumer`, `SubConsumer`, `ConsumerChannel`, `ConsumerProcess`: receive,
  ack, redelivery, channel lifecycle, and reconnect behavior.
- `StateManager`, `ProcessManager`, event types, and state monitors: explicit
  lifecycle machinery.
- `CommandExtensions` and `Serializer`: command conversion and framing.

### Protocol And Transport

DotPulsar includes its protocol file at:

```text
.research/clients/pulsar-dotpulsar/src/DotPulsar/Internal/PulsarApi.proto
```

Command extension methods convert typed protocol commands into `BaseCommand`.
`Serializer` handles frame serialization. `Connection` exposes typed `Send`
overloads for connect, lookup, producer, subscribe, send, ack, flow, seek, close,
ping, and pong commands.

### Producer Flow

The producer flow is:

1. `PulsarClient.CreateProducer` validates options and compression support.
2. `Producer` creates a producer channel factory and sub-producers.
3. `ConnectionPool.FindConnectionForTopic` performs lookup.
4. `Connection.Send(CommandProducer, channel)` registers the channel.
5. Sends become `SendOp` values dispatched through `SubProducer`.
6. `ProducerChannel` sends `SendPackage` frames through `Connection`.
7. Send receipts complete the associated task.

### Consumer Flow

The consumer flow is:

1. `PulsarClient.CreateConsumer` builds a `Consumer`.
2. `Consumer` creates one or more `SubConsumer` instances.
3. `ConsumerChannelFactory` subscribes on a topic connection.
4. `ConsumerChannel` sends cached flow permits.
5. `ChannelManager` routes `CommandMessage` frames to the channel.
6. `Receive(CancellationToken)` awaits messages from the channel.
7. Ack and redelivery commands are sent through the channel connection.

### Reader Flow

Reader is first-class and shares much of the consumer channel machinery. This
reinforces that reader is a natural follow-up feature after producer/consumer,
but not required to validate the initial Ruby protocol path.

### Connection, Lookup, And Reconnect Behavior

DotPulsar's connection pool embeds lookup behavior rather than using a separate
lookup service object. It still preserves the same concepts: lookup command,
redirect loop, authoritative response handling, secure URL selection, logical vs
physical URL, and proxy-through-service-url.

Reconnect behavior is modeled through channel state, process managers, and
channel replacement. This is a clear reminder that producer/consumer objects
should survive connection replacement.

### Backpressure And Flow Control

DotPulsar exposes `MaxPendingMessages` on producer options and validates it.
Consumer channels cache `CommandFlow` and adjust permits as messages are
received. This again confirms producer queue limits and consumer permits as MVP
requirements.

### Error Handling

DotPulsar has a rich exception hierarchy with specific exceptions for auth,
authorization, checksum, configuration, connection security, topic not found,
producer busy/fenced, consumer busy, schema issues, unsupported version,
service-not-ready, disposed/closed/faulted states, and too-large messages.

For Ruby, this is the strongest official-client argument for exceptions rather
than result codes.

### Authentication And TLS

DotPulsar supports authentication abstractions and encryption policy choices:
prefer/enforce encrypted or unencrypted broker URLs based on lookup responses.
This is useful for Ruby's future TLS/auth design.

### Batching, Compression, And Chunking

DotPulsar has compression factories and a chunking pipeline. First-pass review
did not focus on batching details; compression/chunking should remain deferred
for Ruby.

### Schema Support

DotPulsar includes many schema implementations: byte arrays, strings, primitive
types, JSON, Avro, Protobuf, and schema definition helpers.

### Testing Strategy

Tests cover public client behavior, internal producers/consumers/readers,
message IDs, schemas, message processing, and integration fixtures. Samples
cover producing, consuming, and reading.

### Packaging And Distribution

DotPulsar is packaged as a .NET/NuGet library with documentation and release
process notes. The public/internal split is clean and worth mirroring with Ruby
namespaces.

### Lessons For The Ruby Client

- Explicit state objects are valuable: producer/consumer/reader states should be
  visible or at least testable.
- Cancellation tokens translate well into Ruby timeout/cancel options.
- Producer and consumer handles should survive channel/connection replacement.
- A smaller official client can still be valid if core protocol behavior is
  solid.
- Ruby should prefer explicit exceptions for public errors.

### Open Questions

- Should Ruby expose public state methods like `connected?`, `closed?`, and
  `state`, or keep state internal initially?
- Should Ruby have a producer send channel/enumerator API later?
- How much reconnect/channel replacement machinery belongs in MVP?

## Client Analysis: Node.js Client

### Summary

The Node.js client is not an independent binary-protocol implementation. It is a
Node addon built with `node-addon-api` that wraps the Pulsar C client, which in
turn wraps the C++ client. Its value for Ruby research is mostly API ergonomics,
async promise wrapping, TypeScript surface design, packaging, and the tradeoffs
of binding to the native C++ implementation.

Key source areas:

- `.research/clients/pulsar-client-node/src`
- `.research/clients/pulsar-client-node/index.d.ts`
- `.research/clients/pulsar-client-node/binding.gyp`
- `.research/clients/pulsar-client-node/pkg`

### Repository And Maintenance Signals

The standalone Apache Node.js client has recent maintenance, npm packaging,
TypeScript declarations, examples, tests, CI workflows, and scripts to download
or build the C++ client dependency. The README explicitly says the library uses
`node-addon-api` to wrap the C++ library.

### Public API

The public API is promise based:

- `new Pulsar.Client({ serviceUrl })`
- `client.createProducer(config) -> Promise<Producer>`
- `client.subscribe(config) -> Promise<Consumer>`
- `client.createReader(config) -> Promise<Reader>`
- `producer.send(message) -> Promise<MessageId>`
- `consumer.receive(timeout?) -> Promise<Message>`
- `consumer.acknowledge(message) -> Promise<null>`
- `reader.readNext(timeout?) -> Promise<Message>`

TypeScript declarations define the main public contract, including producer,
consumer, reader, message, message ID, auth, schema, compression, batching,
chunking, DLQ, key-shared, and TLS options.

### Supported Features

Because Node wraps the C/C++ client, its feature coverage is broad:

- Producers, consumers, readers.
- Promise-based async operations.
- Topic partitions.
- Ack, cumulative ack, negative ack, seek, batch receive.
- Batching, compression, chunking, schemas, Protobuf native schema helper.
- Token, TLS, Basic, Athenz, and OAuth2 auth wrappers.
- Encryption, crypto key readers, dead letter policy, key-shared policy.
- Listener callbacks for consumers/readers.
- TLS options, logging, and connection timeout.

### Missing Or Deferred Features

The Node client does not teach much about native protocol implementation because
that work is delegated to C/C++. It also inherits native addon installation and
platform packaging complexity.

### Internal Architecture

Important pieces include:

- `Client.js`: thin JavaScript wrapper that sets default CA cert path and
  delegates to native binding.
- `index.d.ts`: public TypeScript contract.
- `Client.cc`, `Producer.cc`, `Consumer.cc`, `Reader.cc`: N-API wrappers around
  the C client.
- `ThreadSafeDeferred`: bridge from C callbacks to JavaScript promises.
- `ProducerConfig`, `ConsumerConfig`, `ReaderConfig`: JavaScript option parsing
  into C client configuration.
- `Authentication*.js` and `Authentication.cc`: JS auth helpers and native auth
  binding.

### Protocol And Transport

Node does not directly expose protocol code. Transport, lookup, reconnect,
framing, batching, and flow-control behavior come from the C/C++ client.

### Producer Flow

The Node producer flow is:

1. JavaScript calls `client.createProducer`.
2. Native wrapper parses `ProducerConfig`.
3. Wrapper calls `pulsar_client_create_producer_async`.
4. C callback resolves or rejects a JavaScript promise.
5. `producer.send` builds a C message and calls `pulsar_producer_send_async`.
6. Send callback resolves a JavaScript `MessageId`.

### Consumer Flow

Consumer behavior follows the same wrapper pattern: JavaScript APIs call native
methods, native methods call C client async functions, and callbacks resolve
promises or invoke listeners.

### Reader Flow

Reader is exposed with `readNext`, `hasNext`, `seek`, `seekTimestamp`, `close`,
and listener options, again backed by the C client.

### Connection, Lookup, And Reconnect Behavior

These behaviors are inherited from the C/C++ client. Node mostly forwards config
such as operation timeout, concurrent lookup request, listener name, TLS, and
connection timeout into the native client.

### Backpressure And Flow Control

Node exposes producer `maxPendingMessages`, cross-partition pending limits,
`blockIfQueueFull`, consumer `receiverQueueSize`, and batch receive policy, but
the actual enforcement is native.

### Error Handling

Native C result codes are converted into rejected promises with string messages.
This is ergonomic enough for JavaScript examples, but less structured than
DotPulsar's exception hierarchy or Go/C++ result codes.

### Authentication And TLS

The Node wrapper exposes several auth helpers and TLS settings, mostly mapped
into C client configuration. It also writes Node's root certificates to a local
cert file when no trust cert path is supplied.

### Batching, Compression, And Chunking

Node exposes these options in TypeScript declarations and forwards them into the
C/C++ client. The implementation details are inherited.

### Schema Support

Node exposes schema info and a Protobuf native schema helper in JavaScript, with
native schema conversion support.

### Testing Strategy

Tests cover client, producer, consumer, reader, end-to-end behavior, encryption,
schemas, and Protobuf native schema. Examples cover common auth and producer /
consumer / reader workflows.

### Packaging And Distribution

Packaging is the central lesson. The Node client requires native addon builds
and C++ client artifacts. Scripts exist for Linux, macOS, and Windows, with CI
for build/release. This is powerful but significantly more complex than a pure
language implementation.

### Lessons For The Ruby Client

- A binding-based Ruby client could reach broad feature support faster, but
  native packaging would become the main project risk.
- A promise/future-style public API can be very small even when backed by rich
  native behavior.
- Type declarations are a good model for keeping the public API explicit.
- For the Ruby project goal, Node supports the current preference to start pure
  Ruby and use C/C++ bindings only as a fallback strategy.

### Open Questions

- Should Ruby ever provide an optional native adapter to the C client after a
  pure Ruby MVP exists?
- If Ruby stays pure, how do we keep installation as easy as the Node npm happy
  path without hiding required protocol complexity?

## Client Analysis: Python Client

### Summary

The Python client is based on the Pulsar C++ client and exposes it through a
PyBind11 extension named `_pulsar`, plus Python wrapper modules for schemas,
exceptions, table views, functions helpers, and asyncio. Like Node.js, it is not
an independent protocol implementation. Its main lessons for Ruby are packaging,
native binding ergonomics, and how a language wrapper can add a more idiomatic
async layer without owning the protocol.

Key source areas:

- `.research/clients/pulsar-client-python/src`
- `.research/clients/pulsar-client-python/pulsar`
- `.research/clients/pulsar-client-python/pulsar/asyncio.py`
- `.research/clients/pulsar-client-python/pkg`

### Repository And Maintenance Signals

The standalone Apache Python repository has recent release/build maintenance,
Python package files, CMake, PyBind11 integration, wheel build scripts, manylinux
and macOS packaging support, examples, tests, OAuth2 test setup, and standalone
broker test scripts.

### Public API

The Python API is object oriented and mostly synchronous by default:

- `pulsar.Client(...)`
- `client.create_producer(...)`
- `client.subscribe(...)`
- `client.create_reader(...)`
- `producer.send(...)`
- `producer.send_async(...)`
- `consumer.receive(...)`
- `consumer.acknowledge(...)`
- `reader.read_next(...)`

The `pulsar.asyncio` module wraps the same underlying `_pulsar` objects with
`async def` methods. It turns C++ async callbacks into `asyncio.Future` results.

### Supported Features

Because Python wraps the C++ client, it exposes broad feature coverage:

- Producers, consumers, readers, table views.
- Sync and async/callback APIs through `_pulsar`.
- Python `asyncio` producer and consumer wrappers.
- Topic partitions, regex/multi-topic subscriptions, schemas, and schema info.
- Ack, cumulative ack, negative ack, seek, batch receive.
- Batching, compression, chunking, encryption, crypto, auth, OAuth2, TLS.
- Auto cluster failover and service info provider support.
- Python schema helpers for bytes, Avro, Protobuf, and schema definitions.

### Missing Or Deferred Features

The first-pass review did not identify an independent Python admin client in
this repository. Native behavior is inherited from C++, so the Python source is
not useful for binary protocol architecture beyond wrapper boundaries.

### Internal Architecture

Important pieces include:

- `src/client.cc`, `producer.cc`, `consumer.cc`, `reader.cc`: PyBind11 bindings
  over C++ client objects.
- `src/exceptions.*`: maps C++ result/error behavior into Python exceptions.
- `src/config.cc`: maps Python options into C++ configurations.
- `pulsar/__init__.py`: Python-facing wrapper, docs, message ID wrapper, schema
  integration, and exported enums/classes.
- `pulsar/asyncio.py`: asyncio producer/consumer wrappers over `_pulsar`.
- `pulsar/schema`: Python schema helpers.

### Protocol And Transport

Python delegates protocol, transport, lookup, reconnect, batching, compression,
flow control, and most reliability behavior to the C++ client.

### Producer Flow

The synchronous producer flow wraps C++ async APIs and waits for completion:

1. Python calls `client.create_producer`.
2. PyBind layer calls C++ `createProducerAsync` and waits.
3. Python gets a `_pulsar.Producer` wrapper.
4. `producer.send` calls C++ `sendAsync` and waits.
5. Python `asyncio.Producer.send` creates an asyncio future and completes it
   from the C++ callback.

### Consumer Flow

Consumer creation and receive behavior are inherited from C++. Python wrappers
decode payloads through schema helpers and expose either blocking or asyncio
style calls.

### Reader Flow

Reader is exposed through bindings and Python wrappers, with tests and examples.

### Connection, Lookup, And Reconnect Behavior

These behaviors are inherited from C++. Python exposes related configuration
such as auto cluster failover, TLS, auth, and service info providers.

### Backpressure And Flow Control

Backpressure and flow control are enforced by the underlying C++ client. Python
exposes the relevant configuration and async wrappers.

### Error Handling

Python defines exceptions around the C++ result codes. The asyncio wrapper also
uses a `PulsarException` that carries the underlying `pulsar.Result`.

### Authentication And TLS

The Python repository includes auth/TLS test config, OAuth2 tests, and bindings
to C++ auth facilities.

### Batching, Compression, And Chunking

These are inherited from C++ and exposed through Python options.

### Schema Support

Python adds meaningful language-level value here: bytes, Avro, Protobuf, schema
definition helpers, and encode/decode integration in wrappers.

### Testing Strategy

Tests cover core Pulsar behavior, readers, table views, schemas, asyncio,
OAuth2, interrupted calls, logging, and failover. Test scripts start a Pulsar
standalone service.

### Packaging And Distribution

Packaging is the major concern. The README lists Python version support, C++
compiler, CMake, Pulsar C++ client library, and PyBind11 requirements. The repo
contains wheel build scripts and platform-specific packaging folders.

### Lessons For The Ruby Client

- Python confirms that language bindings can work, but packaging dominates.
- An idiomatic async wrapper can be layered above callback-based native APIs.
- If Ruby builds a pure client, schema helpers can still be written in Ruby even
  if protocol internals stay lower-level.
- Ruby should avoid requiring users to install CMake/C++ dependencies for the
  MVP if possible.

### Open Questions

- If pure Ruby performance is insufficient later, should Ruby use FFI to the C
  client, a native extension, or a separate adapter gem?
- Can Ruby provide an enumerator/fiber-friendly async wrapper after the blocking
  API, similar to Python's separate `asyncio` module?

## Client Analysis: Java Reactive Streams Client

### Summary

The Java Reactive Streams client is not a standalone protocol implementation.
It adapts an existing Java `PulsarClient` into a Reactive Streams / Project
Reactor API. Its main value for Ruby research is not protocol behavior, but
backpressure-aware API design, producer caching, message pipelines, and a
separate higher-level adapter layer.

Key source areas:

- `.research/clients/pulsar-client-reactive/pulsar-client-reactive-api`
- `.research/clients/pulsar-client-reactive/pulsar-client-reactive-adapter`
- `.research/clients/pulsar-client-reactive/pulsar-client-reactive-producer-cache-caffeine`

### Repository And Maintenance Signals

The Apache reactive client repository has recent dependency maintenance, Gradle
modules, CI, API and adapter modules, integration tests, a BOM, Jackson support,
producer cache modules, and Spring Boot integration notes.

### Public API

The public API is reactive and builder oriented:

- `AdaptedReactivePulsarClientFactory.create(pulsarClient)`
- `reactivePulsarClient.messageSender(schema).topic(...).maxInflight(...).build()`
- `messageSender.sendOne(MessageSpec.of(...)) -> Mono<MessageId>`
- Reactive consumers, readers, and pipelines use Reactor-style streams.

It uses `Mono`/`Flux` style composition rather than blocking calls.

### Supported Features

The reactive client supports higher-level reactive access to:

- Message sending.
- Message consuming.
- Message reading.
- Message pipelines.
- Producer caching.
- Reactive backpressure through max in-flight limits.
- Jackson configuration helpers.

Underlying Pulsar protocol features come from the wrapped Java client.

### Missing Or Deferred Features

This client is not a native wire-protocol reference. It should not drive Ruby
MVP internals, but it is useful for a future Ruby async/fiber/enumerator layer.

### Internal Architecture

Important pieces include:

- API module: reactive interfaces, specs, builders, message specs, message
  results, and pipelines.
- Adapter module: adapts Java `PulsarClient`, producers, consumers, and readers.
- Producer cache: default concurrent-map cache and optional Caffeine cache.
- `InflightLimiter`: local backpressure for in-flight messages.
- Integration tests: sender, consumer, reader, and pipeline E2E tests.

### Protocol And Transport

Protocol and transport are inherited from the Java client. This client adds API
and flow-control behavior above the official Java implementation.

### Producer Flow

Reactive sends can reuse cached producers. The sender applies `maxInflight` to
limit local pending messages and coordinate Reactive Streams demand with Pulsar
send completion.

### Consumer Flow

Reactive consumers adapt Java consumers into stream processing. Message results
represent how processed messages should be acked or otherwise completed.

### Reader Flow

Reactive readers adapt Java readers into stream-style APIs.

### Connection, Lookup, And Reconnect Behavior

Inherited from the Java client.

### Backpressure And Flow Control

This is the key contribution. The reactive client applies local reactive
backpressure above Pulsar's own producer pending-message and consumer permit
mechanisms. It shows that public API backpressure can be layered on top of the
core protocol client.

### Error Handling

Reactive APIs propagate errors through Reactor streams and specialized
exceptions such as message sending exceptions. Underlying broker/protocol errors
come from the Java client.

### Authentication And TLS

Inherited from the Java client.

### Batching, Compression, And Chunking

Inherited from the Java client and selected through adapted producer/consumer
configuration.

### Schema Support

The reactive API is schema-aware because it adapts the Java typed client.

### Testing Strategy

The repo includes API tests, adapter tests, producer cache tests, and end-to-end
integration tests for sender, consumer, reader, and pipelines.

### Packaging And Distribution

The project publishes multiple Gradle/Maven artifacts: API, adapter, BOM,
Jackson support, Caffeine producer cache, and shaded Caffeine cache.

### Lessons For The Ruby Client

- Keep the core Ruby client small first; a reactive/fiber/enumerator layer can
  be a separate adapter later.
- Producer caching is useful for high-level send APIs that publish to many
  topics.
- Backpressure exists at multiple layers: protocol permits, producer pending
  queues, and high-level stream demand.
- The core client should expose enough hooks for a future higher-level adapter
  without building that adapter in MVP.

### Open Questions

- Should a future Ruby async layer be part of the same gem or a companion gem?
- Would Ruby `Enumerator`, `Fiber::Scheduler`, or an `async`-gem adapter be the
  best equivalent to Reactive Streams?

## Final Cross-Client Comparison

### Implementation Categories

The official clients fall into three categories:

- Native protocol implementations: Java, C++, Go, DotPulsar.
- Native bindings over C/C++: Node.js, Python.
- Higher-level adapter over an official client: Java Reactive Streams.

For the Ruby project, the most useful implementation references are Java, C++,
Go, and DotPulsar. Node.js and Python are most useful for understanding native
binding tradeoffs. Java Reactive Streams is most useful for future high-level
async or streaming APIs.

### Common Public API Concepts

All core clients expose the same conceptual model:

- Client.
- Producer.
- Consumer.
- Reader.
- Message.
- Message ID.
- Configuration/options.
- Sync, async, callback, promise, or task-based operation style.

Producer and consumer are the required MVP objects. Reader is common enough that
Ruby should reserve API space for it, but it can be deferred.

### Common Internal Components

The native clients repeatedly converge on:

- Protocol command/frame layer.
- Connection object.
- Connection pool.
- Lookup service or lookup behavior.
- Request ID generation and response correlation.
- Producer registration and pending send queue.
- Consumer subscription and local receive queue.
- Flow permit accounting.
- Message ID model.
- Error/result mapping.
- Reconnect/channel replacement behavior.

These should be treated as core Ruby architecture, not optional refactors.

### Backpressure Common Ground

Every serious client has multiple backpressure layers:

- Producer pending-message limits.
- Queue-full behavior.
- Optional client-wide memory limits.
- Consumer receiver queues.
- Broker flow permits.
- Higher-level stream demand in the reactive adapter.

Ruby MVP must include producer queue limits and consumer flow permits. A
client-wide memory limit can be deferred, but the design should leave a place
for it.

### Error Handling Common Ground

Error designs vary by language:

- Java: typed exceptions and failed futures.
- C++: `Result` enum and newer `Error` values.
- Go: `error` plus result codes.
- DotPulsar: typed exceptions.
- Node/Python: native result codes mapped into rejected promises or exceptions.

Ruby should expose typed exceptions. Internally, it can keep a result-code style
mapping from broker/server errors to exception classes.

### Native Binding Tradeoff

Node.js and Python prove the binding path is viable, but both inherit native
dependency and release complexity:

- C++ client dependency.
- C/C++ compiler and build tooling.
- Platform-specific packaging.
- Binary artifact workflows.
- Harder install story for users.

For this project, the first implementation should remain a pure Ruby binary
protocol client unless later research or benchmarks show that it is impractical.

### Recommended Ruby Direction

Build a native Ruby client over the Pulsar binary protocol with a small public
surface:

- `Pulsar::Client`.
- `Pulsar::Producer`.
- `Pulsar::Consumer`.
- `Pulsar::Message`.
- `Pulsar::MessageId`.
- `Pulsar::Error` subclasses.

Internally, include:

- `Pulsar::Protocol`.
- `Pulsar::Connection`.
- `Pulsar::ConnectionPool`.
- `Pulsar::RPC`.
- `Pulsar::LookupService`.
- Producer pending-send queue.
- Consumer receive queue and permit accounting.
- Explicit connection/producer/consumer states.

### MVP Recommendation

The first Ruby MVP should focus on non-partitioned topics over `pulsar://`:

- Connect and complete Pulsar handshake.
- Lookup topic owner.
- Create producer.
- Send unbatched byte/string messages.
- Receive send receipts and return structured message IDs.
- Create consumer.
- Receive messages.
- Acknowledge messages.
- Replenish flow permits.
- Enforce basic producer and consumer queue limits.
- Support operation timeouts.
- Run integration tests against local Pulsar standalone.

Defer TLS, auth, partitioned topics, reader, batching, compression, chunking,
schemas, transactions, interceptors, admin API, metrics, and high-level async
adapters.

### Future API Layers

After the core client works, official clients suggest these follow-up layers:

- Reader.
- Partitioned topics.
- TLS/auth.
- Batching and compression.
- Schemas.
- Fiber/async/enumerator API.
- Optional stream-style adapter.
- Admin client.
- Optional native adapter only if needed.
