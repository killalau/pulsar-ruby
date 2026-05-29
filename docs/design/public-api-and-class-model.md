# Public API And Class Model

This document defines the initial public API and class relationships for the
Ruby Apache Pulsar client MVP. It should guide gem scaffolding so the first code
shape matches the design instead of needing immediate rearrangement.

## Goals

- Present a small, idiomatic Ruby API.
- Keep protocol, threading, transport, and generated protobuf details internal.
- Make the MVP easy to use for basic producer and consumer workflows.
- Leave clear extension points for Fiber support, TLS/auth, partitioned topics,
  batching, schemas, and reconnect improvements.

## Public API Principles

- Prefer keyword arguments over builder objects.
- Public methods are blocking by default and accept timeouts where they wait.
- Public objects expose Pulsar concepts, not implementation mechanics.
- Errors are raised as typed Ruby exceptions.
- Close methods are explicit and idempotent.
- Internal threads, queues, futures, request IDs, frames, and protobuf commands
  are not visible to users.

## MVP Public API Example

```ruby
client = Pulsar::Client.new("pulsar://localhost:6650")

producer = client.producer(
  topic: "persistent://public/default/orders"
)

consumer = client.consumer(
  topic: "persistent://public/default/orders",
  subscription: "orders-ruby"
)

message_id = producer.send("created", timeout: 5)
message = consumer.receive(timeout: 5)

puts message.payload
consumer.ack(message)

producer.close
consumer.close
client.close
```

The MVP should also support block-style cleanup:

```ruby
Pulsar::Client.open("pulsar://localhost:6650") do |client|
  producer = client.producer(topic: "persistent://public/default/orders")
  producer.send("created")
end
```

## Public Classes

### `Pulsar::Client`

The top-level user entry point.

Responsibilities:

- Parse and hold client-level configuration.
- Own the internal runtime and connection pool.
- Create producers and consumers.
- Close all owned producers, consumers, and connections.

Important methods:

```ruby
Pulsar::Client.new(service_url, **options)
Pulsar::Client.open(service_url, **options) { |client| ... }

client.producer(topic:, **options)
client.consumer(topic:, subscription:, **options)
client.close
client.closed?
```

Initial options:

- `operation_timeout`: default timeout for broker operations.
- `connection_timeout`: timeout for opening broker connections.
- `logger`: optional logger.

Deferred options:

- `authentication`
- `tls`
- `listener_name`
- `proxy_url`
- `stats_interval`

### `Pulsar::Producer`

Public handle for sending messages to one topic.

Responsibilities:

- Provide blocking send methods.
- Expose producer topic and closed state.
- Close the broker-side producer.

Important methods:

```ruby
producer.topic
producer.send(payload, properties: {}, key: nil, event_time: nil, timeout: nil)
producer.close
producer.closed?
```

MVP behavior:

- `payload` may be a `String` or bytes-like object.
- Sends are unbatched.
- `send` returns a `Pulsar::MessageId`.
- Send timeout raises `Pulsar::TimeoutError`.

Deferred behavior:

- Async send.
- Batching.
- Compression.
- Chunking.
- Schema-aware message encoding.
- Partition routing.

### `Pulsar::Consumer`

Public handle for receiving messages from one topic subscription.

Responsibilities:

- Provide blocking receive.
- Ack messages.
- Manage flow permits through the internal implementation.
- Close the broker-side consumer.

Important methods:

```ruby
consumer.topic
consumer.subscription
consumer.receive(timeout: nil)
consumer.ack(message)
consumer.close
consumer.closed?
```

MVP behavior:

- `receive` returns a `Pulsar::Message`.
- `receive(timeout:)` raises `Pulsar::TimeoutError` when no message arrives.
- `ack` accepts a `Pulsar::Message` or `Pulsar::MessageId`.

Deferred behavior:

- Negative ack.
- Cumulative ack.
- Redelivery controls.
- Dead letter policy.
- Multi-topic consumer.
- Pattern subscription.

### `Pulsar::Message`

Immutable received message object.

Responsibilities:

- Expose payload and metadata.
- Carry the message ID needed for ack.

Important methods:

```ruby
message.payload
message.message_id
message.properties
message.key
message.topic
message.publish_time
message.event_time
```

MVP behavior:

- Payload is returned as a binary `String`.
- Metadata fields are exposed when present.

### `Pulsar::MessageId`

Value object representing a Pulsar message position.

Responsibilities:

- Represent ledger ID, entry ID, partition index, and batch index.
- Support equality and readable inspection.
- Support use as an ack target.

Important methods:

```ruby
message_id.ledger_id
message_id.entry_id
message_id.partition_index
message_id.batch_index
```

### `Pulsar::Error` Hierarchy

Typed exceptions should make operational failures rescuable.

Initial hierarchy:

```ruby
Pulsar::Error
Pulsar::TimeoutError
Pulsar::ConnectionError
Pulsar::ClosedError
Pulsar::AuthenticationError
Pulsar::AuthorizationError
Pulsar::TopicNotFoundError
Pulsar::ProducerBusyError
Pulsar::ConsumerBusyError
Pulsar::BrokerError
Pulsar::ProtocolError
Pulsar::ConfigurationError
```

`AuthenticationError` can exist before auth support as a mapped broker error.

## Internal Class Model

Public classes should be thin wrappers around internal implementations. The
internal classes can evolve without changing the user API.

```text
Pulsar::Client
  owns Internal::ClientImpl
    owns Internal::Runtime
    owns Internal::ConnectionPool
      owns Internal::Connection
        owns Internal::Transport
        owns Internal::FrameCodec
        owns request map and reader thread

Pulsar::Producer
  wraps Internal::ProducerImpl
    uses Internal::Connection
    tracks producer id, sequence id, pending sends

Pulsar::Consumer
  wraps Internal::ConsumerImpl
    uses Internal::Connection
    tracks consumer id, permits, receive queue
```

## Internal Classes

### `Pulsar::Internal::ClientImpl`

Coordinates the internal runtime, lookup service, connection pool, producers,
and consumers.

Responsibilities:

- Hold normalized client configuration.
- Allocate producer and consumer IDs.
- Own cleanup order.
- Hide internal state from `Pulsar::Client`.

### `Pulsar::Internal::Runtime`

Abstraction over concurrency primitives.

MVP implementation:

- `Pulsar::Internal::ThreadRuntime`

Responsibilities:

- Create promises.
- Create bounded queues.
- Spawn and stop background work.
- Provide timeout helpers.
- Wake blocked operations during shutdown.

This boundary keeps future Fiber support possible without rewriting producer
and consumer public APIs.

### `Pulsar::Internal::Transport`

Abstraction over socket I/O.

MVP implementation:

- `Pulsar::Internal::TcpTransport`

Responsibilities:

- Connect to a physical broker address.
- Read exact byte counts.
- Write encoded frames.
- Close the socket.

Future implementations:

- `TlsTransport`
- Fiber-scheduler-aware transport
- Proxy-aware transport

### `Pulsar::Internal::ConnectionPool`

Owns broker connections and lookup results.

MVP responsibilities:

- Open a connection to the service URL broker.
- Reuse the connection for non-partitioned producer and consumer operations.
- Close all connections.

Deferred responsibilities:

- Topic lookup redirection.
- Partition metadata.
- Multiple broker connections.
- Connection eviction and reconnection policy.

### `Pulsar::Internal::Connection`

Owns one broker connection and its protocol state.

Responsibilities:

- Open transport.
- Perform connect handshake.
- Allocate request IDs.
- Maintain pending request map.
- Start and stop reader loop.
- Serialize writes.
- Route decoded commands to producer/consumer handlers.
- Fail pending operations on close or protocol error.

The connection is the main owner of threading details in the MVP.

### `Pulsar::Internal::FrameCodec`

Encodes and decodes Pulsar binary protocol frames.

Responsibilities:

- Encode command frames.
- Encode command plus metadata plus payload frames.
- Decode command frames.
- Decode message metadata and payload frames.
- Validate frame sizes.

It should not know about producer, consumer, or connection lifecycle.

### `Pulsar::Internal::CommandFactory`

Builds protobuf commands for the MVP operations.

Responsibilities:

- Connect command.
- Producer creation command.
- Subscribe command.
- Send command metadata.
- Flow command.
- Ack command.
- Close producer and close consumer commands.

This keeps generated protobuf details out of producer and consumer logic.

### `Pulsar::Internal::ProducerImpl`

State machine for one producer.

Responsibilities:

- Create broker-side producer.
- Track producer ID and sequence IDs.
- Maintain pending send promises.
- Convert send receipts into public `MessageId` values.
- Fail pending sends on close or connection loss.

### `Pulsar::Internal::ConsumerImpl`

State machine for one consumer.

Responsibilities:

- Create broker-side subscription.
- Track consumer ID.
- Maintain receive queue.
- Send flow permits.
- Convert incoming frames into public `Message` values.
- Send ack commands.
- Fail waiting receives on close.

## Ownership Rules

- `Pulsar::Client` owns all producers and consumers it creates.
- Closing a client closes its producers, consumers, and connections.
- Closing a producer or consumer removes it from the client ownership set.
- Public objects may hold internal implementations, but internal objects should
  not expose themselves through the public API.
- The connection owns request IDs and the request map.
- Producer and consumer IDs are allocated by `ClientImpl` or a shared internal
  ID allocator.

## Lifecycle Rules

### Client

```text
initialized -> open -> closing -> closed
```

`Client.new` may eagerly open a connection or defer connection until producer or
consumer creation. The MVP should prefer lazy broker connection so client
construction only validates local configuration.

### Producer

```text
initialized -> creating -> ready -> closing -> closed
                         -> failed
```

Producer creation should block until the broker confirms creation or the
operation times out.

### Consumer

```text
initialized -> subscribing -> ready -> closing -> closed
                           -> failed
```

Consumer creation should block until the broker confirms subscription or the
operation times out.

## Public API Decisions To Seal Before Scaffolding

These should be treated as initial decisions:

- `Pulsar::Client.new(service_url, **options)`
- `Pulsar::Client.open(service_url, **options) { |client| ... }`
- `client.producer(topic:, **options)`
- `client.consumer(topic:, subscription:, **options)`
- `producer.send(payload, **options)`
- `consumer.receive(timeout: nil)`
- `consumer.ack(message_or_message_id)`
- `close` and `closed?` on client, producer, and consumer.
- Typed exceptions under `Pulsar::Error`.

These should remain deferred:

- Builder API.
- Async public API.
- Fiber-native public API.
- Schema-specific producer/consumer classes.
- Admin API.

## Extension Points

### Fiber Runtime

The public API should not expose thread primitives. A future Fiber-aware version
can add an internal runtime and transport implementation while preserving the
same public `Client`, `Producer`, and `Consumer` objects.

### TLS And Authentication

TLS and auth should be modeled as client options, but the MVP implementation can
raise `ConfigurationError` if they are provided before support is implemented.
The transport and command factory boundaries are the right places to add them.

### Partitioned Topics

Partitioned topics should be added behind `ProducerImpl` and `ConsumerImpl`.
The public API should not need to change when a topic resolves to multiple
partitions.

### Batching And Compression

Batching and compression should be internal producer features controlled by
producer options. `producer.send` should keep the same return behavior.

### Schemas

Schema support can be added later through producer and consumer options. The MVP
message object should keep payload bytes simple so schema decoding does not
affect early protocol work.

## Open Questions

- Should `Client.new` connect eagerly or lazily? Recommendation: lazy.
- Should `receive(timeout: nil)` block forever when timeout is nil, or inherit
  the client operation timeout? Recommendation: inherit operation timeout unless
  the user explicitly passes `timeout: false` or a similar sentinel later.
- Should `producer.send` accept symbols or only strings for property keys?
  Recommendation: accept strings initially and normalize later if needed.
- Should `close` wait for in-flight sends by default? Recommendation: yes, up
  to the operation timeout.
- Should public objects expose state symbols beyond `closed?`? Recommendation:
  not for MVP.

## Scaffolding Implication

The gem scaffold should create public files and internal files separately:

```text
lib/pulsar.rb
lib/pulsar/client.rb
lib/pulsar/producer.rb
lib/pulsar/consumer.rb
lib/pulsar/message.rb
lib/pulsar/message_id.rb
lib/pulsar/errors.rb

lib/pulsar/internal/client_impl.rb
lib/pulsar/internal/thread_runtime.rb
lib/pulsar/internal/connection_pool.rb
lib/pulsar/internal/connection.rb
lib/pulsar/internal/tcp_transport.rb
lib/pulsar/internal/frame_codec.rb
lib/pulsar/internal/command_factory.rb
lib/pulsar/internal/producer_impl.rb
lib/pulsar/internal/consumer_impl.rb
```

This split gives the project a clean first shape and protects users from
internal design changes while the protocol implementation matures.
