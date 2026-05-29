# Technical Choices

This document records the first set of technical decisions for the pure Ruby
Apache Pulsar client MVP.

## Decision Summary

| Area | Decision For MVP |
| --- | --- |
| Implementation approach | Pure Ruby native binary protocol client |
| Public API style | Ruby keyword/options API |
| Concurrency model | Background network thread plus queues |
| Protobuf strategy | Generated Ruby classes with `google-protobuf` |
| Socket layer | Ruby stdlib `TCPSocket` for `pulsar://` |
| TLS/auth | Deferred, but connection API leaves extension points |
| Request primitive | Small internal future using `Mutex` and `ConditionVariable` |
| Dependencies | Minimize dependencies; prefer only `google-protobuf` initially |
| Frame encoding | Implement in Ruby from protocol docs and official client references |
| Integration tests | Local Pulsar standalone through Docker Compose |

## Implementation Approach

Decision: build a pure Ruby native binary protocol client for the MVP.

Why:

- Node.js and Python show that C/C++ bindings can provide broad feature coverage,
  but they shift a large part of the project into native dependency packaging.
- A pure Ruby client should be easier to install as a gem.
- The MVP scope is small enough to implement directly: connect, lookup, produce,
  consume, ack, permits, queue limits, and basic reconnect.
- The project goal is to design an idiomatic Ruby client, not only expose the C++
  client through Ruby.

C++ bindings remain a fallback option if pure Ruby proves impractical for
performance or protocol complexity.

## Public API Style

Decision: use a Ruby keyword/options API instead of Java-style builders.

Target MVP shape:

```ruby
client = Pulsar::Client.new("pulsar://localhost:6650")

producer = client.producer(
  topic: "persistent://public/default/test"
)

consumer = client.consumer(
  topic: "persistent://public/default/test",
  subscription: "ruby-sub"
)

message_id = producer.send("hello", timeout: 5)
message = consumer.receive(timeout: 5)
consumer.ack(message)
```

Why:

- Ruby keyword arguments are more idiomatic than builder chains for a compact MVP.
- The API still preserves official-client concepts: client, producer, consumer,
  message, and message ID.
- Future features can extend options without changing the basic shape.

## Concurrency Model

Decision: use a background network thread plus internal queues for the MVP.
See [Concurrency Model Analysis](concurrency-model.md) for the detailed
tradeoff report.

The public API can be blocking, while the connection thread continuously reads
broker frames and dispatches responses or messages to internal structures.

Expected internal pieces:

- One connection read loop per broker connection.
- A write path protected by a mutex or write queue.
- Pending request map keyed by request ID.
- Producer pending-send queue.
- Consumer receive queue.
- Shutdown path that wakes waiting calls and closes sockets.

Why:

- Pulsar requires a continuous read loop for send receipts, messages, close
  events, and reconnect signals.
- A fully blocking one-operation-at-a-time socket model does not fit broker push
  messages.
- A background thread avoids committing to a third-party async framework before
  the MVP proves the protocol path.

Deferred:

- Fiber scheduler integration.
- `async` gem adapter.
- Stream/enumerator API.

## Protobuf Strategy

Decision: use generated Ruby classes with the `google-protobuf` gem.

Why:

- Official clients use `PulsarApi.proto` as the command model.
- Generated classes reduce hand-written protocol mistakes.
- Hand-coding only the MVP commands would create migration risk when adding
  features such as batching, schemas, TLS/auth, and transactions.

Implementation notes:

- Keep the source `PulsarApi.proto` under a clearly documented location.
- Generate Ruby protocol classes as part of development.
- Commit generated protocol code unless generation becomes reliable and simple
  for every contributor.

Open follow-up:

- Decide exact proto source path and generation command during gem scaffolding.

## Socket And TLS Layer

Decision: use Ruby stdlib `TCPSocket` for MVP `pulsar://` support.

Why:

- The MVP only targets plaintext local standalone Pulsar.
- `TCPSocket` keeps dependencies small.
- TLS/auth can be layered later through `OpenSSL::SSL::SSLSocket` and auth
  provider objects.

Required extension points:

- Connection options should keep service URL, logical broker URL, physical broker
  URL, and TLS/auth settings separate.
- Do not hard-code plaintext assumptions into protocol or lookup objects.

## Request Primitive

Decision: implement a small internal future using `Mutex` and
`ConditionVariable`.

The future should support:

- Complete with value.
- Complete with error.
- Wait with timeout.
- Check completion state.
- Wake waiters during shutdown.

Why:

- Official clients all need request/response correlation.
- A tiny internal future avoids adding `concurrent-ruby` before there is a clear
  need.
- The primitive can later be wrapped by a fiber/async API.

## Dependency Policy

Decision: keep runtime dependencies minimal.

Expected initial runtime dependency:

- `google-protobuf`

Avoid for MVP:

- Native extensions.
- EventMachine.
- `async`.
- `concurrent-ruby`, unless the internal future/queue implementation becomes
  needlessly complex.

## Frame Encoding

Decision: implement Pulsar frame encoding and decoding in Ruby.

MVP frame work:

- Serialize `BaseCommand`.
- Prefix command size and frame size correctly.
- Encode message metadata and payload for sends.
- Decode incoming command frames.
- Decode incoming message metadata and payload.
- Route responses by request ID, producer ID, or consumer ID.

References:

- Apache binary protocol documentation.
- Java `ClientCnx` and protocol command usage.
- C++ `Commands` and `ClientConnection`.
- Go `internal/commands.go`, `rpc_client.go`, and `connection.go`.
- DotPulsar `Serializer`, `Connection`, and `ChannelManager`.

## Testing Environment

Decision: use Docker Compose with Pulsar standalone for integration tests.

Test layers:

- Unit tests for frame encoding/decoding.
- Unit tests for internal future, request map, message ID, and queue behavior.
- Integration tests against local Pulsar standalone.

MVP integration scenarios:

- Connect to `pulsar://localhost:6650`.
- Create producer.
- Create consumer.
- Send one message.
- Receive the message.
- Ack the message.
- Verify send returns a structured message ID.

## Deferred Decisions

These decisions are intentionally deferred until after MVP protocol proof:

- TLS implementation details.
- Authentication provider API.
- Partitioned topic behavior.
- Reader API.
- Batching and compression.
- Schema API.
- Transaction support.
- Admin client.
- Metrics and tracing.
- Fiber/async/reactive adapter.
- Optional C/C++ binding adapter.
