# Protocol Notes

This document tracks Pulsar binary protocol findings that matter for a native
Ruby client.

## Current Status

Java, C++, Go, and DotPulsar provide native protocol references. Node.js and
Python inherit protocol behavior from C/C++. Java Reactive Streams inherits
protocol behavior from Java.

## Java Client Findings

The Java client implements the Pulsar binary protocol directly. The shared
protocol file is:

```text
.research/clients/pulsar/pulsar-common/src/main/proto/PulsarApi.proto
```

Core commands for an MVP producer/consumer path:

- `CommandConnect`
- `CommandConnected`
- `CommandLookupTopic`
- `CommandPartitionedTopicMetadata`
- `CommandProducer`
- `CommandProducerSuccess`
- `CommandSend`
- `CommandSendReceipt`
- `CommandSendError`
- `CommandSubscribe`
- `CommandMessage`
- `CommandAck`
- `CommandAckResponse`
- `CommandFlow`

## Minimum Producer Sequence

1. Open TCP connection to the Pulsar service URL.
2. Send `CommandConnect`.
3. Receive `CommandConnected`.
4. Lookup topic owner with `CommandLookupTopic`.
5. Connect to the owning broker if lookup redirects.
6. Register producer with `CommandProducer`.
7. Receive `CommandProducerSuccess`.
8. Send message metadata and payload with `CommandSend`.
9. Receive `CommandSendReceipt` or `CommandSendError`.

## Minimum Consumer Sequence

1. Open TCP connection to the Pulsar service URL.
2. Send `CommandConnect`.
3. Receive `CommandConnected`.
4. Lookup topic owner with `CommandLookupTopic`.
5. Connect to the owning broker if lookup redirects.
6. Subscribe with `CommandSubscribe`.
7. Send initial permits with `CommandFlow`.
8. Receive messages with `CommandMessage`.
9. Acknowledge messages with `CommandAck`.
10. Replenish permits with `CommandFlow` as messages leave the local queue.

## Ruby Implications

- Generate or otherwise maintain Ruby protobuf bindings for `PulsarApi.proto`.
- Keep frame encoding/decoding isolated from producer and consumer behavior.
- Model request IDs because lookup, metadata, producer registration, and ack
  responses are correlated asynchronously.
- Model message IDs early, including ledger ID, entry ID, partition, and batch
  fields, even if batch support is deferred.
- Treat flow permits as an MVP requirement for consumers.

## C++ Client Findings

The C++ client carries its own copy of the protocol file:

```text
.research/clients/pulsar-client-cpp/proto/PulsarApi.proto
```

Its `Commands` helper builds binary protocol frames for connect, lookup,
partition metadata, producer registration, subscribe, send, ack, flow, ping,
pong, seek, unsubscribe, and close operations. This reinforces the idea that the
Ruby client should have a dedicated protocol command builder.

Transport and framing are handled by `ClientConnection`, using Boost.Asio or
standalone Asio. It owns TCP connect, optional TLS handshake, Pulsar
connect/auth handshake, command reads, incoming message parsing, checksum
verification, pending request completion, and write completion.

Additional Ruby implications from C++:

- Keep command construction separate from socket IO.
- Keep logical and physical broker addresses separate to leave room for proxy
  and redirected-cluster behavior.
- Represent server/protocol errors with enough detail to map them into Ruby
  exceptions.
- Add checksum handling to the protocol roadmap, even if MVP starts with the
  smallest path needed for local standalone compatibility.

## Go Client Findings

The Go client uses generated protobuf bindings from:

```text
.research/clients/pulsar-client-go/pulsar/internal/pulsar_proto/PulsarApi.proto
```

Its protocol responsibilities are split across:

- `internal/commands.go`: builds `BaseCommand` messages.
- `internal/rpc_client.go`: owns request IDs, producer IDs, consumer IDs, request
  timeout, request/response correlation, and lookup service creation.
- `internal/connection.go`: owns TCP/TLS connection, handshake, pending request
  map, write queue, consumer handlers, producer listeners, keepalive, and close
  notification.
- `internal/lookup_service.go`: owns lookup, redirects, partition metadata,
  schema lookup, and logical/physical broker address selection.

Go confirms that a native Ruby client should probably have a thin RPC layer
between raw connection IO and producer/consumer objects. That layer can own
request IDs, timeouts, and response correlation.

## DotPulsar Client Findings

DotPulsar keeps the protocol file at:

```text
.research/clients/pulsar-dotpulsar/src/DotPulsar/Internal/PulsarApi.proto
```

Protocol responsibilities are split across:

- `CommandExtensions`: converts typed commands into `BaseCommand`.
- `Serializer`: serializes commands, metadata, and payload frames.
- `Connection`: has typed `Send` overloads for protocol commands and owns
  stream writes.
- `ChannelManager`: correlates producer and consumer IDs with active channels
  and routes broker responses/messages.
- `ConnectionPool`: performs lookup, redirect handling, secure URL selection,
  and logical/physical URL handling.

DotPulsar does not expose a separate RPC object like Go, but its
`Connection`/`ChannelManager` split provides the same essential request and
channel correlation behavior.

## Binding And Adapter Clients

Node.js and Python do not add independent protocol implementations. They bind to
the C/C++ client and therefore inherit lookup, connection, reconnect, framing,
send, receive, ack, flow-control, batching, compression, and TLS/auth behavior
from C++.

Java Reactive Streams does not add a protocol implementation either. It adapts
the Java client into a Reactive Streams API.

## Protocol Conclusion For Ruby

The core protocol references for Ruby should be:

1. Java for full behavior and compatibility expectations.
2. C++ for native transport, command builder, and queueing details.
3. Go for RPC/request correlation and goroutine/channel separation.
4. DotPulsar for connection/channel state and typed command sending.

Ruby should not model its protocol layer after Node.js, Python, or Java Reactive
Streams because those projects delegate protocol behavior to another client.
