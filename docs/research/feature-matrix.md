# Feature Matrix

This matrix tracks feature support and design notes across official Apache
Pulsar clients. It starts with the Java baseline and will be expanded as each
client is analyzed.

Legend:

- `Yes`: supported in the analyzed client.
- `Partial`: supported with important limits or through another layer.
- `Deferred`: candidate to defer in the Ruby MVP.
- `Pending`: not analyzed yet.

| Feature | Java | C++ | Go | C#/DotPulsar | Node.js | Python | Java Reactive Streams | Ruby MVP Candidate |
| --- | --- | --- | --- | --- | --- | --- | --- | --- |
| Client builder/configuration | Yes | Yes | Yes | Yes | Yes | Yes | Yes | Yes |
| Producer | Yes | Yes | Yes | Yes | Yes | Yes | Yes | Yes |
| Consumer | Yes | Yes | Yes | Yes | Yes | Yes | Yes | Yes |
| Reader | Yes | Yes | Yes | Yes | Yes | Yes | Yes | Maybe |
| Sync API | Yes | Yes | Yes | No | No | Yes | Pending | Yes |
| Async API | Yes | Yes | Partial | Yes | Yes | Yes | Yes | Internal first |
| Topic lookup | Yes | Yes | Yes | Yes | Yes | Yes | Via Java | Yes |
| Partitioned topics | Yes | Yes | Yes | Yes | Yes | Yes | Via Java | Deferred |
| Message IDs | Yes | Yes | Yes | Yes | Yes | Yes | Yes | Yes |
| Ack | Yes | Yes | Yes | Yes | Yes | Yes | Yes | Yes |
| Negative ack | Yes | Yes | Yes | No | Yes | Yes | Pending | Deferred |
| Cumulative ack | Yes | Yes | Yes | Yes | Yes | Yes | Pending | Deferred |
| Flow permits | Yes | Yes | Yes | Yes | Yes | Yes | Via Java | Yes |
| Producer backpressure | Yes | Yes | Yes | Yes | Yes | Yes | Yes | Yes |
| Batching | Yes | Yes | Yes | Pending | Yes | Yes | Pending | Deferred |
| Compression | Yes | Yes | Yes | Yes | Yes | Yes | Pending | Deferred |
| Chunking | Yes | Yes | Yes | Yes | Yes | Yes | Pending | Deferred |
| Schemas | Yes | Yes | Yes | Yes | Yes | Yes | Yes | Deferred |
| TLS | Yes | Yes | Yes | Yes | Yes | Yes | Via Java | Deferred |
| Authentication | Yes | Yes | Yes | Yes | Yes | Yes | Via Java | Deferred |
| Transactions | Yes | Partial | Yes | No | Pending | Pending | Via Java | Deferred |
| Interceptors | Yes | Yes | Yes | No | Pending | Pending | Via Java | Deferred |
| Admin API | Yes | No | Yes | No | No | No | No | Deferred |
| Metrics | Yes | Yes | Yes | Partial | Yes | Partial | Partial | Deferred |

## Initial Java Notes

The Java client supports almost every major client feature category. For Ruby,
the important MVP candidates are the features required for a real producer and
consumer over the binary protocol: configuration, connection, lookup, producer,
consumer, message ID, ack, flow permits, and queue limits.

## Initial C++ Notes

The C++ client confirms the same core MVP shape as Java: client configuration,
connection pool, binary lookup, producer, consumer, message ID, ack, flow
permits, and queue limits. Its public API is more compact and uses result codes
plus callbacks instead of Java builders and futures. It also has a C wrapper,
but pure Ruby binary protocol work remains the preferred first direction.

## Initial Go Notes

The Go client again confirms the core MVP shape. Its distinct contribution is a
simple blocking public API with contexts layered over asynchronous goroutines,
channels, request IDs, and connection handlers. For Ruby, this suggests blocking
methods with timeout/cancellation options can be ergonomic while internals remain
async.

## Initial DotPulsar Notes

DotPulsar contributes explicit state management, async disposal, cancellation
tokens, channel replacement, and typed exceptions. It appears narrower than Java
or Go, but validates that an official client can choose a smaller public surface
while still implementing native binary protocol behavior.

## Initial Node.js Notes

The Node.js client is a native addon over the Pulsar C/C++ client rather than an
independent protocol implementation. Its feature support mostly follows C++.
For Ruby, its most important lesson is the packaging tradeoff of native bindings:
feature depth comes quickly, but installation and release complexity rise.

## Initial Python Notes

The Python client also wraps the C++ client, this time through PyBind11 and a
Python wrapper package. Its `asyncio` module shows how a language-specific async
API can be layered over callback-based native behavior, but it reinforces that
native dependency packaging is a large burden.

## Initial Java Reactive Streams Notes

The Java Reactive Streams client adapts the Java client rather than replacing
it. Its key lesson is layered API design: a future Ruby async/streaming adapter
can sit above a smaller core client once the core protocol behavior is stable.
