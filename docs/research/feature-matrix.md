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
| Client builder/configuration | Yes | Pending | Pending | Pending | Pending | Pending | Pending | Yes |
| Producer | Yes | Pending | Pending | Pending | Pending | Pending | Pending | Yes |
| Consumer | Yes | Pending | Pending | Pending | Pending | Pending | Pending | Yes |
| Reader | Yes | Pending | Pending | Pending | Pending | Pending | Pending | Maybe |
| Sync API | Yes | Pending | Pending | Pending | Pending | Pending | Pending | Yes |
| Async API | Yes | Pending | Pending | Pending | Pending | Pending | Pending | Internal first |
| Topic lookup | Yes | Pending | Pending | Pending | Pending | Pending | Pending | Yes |
| Partitioned topics | Yes | Pending | Pending | Pending | Pending | Pending | Pending | Deferred |
| Message IDs | Yes | Pending | Pending | Pending | Pending | Pending | Pending | Yes |
| Ack | Yes | Pending | Pending | Pending | Pending | Pending | Pending | Yes |
| Negative ack | Yes | Pending | Pending | Pending | Pending | Pending | Pending | Deferred |
| Cumulative ack | Yes | Pending | Pending | Pending | Pending | Pending | Pending | Deferred |
| Flow permits | Yes | Pending | Pending | Pending | Pending | Pending | Pending | Yes |
| Producer backpressure | Yes | Pending | Pending | Pending | Pending | Pending | Pending | Yes |
| Batching | Yes | Pending | Pending | Pending | Pending | Pending | Pending | Deferred |
| Compression | Yes | Pending | Pending | Pending | Pending | Pending | Pending | Deferred |
| Chunking | Yes | Pending | Pending | Pending | Pending | Pending | Pending | Deferred |
| Schemas | Yes | Pending | Pending | Pending | Pending | Pending | Pending | Deferred |
| TLS | Yes | Pending | Pending | Pending | Pending | Pending | Pending | Deferred |
| Authentication | Yes | Pending | Pending | Pending | Pending | Pending | Pending | Deferred |
| Transactions | Yes | Pending | Pending | Pending | Pending | Pending | Pending | Deferred |
| Interceptors | Yes | Pending | Pending | Pending | Pending | Pending | Pending | Deferred |
| Admin API | Yes | Pending | Pending | Pending | Pending | Pending | Pending | Deferred |
| Metrics | Yes | Pending | Pending | Pending | Pending | Pending | Pending | Deferred |

## Initial Java Notes

The Java client supports almost every major client feature category. For Ruby,
the important MVP candidates are the features required for a real producer and
consumer over the binary protocol: configuration, connection, lookup, producer,
consumer, message ID, ack, flow permits, and queue limits.
