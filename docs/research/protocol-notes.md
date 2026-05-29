# Protocol Notes

This document tracks Pulsar binary protocol findings that matter for a native
Ruby client.

## Current Status

Java client baseline started. Other official clients are pending.

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
