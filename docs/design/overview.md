# Design Overview

This folder contains design notes for the Ruby Apache Pulsar client.

## Contents

- [Concurrency Model Analysis](concurrency-model.md): Detailed analysis of the
  recommended background-thread concurrency model, including tradeoffs,
  examples, alternatives, and Ruby-specific risks.
- [Connection Handshake](connection-handshake.md): Implemented connect
  handshake, request ID allocation, pending request routing, background reader
  behavior, broker error mapping, ping/pong, and connection-loss handling.
- [Command Factory](command-factory.md): Internal protobuf command construction
  for producer, send, subscribe, flow, ack, lookup, close, and pong commands.
- [Consumer Implementation](consumer-impl.md): Internal subscription creation,
  flow permits, permit replenishment, queued receive behavior, ack, and close
  commands.
- [Frame Codec](frame-codec.md): Implemented binary frame encoding and decoding
  behavior, current limits, and validation rules.
- [Lookup Service](lookup-service.md): Binary topic lookup behavior, current
  client wiring, and remaining lookup limitations.
- [Public API And Class Model](public-api-and-class-model.md): Proposed public
  Ruby API, public/internal class responsibilities, ownership rules, lifecycle
  states, and extension points before gem scaffolding.
- [Reconnect Policy](reconnect-policy.md): MVP reconnect behavior, retry
  boundaries, and deferred full-retry work.
- [Protocol Definitions](protocol-definitions.md): Source, generation workflow,
  packaging rule, and runtime dependency notes for Pulsar protobuf classes.
- [Producer Implementation](producer-impl.md): Internal producer creation,
  unbatched send behavior, message receipt mapping, close commands, pending-send
  limits, and remaining producer work.
- [Technical Choices](technical-choices.md): Analysis and recommended decisions
  for Ruby implementation choices before gem scaffolding.
- [TCP Transport](tcp-transport.md): Plaintext TCP transport boundary,
  implemented behavior, error mapping, and deferred transport features.
