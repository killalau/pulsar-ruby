# Official Apache Pulsar Links

This page collects official Apache Pulsar references that are useful while
researching and designing a Ruby client.

## Core Project

- [Apache Pulsar homepage](https://pulsar.apache.org/): Project overview,
  concepts, and links into the documentation.
- [Apache Pulsar GitHub repository](https://github.com/apache/pulsar): Main
  Apache Pulsar source repository.

## Client Development

- [Pulsar client libraries](https://pulsar.apache.org/docs/client-libraries/):
  Current official and third-party client library list. This is useful for
  checking which languages are first-class and for comparing client capabilities.
- [Developing the Pulsar binary protocol](https://pulsar.apache.org/docs/next/developing-binary-protocol/):
  Official development documentation for Pulsar's binary protocol. This is one
  of the key references if this project implements a native Ruby client.

## Protocol And APIs

- [Pulsar concepts](https://pulsar.apache.org/docs/concepts-overview/):
  Official conceptual model for topics, subscriptions, producers, consumers, and
  message retention behavior.
- [Pulsar WebSocket API](https://pulsar.apache.org/docs/client-libraries-websocket/):
  Official language-agnostic WebSocket interface. This may be useful for a
  simpler Ruby client surface or for early compatibility testing.
- [Pulsar Admin API](https://pulsar.apache.org/admin-rest-api/): Official REST
  API reference for administrative operations.

## Compatibility And Feature Comparison

- [Client feature matrix](https://pulsar.apache.org/docs/client-feature-matrix/):
  Official matrix for comparing feature support across client libraries.
