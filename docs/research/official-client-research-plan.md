# Official Client Research Plan

This plan defines how we will study the official Apache Pulsar clients before
designing the Ruby client. The goal is to analyze each client consistently, find
the common ground across implementations, and then turn that into a Ruby design.

## Goal

Build enough understanding of the official Pulsar clients to answer:

- What behavior is fundamental to every Pulsar client?
- Which architecture patterns repeat across implementations?
- Which client features are essential for a Ruby MVP?
- Which features can be deferred without painting the project into a corner?
- What Ruby-specific design choices should we make for API, concurrency,
  packaging, testing, and protocol support?

## Clients To Study

Study all official language-specific clients currently cloned under
`.research/clients/`:

- Java client: `.research/clients/pulsar`
- Java Reactive Streams client: `.research/clients/pulsar-client-reactive`
- C++ client: `.research/clients/pulsar-client-cpp`
- Python client: `.research/clients/pulsar-client-python`
- Go client: `.research/clients/pulsar-client-go`
- Node.js client: `.research/clients/pulsar-client-node`
- C#/DotPulsar client: `.research/clients/pulsar-dotpulsar`

## Research Order

1. Java client
2. C++ client
3. Go client
4. C#/DotPulsar client
5. Node.js client
6. Python client
7. Java Reactive Streams client
8. Final cross-client comparison and Ruby design summary

Java comes first because it is the most complete reference implementation and
lives near shared protocol definitions. C++, Go, and DotPulsar follow because
they are useful references for protocol, transport, and non-JVM architecture.
Node.js and Python are useful for packaging, bindings, and higher-level language
ergonomics. Java Reactive Streams comes later because it is more specialized and
most useful after the core client model is understood.

## Per-Client Analysis Template

Use the same structure for each client analysis:

```markdown
# Client Analysis: <Client Name>

## Summary

## Repository And Maintenance Signals

## Public API

## Supported Features

## Missing Or Deferred Features

## Internal Architecture

## Protocol And Transport

## Producer Flow

## Consumer Flow

## Reader Flow

## Connection, Lookup, And Reconnect Behavior

## Backpressure And Flow Control

## Error Handling

## Authentication And TLS

## Batching, Compression, And Chunking

## Schema Support

## Testing Strategy

## Packaging And Distribution

## Lessons For The Ruby Client

## Open Questions
```

## Cross-Client Comparison

After the per-client analysis is complete, create a final comparison that covers:

- Common public API concepts.
- Common internal components.
- Feature support differences.
- Protocol and transport implementation differences.
- Concurrency model differences.
- Error, retry, reconnect, and backpressure behavior.
- Testing approaches.
- Packaging and dependency tradeoffs.
- Lessons that directly shape the Ruby client design.

## Expected Research Documents

Create or update these documents as the research progresses:

- `official-client-comparison.md`: Per-client notes and cross-client comparison.
- `feature-matrix.md`: Feature support across official clients.
- `protocol-notes.md`: Binary protocol, lookup, producer, consumer, ack, and
  flow-control notes.
- `architecture-patterns.md`: Shared architecture patterns across clients.
- `ruby-design-considerations.md`: Ruby-specific API, concurrency, dependency,
  and packaging decisions.
- `mvp-scope.md`: Proposed first implementation scope for the Ruby client.

Each new Markdown document must be linked from `docs/research/overview.md`.

## Completion Criteria

The research phase is complete when:

- Every official client has been analyzed with the shared template.
- The feature matrix has enough detail to separate MVP, near-term, and deferred
  features.
- The protocol notes explain the minimum flows needed for producer, consumer,
  receive, ack, lookup, and reconnect.
- The architecture notes identify the components the Ruby client should include.
- The Ruby design notes make explicit recommendations for the first Ruby API and
  implementation approach.
- The MVP scope is specific enough to start implementation without re-deciding
  major design questions.
