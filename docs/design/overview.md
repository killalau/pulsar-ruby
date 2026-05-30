# Design Overview

This folder contains design notes for the Ruby Apache Pulsar client.

## Contents

- [Concurrency Model Analysis](concurrency-model.md): Detailed analysis of the
  recommended background-thread concurrency model, including tradeoffs,
  examples, alternatives, and Ruby-specific risks.
- [Public API And Class Model](public-api-and-class-model.md): Proposed public
  Ruby API, public/internal class responsibilities, ownership rules, lifecycle
  states, and extension points before gem scaffolding.
- [Protocol Definitions](protocol-definitions.md): Source, generation workflow,
  packaging rule, and runtime dependency notes for Pulsar protobuf classes.
- [Technical Choices](technical-choices.md): Analysis and recommended decisions
  for Ruby implementation choices before gem scaffolding.
