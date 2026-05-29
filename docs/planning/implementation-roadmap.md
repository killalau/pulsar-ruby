# Implementation Roadmap

This roadmap turns the current research and design notes into ordered build
steps for the pure Ruby Apache Pulsar client MVP.

## Roadmap Principles

- Build narrow, testable layers.
- Keep public API files separate from internal implementation files.
- Prove protocol correctness before adding convenience features.
- Commit each completed milestone independently.
- Keep documentation updated as decisions change during implementation.

## Milestone 1: Gem Scaffold

Goal: create the project shape without implementing broker behavior yet.

Tasks:

- Create gem structure under `lib/`.
- Add `Pulsar` module and version file.
- Add public shell classes:
  - `Pulsar::Client`
  - `Pulsar::Producer`
  - `Pulsar::Consumer`
  - `Pulsar::Message`
  - `Pulsar::MessageId`
  - `Pulsar::Error` hierarchy
- Add internal namespace under `Pulsar::Internal`.
- Add RSpec setup.
- Add basic tests for requiring the gem and constructing value objects.

Exit criteria:

- `bundle exec rspec` runs.
- Public files match the class model document.
- No network or Pulsar broker required.

## Milestone 2: Public API Shell

Goal: seal the first public API behavior before protocol code arrives.

Tasks:

- Implement `Pulsar::Client.new`.
- Implement `Pulsar::Client.open` block cleanup.
- Implement `close` and `closed?` semantics for client shell.
- Implement immutable `Message`.
- Implement comparable `MessageId`.
- Define timeout/configuration defaults.
- Raise `ConfigurationError` for unsupported TLS/auth options during MVP.

Exit criteria:

- Public API examples from the design doc run against stub internals where
  possible.
- Close behavior is idempotent.
- Errors are typed.

## Milestone 3: Internal Runtime

Goal: build the thread-based primitives before socket work.

Tasks:

- Implement `Pulsar::Internal::ThreadRuntime`.
- Implement internal `Promise`.
- Implement bounded queue helper or wrapper.
- Add timeout behavior.
- Add shutdown/wakeup behavior.
- Add tests for:
  - fulfill
  - reject
  - timeout
  - double completion
  - shutdown while waiting
  - bounded queue behavior

Exit criteria:

- Runtime tests cover the core concurrency risks.
- No socket or protobuf code required.

## Milestone 4: Protocol Definitions

Goal: bring in Pulsar protobuf definitions and make command objects available.

Tasks:

- Copy or vendor the upstream `PulsarApi.proto` source in a documented location.
- Add `google-protobuf` dependency.
- Decide and document generated file location.
- Generate Ruby protobuf classes.
- Add a repeatable generation command or Rake task.
- Add tests that instantiate key command classes.

Exit criteria:

- Generated protobuf code loads successfully.
- Code generation can be repeated by a contributor.
- Documentation names the upstream source and generation workflow.

## Milestone 5: Frame Codec

Goal: encode and decode Pulsar binary protocol frames.

Tasks:

- Implement `Pulsar::Internal::FrameCodec`.
- Encode command-only frames.
- Encode command plus metadata plus payload frames.
- Decode command-only frames.
- Decode message frames into command, metadata, and payload parts.
- Validate frame and command sizes.
- Add unit tests using known frame fixtures or generated round trips.

Exit criteria:

- Frame codec has focused unit coverage.
- Codec does not depend on connection, producer, or consumer state.

## Milestone 6: TCP Transport

Goal: implement plaintext broker I/O behind an internal abstraction.

Tasks:

- Implement `Pulsar::Internal::TcpTransport`.
- Connect to `pulsar://` host and port.
- Read exact byte counts.
- Write encoded bytes.
- Close idempotently.
- Map socket errors to typed internal errors.

Exit criteria:

- Transport can be unit-tested with a local TCP test server.
- TLS/auth remain explicitly unsupported.

## Milestone 7: Connection Handshake

Goal: connect to a broker and complete the Pulsar command handshake.

Tasks:

- Implement `Pulsar::Internal::Connection`.
- Allocate request IDs.
- Start background reader thread.
- Send connect command.
- Route connected/error responses.
- Maintain pending request map.
- Implement ping/pong if needed for stable integration tests.
- Fail pending requests on close.

Exit criteria:

- Integration test can connect to local Pulsar standalone.
- Failed connection produces typed errors.
- Closing wakes waiting operations.

## Milestone 8: Producer MVP

Goal: send one unbatched message and receive a broker receipt.

Tasks:

- Implement `Pulsar::Internal::ProducerImpl`.
- Implement public `client.producer`.
- Send producer creation command.
- Track producer ID.
- Track sequence IDs.
- Encode message metadata and payload.
- Handle send receipts.
- Return public `Pulsar::MessageId`.
- Enforce max pending sends.

Exit criteria:

- Integration test creates a producer and sends one message.
- Send timeout and close behavior are tested.

## Milestone 9: Consumer MVP

Goal: subscribe, receive, and ack messages.

Tasks:

- Implement `Pulsar::Internal::ConsumerImpl`.
- Implement public `client.consumer`.
- Send subscribe command.
- Track consumer ID.
- Maintain receive queue.
- Send flow permits.
- Decode incoming messages into `Pulsar::Message`.
- Implement `consumer.receive`.
- Implement `consumer.ack`.

Exit criteria:

- Integration test sends, receives, and acks one message through Pulsar
  standalone.
- Receive timeout and close behavior are tested.

## Milestone 10: Close And Cleanup Semantics

Goal: make lifecycle behavior reliable.

Tasks:

- Close producers and consumers broker-side.
- Ensure client close closes owned producers, consumers, and connections.
- Wake blocked sends and receives on close.
- Verify close is idempotent.
- Add tests for close during pending send and pending receive.

Exit criteria:

- No background thread remains after client close.
- Repeated close calls are safe.

## Milestone 11: Basic Reconnect Policy

Goal: define minimal resilience without overbuilding.

Tasks:

- Detect connection loss from reader thread.
- Fail or retry pending requests according to MVP policy.
- Reconnect broker connection.
- Recreate producer/consumer state where practical.
- Document operations that are not retried in MVP.

Exit criteria:

- Integration or controlled socket tests cover connection loss.
- Behavior is documented and predictable.

## Milestone 12: MVP Documentation

Goal: make the first implementation usable.

Tasks:

- Add README quickstart.
- Document supported and unsupported features.
- Document local Pulsar standalone setup.
- Document timeouts and close behavior.
- Update design docs if implementation diverged.

Exit criteria:

- A new contributor can run tests and send/receive one message locally.
- Public docs match implemented behavior.

## First Coding Slice

The recommended first coding slice is Milestone 1 plus the low-risk parts of
Milestone 2:

- Gem scaffold.
- Public class files.
- Error hierarchy.
- `MessageId` value object.
- `Message` value object.
- Client shell with block cleanup and idempotent close.
- RSpec setup.

This gives the project a real Ruby shape while avoiding protocol complexity in
the first implementation commit.

## Dependencies Between Milestones

```text
Gem Scaffold
  -> Public API Shell
  -> Internal Runtime
  -> Protocol Definitions
  -> Frame Codec
  -> TCP Transport
  -> Connection Handshake
  -> Producer MVP
  -> Consumer MVP
  -> Close And Cleanup
  -> Basic Reconnect
  -> MVP Documentation
```

Some work can happen in parallel after the scaffold:

- Runtime primitives and protobuf generation can be developed independently.
- Frame codec can be tested before real socket connections exist.
- Public value objects can be completed before protocol integration.

## Current Open Decisions

- Exact Ruby version support.
- Whether generated protobuf files are committed or generated during release.
- Whether `receive(timeout: nil)` inherits operation timeout or blocks forever.
- Exact reconnect retry policy.
- Whether the first integration harness uses Docker Compose only or also a mock
  protocol server.
