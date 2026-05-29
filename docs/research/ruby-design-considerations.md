# Ruby Design Considerations

This document turns official-client research into Ruby-specific design notes.

## Current Status

Initial notes are based on the Java client baseline. These recommendations
should be revisited after C++, Go, DotPulsar, Node.js, Python, and Java Reactive
Streams are analyzed.

## Initial Recommendations

- Start with a single gem and internal namespaces; split packages only if the
  project grows enough to need separate admin, auth, or protocol gems.
- Expose a simple builder or keyword-argument API that feels natural in Ruby,
  while preserving the same concepts as official clients.
- Provide blocking producer and consumer APIs first.
- Build internals around asynchronous request tracking, because the binary
  protocol is inherently asynchronous.
- Choose a concurrency model before implementation starts. The main candidates
  are a background thread with queues, Ruby fibers with a scheduler, or an
  event-loop dependency.
- Avoid native extensions in the first implementation unless protocol
  performance forces a different choice.
- Treat protobuf generation as part of the build or development workflow.
- Define a small exception hierarchy before the first network implementation.
- Make flow-control permits and queue limits part of the first consumer design.
- Keep auth, TLS, batching, compression, schemas, transactions, and admin APIs as
  explicit extension points, but defer them from the first MVP unless later
  research changes the priority.

## Open Decisions

- Ruby concurrency model.
- Whether the MVP includes `Reader`.
- Public API style: builders, keyword constructors, or both.
- First supported Ruby versions.
- Protobuf dependency and generation workflow.
- Whether integration tests should launch Pulsar through Docker, an existing
  local broker, or both.
