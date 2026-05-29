# Ruby Design Considerations

This document turns official-client research into Ruby-specific design notes.

## Current Status

Initial notes are based on all official clients studied so far: Java, C++,
Go, DotPulsar, Node.js, Python, and Java Reactive Streams.

## Initial Recommendations

- Start with a single gem and internal namespaces; split packages only if the
  project grows enough to need separate admin, auth, or protocol gems.
- Expose a simple builder or keyword-argument API that feels natural in Ruby,
  while preserving the same concepts as official clients.
- Provide blocking producer and consumer APIs first.
- Build internals around asynchronous request tracking, because the binary
  protocol is inherently asynchronous.
- Add an internal RPC/request layer between connection IO and producer/consumer
  objects.
- Let blocking public calls accept timeout/cancellation options rather than
  forcing users into an async API on day one.
- Choose a concurrency model before implementation starts. The main candidates
  are a background thread with queues, Ruby fibers with a scheduler, or an
  event-loop dependency.
- Avoid native extensions in the first implementation unless protocol
  performance forces a different choice.
- Treat protobuf generation as part of the build or development workflow.
- Define a small exception hierarchy before the first network implementation.
- Include explicit object states for client, producer, consumer, connection, and
  channel internals.
- Use public exceptions for closed/disposed/faulted objects rather than returning
  status codes.
- Make flow-control permits and queue limits part of the first consumer design.
- Keep logical broker address and physical connection address separate
  internally, even if the public MVP only accepts one service URL.
- Prefer a pure Ruby binary protocol implementation first; treat C/C++ bindings
  as a fallback path because native extension packaging would become a major
  project concern.
- Do not choose the binding path just to get feature breadth quickly; Node.js
  and Python show that bindings shift much of the project into native artifact
  packaging.
- Consider a separate async/fiber-friendly Ruby layer after the blocking API,
  similar to Python's separate `pulsar.asyncio` module.
- Keep any future stream/reactive API as a layer above the core client, not part
  of the MVP core.
- Keep auth, TLS, batching, compression, schemas, transactions, and admin APIs as
  explicit extension points, but defer them from the first MVP unless later
  research changes the priority.

## Open Decisions

- Ruby concurrency model.
- Whether the MVP includes `Reader`.
- Public API style: builders, keyword constructors, or both.
- Timeout/cancellation API for blocking `send` and `receive`.
- First supported Ruby versions.
- Protobuf dependency and generation workflow.
- Whether integration tests should launch Pulsar through Docker, an existing
  local broker, or both.
- Whether MVP should include a simple client-wide memory limit or only per
  producer/consumer queue limits.
- Public object state API: predicates only, symbolic `state`, or internal-only
  for MVP.
