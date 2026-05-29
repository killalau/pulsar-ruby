# Concurrency Model Analysis

This report examines the recommended MVP concurrency model for a pure Ruby
Apache Pulsar client: a blocking Ruby public API backed by an internal network
thread, request promises, and bounded queues.

## Recommendation

Use one background network thread per broker connection for the MVP.

The thread owns the socket read loop and continuously receives broker frames. It
dispatches those frames to internal state:

- Request responses complete waiting promises by request ID.
- Send receipts complete producer send promises by producer/sequence ID.
- Message frames are pushed into consumer receive queues by consumer ID.
- Close, error, and reconnect events update connection, producer, and consumer
  state.

Public methods can remain ordinary blocking Ruby methods:

```ruby
message_id = producer.send("hello", timeout: 5)
message = consumer.receive(timeout: 5)
consumer.ack(message)
```

Internally, those blocking calls wait on small promises implemented with
`Mutex` and `ConditionVariable`.

## Why Pulsar Needs Internal Concurrency

Pulsar is not a simple request/response protocol.

The broker can send data and control messages independently of the user's
current method call. A consumer must receive pushed messages even when the user
is not inside a `receive` call yet. A producer must receive send receipts while
other operations may be happening. The connection must also handle ping/pong,
close events, errors, and reconnect triggers.

That means a client needs a continuous read path. If the public API directly
owned all socket reads, operations would block each other and broker-pushed
messages could be missed or delayed.

## Target Internal Shape

```text
Application thread
  producer.send
      |
      v
  create promise
  write frame under write lock
  wait on promise with timeout

Background connection thread
  read broker frame
  decode command
  route response/message/error
  complete promise or enqueue message
```

Core internal objects:

- `Connection`: owns socket, reader thread, request IDs, pending requests, and
  write synchronization.
- `Promise`: internal waitable result with success, failure, timeout, and
  shutdown behavior.
- `Producer`: tracks producer ID, sequence IDs, pending sends, and send receipt
  completion.
- `Consumer`: tracks consumer ID, receive queue, available permits, and ack
  commands.
- `Dispatcher`: routes decoded broker commands to the right connection,
  producer, or consumer handler.

## Pros

### Simple Public API

Ruby users can start with blocking calls and timeouts. They do not need to opt
into fibers, callbacks, event loops, or futures to send and receive messages.

### Good Fit For Broker Push

The read loop exists independently of user calls, so broker-pushed messages,
send receipts, pings, errors, and close events can be handled promptly.

### Minimal Runtime Dependencies

Ruby already provides `Thread`, `Mutex`, `ConditionVariable`, `Queue`, and
socket APIs. The MVP does not need `async`, EventMachine, or `concurrent-ruby`
before the protocol implementation proves itself.

### Clear Request Correlation

Pulsar commands include request IDs for many operations. A pending-request map
plus internal promises maps naturally to the protocol.

### Natural Backpressure Points

Consumer queues and producer pending-send queues can be bounded. When limits
are reached, public calls can block, time out, or raise a specific exception.

### Easy To Wrap Later

A blocking core can later be wrapped in Fiber, async, or callback APIs. The
important requirement is that blocking waits happen in a small number of places
instead of being scattered through protocol code.

## Cons

### Thread Safety Must Be Designed Carefully

Shared maps, queues, producer state, consumer state, and connection state need
clear ownership rules. Accidental lock ordering problems can cause deadlocks.

### Shutdown Is Subtle

Closing the client must wake blocked sends, receives, lookups, and close calls.
The reader thread must stop even if the socket is blocked. Pending promises need
to complete with a close error.

### Debugging Races Requires Discipline

Intermittent bugs may only appear under disconnect, timeout, and high-volume
message scenarios. Tests must cover forced close, delayed responses, and
consumer queue saturation.

### One Thread Per Connection Has A Cost

This is acceptable for an MVP and many normal Ruby applications, but it is not
ideal for thousands of broker connections. Connection pooling and multiplexing
should keep the connection count small.

### Blocking API Can Hide Slow Operations

Blocking methods are easy to use, but callers need timeouts and clear errors.
Every operation that waits on broker state should accept or inherit an operation
timeout.

## When This Approach Should Be Used

Use this model when:

- The protocol can push messages or control events at any time.
- The public API should be simple and synchronous.
- The client needs only a small number of long-lived network connections.
- The team wants to minimize runtime dependencies.
- A future async API is useful, but not required for the first release.
- Request/response correlation can be represented with request IDs and
  promises.

This is a strong fit for the Ruby Pulsar MVP because the first goal is protocol
correctness and installability, not maximum concurrency abstraction.

## When To Avoid It

Avoid or revisit this model when:

- The expected application has thousands of independent broker connections.
- The gem must integrate directly with a specific event loop from day one.
- The primary API must be non-blocking or Fiber-native.
- Message processing work is CPU-heavy and would contend heavily under Ruby's
  Global VM Lock.
- The internal state machines become too complex to reason about with shared
  locks.

In those cases, an async runtime, connection event loop, or native extension may
be worth reconsidering.

## Official Client Parallels

The exact runtime differs by language, but the official clients share the same
core pattern: keep a continuous network path, track pending requests, and route
broker frames to producer or consumer state.

### Java Client

The Java client uses Netty event loops and `CompletableFuture`. Its public API
offers both blocking and async methods, while the implementation routes broker
events through connection and producer/consumer objects.

Ruby should not copy Netty, but it should copy the separation:

- Network events are handled by an internal runtime.
- User-facing blocking methods wait on async results.
- Producer and consumer objects are stateful protocol participants.

### Go Client

The Go client uses goroutines, channels, pending request maps, and callbacks.
Its connection object has incoming request channels, write request channels,
close channels, and a `pendingReqs` map keyed by request ID.

Relevant local reference:

- `.research/clients/pulsar-client-go/pulsar/internal/connection.go`

This is close to the Ruby recommendation in spirit. Ruby's `Thread`, `Queue`,
`Mutex`, and `ConditionVariable` are less lightweight than goroutines and
channels, but the architecture translates well for a small number of
connections.

### C++ Client

The C++ client uses Asio, futures, promises, pending write buffers, and typed
pending request maps. The connection tracks pending requests for lookup,
schema, last-message-id, and other operations.

Relevant local reference:

- `.research/clients/pulsar-client-cpp/lib/ClientConnection.h`

Ruby should copy the request ownership idea, not the Asio implementation.

### DotPulsar

DotPulsar uses C# tasks, async methods, cancellation tokens, and channel-like
abstractions. Its connection interface exposes async `Send` operations and
routes producer and consumer commands through connection/channel objects.

Relevant local reference:

- `.research/clients/pulsar-dotpulsar/src/DotPulsar/Internal/Abstractions/IConnection.cs`

The useful lesson is the shape of the boundaries: connection, producer channel,
consumer channel, cancellation, and state transitions are separate concerns.

## Comparable Ruby Patterns

The proposed model uses common Ruby building blocks rather than a specialized
runtime:

- `Thread` for long-lived background work.
- `Queue` or `SizedQueue` for producer/consumer handoff.
- `Mutex` for protecting shared maps and state.
- `ConditionVariable` for promise-style waiting.
- `IO.select` or blocking socket reads for the connection loop.

Several Ruby libraries and servers use this broad methodology: keep blocking
user-facing code simple while moving coordination or I/O work into controlled
threads and queues. Examples include thread-pool web servers such as Puma and
background-job systems such as Sidekiq. Those are not protocol clients, but
they prove that Ruby applications commonly rely on long-lived threads, bounded
queues, and explicit shutdown behavior.

For protocol-client design, the stronger examples are the official Pulsar
clients above. They use language-specific async primitives, but the same
underlying architecture appears repeatedly.

## Ruby-Specific Considerations

### Global VM Lock

CRuby has a Global VM Lock, so Ruby threads do not execute Ruby CPU work in
parallel. This is acceptable here because the background thread is mostly doing
socket I/O, decoding frames, and moving messages into queues. Socket I/O releases
the thread to wait without busy-spinning the application.

### Lock Ownership

The MVP should define strict ownership rules:

- The connection owns the socket and request ID allocation.
- Pending request maps are only accessed under a connection mutex.
- Producer pending sends are only accessed under a producer mutex.
- Consumer receive queues are the only handoff path for messages.
- Callbacks invoked by the reader thread should be short and non-blocking.

### Timeouts

Every wait should have a timeout path:

- Connect timeout.
- Operation timeout.
- Send timeout.
- Receive timeout.
- Close timeout.

Timeouts should remove pending requests where appropriate and complete promises
exactly once.

### Bounded Queues

Unbounded queues make early implementation easier but hide memory problems.
Prefer bounded queues for:

- Consumer prefetch/receive queue.
- Producer pending-send queue.
- Connection write queue if writes are serialized through a queue.

The public API should expose limits through options with conservative defaults.

### Error Propagation

Connection errors must be broadcast to all affected waiters:

- Pending request promises fail.
- Pending send promises fail or enter retry logic.
- Blocking receives wake with a typed connection/closed error.
- Producer and consumer state transitions are visible.

## Alternative Models

### Fully Blocking Socket Per Operation

This model would let `send`, `receive`, and `ack` directly read and write the
socket.

Pros:

- Very small implementation.
- Easy to understand for simple request/response protocols.

Cons:

- Does not fit broker-pushed messages.
- Cannot safely handle send receipts while a consumer is waiting.
- Makes reconnect and close handling fragile.

Verdict: not suitable for Pulsar.

### Fiber Scheduler Or `async` First

This model would build the client around Ruby fibers and non-blocking I/O.

Pros:

- Good fit for high-concurrency Ruby applications.
- Can avoid one native thread per connection.
- Natural integration with async applications.

Cons:

- Adds a framework decision before the protocol core is proven.
- Blocking users still need wrappers.
- The gem may become harder to use in applications that do not already use that
  runtime.

Verdict: attractive later, but not the MVP default.

### EventMachine-Style Reactor

This model would use an event loop and callbacks.

Pros:

- Mature pattern for network clients.
- Efficient for many sockets.

Cons:

- Less idiomatic for modern Ruby applications.
- Callback-heavy internals can make protocol state harder to test.
- Forces an event-loop dependency.

Verdict: not recommended for MVP.

### Native C++ Wrapper

This model would delegate concurrency and protocol work to the C++ client.

Pros:

- More feature coverage sooner.
- Proven transport and protocol behavior.
- Potentially better throughput.

Cons:

- Native packaging burden.
- Harder debugging for Ruby users.
- Ruby API becomes constrained by wrapper boundaries.
- Less ownership of protocol learning and design.

Verdict: keep as fallback, not the current plan.

## Recommended MVP Design Rules

- Keep the public API blocking and timeout-aware.
- Keep the reader thread internal and invisible.
- Never call user callbacks from the reader thread in the MVP.
- Complete every promise exactly once.
- Use bounded queues for user-visible buffering.
- Make shutdown wake every blocked wait.
- Keep lock ordering documented in code comments where multiple locks interact.
- Add tests for timeouts, close while waiting, broker error responses, queue
  saturation, and reconnect-triggered failures.

## Minimal Internal Sketch

```ruby
class Promise
  def initialize
    @mutex = Mutex.new
    @condition = ConditionVariable.new
    @completed = false
    @value = nil
    @error = nil
  end

  def fulfill(value)
    complete(value, nil)
  end

  def reject(error)
    complete(nil, error)
  end

  def wait(timeout:)
    @mutex.synchronize do
      @condition.wait(@mutex, timeout) unless @completed
      raise TimeoutError unless @completed
      raise @error if @error
      @value
    end
  end

  private

  def complete(value, error)
    @mutex.synchronize do
      return if @completed

      @completed = true
      @value = value
      @error = error
      @condition.broadcast
    end
  end
end
```

The real implementation will need project-specific exception types and careful
timeout cleanup, but this shows the primitive shape.

## Conclusion

A background network thread plus internal queues is the best MVP concurrency
model for a pure Ruby Pulsar client. It matches the protocol's need for
continuous broker reads, keeps the public API Ruby-friendly, avoids premature
async dependencies, and mirrors the same separation of concerns found in the
official clients.

The main risk is not performance; it is correctness around locking, shutdown,
timeouts, and reconnect behavior. The MVP should therefore invest early in
small concurrency primitives and focused tests before adding broader Pulsar
features.
