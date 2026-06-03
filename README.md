# pulsar-ruby

A pure Ruby client for [Apache Pulsar](https://pulsar.apache.org/).

`pulsar-ruby` implements the Pulsar binary protocol in Ruby (no C++ bindings or native
extensions). The goal is a small, idiomatic API for producing and consuming messages on a
local or self-hosted broker, with room to grow into TLS, authentication, batching, and
other features over time.

**Status:** Published MVP prerelease. Version `0.1.0.pre` is available on
[RubyGems](https://rubygems.org/gems/pulsar-ruby). The public API, protocol
definitions, frame codec, plaintext TCP transport, connection reader,
producer/consumer protocol paths, close cleanup, and conservative reconnect are
in place. Supported integration scenarios are verified against Pulsar
standalone.

## Requirements

- Ruby >= 3.0

## Installation

Install the current prerelease from [RubyGems](https://rubygems.org/gems/pulsar-ruby):

```bash
gem install pulsar-ruby --pre
```

Or add the prerelease to your Gemfile:

```ruby
# Gemfile
gem "pulsar-ruby", "0.1.0.pre"
```

For local development, clone the repository and use Bundler:

```bash
git clone https://github.com/killalau/pulsar-ruby.git
cd pulsar-ruby
bundle install
```

## Quickstart

Start a local standalone broker:

```bash
docker compose up -d pulsar
```

Produce, receive, and acknowledge one message:

```ruby
require "pulsar"

Pulsar::Client.open("pulsar://localhost:6650") do |client|
  producer = client.producer(
    topic: "persistent://public/default/orders",
    max_pending_messages: 1000
  )

  consumer = client.consumer(
    topic: "persistent://public/default/orders",
    subscription: "orders-ruby"
  )

  message_id = producer.send("created", timeout: 5)
  message = consumer.receive(timeout: 5)

  puts message.payload
  consumer.ack(message)
end
```

`producer.send`, `consumer.receive`, and `consumer.ack` are wired to the
plaintext Pulsar binary protocol path for non-partitioned topics.

## Supported MVP features

- Plaintext `pulsar://` broker connections.
- Binary protocol connect handshake.
- Topic lookup before producer or consumer creation.
- Non-partitioned topic producers.
- Non-partitioned topic consumers with one subscription.
- Single, unbatched string or byte payload messages.
- Message properties, key, event time, and structured message IDs.
- Individual acknowledgement.
- Consumer flow permits.
- Producer pending-send limits.
- Operation and receive timeouts.
- Idempotent producer, consumer, and client close.
- Conservative reconnect after a dropped connection.

## Deferred features

- TLS.
- Authentication.
- Partitioned topics.
- Reader API.
- Multi-topic consumers.
- Subscription type options beyond the current default broker behavior.
- Batching.
- Compression.
- Chunking.
- Schemas beyond raw byte/string payloads.
- Negative ack and delayed reconsume.
- Dead letter and retry topics.
- Transactions.
- Interceptors.
- Admin API.
- Metrics and tracing.

## Timeouts and reconnect

`Pulsar::Client.new` accepts `operation_timeout:` and `connection_timeout:`.
`producer.send` and `consumer.receive` also accept `timeout:` for the current
operation.

Reconnect is intentionally conservative. If the socket drops, in-flight
operations fail with `Pulsar::ConnectionError`. Existing producer and consumer
objects remain usable; the next operation lazily opens a replacement connection
and recreates broker-side producer or consumer state. The failed in-flight
operation is not silently retried.

See [docs/design/reconnect-policy.md](docs/design/reconnect-policy.md) for the
design rationale.

## Development

```bash
bundle install
bundle exec rake verify
```

`bundle exec rake verify` runs RuboCop and the normal RSpec suite.

Run the local Pulsar standalone integration spec:

```bash
docker compose up -d pulsar
bundle exec rake spec:integration
```

Build the local gem package:

```bash
gem build pulsar-ruby.gemspec
```

Run the installed-gem smoke test against local Pulsar standalone:

```bash
docker compose up -d pulsar
bundle exec rake smoke:local
```

`smoke:local` builds the gem, installs that built artifact into a temporary gem
home, requires `pulsar`, and performs one produce/consume/ack round trip.

### Git hooks

This repository includes a versioned pre-push hook in `.githooks/pre-push`.
Enable it for this checkout with:

```bash
git config core.hooksPath .githooks
```

After that, `git push` runs `bundle exec rake verify` before sending commits.
The hook does not run standalone integration specs because they require a local
broker and are slower than the regular push guard.

### Project layout

| Path | Purpose |
|------|---------|
| `lib/pulsar/` | Public API (`Client`, `Producer`, `Consumer`, `Message`, …) |
| `lib/pulsar/internal/` | Thread runtime, promises, bounded queues (not public API) |
| `docs/` | Research, design, and implementation planning |
| `script/` | Local release and smoke-test scripts |
| `spec/` | RSpec tests |

See [docs/overview.md](docs/overview.md) for the documentation index.

## MVP scope

The `0.1.0.pre` release targets plaintext `pulsar://` connections,
single-topic producer and consumer workflows, unbatched messages,
acknowledgements, flow control, and basic reconnect behavior. TLS,
authentication, partitioned topics, batching, compression, and admin APIs are
explicitly out of scope for the MVP.

Details: [docs/research/mvp-scope.md](docs/research/mvp-scope.md).

## Why another Ruby client?

Ruby is not a first-class official Pulsar client today. Existing gems are unmaintained,
incomplete, or wrap native code. This project aims for a maintained, pure-Ruby
implementation with a clear public API and testable layers.

Background: [docs/research/ruby-client-landscape.md](docs/research/ruby-client-landscape.md).

## License

Licensed under the [Apache License, Version 2.0](LICENSE), aligned with the Apache
Pulsar ecosystem. The software is provided as-is, without warranty.
