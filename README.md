# pulsar-ruby

A pure Ruby client for [Apache Pulsar](https://pulsar.apache.org/).

`pulsar-ruby` implements the Pulsar binary protocol in Ruby (no C++ bindings or native
extensions). The goal is a small, idiomatic API for producing and consuming messages on a
local or self-hosted broker, with room to grow into TLS, authentication, batching, and
other features over time.

**Status:** Early development. The public API, protocol definitions, frame codec,
plaintext TCP transport, connection reader, and initial producer/consumer protocol
paths are in place. The producer/consumer happy path and current supported
integration scenarios are verified against Pulsar standalone.

## Requirements

- Ruby >= 3.0

## Installation

The gem is not published on [RubyGems](https://rubygems.org/) yet. Install from GitHub:

```ruby
# Gemfile
gem "pulsar-ruby", github: "killalau/pulsar-ruby"
```

Or clone the repository and use Bundler locally:

```bash
git clone https://github.com/killalau/pulsar-ruby.git
cd pulsar-ruby
bundle install
```

## Planned usage

The target API for the MVP looks like this:

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

Today, `producer.send`, `consumer.receive`, and `consumer.ack` are wired to the
plaintext protocol path for non-partitioned topics. TLS, authentication, batching,
compression, partitioned topics, and schemas beyond raw payloads are still deferred.

## Development

```bash
bundle install
bundle exec rspec
```

Run the local Pulsar standalone integration spec:

```bash
docker compose up -d pulsar
bundle exec rake spec:integration
```

### Project layout

| Path | Purpose |
|------|---------|
| `lib/pulsar/` | Public API (`Client`, `Producer`, `Consumer`, `Message`, …) |
| `lib/pulsar/internal/` | Thread runtime, promises, bounded queues (not public API) |
| `docs/` | Research, design, and implementation planning |
| `spec/` | RSpec tests |

See [docs/overview.md](docs/overview.md) for the documentation index.

## MVP scope

The first release targets plaintext `pulsar://` connections, single-topic producer and
consumer workflows, unbatched messages, acknowledgements, flow control, and basic
reconnect behavior. TLS, authentication, partitioned topics, batching, compression, and
admin APIs are explicitly out of scope for the MVP.

Details: [docs/research/mvp-scope.md](docs/research/mvp-scope.md).

## Why another Ruby client?

Ruby is not a first-class official Pulsar client today. Existing gems are unmaintained,
incomplete, or wrap native code. This project aims for a maintained, pure-Ruby
implementation with a clear public API and testable layers.

Background: [docs/research/ruby-client-landscape.md](docs/research/ruby-client-landscape.md).

## License

Licensed under the [Apache License, Version 2.0](LICENSE), aligned with the Apache
Pulsar ecosystem. The software is provided as-is, without warranty.
