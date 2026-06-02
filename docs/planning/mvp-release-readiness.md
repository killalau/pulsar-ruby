# MVP Release Readiness

This note records the current state of the Ruby Apache Pulsar client after the
initial MVP implementation work.

## Current Status

The MVP implementation is functionally complete for the first local
producer/consumer target:

- Connects to a broker with plaintext `pulsar://`.
- Performs the Pulsar binary protocol handshake.
- Looks up a topic before producer or consumer creation.
- Creates producers for non-partitioned topics.
- Sends single unbatched string or byte payload messages.
- Returns structured `Pulsar::MessageId` values from send receipts.
- Creates consumers for non-partitioned topics and subscriptions.
- Receives messages.
- Acknowledges messages individually.
- Maintains consumer flow permits.
- Enforces bounded producer pending-send behavior.
- Supports operation and receive timeouts.
- Closes clients, producers, consumers, queues, and connections idempotently.
- Recovers from simple dropped-connection cases by lazily reconnecting and
  reattaching existing producer and consumer objects.

## Verified Locally

The current verification baseline is:

- `bundle exec rake`
- `bundle exec rake spec:integration`
- `bundle exec rake smoke:local`
- `gem build pulsar-ruby.gemspec`

The integration task expects a local Pulsar standalone broker from Docker
Compose:

```bash
docker compose up -d pulsar
bundle exec rake spec:integration
bundle exec rake smoke:local
```

The smoke task builds the gem, installs the built artifact into a temporary gem
home, and runs one produce/consume/ack round trip through `require 'pulsar'`.

## Release Decisions

The first MVP release should use these conservative decisions:

- Support Ruby `>= 3.0`, matching the gemspec.
- Commit generated protobuf Ruby files and the vendored upstream
  `PulsarApi.proto` source so installing the gem does not require `protoc`.
- Keep the public API small: `Pulsar::Client`, `Pulsar::Producer`,
  `Pulsar::Consumer`, `Pulsar::Message`, `Pulsar::MessageId`, and typed errors.
- Treat TLS and authentication options as unsupported MVP configuration.
- Treat reconnect and retry separately. MVP reconnect restores usable producer
  and consumer objects after a dropped connection; it does not retry the failed
  in-flight operation.

## Remaining Before Publishing

Before publishing to RubyGems, do a final release pass:

- Confirm gem name availability and ownership.
- Publish `0.1.0.pre` as the first public prerelease.
- Confirm the release notes in `CHANGELOG.md`.
- Run the verification baseline on a clean checkout.
- Run the installed-gem smoke test from the built artifact.

Use [Release Runbook](../release-runbook.md) for the concrete release sequence.

## Deferred Product Work

The next feature phase should not be folded into the MVP release. It should
start from explicit designs for:

- TLS.
- Authentication.
- Partitioned topics.
- Batching and compression.
- Subscription options.
- Redelivery, negative ack, dead letter, and retry topics.
- Full retry policy for failed in-flight sends.
- Metrics and tracing.
