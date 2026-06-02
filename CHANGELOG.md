# Changelog

All notable changes to this project will be documented in this file.

## 0.1.0.pre - Unreleased

Initial MVP prerelease for the pure Ruby Apache Pulsar client.

### Added

- Plaintext `pulsar://` broker connections.
- Pulsar binary protocol connect handshake.
- Topic lookup before producer and consumer creation.
- Non-partitioned topic producers and consumers.
- Single unbatched string or byte payload sends.
- Message properties, key, event time, and structured message IDs.
- Consumer receive and individual acknowledgement.
- Consumer flow permits.
- Producer pending-send limits.
- Operation, send, and receive timeouts.
- Idempotent client, producer, consumer, queue, and connection close behavior.
- Conservative reconnect for simple dropped-connection cases.
- Typed client errors for configuration, broker, protocol, connection, timeout,
  closed-resource, authentication, authorization, topic-not-found, producer-busy,
  and consumer-busy failures.
- Standalone integration tests against local Apache Pulsar.
- Installed-gem smoke test through `bundle exec rake smoke:local`.
- RuboCop and RSpec verification through `bundle exec rake verify`.

### Deferred

- TLS.
- Authentication.
- Partitioned topics.
- Reader API.
- Multi-topic consumers.
- Public subscription type options.
- Batching.
- Compression.
- Chunking.
- Schemas beyond raw string or byte payloads.
- Negative ack and delayed reconsume.
- Dead letter and retry topics.
- Transactions.
- Interceptors.
- Admin API.
- Metrics and tracing.
- Full retry policy for failed in-flight sends.

### Verification

Before publishing this release, run:

```bash
bundle exec rake verify
docker compose up -d pulsar
bundle exec rake spec:integration
bundle exec rake smoke:local
gem build pulsar-ruby.gemspec
```
