# Lookup Service

This document records the first binary topic lookup implementation for the MVP.

## Implemented Behavior

`Pulsar::Internal::LookupService` currently supports:

- Building `CommandLookupTopic` through `CommandFactory`.
- Sending lookup requests through the current connection.
- Returning `brokerServiceUrl` for successful `Connect` responses.
- Raising `Pulsar::BrokerError` for failed lookup responses.

`Pulsar::Client` now performs lookup before creating producers or consumers.

## Current Limits

The first lookup implementation only accepts `Connect` responses. It does not
yet support:

- `Redirect` responses.
- Reconnecting to a different physical broker URL.
- TLS broker service URLs.
- `proxy_through_service_url`.
- Partition metadata lookup.

Those are required before the client can be considered robust against a real
cluster, but this gives the public producer and consumer paths the same basic
lookup step used by official clients.
