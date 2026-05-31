# Command Factory

This document records the internal protobuf command builder used by the MVP
protocol layers.

## Purpose

`Pulsar::Internal::CommandFactory` keeps generated protobuf details out of
producer, consumer, and connection code.

It currently builds:

- Producer creation commands.
- Unbatched send commands plus message metadata.
- Subscribe commands.
- Flow permit commands.
- Individual ack commands.
- Lookup commands.
- Close producer commands.
- Close consumer commands.

## Implemented API

```ruby
Pulsar::Internal::CommandFactory.producer(...)
Pulsar::Internal::CommandFactory.send_message(...)
Pulsar::Internal::CommandFactory.subscribe(...)
Pulsar::Internal::CommandFactory.flow(...)
Pulsar::Internal::CommandFactory.ack(...)
Pulsar::Internal::CommandFactory.lookup(...)
Pulsar::Internal::CommandFactory.close_producer(...)
Pulsar::Internal::CommandFactory.close_consumer(...)
```

The generated protobuf field named `send` collides with Ruby's `Object#send`.
When reading that field directly in low-level code or tests, use protobuf
string-key access:

```ruby
command["send"]
```

## Current Limits

The factory only covers commands needed by the first producer/consumer MVP
path. Partition metadata, ping/pong, and negative ack commands are still
pending.
