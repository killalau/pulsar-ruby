# Protocol Definitions

This document records how the project vendors and generates Ruby classes from
Apache Pulsar's protobuf protocol definitions.

## Source

The vendored source file is:

- `proto/PulsarApi.proto`

It was copied from the local Apache Pulsar research clone:

- `.research/clients/pulsar/pulsar-common/src/main/proto/PulsarApi.proto`

That upstream file is the shared protocol definition used by Apache Pulsar's
Java client and broker-side protocol code.

## Generated Ruby File

The generated Ruby protobuf file is:

- `lib/pulsar/proto/PulsarApi_pb.rb`

The stable Ruby require wrapper is:

- `lib/pulsar/proto/pulsar_api_pb.rb`

Use the lowercase wrapper path in handwritten code:

```ruby
require "pulsar/proto/pulsar_api_pb"
```

The generated file defines protocol classes under `Pulsar::Proto`, such as:

- `Pulsar::Proto::BaseCommand`
- `Pulsar::Proto::CommandConnect`
- `Pulsar::Proto::MessageIdData`
- `Pulsar::Proto::MessageMetadata`

## Generation Command

Regenerate protobuf classes with:

```bash
bundle exec rake proto:generate
```

The Rake task runs:

```bash
protoc --proto_path=proto --ruby_out=lib/pulsar/proto proto/PulsarApi.proto
```

## Dependency Version

The project currently pins `google-protobuf` to the 3.25 series because the
local `protoc` generator emits Ruby code that uses the protobuf 3.x runtime
builder API.

If the project later upgrades `protoc` and generated output, revisit the
`google-protobuf` version constraint and rerun the protocol tests.

## Packaging Rule

Both files are committed and packaged:

- The vendored `.proto` source gives contributors the exact protocol definition.
- The generated Ruby file lets the gem load without requiring every user to have
  `protoc` installed.

Protocol generation should be repeated whenever `proto/PulsarApi.proto` changes.
