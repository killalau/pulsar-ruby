# Ruby Client Landscape

This project aims to research, design, and implement a Ruby client for Apache
Pulsar. The first research finding is that the existing Ruby client ecosystem is
thin and does not currently appear to have a clearly maintained, production-grade
option.

## Current Situation

Apache Pulsar's currently documented client libraries are Java, C++, Python, Go,
Node.js, and C#/DotPulsar. Ruby is not listed as an official client in the
current Apache Pulsar client documentation.

The main Apache Pulsar repository also lists the Ruby client under
"Archived/Halted", which is an important signal that Ruby support is not an
active first-class Apache-maintained client today.

## Existing Ruby Projects

### `pulsar-client-ruby`

- Source: https://github.com/apache/pulsar-client-ruby
- Gem: https://rubygems.org/gems/pulsar-client-ruby
- Published as `0.1.0` on August 26, 2022.
- Described as an Apache Pulsar native client for Ruby.
- The GitHub repository shows very little project activity, with only one commit
  visible during initial research and no published GitHub releases.
- Assessment: not suitable to assume as a maintained production dependency.

### `pulsar-client`

- Gem: https://rubygems.org/gems/pulsar-client
- Wraps the Apache Pulsar C++ client with Ruby bindings.
- Latest release found during initial research was `2.6.1.pre.beta.2`, published
  on April 8, 2021.
- Depends on native build tooling and Rice.
- Assessment: potentially useful as historical reference for C++ binding
  strategy, but it appears old and pre-release quality.

### `pulsar_sdk`

- Docs: https://www.rubydoc.info/gems/pulsar_sdk
- Described as a pure Ruby client for Apache Pulsar that follows the Pulsar
  binary protocol.
- Initial documentation review showed meaningful feature gaps, including missing
  TLS/authentication, batching, compression, unit tests, and incomplete schema
  and admin support.
- Assessment: useful as a protocol/reference implementation to inspect, but not
  enough evidence yet to treat it as complete or production-ready.

### `pulsar-client-more`

- Docs: https://www.rubydoc.info/gems/pulsar-client-more/Pulsar
- A RubyDoc-indexed Pulsar client namespace with generated documentation seen in
  2026.
- Initial search did not find enough repository or maintainer signal to evaluate
  it confidently.
- Assessment: needs deeper source review before we can decide whether it is
  useful as reference material.

## Initial Conclusion

There does not appear to be a well-maintained, clearly active Ruby client for
Apache Pulsar today. For this project, that means we should treat the existing
Ruby libraries as research inputs, not as foundations to rely on directly.

Likely implementation directions to research next:

- Implement the Pulsar binary protocol directly in Ruby.
- Use the official Pulsar WebSocket API for a simpler initial client surface.
- Bind to or wrap the official C++ client, accepting native extension complexity.
- Define a minimal producer/consumer MVP first, then expand into authentication,
  TLS, batching, compression, schemas, partitioned topics, and admin APIs.

## Reference Links

- Apache Pulsar: https://pulsar.apache.org/
- Apache Pulsar client libraries: https://pulsar.apache.org/docs/client-libraries/
- Apache Pulsar source repository: https://github.com/apache/pulsar
- Pulsar binary protocol documentation:
  https://pulsar.apache.org/docs/next/developing-binary-protocol/
