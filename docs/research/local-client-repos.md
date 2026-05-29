# Local Client Repositories

Official Apache Pulsar client repositories are cloned locally for research under:

```text
.research/clients/
```

The `.research/` folder is ignored by Git. These repositories are external
reference material and should not be committed to this project.

## Repository List

- `pulsar`: Java client and shared protocol definitions.
  - Source: https://github.com/apache/pulsar
- `pulsar-client-reactive`: Java Reactive Streams client.
  - Source: https://github.com/apache/pulsar-client-reactive
- `pulsar-client-cpp`: C++ client.
  - Source: https://github.com/apache/pulsar-client-cpp
- `pulsar-client-python`: Python bindings.
  - Source: https://github.com/apache/pulsar-client-python
- `pulsar-client-go`: Go client.
  - Source: https://github.com/apache/pulsar-client-go
- `pulsar-client-node`: Node.js client.
  - Source: https://github.com/apache/pulsar-client-node
- `pulsar-dotpulsar`: C#/DotPulsar client.
  - Source: https://github.com/apache/pulsar-dotpulsar

## Usage Notes

- Clone these repositories with `--depth 1` for initial research.
- Treat them as read-only references for architecture, feature, protocol, and
  design analysis.
- Do not edit the cloned repositories unless experimenting on an explicitly
  disposable branch.
- If release or maintenance history becomes important, unshallow only the
  specific repository being studied.
