# Integration Test Backlog

This note tracks standalone integration scenarios that are useful but should
wait until the required client behavior exists.

## Active In MVP

The current integration suite covers:

- Produce, receive, and acknowledge one message.
- Multiple messages in order.
- Message properties, key, and event time round-trip.
- Receive timeout on an empty topic.
- Multiple producers on one topic.
- Closed producer and consumer behavior.

## Add After Required Features

- Ack prevents redelivery after reopening a subscription: requires stronger
  subscription-position assumptions and should be added with explicit
  redelivery/ack-timeout policy coverage.
- Unacked messages redeliver after consumer close: requires redelivery policy
  coverage and possibly ack timeout configuration.
- Multiple consumers on one subscription: useful once subscription type options
  are public, starting with `Exclusive` busy/error behavior and later `Shared`.
- Broker restart or connection loss recovery: belongs with the basic reconnect
  implementation and should verify the documented retry policy.
