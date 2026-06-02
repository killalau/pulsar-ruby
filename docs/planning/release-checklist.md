# Release Checklist

This checklist records the release steps for the first MVP gem and future
maintenance releases.

## Before Release

- Confirm the working tree is clean.
- Confirm the intended version in `lib/pulsar/version.rb`.
- Confirm `CHANGELOG.md` has an entry for the release.
- Confirm the release entry documents supported and deferred features.
- Confirm RubyGems gem name ownership and publishing access.
- Confirm RubyGems MFA is enabled for the publishing account.

## Verification

Run the fast local verification:

```bash
bundle exec rake verify
```

Run the broker-backed verification:

```bash
docker compose up -d pulsar
bundle exec rake spec:integration
bundle exec rake smoke:local
```

Build the gem:

```bash
gem build pulsar-ruby.gemspec
```

The smoke task also builds the gem, but the explicit build command should still
be run before publishing so the final artifact path and version are visible.

## Publish Decision

The first public package should be `0.1.0.pre`. This marks the first RubyGems
artifact as a prerelease while users test the API.

Use `0.1.0` later when the MVP API is ready to be the first stable public
baseline.

## After Release

- Tag the release commit.
- Push the tag.
- Confirm the package page on RubyGems.
- Run a clean install check from RubyGems after the package is available.
- Update planning docs if the next feature phase changes priority.
