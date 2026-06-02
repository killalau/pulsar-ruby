# Release Runbook

This runbook records the steps for publishing a new `pulsar-ruby` gem release.

## 1. Choose Version

Choose the next version number before editing files.

Use prerelease versions while the API is still being tested publicly:

```text
0.1.0.pre
0.1.0.pre.2
```

Use a stable version once the API is ready to be the public baseline:

```text
0.1.0
```

## 2. Bump Version

Update `lib/pulsar/version.rb`:

```ruby
module Pulsar
  VERSION = '0.1.0.pre'
end
```

Refresh `Gemfile.lock` so the local path gem version matches:

```bash
bundle install
```

## 3. Update Release Notes

Update `CHANGELOG.md`:

- Add or update the entry for the release version.
- Keep unreleased entries marked `Unreleased` until after `gem push` succeeds.
- List important additions, fixes, deferred features, and verification commands.

## 4. Preflight Checks

Confirm the working tree is clean before final verification:

```bash
git status --short
```

Confirm RubyGems credentials are available:

```bash
gem signin
```

`gem signin` only needs to be run when credentials are missing or expired. Use a
minimal API key scope for publishing: `push_rubygem`.

Confirm RubyGems MFA is enabled for the publishing account.

## 5. Verification

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

## 6. Commit Release Prep

Commit the version and release-note changes after verification passes:

```bash
git add lib/pulsar/version.rb Gemfile.lock CHANGELOG.md
git commit -m "Prepare VERSION release"
```

Include any release-doc updates in the same commit when applicable.

## 7. Publish

Publish the built artifact:

```bash
gem push pulsar-ruby-VERSION.gem
```

For example:

```bash
gem push pulsar-ruby-0.1.0.pre.gem
```

If MFA is requested, enter the one-time code from the RubyGems account.

Do not reuse a version number after publishing. If a release needs a fix,
publish a new version.

## 8. Confirm Publication

Check the public gem page:

```text
https://rubygems.org/gems/pulsar-ruby
```

Or query RubyGems:

```bash
gem info -r --prerelease pulsar-ruby
```

RubyGems search can lag for new prereleases, so the web page or RubyGems API may
show the package before `gem search` does.

## 9. Mark Changelog Released

After `gem push` succeeds, update `CHANGELOG.md` from:

```markdown
## VERSION - Unreleased
```

to:

```markdown
## VERSION - YYYY-MM-DD
```

Commit that changelog update.

## 10. Tag Release

Create and push a tag for the published version:

```bash
git tag vVERSION
git push origin vVERSION
```

For example:

```bash
git tag v0.1.0.pre
git push origin v0.1.0.pre
```

## 11. Post-Release Check

Run a clean install check from RubyGems after the package is available:

```bash
gem install pulsar-ruby --pre
ruby -e "require 'pulsar'; puts Pulsar::VERSION"
```

Update planning docs if the next feature phase changes priority.
