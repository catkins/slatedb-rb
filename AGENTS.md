# Agent Working Agreement

Before committing or pushing changes, always run project lint checks and address any issues:

- `cargo clippy --all-targets --all-features`
- `bundle exec rubocop`

Do not skip linting, and do not commit with unresolved lint offenses.

Use mise for the project toolchain and common commands:

- `mise install`
- `mise run test`
- `mise run lint`

Release pipeline notes:

- The Buildkite release pipeline slug is `slatedb-rb-release`.
- Keep the release pipeline non-public/private.
- Release builds are intended to run from git tags (`vX.Y.Z`) and publish through the RubyGems OIDC API key role, not a long-lived RubyGems token.
- Tag must match `SlateDb::VERSION` in `lib/slatedb/version.rb` (`release:verify-tag` enforces this).
- Use `mise run release:build-gem` with `RELEASE_PLATFORM` for native gem builds.
- Publish a generic `ruby` platform gem plus native gems; set `DRY_RUN=true` (or trigger a manual untagged build) to exercise packaging without publishing.
