# Agent Working Agreement

Before committing or pushing changes, always run project lint checks and address any issues:

- `cargo clippy --all-targets --all-features`
- `bundle exec rubocop`

Do not skip linting, and do not commit with unresolved lint offenses.

Use mise for the project toolchain and common commands:

- `mise install`
- `mise run test`
- `mise run lint`

## Cursor Cloud / sandbox setup

Cloud Agents boot from a configured environment and then run the `install`
script in `.cursor/environment.json`, which bootstraps everything this repo
needs:

- Installs `mise` if it is missing (`curl https://mise.run | sh`).
- Runs `mise install` to provision the Ruby and Rust toolchains pinned in `.mise.toml`.
- Runs `mise run ci:install-system-deps` for native build packages (`libclang-dev`).
- Runs `mise run deps` (`bundle install`) for gem dependencies.

If a shell starts without `mise` on `PATH`, activate it first:

```bash
export PATH="$HOME/.local/bin:$PATH"
eval "$(mise activate bash)"
```

Prefer baking `mise` and the system packages into the environment snapshot so
each boot only refreshes commit-sensitive dependencies.

Release pipeline notes:

- The Buildkite release pipeline slug is `slatedb-rb-release`.
- Keep the release pipeline non-public/private.
- Release builds are intended to run from git tags (`vX.Y.Z`) and publish through the RubyGems OIDC API key role, not a long-lived RubyGems token.
- Tag must match `SlateDb::VERSION` in `lib/slatedb/version.rb` (`release:verify-tag` enforces this).
- Cut a release with `mise run release:cut X.Y.Z` (bumps `version.rb`, commits, tags, and pushes). Use `--no-push` to stop before pushing.
- Use `mise run release:build-gem` with `RELEASE_PLATFORM` for native gem builds.
- Publish a generic `ruby` platform gem plus native gems; set `DRY_RUN=true` (or trigger a manual untagged build) to exercise packaging without publishing.
