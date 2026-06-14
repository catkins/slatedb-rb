# Agent Working Agreement

Before committing or pushing changes, always run project lint checks and address any issues:

- `cargo clippy --all-targets --all-features`
- `bundle exec rubocop`

Do not skip linting, and do not commit with unresolved lint offenses.

Use mise for the project toolchain and common commands:

- `mise install`
- `mise run test`
- `mise run lint`
