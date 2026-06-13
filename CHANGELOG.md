# Changelog

All notable changes to this project will be documented in this file.

The format is based on [Keep a Changelog](https://keepachangelog.com/en/1.1.0/),
and this project adheres to [Semantic Versioning](https://semver.org/spec/v2.0.0.html).

## [0.3.0]

### Changed

- Upgraded the underlying `slatedb` crate from `0.12.1` to `0.13.0`.
- Writing a merge operand (`#merge`) without a configured `merge_operator` is now
  rejected eagerly at write time rather than surfacing on read. This reflects an
  upstream behaviour change in slatedb 0.13.

### Added

- User-defined sequence numbers for writes. The new `seqnum:` option is accepted
  by `Database#put`, `Database#delete`, `Database#merge`, `Database#write`,
  `Database#batch`, and `Transaction#commit`. When provided, the value overrides
  the internally generated sequence number and must be strictly greater than the
  current maximum sequence number.
- `Database#refresh_manifest`, which forces the database to refresh its view of
  the manifest from the object store.
- `Transaction#commit` now accepts `await_durable:` and `seqnum:` keyword
  arguments (previously commit options were not exposed in Ruby).
