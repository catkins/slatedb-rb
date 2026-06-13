# Changelog

All notable changes to this project will be documented in this file.

The format is based on [Keep a Changelog](https://keepachangelog.com/en/1.1.0/),
and this project adheres to [Semantic Versioning](https://semver.org/spec/v2.0.0.html).

## [0.3.1]

### Changed

- Upgraded the underlying `slatedb` crate from `0.13.0` to `0.13.1`. This is an
  upstream republish with a source-identical tree; there are no API or
  behaviour changes in the bindings.

## [0.3.0]

### Changed

- Upgraded the underlying `slatedb` crate from `0.12.1` to `0.13.0`.
- Writing a merge operand (`#merge`) without a configured `merge_operator` is now
  rejected eagerly at write time rather than surfacing on read. This reflects an
  upstream behaviour change in slatedb 0.13.
- **Breaking (admin):** the JSON shape returned by `Admin#read_manifest` and
  `Admin#list_manifests` changed because slatedb 0.13 now returns structured
  manifest values. `#read_manifest` now returns a JSON object
  (`{"id": ..., ...manifest state...}`) instead of an `[id, manifest]` tuple,
  and each entry from `#list_manifests` is now the full manifest state rather
  than file metadata (the `location`, `size`, and `last_modified` fields are no
  longer present).
- `Admin#run_gc` now uses the upstream default garbage-collector options, which
  in slatedb 0.13 include a clone-detach pass. This is a no-op for databases
  that are not clones.

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
