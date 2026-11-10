# SlateDB Ruby

Ruby bindings for [SlateDB](https://slatedb.io), a cloud-native embedded key-value store built on object storage.

[![Build status](https://badge.buildkite.com/a7ae51f3a0bc7809cf66981641ec47b3c70db8cf349a5e462f.svg)](https://buildkite.com/catkins-test/slatedb-rb) [![Gem Version](https://badge.fury.io/rb/slatedb.svg)](https://badge.fury.io/rb/slatedb)

## Production Readiness

These bindings are still in early development, and while SlateDB itself is used in Production, these bindings have yet to be. Contributions are welcome!

## Installation

Add this line to your application's Gemfile:

```ruby
gem 'slatedb'
```

And then execute:

```bash
bundle install
```

Or install it yourself as:

```bash
gem install slatedb
```

Precompiled native gems are published for common Linux and Darwin platforms. Building from source still requires a Rust toolchain.

## Upgrading to SlateDB 0.18.1

This update moves the Rust engine from 0.17.0 to 0.18.1. Existing database files
remain readable. New Ruby options keep the existing defaults unless enabled.

- Two-phase checkpoints: `begin_checkpoint` returns a handle with `id` and `wait`.
  Both checkpoint methods accept `scope: :all` to capture pending writes, or
  `scope: :durable` (the existing default) for writes already durable.
- Point reads accept `max_concurrent_sources:` on databases, readers, snapshots,
  and transactions; database metadata reads accept it too.
- `Database.open` accepts `experimental_incremental_compaction:` and
  `sst_block_alignment:`. Both are disabled by default; see the warnings below.
- Snapshot and transaction range/prefix scans now accept `order:`, like database
  and reader scans. Reader `merge_operator:` is now applied rather than ignored.
- **Breaking:** Ruby Proc/lambda merge operators are no longer accepted. They
  silently used concatenation on other threads, which could change stored values.
  Keep `:concat` / `:string_concat` for concatenation. For custom updates, use a
  serializable transaction that reads the value and writes the computed result;
  handle `SlateDb::TransactionError` conflicts in the application. Databases with
  unresolved custom merge operands need an application-specific migration before
  upgrading. Do not reopen them with `:concat` unless concatenation was their
  intended merge rule.
- Reader disk-cache keywords remain supported, but upstream deprecated the
  underlying `CachedObjectStore` in 0.18.1. The new mirror and Rust trait-based
  extension APIs are not exposed by this gem yet.

Upstream also fixes clone-compaction retention/data-loss cases, writer ownership
validation after WAL replay, write stalls, upload retry/concurrency behavior, and
iterator fetch cancellation. It improves memtable seeks and point-read/scan work.
See the full [0.18.0 release notes](https://github.com/slatedb/slatedb/releases/tag/v0.18.0)
and [0.18.1 release notes](https://github.com/slatedb/slatedb/releases/tag/v0.18.1).

## Usage

### Basic Operations

```ruby
require 'slatedb'

# Open a database with in-memory storage (for testing)
db = SlateDb::Database.open("/tmp/mydb")

# Store a value
db.put("hello", "world")

# Retrieve a value
value = db.get("hello")  # => "world"

# Delete a value
db.delete("hello")

# Close the database
db.close
```

### Block Form (Recommended)

The block form automatically closes the database when the block exits:

```ruby
SlateDb::Database.open("/tmp/mydb") do |db|
  db.put("key", "value")
  db.get("key")  # => "value"
end  # automatically closed
```

### Point-Read Concurrency

```ruby
db.get("key", max_concurrent_sources: 4, cache_blocks: false)
db.get_entry("key", max_concurrent_sources: 4) # value plus sequence/timestamps
```

`max_concurrent_sources` defaults to 1 (serial fetching); zero also means 1.
SlateDB checks the newest source alone, then fetches up to this many sources
concurrently after a miss. Higher values can reduce latency but increase object
store requests and bandwidth. The same keyword works with `get` on readers,
snapshots, and transactions. It does not change isolation or durability.

### Compaction Controls

SlateDB 0.17 adds controls for how long compaction inputs remain protected from
garbage collection and when to consolidate sorted runs:

```ruby
SlateDb::Database.open("mydb",
                       url: "s3://mybucket/path",
                       compactor_checkpoint_lifetime: 30 * 60_000,
                       sorted_run_consolidation_threshold: 8) do |db|
  db.put("key", "value")
end
```

`compactor_checkpoint_lifetime` is in milliseconds and defaults to 15 minutes.
Reads, scans, snapshots, and transactions using old compaction inputs must finish
within that time; after it expires, garbage collection can remove those inputs.

`sorted_run_consolidation_threshold` starts fallback consolidation when the
projected sorted-run count exceeds the threshold. It is not a hard limit.
Zero disables it (the default); use at least two to enable it. SlateDB warns and
disables the trigger for one. Low thresholds can increase storage reads and writes.

SlateDB 0.18 adds an opt-in experimental mode:

```ruby
SlateDb::Database.open("mydb", url: "s3://mybucket/path",
                       experimental_incremental_compaction: true) do |db|
  db.put("key", "value")
end
```

This publishes output from sorted-run compactions at most once a minute before
the full compaction finishes. It can reduce read amplification and allow earlier
storage reclamation after the compactor checkpoint expires. Compactions with L0
inputs still publish only when complete. It is disabled by default, experimental,
and may change or disappear upstream.

### SST Block Alignment

`Database.open(..., sst_block_alignment: true)` pads SST data blocks to 4 KiB
boundaries, including oversized entries. It is disabled by default, uses more
storage, and cannot be combined with compression.

**Upgrade every reader before enabling alignment on a writer.** Older engines
that do not understand padded SSTs cannot read them. Once padded files exist,
rolling those readers back is not safe; disabling the option does not rewrite
existing files.

### Key Size

Keys can now exceed the previous 65,535-byte limit, up to SlateDB's new
4,294,967,295-byte limit. Very large keys still require enough memory to process.

### Persistent Storage

For persistent storage, provide an object store URL:

```ruby
# Local filesystem
SlateDb::Database.open("/tmp/mydb", url: "file:///tmp/mydb") do |db|
  db.put("key", "value")
end

# S3 (requires AWS credentials)
SlateDb::Database.open("mydb", url: "s3://mybucket/path") do |db|
  db.put("key", "value")
end

# Azure Blob Storage
SlateDb::Database.open("mydb", url: "az://container/path") do |db|
  db.put("key", "value")
end

# Google Cloud Storage
SlateDb::Database.open("mydb", url: "gs://bucket/path") do |db|
  db.put("key", "value")
end
```

#### Cloud Storage Credentials

SlateDB uses the [object_store](https://docs.rs/object_store) crate, which automatically discovers credentials from standard environment variables and configuration files:

**AWS S3:**
- Environment variables: `AWS_ACCESS_KEY_ID`, `AWS_SECRET_ACCESS_KEY`, `AWS_SESSION_TOKEN`, `AWS_REGION`
- Credential files: `~/.aws/credentials`, `~/.aws/config`
- IAM roles (when running on EC2/ECS/EKS)
- Web identity tokens (for IRSA on EKS)

**Azure Blob Storage:**
- Environment variables: `AZURE_STORAGE_ACCOUNT_NAME`, `AZURE_STORAGE_ACCOUNT_KEY`, `AZURE_STORAGE_SAS_TOKEN`
- Azure CLI credentials: `az login`
- Managed Identity (when running on Azure)

**Google Cloud Storage:**
- Environment variables: `GOOGLE_SERVICE_ACCOUNT`, `GOOGLE_SERVICE_ACCOUNT_PATH`, `GOOGLE_SERVICE_ACCOUNT_KEY`
- Application Default Credentials: `gcloud auth application-default login`
- Service account key file: `GOOGLE_APPLICATION_CREDENTIALS=/path/to/key.json`

Example with explicit AWS credentials:

```ruby
# Set credentials via environment
ENV['AWS_ACCESS_KEY_ID'] = 'your-access-key'
ENV['AWS_SECRET_ACCESS_KEY'] = 'your-secret-key'
ENV['AWS_REGION'] = 'us-east-1'

SlateDb::Database.open("mydb", url: "s3://mybucket/path") do |db|
  db.put("key", "value")
end
```

### Options

#### Put Options

```ruby
# Set TTL (time-to-live) in milliseconds
db.put("key", "value", ttl: 60_000)  # expires in 60 seconds

# Don't wait for durability
db.put("key", "value", await_durable: false)

# Supply an explicit sequence number (SlateDB >= 0.13.0)
db.put("key", "value", seqnum: 42)
```

#### User-Supplied Sequence Numbers

By default SlateDB assigns a monotonically increasing sequence number to every
write. Since SlateDB 0.13.0 you can instead supply your own via `seqnum:`. The
value must be **strictly greater** than the current maximum sequence number, or
the write is rejected with `SlateDb::InvalidArgumentError`. This is useful when
replaying an external log or coordinating sequence numbers across systems.

```ruby
db.put("key", "value", seqnum: 1_000)
db.delete("old", seqnum: 1_001)
db.merge("counter", "5", seqnum: 1_002)        # requires a merge operator
db.write(batch, seqnum: 1_003)                 # applied across the batch
db.batch(seqnum: 1_004) { |b| b.put("k", "v") }

# The sequence number is reflected in the stored record
db.put("key", "value", seqnum: 2_000)
db.get_key_value("key")[:seq]  # => 2000

# On a transaction it is supplied at commit time
txn = db.begin_transaction
txn.put("k", "v")
txn.commit(seqnum: 3_000)
```

#### Get Options

```ruby
# Filter by durability level
db.get("key", durability_filter: "memory")
db.get("key", durability_filter: "remote")

# Include uncommitted data
db.get("key", dirty: true)
```

#### Key-Value Metadata

SlateDB can return the full key-value record, including storage metadata:

```ruby
db.put("key", "value")
entry = db.get_key_value("key")
# => { key: "key", value: "value", seq: 1, create_ts: 1_765_000_000_000, expire_ts: nil }

entry[:value]     # => "value"
entry[:seq]       # SlateDB sequence number
entry[:create_ts] # creation timestamp in milliseconds
entry[:expire_ts] # expiration timestamp in milliseconds, or nil

# Alias for the same API
db.get_entry("key")

# The same read options accepted by #get are supported
db.get_key_value("key", durability_filter: "memory", cache_blocks: false)
```

Missing keys return `nil`, matching `#get`.

#### Delete Options

```ruby
# Don't wait for durability
db.delete("key", await_durable: false)

# Supply an explicit sequence number (SlateDB >= 0.13.0)
db.delete("key", seqnum: 42)
```

### Scanning

Iterate over key ranges using the `scan` method:

```ruby
# Scan all keys from "a" onwards
db.scan("a").each do |key, value|
  puts "#{key}: #{value}"
end

# Scan a specific range [start, end)
db.scan("a", "z").each do |key, value|
  puts "#{key}: #{value}"
end

# Scan in descending key order
db.scan("a", "z", order: :desc).each do |key, value|
  puts "#{key}: #{value}"
end

# Use Enumerable methods
keys = db.scan("user:").map { |k, v| k }
users = db.scan("user:").select { |k, v| v.include?("active") }

# Convert to array
all_entries = db.scan("").to_a
```

#### Prefix Scanning

Scan all keys with a given prefix using `scan_prefix`:

```ruby
# Scan all keys starting with "user:"
db.scan_prefix("user:").each do |key, value|
  puts "#{key}: #{value}"
end

# Block form
db.scan_prefix("order:") do |key, value|
  puts "#{key}: #{value}"
end

# Prefix scans can also run in descending key order
db.scan_prefix("user:", order: :desc).each do |key, value|
  puts "#{key}: #{value}"
end

# Works with transactions, snapshots, and readers too
db.transaction do |txn|
  txn.scan_prefix("item:").each do |k, v|
    puts "#{k}: #{v}"
  end
end
```

You can narrow a prefix scan to a sub-range using the `from:` (inclusive) and
`to:` (exclusive) options (SlateDB >= 0.14.0). Both are key *suffixes* that are
appended to the prefix, so they let you paginate or resume within a prefix
without building full-key ranges by hand. The scan never escapes the prefix.

```ruby
# Keys "user:100" (inclusive) up to "user:200" (exclusive)
db.scan_prefix("user:", from: "100", to: "200").each do |key, value|
  puts "#{key}: #{value}"
end

# Only a lower bound: from "user:500" to the end of the prefix
db.scan_prefix("user:", from: "500")

# Only an upper bound: from the start of the prefix up to (but not including) "user:100"
db.scan_prefix("user:", to: "100")

# Sub-ranges compose with order: and work on transactions, snapshots, and readers
db.scan_prefix("user:", from: "100", to: "200", order: :desc)
```

### Merge Operations

Merge operations allow you to combine values without reading them first, useful for counters, append-only logs, and similar patterns:

```ruby
# Open with a built-in merge operator
SlateDb::Database.open("/tmp/mydb", merge_operator: :string_concat) do |db|
  # Merge appends to existing values (or creates if key doesn't exist)
  db.merge("log", "line1\n")
  db.merge("log", "line2\n")
  db.merge("log", "line3\n")

  db.get("log")  # => "line1\nline2\nline3\n"
end

# Merge with options
db.merge("key", "value", ttl: 60_000, await_durable: false)

# Works in transactions and batches
db.transaction do |txn|
  txn.merge("counter", "1")
end

db.batch do |b|
  b.merge("key", "a")
   .merge("key", "b")
end
```

#### Available Merge Operators

- `:string_concat` (or `:concat`) - Concatenates byte values (built-in)

Ruby Proc/lambda operators are rejected. Use a serializable read/modify/write
transaction for custom logic, with application-level conflict handling:

```ruby
db.transaction(isolation: :serializable) do |txn|
  txn.put("visits", (txn.get("visits").to_i + 1).to_s)
end
```

### Write Batches

Perform multiple writes atomically:

```ruby
# Create a batch manually
batch = SlateDb::WriteBatch.new
batch.put("key1", "value1")
batch.put("key2", "value2", ttl: 60_000)
batch.delete("old_key")
db.write(batch)

# Or use the block helper
db.batch do |b|
  b.put("key1", "value1")
  b.put("key2", "value2")
  b.delete("old_key")
end
```

### Transactions

ACID transactions with snapshot or serializable isolation:

```ruby
# Block form (recommended) - auto-commits on success, rolls back on exception
db.transaction do |txn|
  balance = txn.get("balance").to_i
  txn.put("balance", (balance - 100).to_s)
  txn.put("withdrawal", "100")
end

# With serializable isolation for strict consistency
db.transaction(isolation: :serializable) do |txn|
  counter = txn.get("counter").to_i
  txn.put("counter", (counter + 1).to_s)
end

# Manual transaction management
txn = db.begin_transaction(isolation: :snapshot)
txn.put("key", "value")
txn.commit  # or txn.rollback
```

Transaction operations:

```ruby
db.transaction do |txn|
  # Read
  value = txn.get("key")

  # Write
  txn.put("key", "value")
  txn.put("expiring", "data", ttl: 30_000)

  # Delete
  txn.delete("old_key")

  # Scan
  txn.scan("prefix:").each do |k, v|
    puts "#{k}: #{v}"
  end

  # Scan with prefix
  txn.scan_prefix("user:").each do |k, v|
    puts "#{k}: #{v}"
  end
end
```

#### Explicit Read Tracking

In serializable transactions, use `mark_read` to explicitly track keys for conflict detection without actually reading them:

```ruby
db.transaction(isolation: :serializable) do |txn|
  # Mark keys as read for conflict detection
  txn.mark_read(["key1", "key2", "key3"])

  # Now if another transaction modifies key1/key2/key3,
  # this transaction will fail on commit
  txn.put("result", "computed_value")
end
```

### Checkpoints

Create durable checkpoints for backup or read replica purposes:

```ruby
SlateDb::Database.open("/tmp/mydb", url: "file:///tmp/mydb") do |db|
  db.put("key", "value")
  db.flush

  # Create a checkpoint
  checkpoint = db.create_checkpoint
  puts "Checkpoint ID: #{checkpoint[:id]}"
  puts "Manifest ID: #{checkpoint[:manifest_id]}"

  # Create a named checkpoint with lifetime
  checkpoint = db.create_checkpoint(
    name: "before-migration",
    lifetime: 3_600_000  # 1 hour in milliseconds
  )
end
```

`create_checkpoint` waits until the checkpoint is stored. Its default
`scope: :durable` preserves the existing behavior. To include writes made with
`await_durable: false`, use `scope: :all`; no separate `flush` is needed.

To separate capturing the write boundary from waiting for storage:

```ruby
db.put("key", "before", await_durable: false)
handle = db.begin_checkpoint(scope: :all, name: "backup", lifetime: 3_600_000)
id = handle.id                      # UUID is available immediately
db.put("key", "after")             # not part of this checkpoint
checkpoint = handle.wait            # => { id: id, manifest_id: ... }
```

With `scope: :all`, writes completed before `begin_checkpoint` starts are included;
writes started after it returns are excluded. Concurrent writes during the call
may be included. Starting the checkpoint can still wait for internal flush work;
it is not a fully nonblocking Ruby call. `wait` releases the Ruby GVL while
waiting, consumes the handle, and raises `SlateDb::ClosedError` if called again.
`id` remains readable. Dropping the handle does not cancel or delete the checkpoint.
If waiting fails, use the UUID with `Admin#list_checkpoints` to check its outcome
before starting another checkpoint.

### Snapshots

Point-in-time consistent reads:

```ruby
# Block form (recommended)
db.snapshot do |snap|
  # All reads see the same consistent state
  value1 = snap.get("key1")
  value2 = snap.get("key2")
  
  snap.scan("prefix:").each do |k, v|
    puts "#{k}: #{v}"
  end
end  # automatically closed

# Manual management
snap = db.snapshot
value = snap.get("key")
snap.close
```

Range and prefix scans on snapshots and transactions accept the same `order:`
keyword as databases and readers, including `:asc`/`:ascending` and
`:desc`/`:descending`. Bounds remain lower-inclusive and upper-exclusive in
either direction.

### Reader (Read-Only Access)

Open a database in read-only mode, useful for replicas:

```ruby
# Basic read-only access
SlateDb::Reader.open("/tmp/mydb", url: "s3://bucket/path") do |reader|
  value = reader.get("key")
  
  reader.scan("prefix:").each do |k, v|
    puts "#{k}: #{v}"
  end

  # Range and prefix scans accept :asc/:ascending or :desc/:descending.
  reader.scan("user:100", "user:200", order: :desc).each do |k, v|
    puts "#{k}: #{v}"
  end
  reader.scan_prefix("user:", from: "100", to: "200", order: :desc).each do |k, v|
    puts "#{k}: #{v}"
  end
end

# Open at a specific checkpoint (pins the reader to that checkpoint's state)
SlateDb::Reader.open("/tmp/mydb", 
                     url: "s3://bucket/path",
                     checkpoint_id: "uuid-here") do |reader|
  reader.get("key")
end

# Follow the latest state without managing a checkpoint (SlateDB >= 0.15.0).
# This performs no object-store writes but offers no protection from garbage
# collection, so it suits read-only or mirrored databases. By default (neither
# checkpoint_id nor follow_latest set) the reader creates and periodically
# refreshes its own checkpoint instead.
SlateDb::Reader.open("/tmp/mydb",
                     url: "s3://bucket/path",
                     follow_latest: true) do |reader|
  reader.get("key")
end

# Enable the reader's on-disk cache and cap its open file handles
# (max_open_file_handles, added in SlateDB 0.13.0, only takes effect when
# cache_root is set, since that is what enables the cached object store).
SlateDb::Reader.open("/tmp/mydb",
                     url: "s3://bucket/path",
                     cache_root: "/var/cache/slatedb",
                     max_open_file_handles: 256) do |reader|
  reader.get("key")
end
```

When the writer uses `merge_operator: :concat`, pass the same operator to
`Reader.open` so WAL replay and reads apply the correct merge rule. Unknown
operator names now raise `SlateDb::InvalidArgumentError` instead of being ignored.

The `cache_root` and `max_open_file_handles` keywords still work, but SlateDB
0.18.1 deprecates this legacy disk cache in favor of its separate mirror crate or
tiered block cache. Those replacement APIs are not yet exposed here.

### Admin Operations

Administrative operations for database management:

```ruby
admin = SlateDb::Admin.new("/tmp/mydb", url: "s3://bucket/path")

# Manifests
json = admin.read_manifest           # Latest manifest as JSON
json = admin.read_manifest(123)      # Specific manifest by ID
json = admin.list_manifests          # List all manifests
json = admin.list_manifests(start: 1, end_id: 10)  # Range query

# Checkpoints
result = admin.create_checkpoint(name: "backup-2024")
# => { id: "uuid-string", manifest_id: 7 }

checkpoints = admin.list_checkpoints
checkpoints = admin.list_checkpoints(name: "backup")  # Filter by name

admin.refresh_checkpoint("uuid", lifetime: 3600_000)  # Extend lifetime
admin.delete_checkpoint("uuid")

# Garbage Collection
admin.run_gc                                    # Run with default settings
admin.run_gc(min_age: 3600_000)                 # Set min age for all directories (1 hour)

# Delete a database (SlateDB >= 0.16.0)
would_delete = admin.delete_db                  # Dry run: returns the object paths
                                                # that *would* be deleted, deleting nothing
deleted = admin.delete_db(confirm: true)        # Actually delete; returns the removed paths
```

`delete_db` is idempotent, and a confirmed delete of a directory that is not a
SlateDB database (no manifest and no in-progress deletion marker) is refused, so
a mistyped path cannot wipe an unrelated directory. For a cloned database, any
checkpoints it pinned in parent databases are stripped before its own objects
are removed.

### Flushing

Ensure all writes are persisted:

```ruby
db.put("key", "value")
db.flush
```

By default, `flush` flushes the write-ahead log (WAL) if enabled, otherwise the
memtable. You can choose explicitly with `flush_type:` (SlateDB >= 0.16.0):

```ruby
db.flush(flush_type: :wal)       # Flush the write-ahead log
db.flush(flush_type: :memtable)  # Freeze and flush memtable entries to object storage
```

### Closing

Closing flushes the active memtable first by default, so durable writes are not
lost. You can control the final flush when closing (SlateDB >= 0.16.0):

```ruby
db.close                       # Flush the memtable, then close (default)
db.close(flush_type: :wal)     # Flush the WAL before closing
db.close(flush: false)         # Close without a final flush; writes that are not
                               # yet durable may be lost
```

## Thread Safety

**SlateDB is fully thread-safe and optimized for concurrent access.**

- The `Database` class can be safely shared across multiple Ruby threads
- All operations (get, put, delete, scan, transactions) are thread-safe
- The Ruby bindings release the Global VM Lock (GVL) during I/O operations, allowing other Ruby threads to run concurrently
- Perfect for use with multi-threaded Ruby applications like Puma, Sidekiq, and concurrent test suites

```ruby
db = SlateDb::Database.open("/tmp/mydb")

# Safe to use from multiple threads
threads = 10.times.map do |i|
  Thread.new do
    db.put("key-#{i}", "value-#{i}")
    db.get("key-#{i}")
  end
end

threads.each(&:join)
```

**Implementation details:**
- The underlying SlateDB library uses `Arc` (atomic reference counting) and `RwLock` for internal state management
- I/O operations release the Ruby GVL using `rb_thread_call_without_gvl`, preventing blocking other threads
- A shared Tokio multi-threaded runtime handles all async operations efficiently

## Error Handling

SlateDB defines several exception classes:

```ruby
begin
  db.put("", "value")  # empty key
rescue SlateDb::InvalidArgumentError => e
  puts "Invalid argument: #{e.message}"
rescue SlateDb::TransactionError => e
  puts "Transaction conflict: #{e.message}"
rescue SlateDb::Error => e
  puts "SlateDB error: #{e.message}"
end
```

Exception hierarchy:

- `SlateDb::Error` - Base class (inherits from `StandardError`)
  - `SlateDb::TransactionError` - Transaction conflicts
  - `SlateDb::ClosedError` - Database has been closed
  - `SlateDb::UnavailableError` - Storage/network unavailable
  - `SlateDb::InvalidArgumentError` - Invalid arguments
  - `SlateDb::DataError` - Data corruption or format errors
  - `SlateDb::InternalError` - Internal errors

## Requirements

- Ruby 3.3+
- Rust toolchain (for building from source)

## Releasing

Releases publish a generic `ruby` platform gem and **precompiled native gems**
for six platforms (`x86_64-linux`, `aarch64-linux`, `x86_64-linux-musl`,
`aarch64-linux-musl`, `arm64-darwin`, `x86_64-darwin`) to
[rubygems.org](https://rubygems.org/gems/slatedb). The generic gem keeps
RubyGems' latest-version metadata aligned; supported platforms should still
resolve to their matching precompiled native gem.

### Cutting a release

The gem version comes from `lib/slatedb/version.rb`, not from the tag. Use the
mise helper to bump, commit, tag, and push in one step (must be on `main` with a
clean working tree):

```bash
mise run release:cut 0.4.3
# or: mise run release:cut 0.4.3 --no-push
```

That updates `SlateDb::VERSION`, commits `Release v0.4.3`, creates tag `v0.4.3`,
and pushes the commit + tag (unless `--no-push`). The tag triggers the release
pipeline.

The `release:verify-tag` step fails the build if the tag and
`SlateDb::VERSION` disagree, so the tag (`v0.4.3`) must match `version.rb`
(`0.4.3`).

To exercise packaging without publishing, trigger a manual build on the
`slatedb-rb-release` pipeline or set `DRY_RUN=true`.

## Development

After checking out the repo, run:

```bash
bundle install
bundle exec rake compile
bundle exec rake spec
```

To run specific tests:

```bash
bundle exec rspec spec/database_spec.rb
bundle exec rspec spec/transaction_spec.rb
```

## Contributing

Bug reports and pull requests are welcome on GitHub at https://github.com/catkins/slatedb-rb.

Also, find me on the [SlateDB Discord Server](https://discord.gg/mHYmGy5MgA).

## License

Apache-2.0
