# SlateDB Ruby

Ruby bindings for [SlateDB](https://slatedb.io), a cloud-native embedded key-value store built on object storage.

[![Build status](https://badge.buildkite.com/a7ae51f3a0bc7809cf66981641ec47b3c70db8cf349a5e462f.svg)](https://buildkite.com/catkins-test/slatedb-rb) [![Gem Version](https://badge.fury.io/rb/slatedb.svg)](https://badge.fury.io/rb/slatedb)

## Production Readiness

These bindings are still in early development, and while SlateDB itself is used in Production, these bindings have yet to be. Contributions are welcome!

### TODO

- [ ] Cross-compile native extensions

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

> [!IMPORTANT]
> This gem currently requires a working Rust toolchain to install until the dependencies are cross-compiled.

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

Restrict a prefix scan to a range of key **suffixes** (the part of each key
after the prefix) by passing a `Range` as `suffix:` (requires SlateDB >=
0.14.0). A bound `s` selects the full key `prefix + s`, so it lets you page or
window within a prefix without materialising the whole set:

```ruby
# Keys "event:2024".."event:2025", i.e. every 2024 event (exclusive end)
db.scan_prefix("event:", suffix: "2024"..."2025").each do |key, value|
  puts key
end

# Inclusive end (..), endless and beginless ranges are all supported
db.scan_prefix("event:", suffix: "2024".."2026") # 2024 through 2026 inclusive
db.scan_prefix("event:", suffix: "2024"..)        # 2024 and later
db.scan_prefix("event:", suffix: ..."2024")       # everything before 2024

# Available on transactions, snapshots, and readers too
snap.scan_prefix("event:", suffix: "2024"..."2025")
```

Range bounds must be strings (a non-String bound raises `ArgumentError`). Each
bound must be a non-empty string and the range must be non-empty, otherwise
`SlateDb::InvalidArgumentError` is raised. Beginless/endless ranges leave the
corresponding side unbounded.

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

#### Custom Merge Operators

You can provide a Ruby Proc/lambda as a custom merge operator:

```ruby
# Counter merge operator (adds numbers)
counter_merge = ->(key, existing, new_value) {
  existing_num = existing ? existing.to_i : 0
  (existing_num + new_value.to_i).to_s
}

SlateDb::Database.open("/tmp/mydb", merge_operator: counter_merge) do |db|
  db.merge("visits", "1")
  db.merge("visits", "1")
  db.merge("visits", "1")

  db.get("visits")  # => "3"
end

# Max value merge operator
max_merge = ->(key, existing, new_value) {
  existing_num = existing ? existing.to_i : 0
  new_num = new_value.to_i
  [existing_num, new_num].max.to_s
}

SlateDb::Database.open("/tmp/mydb", merge_operator: max_merge) do |db|
  db.merge("high_score", "100")
  db.merge("high_score", "250")
  db.merge("high_score", "150")

  db.get("high_score")  # => "250"
end
```

The proc receives three arguments:
- `key` - The key being merged
- `existing` - The existing value (nil if no value exists)
- `new_value` - The new merge operand

**Note:** Custom Proc merge operators work best with direct `db.merge()` calls. When used with transactions or batches, some merge operations may be processed on background threads and fall back to string concatenation.

#### Available Merge Operators

- `:string_concat` (or `:concat`) - Concatenates byte values (built-in)
- Any `Proc` or `lambda` - Custom merge logic

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

### Reader (Read-Only Access)

Open a database in read-only mode, useful for replicas:

```ruby
# Basic read-only access
SlateDb::Reader.open("/tmp/mydb", url: "s3://bucket/path") do |reader|
  value = reader.get("key")
  
  reader.scan("prefix:").each do |k, v|
    puts "#{k}: #{v}"
  end
end

# Open at a specific checkpoint
SlateDb::Reader.open("/tmp/mydb", 
                     url: "s3://bucket/path",
                     checkpoint_id: "uuid-here") do |reader|
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
admin.run_gc(manifest_min_age: 86400_000)       # Custom age for manifest (1 day)
admin.run_gc(wal_min_age: 60_000)               # Custom age for WAL (1 minute)
admin.run_gc(compacted_min_age: 60_000)         # Custom age for compacted (1 minute)
```

### Flushing

Ensure all writes are persisted:

```ruby
db.put("key", "value")
db.flush
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
