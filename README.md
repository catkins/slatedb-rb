# SlateDB Ruby

Ruby bindings for [SlateDB](https://slatedb.io), a cloud-native embedded key-value store built on object storage.

[![Build status](https://badge.buildkite.com/a7ae51f3a0bc7809cf66981641ec47b3c70db8cf349a5e462f.svg)](https://buildkite.com/catkins-test/slatedb-rb) [![Gem Version](https://badge.fury.io/rb/slatedb.svg)](https://badge.fury.io/rb/slatedb)

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
```

#### Get Options

```ruby
# Filter by durability level
db.get("key", durability_filter: "memory")
db.get("key", durability_filter: "remote")

# Include uncommitted data
db.get("key", dirty: true)
```

#### Delete Options

```ruby
# Don't wait for durability
db.delete("key", await_durable: false)
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

# Use Enumerable methods
keys = db.scan("user:").map { |k, v| k }
users = db.scan("user:").select { |k, v| v.include?("active") }

# Convert to array
all_entries = db.scan("").to_a
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

- Ruby 3.1+
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

## License

Apache-2.0
