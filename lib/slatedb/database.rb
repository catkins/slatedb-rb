# frozen_string_literal: true

module SlateDb
  class Database # rubocop:disable Metrics/ClassLength
    private_class_method :new

    class << self
      # Open a database at the given path.
      #
      # @param path [String] The path identifier for the database
      # @param url [String, nil] Optional object store URL (e.g., "s3://bucket/path")
      # @param merge_operator [Symbol, String, Proc, nil] Optional merge operator.
      #   Can be a symbol/string ("string_concat" or "concat") or a Proc/lambda
      #   that takes (key, existing_value, new_value) and returns the merged value.
      # @yield [db] If a block is given, yields the database and ensures it's closed
      # @return [Database] The opened database (or block result if block given)
      #
      # @example Open a database
      #   db = SlateDb::Database.open("/tmp/mydb")
      #   db.put("key", "value")
      #   db.close
      #
      # @example Open with block (auto-close)
      #   SlateDb::Database.open("/tmp/mydb") do |db|
      #     db.put("key", "value")
      #   end # automatically closed
      #
      # @example Open with S3 backend
      #   db = SlateDb::Database.open("/tmp/mydb", url: "s3://mybucket/path")
      #
      # @example Open with a custom merge operator (Proc)
      #   # Custom merge that adds numbers
      #   db = SlateDb::Database.open("/tmp/mydb", merge_operator: ->(key, existing, new_val) {
      #     existing_num = existing ? existing.to_i : 0
      #     (existing_num + new_val.to_i).to_s
      #   })
      #   db.merge("counter", "5")
      #   db.merge("counter", "3")
      #   db.get("counter") # => "8"
      #
      def open(path, url: nil, merge_operator: nil)
        opts = {}

        case merge_operator
        when Symbol, String
          opts[:merge_operator] = merge_operator.to_s
        when Proc
          # Store the proc to prevent GC and pass to Rust
          @_merge_operator_proc = merge_operator
          opts[:merge_operator_proc] = merge_operator
        end

        db = _open(path, url, opts)

        if block_given?
          begin
            yield db
          ensure
            begin
              db.close
            rescue StandardError
              nil
            end
          end
        else
          db
        end
      end
    end

    # Get a value by key.
    #
    # @param key [String] The key to look up
    # @param durability_filter [String, nil] Filter by durability level ("remote" or "memory")
    # @param dirty [Boolean, nil] Whether to include uncommitted data
    # @param cache_blocks [Boolean, nil] Whether to cache blocks
    # @return [String, nil] The value, or nil if not found
    #
    # @example Basic get
    #   value = db.get("mykey")
    #
    # @example Get with options
    #   value = db.get("mykey", durability_filter: "memory", dirty: true)
    #
    def get(key, durability_filter: nil, dirty: nil, cache_blocks: nil)
      opts = {}
      opts[:durability_filter] = durability_filter.to_s if durability_filter
      opts[:dirty] = dirty unless dirty.nil?
      opts[:cache_blocks] = cache_blocks unless cache_blocks.nil?

      if opts.empty?
        _get(key)
      else
        _get_with_options(key, opts)
      end
    end

    # Get a key-value pair with SlateDB metadata.
    #
    # @param key [String] The key to look up
    # @param durability_filter [String, nil] Filter by durability level ("remote" or "memory")
    # @param dirty [Boolean, nil] Whether to include uncommitted data
    # @param cache_blocks [Boolean, nil] Whether to cache blocks
    # @return [Hash, nil] A hash with :key, :value, :seq, :create_ts, and :expire_ts, or nil if not found
    #
    # @example Inspect metadata
    #   entry = db.get_key_value("mykey")
    #   entry[:value] # => "myvalue"
    #   entry[:seq]   # => sequence number
    #
    def get_key_value(key, durability_filter: nil, dirty: nil, cache_blocks: nil)
      opts = {}
      opts[:durability_filter] = durability_filter.to_s if durability_filter
      opts[:dirty] = dirty unless dirty.nil?
      opts[:cache_blocks] = cache_blocks unless cache_blocks.nil?

      if opts.empty?
        _get_key_value(key)
      else
        _get_key_value_with_options(key, opts)
      end
    end

    alias get_entry get_key_value

    # Store a key-value pair.
    #
    # @param key [String] The key to store
    # @param value [String] The value to store
    # @param ttl [Integer, nil] Time-to-live in milliseconds
    # @param await_durable [Boolean] Whether to wait for durability (default: true)
    # @param seqnum [Integer, nil] Optional user-defined sequence number for the
    #   write. When a positive value is given it overrides the internally
    #   generated sequence number and must be strictly greater than the current
    #   maximum sequence number. A value of 0 is treated the same as nil
    #   (auto-assign).
    # @return [void]
    #
    # @example Basic put
    #   db.put("mykey", "myvalue")
    #
    # @example Put with TTL
    #   db.put("mykey", "myvalue", ttl: 60_000) # expires in 60 seconds
    #
    # @example Put without waiting for durability
    #   db.put("mykey", "myvalue", await_durable: false)
    #
    # @example Put with an explicit sequence number
    #   db.put("mykey", "myvalue", seqnum: 42)
    #
    def put(key, value, ttl: nil, await_durable: nil, seqnum: nil)
      opts = {}
      opts[:ttl] = ttl if ttl
      opts[:await_durable] = await_durable unless await_durable.nil?
      opts[:seqnum] = seqnum if seqnum

      if opts.empty?
        _put(key, value)
      else
        _put_with_options(key, value, opts)
      end
    end

    # Delete a key.
    #
    # @param key [String] The key to delete
    # @param await_durable [Boolean] Whether to wait for durability (default: true)
    # @param seqnum [Integer, nil] Optional user-defined sequence number for the write
    # @return [void]
    #
    # @example Basic delete
    #   db.delete("mykey")
    #
    # @example Delete without waiting for durability
    #   db.delete("mykey", await_durable: false)
    #
    def delete(key, await_durable: nil, seqnum: nil)
      opts = {}
      opts[:await_durable] = await_durable unless await_durable.nil?
      opts[:seqnum] = seqnum if seqnum

      if opts.empty?
        _delete(key)
      else
        _delete_with_options(key, opts)
      end
    end

    # Scan a range of keys.
    #
    # @param start_key [String] The start key (inclusive)
    # @param end_key [String, nil] The end key (exclusive). If nil, scans to end.
    # @param durability_filter [String, nil] Filter by durability level ("remote" or "memory")
    # @param dirty [Boolean, nil] Whether to include uncommitted data
    # @param read_ahead_bytes [Integer, nil] Number of bytes to read ahead
    # @param cache_blocks [Boolean, nil] Whether to cache blocks
    # @param max_fetch_tasks [Integer, nil] Maximum number of fetch tasks
    # @param order [Symbol, String, nil] Iteration order (:asc/:ascending or :desc/:descending)
    # @return [Iterator] An iterator over key-value pairs
    #
    # @example Basic scan
    #   iter = db.scan("a")
    #   while entry = iter.next_entry
    #     key, value = entry
    #     puts "#{key}: #{value}"
    #   end
    #
    # @example Scan with range
    #   iter = db.scan("a", "z")
    #
    # @example Scan with block
    #   db.scan("user:") do |key, value|
    #     puts "#{key}: #{value}"
    #   end
    #
    def scan(start_key, end_key = nil, durability_filter: nil, dirty: nil,
             read_ahead_bytes: nil, cache_blocks: nil, max_fetch_tasks: nil, order: nil, &)
      opts = scan_options(
        durability_filter: durability_filter,
        dirty: dirty,
        read_ahead_bytes: read_ahead_bytes,
        cache_blocks: cache_blocks,
        max_fetch_tasks: max_fetch_tasks,
        order: order
      )

      iter = if opts.empty?
               _scan(start_key, end_key)
             else
               _scan_with_options(start_key, end_key, opts)
             end

      if block_given?
        iter.each(&)
      else
        iter
      end
    end

    # Scan all keys with a given prefix.
    #
    # @param prefix [String] The key prefix to scan
    # @param durability_filter [String, nil] Filter by durability level ("remote" or "memory")
    # @param dirty [Boolean, nil] Whether to include uncommitted data
    # @param read_ahead_bytes [Integer, nil] Number of bytes to read ahead
    # @param cache_blocks [Boolean, nil] Whether to cache blocks
    # @param max_fetch_tasks [Integer, nil] Maximum number of fetch tasks
    # @param order [Symbol, String, nil] Iteration order (:asc/:ascending or :desc/:descending)
    # @return [Iterator] An iterator over key-value pairs
    #
    # @example Scan all user keys
    #   db.scan_prefix("user:") do |key, value|
    #     puts "#{key}: #{value}"
    #   end
    #
    def scan_prefix(prefix, durability_filter: nil, dirty: nil,
                    read_ahead_bytes: nil, cache_blocks: nil, max_fetch_tasks: nil, order: nil, &)
      opts = scan_options(
        durability_filter: durability_filter,
        dirty: dirty,
        read_ahead_bytes: read_ahead_bytes,
        cache_blocks: cache_blocks,
        max_fetch_tasks: max_fetch_tasks,
        order: order
      )

      iter = if opts.empty?
               _scan_prefix(prefix)
             else
               _scan_prefix_with_options(prefix, opts)
             end

      if block_given?
        iter.each(&)
      else
        iter
      end
    end

    def scan_options(durability_filter:, dirty:, read_ahead_bytes:, cache_blocks:,
                     max_fetch_tasks:, order:)
      opts = {}
      opts[:durability_filter] = durability_filter.to_s if durability_filter
      opts[:dirty] = dirty unless dirty.nil?
      opts[:read_ahead_bytes] = read_ahead_bytes if read_ahead_bytes
      opts[:cache_blocks] = cache_blocks unless cache_blocks.nil?
      opts[:max_fetch_tasks] = max_fetch_tasks if max_fetch_tasks
      opts[:order] = order.to_s if order
      opts
    end

    private :scan_options

    # Write a batch of operations atomically.
    #
    # @param batch [WriteBatch] The batch to write
    # @param await_durable [Boolean] Whether to wait for durability (default: true)
    # @param seqnum [Integer, nil] Optional user-defined sequence number for the batch
    # @return [void]
    #
    # @example Write a batch
    #   batch = SlateDb::WriteBatch.new
    #   batch.put("key1", "value1")
    #   batch.put("key2", "value2")
    #   batch.delete("key3")
    #   db.write(batch)
    #
    # @example Using batch block helper
    #   db.batch do |b|
    #     b.put("key1", "value1")
    #     b.put("key2", "value2")
    #   end
    #
    def write(batch, await_durable: nil, seqnum: nil)
      opts = {}
      opts[:await_durable] = await_durable unless await_durable.nil?
      opts[:seqnum] = seqnum if seqnum

      if opts.empty?
        _write(batch)
      else
        _write_with_options(batch, opts)
      end
    end

    # Merge a value into the database.
    #
    # @param key [String] The key to merge into
    # @param value [String] The merge operand to apply
    # @param ttl [Integer, nil] Time-to-live in milliseconds
    # @param await_durable [Boolean] Whether to wait for durability (default: true)
    # @param seqnum [Integer, nil] Optional user-defined sequence number for the write
    # @return [void]
    #
    # @example Merge with string concatenation operator
    #   db = SlateDb::Database.open("/tmp/mydb", merge_operator: :string_concat)
    #   db.merge("key", "part1")
    #   db.merge("key", "part2")
    #
    def merge(key, value, ttl: nil, await_durable: nil, seqnum: nil)
      opts = {}
      opts[:ttl] = ttl if ttl
      opts[:await_durable] = await_durable unless await_durable.nil?
      opts[:seqnum] = seqnum if seqnum

      if opts.empty?
        _merge(key, value)
      else
        _merge_with_options(key, value, opts)
      end
    end

    # Create and write a batch using a block.
    #
    # @param await_durable [Boolean] Whether to wait for durability (default: true)
    # @param seqnum [Integer, nil] Optional user-defined sequence number for the batch
    # @yield [batch] Yields a WriteBatch to the block
    # @return [void]
    #
    # @example
    #   db.batch do |b|
    #     b.put("key1", "value1")
    #     b.put("key2", "value2")
    #     b.delete("old_key")
    #   end
    #
    def batch(await_durable: nil, seqnum: nil)
      b = WriteBatch.new
      yield b
      write(b, await_durable: await_durable, seqnum: seqnum)
    end

    # Begin a new transaction.
    #
    # @param isolation [Symbol, String] Isolation level (:snapshot or :serializable)
    # @yield [txn] If a block is given, yields the transaction and auto-commits/rollbacks
    # @return [Transaction, Object] The transaction (or block result if block given)
    #
    # @example Manual transaction management
    #   txn = db.begin_transaction
    #   txn.put("key", "value")
    #   txn.commit
    #
    # @example Block-based transaction (auto-commit)
    #   db.transaction do |txn|
    #     txn.put("key", "value")
    #     txn.get("other_key")
    #   end # automatically committed
    #
    # @example Serializable isolation
    #   db.transaction(isolation: :serializable) do |txn|
    #     val = txn.get("counter")
    #     txn.put("counter", (val.to_i + 1).to_s)
    #   end
    #
    def begin_transaction(isolation: nil)
      isolation_str = isolation&.to_s
      _begin_transaction(isolation_str)
    end

    # Execute a block within a transaction.
    #
    # The transaction is automatically committed if the block succeeds,
    # or rolled back if an exception is raised.
    #
    # @param isolation [Symbol, String] Isolation level (:snapshot or :serializable)
    # @yield [txn] Yields the transaction to the block
    # @return [Object] The result of the block
    #
    # @example
    #   result = db.transaction do |txn|
    #     old_val = txn.get("counter") || "0"
    #     new_val = (old_val.to_i + 1).to_s
    #     txn.put("counter", new_val)
    #     new_val
    #   end
    #
    def transaction(isolation: nil)
      txn = begin_transaction(isolation: isolation)
      begin
        result = yield txn
        txn.commit
        result
      rescue StandardError
        begin
          txn.rollback
        rescue StandardError
          nil
        end
        raise
      end
    end

    # Create a snapshot for consistent reads.
    #
    # @yield [snapshot] If a block is given, yields the snapshot and auto-closes
    # @return [Snapshot, Object] The snapshot (or block result if block given)
    #
    # @example Manual snapshot management
    #   snapshot = db.snapshot
    #   snapshot.get("key")
    #   snapshot.close
    #
    # @example Block-based snapshot (auto-close)
    #   db.snapshot do |snap|
    #     snap.get("key1")
    #     snap.get("key2")
    #   end # automatically closed
    #
    def snapshot
      snap = _snapshot

      if block_given?
        begin
          yield snap
        ensure
          begin
            snap.close
          rescue StandardError
            nil
          end
        end
      else
        snap
      end
    end

    # Create a checkpoint of the database.
    #
    # @param lifetime [Integer, nil] Checkpoint lifetime in milliseconds
    # @param name [String, nil] Optional name for the checkpoint
    # @return [Hash] Hash with :id (UUID string) and :manifest_id (integer)
    #
    # @example Create a named checkpoint
    #   checkpoint = db.create_checkpoint(name: "before-migration")
    #   puts "Checkpoint ID: #{checkpoint[:id]}"
    #
    # @example Create a checkpoint with lifetime
    #   checkpoint = db.create_checkpoint(lifetime: 3600_000) # 1 hour
    #
    def create_checkpoint(lifetime: nil, name: nil)
      opts = {}
      opts[:lifetime] = lifetime if lifetime
      opts[:name] = name if name
      _create_checkpoint(opts)
    end

    # Get database metrics registry.
    #
    # @return [Metrics] Metrics registry
    def metrics
      _metrics
    end
  end
end
