# frozen_string_literal: true

module SlateDb
  class Transaction
    # Get a value by key within the transaction.
    #
    # @param key [String] The key to look up
    # @param durability_filter [String, nil] Filter by durability level
    # @param dirty [Boolean, nil] Whether to include uncommitted data
    # @param cache_blocks [Boolean, nil] Whether to cache blocks
    # @return [String, nil] The value, or nil if not found
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

    # Store a key-value pair within the transaction.
    #
    # @param key [String] The key to store
    # @param value [String] The value to store
    # @param ttl [Integer, nil] Time-to-live in milliseconds
    # @return [void]
    #
    def put(key, value, ttl: nil)
      if ttl
        _put_with_options(key, value, { ttl: ttl })
      else
        _put(key, value)
      end
    end

    # Delete a key within the transaction.
    #
    # @param key [String] The key to delete
    # @return [void]
    #
    def delete(key)
      _delete(key)
    end

    # Merge a value within the transaction.
    #
    # @param key [String] The key to merge into
    # @param value [String] The merge operand to apply
    # @param ttl [Integer, nil] Time-to-live in milliseconds
    # @return [void]
    #
    def merge(key, value, ttl: nil)
      if ttl
        _merge_with_options(key, value, { ttl: ttl })
      else
        _merge(key, value)
      end
    end

    # Scan a range of keys within the transaction.
    #
    # @param start_key [String] The start key (inclusive)
    # @param end_key [String, nil] The end key (exclusive)
    # @return [Iterator] An iterator over key-value pairs
    #
    def scan(start_key, end_key = nil, durability_filter: nil, dirty: nil,
             read_ahead_bytes: nil, cache_blocks: nil, max_fetch_tasks: nil, &)
      opts = {}
      opts[:durability_filter] = durability_filter.to_s if durability_filter
      opts[:dirty] = dirty unless dirty.nil?
      opts[:read_ahead_bytes] = read_ahead_bytes if read_ahead_bytes
      opts[:cache_blocks] = cache_blocks unless cache_blocks.nil?
      opts[:max_fetch_tasks] = max_fetch_tasks if max_fetch_tasks

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

    # Scan all keys with a given prefix within the transaction.
    #
    # @param prefix [String] The key prefix to scan
    # @param durability_filter [String, nil] Filter by durability level
    # @param dirty [Boolean, nil] Whether to include uncommitted data
    # @param read_ahead_bytes [Integer, nil] Number of bytes to read ahead
    # @param cache_blocks [Boolean, nil] Whether to cache blocks
    # @param max_fetch_tasks [Integer, nil] Maximum number of fetch tasks
    # @param from [String, nil] Inclusive lower bound suffix, appended to the
    #   prefix, to start scanning from. Defaults to the start of the prefix.
    # @param to [String, nil] Exclusive upper bound suffix, appended to the
    #   prefix, to stop scanning at. Defaults to the end of the prefix.
    # @return [Iterator] An iterator over key-value pairs
    #
    def scan_prefix(prefix, durability_filter: nil, dirty: nil,
                    read_ahead_bytes: nil, cache_blocks: nil, max_fetch_tasks: nil,
                    from: nil, to: nil, &)
      opts = {
        durability_filter: durability_filter&.to_s,
        dirty: dirty,
        read_ahead_bytes: read_ahead_bytes,
        cache_blocks: cache_blocks,
        max_fetch_tasks: max_fetch_tasks,
        subrange_from: from,
        subrange_to: to
      }.compact

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

    # Mark keys as read for conflict detection.
    #
    # This explicitly tracks reads for conflict checking in serializable isolation,
    # allowing selective read-write conflict detection even when keys weren't
    # actually read via get().
    #
    # @param keys [Array<String>] The keys to mark as read
    # @return [void]
    #
    # @example Mark keys for conflict detection
    #   db.transaction(isolation: :serializable) do |txn|
    #     txn.mark_read(["key1", "key2"])
    #     # These keys will now be checked for conflicts on commit
    #     txn.put("key3", "value")
    #   end
    #
    def mark_read(keys)
      _mark_read(Array(keys))
    end

    # Commit the transaction.
    #
    # @param await_durable [Boolean, nil] Whether to wait for durability (default: true)
    # @param seqnum [Integer, nil] User-supplied sequence number for the commit.
    #   When provided (and non-zero), it is used instead of the internally
    #   generated sequence number and must be strictly greater than the current
    #   maximum sequence number. (Requires SlateDB >= 0.13.0)
    # @return [void]
    #
    # @example Commit a transaction
    #   txn = db.begin_transaction
    #   txn.put("key", "value")
    #   txn.commit
    #
    # @example Commit with an explicit sequence number
    #   txn.commit(seqnum: 99)
    #
    def commit(await_durable: nil, seqnum: nil)
      opts = {}
      opts[:await_durable] = await_durable unless await_durable.nil?
      opts[:seqnum] = seqnum if seqnum

      if opts.empty?
        _commit
      else
        _commit_with_options(opts)
      end
    end
  end
end
