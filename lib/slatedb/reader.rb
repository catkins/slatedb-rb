# frozen_string_literal: true

module SlateDb
  class Reader
    class << self
      # Open a read-only reader at the given path.
      #
      # @param path [String] The path identifier for the database
      # @param url [String, nil] Optional object store URL
      # @param checkpoint_id [String, nil] Optional checkpoint UUID to read at
      # @param manifest_poll_interval [Integer, nil] Poll interval in milliseconds
      # @param checkpoint_lifetime [Integer, nil] Checkpoint lifetime in milliseconds
      # @param max_memtable_bytes [Integer, nil] Maximum memtable size in bytes
      # @param cache_root [String, nil] Root folder for the reader's local on-disk
      #   object-store cache. Setting this enables the cached object store; when it is
      #   not set the cache (and `max_open_file_handles`) has no effect.
      # @param max_open_file_handles [Integer, nil] Maximum number of file handles to keep
      #   open in the reader's file-handle cache. When the limit is reached, the least
      #   recently used handle is closed (default: 1000). Only takes effect when
      #   `cache_root` is set. (Requires SlateDB >= 0.13.0)
      # @param merge_operator [Symbol, String, nil] Optional merge operator ("string_concat" or "concat")
      # @yield [reader] If a block is given, yields the reader and ensures it's closed
      # @return [Reader] The opened reader (or block result if block given)
      #
      # @example Open a reader
      #   reader = SlateDb::Reader.open("/tmp/mydb")
      #   value = reader.get("key")
      #   reader.close
      #
      # @example Open with block (auto-close)
      #   SlateDb::Reader.open("/tmp/mydb") do |reader|
      #     reader.get("key")
      #   end # automatically closed
      #
      # @example Open at a specific checkpoint
      #   reader = SlateDb::Reader.open("/tmp/mydb", checkpoint_id: "uuid-here")
      #
      # @example Enable the on-disk cache and cap its open file handles
      #   reader = SlateDb::Reader.open("/tmp/mydb",
      #                                 cache_root: "/var/cache/slatedb",
      #                                 max_open_file_handles: 256)
      #
      def open(path, url: nil, checkpoint_id: nil,
               manifest_poll_interval: nil, checkpoint_lifetime: nil,
               max_memtable_bytes: nil, cache_root: nil, max_open_file_handles: nil,
               merge_operator: nil)
        opts = {}
        opts[:manifest_poll_interval] = manifest_poll_interval if manifest_poll_interval
        opts[:checkpoint_lifetime] = checkpoint_lifetime if checkpoint_lifetime
        opts[:max_memtable_bytes] = max_memtable_bytes if max_memtable_bytes
        opts[:cache_root] = cache_root if cache_root
        opts[:max_open_file_handles] = max_open_file_handles if max_open_file_handles
        opts[:merge_operator] = merge_operator.to_s if merge_operator

        reader = _open(path, url, checkpoint_id, opts)

        if block_given?
          begin
            yield reader
          ensure
            begin
              reader.close
            rescue StandardError
              nil
            end
          end
        else
          reader
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

    # Scan a range of keys.
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

    # Scan all keys with a given prefix.
    #
    # @param prefix [String] The key prefix to scan
    # @param durability_filter [String, nil] Filter by durability level
    # @param dirty [Boolean, nil] Whether to include uncommitted data
    # @param read_ahead_bytes [Integer, nil] Number of bytes to read ahead
    # @param cache_blocks [Boolean, nil] Whether to cache blocks
    # @param max_fetch_tasks [Integer, nil] Maximum number of fetch tasks
    # @param order [Symbol, String, nil] Iteration order (:asc/:ascending or :desc/:descending)
    # @param suffix [Range, nil] Restrict the scan to a range of key *suffixes*
    #   (the part of each key after +prefix+). See {Database#scan_prefix} for
    #   the semantics. (Requires SlateDB >= 0.14.0)
    # @return [Iterator] An iterator over key-value pairs
    #
    def scan_prefix(prefix, durability_filter: nil, dirty: nil,
                    read_ahead_bytes: nil, cache_blocks: nil, max_fetch_tasks: nil,
                    order: nil, suffix: nil, &)
      opts = SlateDb.scan_options(
        durability_filter: durability_filter, dirty: dirty, read_ahead_bytes: read_ahead_bytes,
        cache_blocks: cache_blocks, max_fetch_tasks: max_fetch_tasks, order: order
      )
      opts.merge!(SlateDb.suffix_range_options(suffix))

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
  end
end
