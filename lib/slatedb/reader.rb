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
      def open(path, url: nil, checkpoint_id: nil,
               manifest_poll_interval: nil, checkpoint_lifetime: nil,
               max_memtable_bytes: nil)
        opts = {}
        opts[:manifest_poll_interval] = manifest_poll_interval if manifest_poll_interval
        opts[:checkpoint_lifetime] = checkpoint_lifetime if checkpoint_lifetime
        opts[:max_memtable_bytes] = max_memtable_bytes if max_memtable_bytes

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
  end
end
