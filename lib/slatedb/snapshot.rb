# frozen_string_literal: true

module SlateDb
  class Snapshot
    # Get a value by key from the snapshot.
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

    # Scan a range of keys from the snapshot.
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

    # Scan all keys with a given prefix from the snapshot.
    #
    # The scan can be narrowed to a sub-range within the prefix using
    # +range_start+ (inclusive) and +range_end+ (exclusive). These bounds are
    # key suffixes appended to the prefix; see Database#scan_prefix.
    #
    # @param prefix [String] The key prefix to scan
    # @param range_start [String, nil] Inclusive lower bound, as a suffix appended to the prefix
    # @param range_end [String, nil] Exclusive upper bound, as a suffix appended to the prefix
    # @param durability_filter [String, nil] Filter by durability level
    # @param dirty [Boolean, nil] Whether to include uncommitted data
    # @param read_ahead_bytes [Integer, nil] Number of bytes to read ahead
    # @param cache_blocks [Boolean, nil] Whether to cache blocks
    # @param max_fetch_tasks [Integer, nil] Maximum number of fetch tasks
    # @return [Iterator] An iterator over key-value pairs
    #
    def scan_prefix(prefix, range_start: nil, range_end: nil, durability_filter: nil, dirty: nil,
                    read_ahead_bytes: nil, cache_blocks: nil, max_fetch_tasks: nil, &)
      opts = {
        durability_filter: durability_filter&.to_s,
        dirty: dirty,
        read_ahead_bytes: read_ahead_bytes,
        cache_blocks: cache_blocks,
        max_fetch_tasks: max_fetch_tasks,
        range_start: range_start,
        range_end: range_end
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
  end
end
