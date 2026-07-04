# frozen_string_literal: true

require_relative "slatedb/version"

# Load the native extension
begin
  RUBY_VERSION =~ /(\d+\.\d+)/
  require "slatedb/#{Regexp.last_match(1)}/slatedb"
rescue LoadError
  require "slatedb/slatedb"
end

module SlateDb
  # Decompose a +Range+ of key suffixes into the option keys understood by the
  # native +scan_prefix+ bindings (+:start+, +:end+, +:end_inclusive+).
  #
  # Bounds are key suffixes relative to a scan prefix (see
  # {Database#scan_prefix}). Beginless and endless ranges map to unbounded
  # sides. Returns an empty Hash when +range+ is nil so callers can splat it
  # into an existing options Hash unconditionally.
  #
  # @param range [Range, nil] A range of String suffixes, or nil.
  # @return [Hash] Option keys for the native scan_prefix methods.
  # @raise [ArgumentError] If +range+ is not a Range (or nil).
  def self.suffix_range_options(range)
    return {} if range.nil?
    raise ArgumentError, "suffix must be a Range, got #{range.class}" unless range.is_a?(Range)

    opts = {}
    opts[:start] = range.begin unless range.begin.nil?
    unless range.end.nil?
      opts[:end] = range.end
      opts[:end_inclusive] = !range.exclude_end?
    end
    opts
  end
end

# Load Ruby class extensions
require_relative "slatedb/database"
require_relative "slatedb/iterator"
require_relative "slatedb/write_batch"
require_relative "slatedb/transaction"
require_relative "slatedb/snapshot"
require_relative "slatedb/reader"
require_relative "slatedb/admin"
require_relative "slatedb/metrics"
