# frozen_string_literal: true

require_relative "slatedb/version"

# Load the native extension
begin
  RUBY_VERSION =~ /(\d+\.\d+)/
  require "slatedb/#{Regexp.last_match(1)}/slatedb"
rescue LoadError
  require "slatedb/slatedb"
end

# Load Ruby class extensions
require_relative "slatedb/database"
require_relative "slatedb/iterator"
require_relative "slatedb/write_batch"
require_relative "slatedb/transaction"
require_relative "slatedb/snapshot"
require_relative "slatedb/reader"
require_relative "slatedb/admin"
