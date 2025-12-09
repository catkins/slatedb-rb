# frozen_string_literal: true

module SlateDb
  class Admin
    class << self
      # Create an admin handle for a database path/object store.
      #
      # @param path [String] Database path
      # @param url [String, nil] Optional object store URL
      # @return [Admin] The admin handle
      #
      # @example
      #   admin = SlateDb::Admin.new("/tmp/mydb")
      #   checkpoints = admin.list_checkpoints
      #
      def new(path, url: nil)
        _new(path, url)
      end
    end

    # Read the latest or a specific manifest as a JSON string.
    #
    # @param id [Integer, nil] Optional manifest id to read. If nil, reads the latest.
    # @return [String, nil] JSON string of the manifest, or nil if no manifests exist
    #
    # @example
    #   json = admin.read_manifest
    #   json = admin.read_manifest(123)
    #
    def read_manifest(id = nil)
      _read_manifest(id)
    end

    # List manifests within an optional [start, end) range as JSON.
    #
    # @param start [Integer, nil] Optional inclusive start id
    # @param end_id [Integer, nil] Optional exclusive end id
    # @return [String] JSON string containing a list of manifest metadata
    #
    # @example
    #   json = admin.list_manifests
    #   json = admin.list_manifests(start: 1, end_id: 10)
    #
    def list_manifests(start: nil, end_id: nil)
      _list_manifests(start, end_id)
    end

    # Create a detached checkpoint.
    #
    # @param lifetime [Integer, nil] Checkpoint lifetime in milliseconds
    # @param source [String, nil] Source checkpoint UUID string to extend/refresh
    # @param name [String, nil] Checkpoint name
    # @return [Hash] Hash with :id (UUID string) and :manifest_id (Integer)
    #
    # @example
    #   result = admin.create_checkpoint(name: "my_checkpoint")
    #   puts result[:id]         # => "uuid-string"
    #   puts result[:manifest_id] # => 7
    #
    def create_checkpoint(lifetime: nil, source: nil, name: nil)
      opts = {}
      opts[:lifetime] = lifetime if lifetime
      opts[:source] = source if source
      opts[:name] = name if name
      _create_checkpoint(opts)
    end

    # List known checkpoints for the database.
    #
    # @param name [String, nil] Optional checkpoint name filter
    # @return [Array<Hash>] Array of checkpoint hashes
    #
    # @example
    #   checkpoints = admin.list_checkpoints
    #   checkpoints.each do |cp|
    #     puts "#{cp[:id]}: #{cp[:name]}"
    #   end
    #
    def list_checkpoints(name: nil)
      _list_checkpoints(name)
    end

    # Refresh a checkpoint's lifetime.
    #
    # @param id [String] Checkpoint UUID string
    # @param lifetime [Integer, nil] New lifetime in milliseconds
    # @return [void]
    #
    # @example
    #   admin.refresh_checkpoint("uuid-here", lifetime: 60_000)
    #
    def refresh_checkpoint(id, lifetime: nil)
      _refresh_checkpoint(id, lifetime)
    end

    # Delete a checkpoint.
    #
    # @param id [String] Checkpoint UUID string
    # @return [void]
    #
    # @example
    #   admin.delete_checkpoint("uuid-here")
    #
    def delete_checkpoint(id)
      _delete_checkpoint(id)
    end

    # Run garbage collection once.
    #
    # @param min_age [Integer, nil] Minimum age in milliseconds for objects to be collected
    # @return [void]
    #
    # @example
    #   admin.run_gc(min_age: 3600_000) # 1 hour
    #
    def run_gc(min_age: nil)
      opts = {}
      opts[:min_age] = min_age if min_age
      _run_gc(opts)
    end
  end
end
