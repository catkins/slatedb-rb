# frozen_string_literal: true

module SlateDb
  class WriteBatch
    # Add a put operation to the batch.
    #
    # @param key [String] The key to store
    # @param value [String] The value to store
    # @param ttl [Integer, nil] Time-to-live in milliseconds
    # @return [self] Returns self for method chaining
    #
    # @example
    #   batch.put("key", "value")
    #   batch.put("key2", "value2", ttl: 60_000)
    #
    def put(key, value, ttl: nil)
      if ttl
        _put_with_options(key, value, { ttl: ttl })
      else
        _put(key, value)
      end
      self
    end

    # Add a delete operation to the batch.
    #
    # @param key [String] The key to delete
    # @return [self] Returns self for method chaining
    #
    # @example
    #   batch.delete("key")
    #
    def delete(key)
      _delete(key)
      self
    end
  end
end
