# frozen_string_literal: true

module SlateDb
  class Iterator
    include Enumerable

    # Iterate over all entries.
    #
    # @yield [key, value] Yields each key-value pair
    # @return [self, Enumerator] Returns self if block given, otherwise an Enumerator
    #
    # @example
    #   iter.each do |key, value|
    #     puts "#{key}: #{value}"
    #   end
    #
    # @example With Enumerable methods
    #   iter.map { |k, v| [k.upcase, v] }
    #   iter.select { |k, v| k.start_with?("user:") }
    #
    def each(&block)
      return to_enum(:each) unless block_given?

      while (entry = next_entry)
        yield entry
      end

      self
    end
  end
end
