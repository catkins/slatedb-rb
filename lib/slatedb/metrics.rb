# frozen_string_literal: true

module SlateDb
  class Metrics
    # Get a metric value by name.
    #
    # @param name [String] Metric name
    # @return [Integer, nil] Current value or nil if not found
    def [](name)
      get(name)
    end

    # Convert all metrics to a hash.
    #
    # @return [Hash] Map of metric name to value
    def to_h
      names.to_h { |metric_name| [metric_name, get(metric_name)] }
    end
  end
end
