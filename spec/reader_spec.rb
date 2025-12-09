# frozen_string_literal: true

require "spec_helper"
require "securerandom"

RSpec.describe SlateDb::Reader do
  let(:db_path) { "test_reader_#{SecureRandom.hex(8)}" }

  after do
    # Clean up any open handles
    GC.start
  end

  describe ".open" do
    # NOTE: Reader requires a persistent object store (not in-memory) to work properly
    # because it needs to read manifests written by a Database instance.
    # These tests verify the basic API structure.

    it "requires an initialized database" do
      # Reader cannot open a path with no manifest
      expect do
        SlateDb::Reader.open("nonexistent_path_#{SecureRandom.hex(8)}")
      end.to raise_error(SlateDb::DataError)
    end
  end

  describe "API structure" do
    it "has the expected class methods" do
      expect(SlateDb::Reader).to respond_to(:open)
    end

    # NOTE: Instance method tests would require a real persistent storage backend
  end

  describe "read-only behavior" do
    it "does not define write methods" do
      # Verify the Reader class doesn't have write methods defined
      expect(SlateDb::Reader.instance_methods).not_to include(:put)
      expect(SlateDb::Reader.instance_methods).not_to include(:delete)
      expect(SlateDb::Reader.instance_methods).not_to include(:write)
    end

    it "defines read methods" do
      expect(SlateDb::Reader.instance_methods).to include(:get)
      expect(SlateDb::Reader.instance_methods).to include(:scan)
      expect(SlateDb::Reader.instance_methods).to include(:close)
    end
  end
end
