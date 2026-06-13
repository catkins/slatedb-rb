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

    # Requires a persistent (file://) object store so the Database's manifest
    # and SSTs are visible to a separate Reader handle.
    context "with a persistent object store" do
      around do |example|
        Dir.mktmpdir("slatedb-reader-test") do |dir|
          @url = "file://#{dir}"
          @path = "reader_db_#{SecureRandom.hex(8)}"
          example.run
        end
      end

      before do
        SlateDb::Database.open(@path, url: @url) do |db|
          db.put("key", "value")
          db.flush
        end
      end

      it "reads data written by a Database" do
        SlateDb::Reader.open(@path, url: @url) do |reader|
          expect(reader.get("key")).to eq("value")
        end
      end

      it "accepts max_open_file_handles (SlateDB >= 0.13.0)" do
        SlateDb::Reader.open(@path, url: @url, max_open_file_handles: 16) do |reader|
          expect(reader.get("key")).to eq("value")
        end
      end
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
