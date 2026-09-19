# frozen_string_literal: true

require "spec_helper"
require "securerandom"

# Flush and close options were introduced in SlateDB 0.16.0. `flush_type`
# selects what is flushed (WAL vs memtable), and `close(flush: false)` closes
# the database without a final flush.
RSpec.describe "Flush and close options" do
  describe "Database#flush" do
    let(:tmpdir) { Dir.mktmpdir("slatedb-flush-test") }

    after do
      FileUtils.rm_rf(tmpdir)
    end

    it "flushes with no arguments (default behavior)" do
      SlateDb::Database.open(tmpdir) do |db|
        db.put("key", "value")
        expect { db.flush }.not_to raise_error
      end
    end

    it "flushes the WAL when flush_type: :wal" do
      SlateDb::Database.open(tmpdir) do |db|
        db.put("key", "value", await_durable: false)
        expect { db.flush(flush_type: :wal) }.not_to raise_error
        expect(db.get("key")).to eq("value")
      end
    end

    it "flushes the memtable when flush_type: :memtable" do
      SlateDb::Database.open(tmpdir) do |db|
        db.put("key", "value", await_durable: false)
        expect { db.flush(flush_type: :memtable) }.not_to raise_error
        expect(db.get("key")).to eq("value")
      end
    end

    it "accepts a string flush_type" do
      SlateDb::Database.open(tmpdir) do |db|
        db.put("key", "value")
        expect { db.flush(flush_type: "wal") }.not_to raise_error
      end
    end

    it "raises InvalidArgumentError for an unknown flush_type" do
      SlateDb::Database.open(tmpdir) do |db|
        expect { db.flush(flush_type: :bogus) }
          .to raise_error(SlateDb::InvalidArgumentError, /flush_type/)
      end
    end
  end

  describe "Database#close" do
    # Use a persistent (file://) object store so data survives across handles.
    around do |example|
      Dir.mktmpdir("slatedb-close-test") do |dir|
        @url = "file://#{dir}/store"
        @path = "close_db_#{SecureRandom.hex(8)}"
        example.run
      end
    end

    after { GC.start }

    it "flushes the memtable before closing by default" do
      db = SlateDb::Database.open(@path, url: @url)
      db.put("key", "value", await_durable: false)
      db.close

      SlateDb::Database.open(@path, url: @url) do |reopened|
        expect(reopened.get("key")).to eq("value")
      end
    end

    it "closes without losing writes already made durable when flush: false" do
      db = SlateDb::Database.open(@path, url: @url)
      db.put("durable", "value") # awaits durability by default
      db.flush
      expect { db.close(flush: false) }.not_to raise_error

      SlateDb::Database.open(@path, url: @url) do |reopened|
        expect(reopened.get("durable")).to eq("value")
      end
    end

    it "accepts an explicit flush_type when closing" do
      db = SlateDb::Database.open(@path, url: @url)
      db.put("key", "value", await_durable: false)
      expect { db.close(flush_type: :memtable) }.not_to raise_error

      SlateDb::Database.open(@path, url: @url) do |reopened|
        expect(reopened.get("key")).to eq("value")
      end
    end

    it "raises InvalidArgumentError for an unknown flush_type" do
      db = SlateDb::Database.open(@path, url: @url)
      expect { db.close(flush_type: :bogus) }
        .to raise_error(SlateDb::InvalidArgumentError, /flush_type/)
    ensure
      begin
        db.close(flush: false)
      rescue StandardError
        nil
      end
    end
  end
end
