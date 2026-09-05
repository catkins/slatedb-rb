# frozen_string_literal: true

require "spec_helper"
require "securerandom"

RSpec.describe "SlateDb::Database#close options" do
  around do |example|
    Dir.mktmpdir("slatedb-close-test") do |dir|
      @url = "file://#{dir}/store"
      @path = "close_db_#{SecureRandom.hex(8)}"
      example.run
    end
  end

  it "closes with the default flush, preserving durable writes" do
    db = SlateDb::Database.open(@path, url: @url)
    db.put("a", "1")
    expect { db.close }.not_to raise_error

    SlateDb::Database.open(@path, url: @url) do |reopened|
      expect(reopened.get("a")).to eq("1")
    end
  end

  it "closes without flushing the active memtable when flush: false" do
    db = SlateDb::Database.open(@path, url: @url)
    db.put("durable", "kept")
    expect { db.close(flush: false) }.not_to raise_error
  end

  # This is the real regression guard for the SlateDB 0.16 durability change:
  # writes no longer block on WriteOptions.await_durable, so the binding must
  # await durability on the returned WriteHandle. `await_durable: true` (the
  # default) must push the write to object storage *before* close. We then
  # close WITHOUT a final flush, so only genuinely-durable writes can survive
  # the reopen — if the handle await were dropped, "kept" would live only in
  # the memtable and be lost here.
  it "makes an await_durable: true write survive a close that skips the flush" do
    db = SlateDb::Database.open(@path, url: @url)
    db.put("durable", "kept", await_durable: true)
    db.close(flush: false)

    SlateDb::Database.open(@path, url: @url) do |reopened|
      expect(reopened.get("durable")).to eq("kept")
    end
  end

  it "raises InvalidArgumentError for an unknown flush_type even when flush: false" do
    db = SlateDb::Database.open(@path, url: @url)
    expect { db.close(flush: false, flush_type: :bogus) }
      .to raise_error(SlateDb::InvalidArgumentError, /flush_type/)
  ensure
    begin
      db.close
    rescue StandardError
      nil
    end
  end

  it "accepts an explicit memtable flush_type" do
    db = SlateDb::Database.open(@path, url: @url)
    db.put("m", "memtable")
    expect { db.close(flush_type: :memtable) }.not_to raise_error

    SlateDb::Database.open(@path, url: @url) do |reopened|
      expect(reopened.get("m")).to eq("memtable")
    end
  end

  it "accepts a wal flush_type" do
    db = SlateDb::Database.open(@path, url: @url)
    db.put("w", "wal")
    expect { db.close(flush_type: :wal) }.not_to raise_error

    SlateDb::Database.open(@path, url: @url) do |reopened|
      expect(reopened.get("w")).to eq("wal")
    end
  end

  it "raises InvalidArgumentError for an unknown flush_type" do
    db = SlateDb::Database.open(@path, url: @url)
    expect { db.close(flush_type: :bogus) }.to raise_error(SlateDb::InvalidArgumentError, /flush_type/)
  ensure
    begin
      db.close
    rescue StandardError
      nil
    end
  end

  it "still auto-closes with the default flush in block form" do
    result = SlateDb::Database.open(@path, url: @url) do |db|
      db.put("block", "closed")
      :ok
    end
    expect(result).to eq(:ok)

    SlateDb::Database.open(@path, url: @url) do |reopened|
      expect(reopened.get("block")).to eq("closed")
    end
  end
end
