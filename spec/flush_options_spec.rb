# frozen_string_literal: true

RSpec.describe "Database#flush" do
  let(:tmpdir) { Dir.mktmpdir("slatedb-test") }

  after do
    FileUtils.rm_rf(tmpdir)
  end

  describe "with no arguments" do
    it "flushes WAL by default" do
      SlateDb::Database.open(tmpdir) do |db|
        db.put("key", "value")
        expect { db.flush }.not_to raise_error
      end
    end
  end

  describe "with flush_type: :wal" do
    it "flushes WAL explicitly" do
      SlateDb::Database.open(tmpdir) do |db|
        db.put("key", "value")
        expect { db.flush(flush_type: :wal) }.not_to raise_error
      end
    end
  end

  describe "with flush_type: :memtable" do
    it "flushes memtable" do
      SlateDb::Database.open(tmpdir) do |db|
        db.put("key", "value")
        expect { db.flush(flush_type: :memtable) }.not_to raise_error
      end
    end
  end

  describe "with flush_type as string" do
    it "accepts string flush_type" do
      SlateDb::Database.open(tmpdir) do |db|
        db.put("key", "value")
        expect { db.flush(flush_type: "wal") }.not_to raise_error
      end
    end
  end

  describe "with invalid flush_type" do
    it "raises InvalidArgumentError" do
      SlateDb::Database.open(tmpdir) do |db|
        db.put("key", "value")
        expect { db.flush(flush_type: :invalid) }.to raise_error(SlateDb::InvalidArgumentError)
      end
    end
  end
end
