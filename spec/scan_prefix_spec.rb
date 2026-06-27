# frozen_string_literal: true

RSpec.describe "scan_prefix" do
  let(:tmpdir) { Dir.mktmpdir("slatedb-test") }

  after do
    FileUtils.rm_rf(tmpdir)
  end

  describe "Database#scan_prefix" do
    it "scans keys with the given prefix" do
      SlateDb::Database.open(tmpdir) do |db|
        db.put("user:1", "alice")
        db.put("user:2", "bob")
        db.put("user:3", "charlie")
        db.put("order:1", "order1")
        db.put("order:2", "order2")

        results = []
        db.scan_prefix("user:") { |k, v| results << [k, v] }

        expect(results.length).to eq(3)
        expect(results.map(&:first)).to all(start_with("user:"))
      end
    end

    it "returns an iterator when no block given" do
      SlateDb::Database.open(tmpdir) do |db|
        db.put("user:1", "alice")
        db.put("user:2", "bob")

        iter = db.scan_prefix("user:")
        expect(iter).to be_a(SlateDb::Iterator)
        expect(iter.to_a.length).to eq(2)
      end
    end

    it "raises InvalidArgumentError for empty prefix" do
      SlateDb::Database.open(tmpdir) do |db|
        expect { db.scan_prefix("") }.to raise_error(SlateDb::InvalidArgumentError)
      end
    end
  end

  describe "Transaction#scan_prefix" do
    it "scans keys with the given prefix including uncommitted writes" do
      SlateDb::Database.open(tmpdir) do |db|
        db.put("user:1", "alice")
        db.put("user:2", "bob")
        db.put("order:1", "order1")

        db.transaction do |txn|
          txn.put("user:3", "charlie")

          results = []
          txn.scan_prefix("user:") { |k, v| results << [k, v] }

          expect(results.length).to eq(3)
          expect(results.map(&:first)).to include("user:3")
        end
      end
    end
  end

  describe "Snapshot#scan_prefix" do
    it "scans keys with the given prefix from snapshot" do
      SlateDb::Database.open(tmpdir) do |db|
        db.put("order:1", "order1")
        db.put("order:2", "order2")
        db.put("item:1", "item1")

        db.snapshot do |snap|
          results = []
          snap.scan_prefix("order:") { |k, v| results << [k, v] }

          expect(results.length).to eq(2)
          expect(results.map(&:first)).to all(start_with("order:"))
        end
      end
    end
  end

  describe "sub-range within a prefix" do
    def seed(db)
      %w[01 02 03 04 05].each { |n| db.put("user:#{n}", "v#{n}") }
      db.put("user", "bare")       # shorter than the prefix
      db.put("uses:01", "other")   # shares no full prefix
    end

    it "bounds the scan with an inclusive start and exclusive end suffix" do
      SlateDb::Database.open(tmpdir) do |db|
        seed(db)

        keys = db.scan_prefix("user:", range_start: "02", range_end: "04").to_a.map(&:first)
        expect(keys).to eq(%w[user:02 user:03])
      end
    end

    it "treats range_start as inclusive" do
      SlateDb::Database.open(tmpdir) do |db|
        seed(db)

        keys = db.scan_prefix("user:", range_start: "03").to_a.map(&:first)
        expect(keys).to eq(%w[user:03 user:04 user:05])
      end
    end

    it "treats range_end as exclusive and leaves the start unbounded" do
      SlateDb::Database.open(tmpdir) do |db|
        seed(db)

        keys = db.scan_prefix("user:", range_end: "03").to_a.map(&:first)
        expect(keys).to eq(%w[user:01 user:02])
      end
    end

    it "never escapes the prefix" do
      SlateDb::Database.open(tmpdir) do |db|
        seed(db)

        keys = db.scan_prefix("user:").to_a.map(&:first)
        expect(keys).to eq(%w[user:01 user:02 user:03 user:04 user:05])
        expect(keys).not_to include("user", "uses:01")
      end
    end

    it "combines with descending order" do
      SlateDb::Database.open(tmpdir) do |db|
        seed(db)

        keys = db.scan_prefix("user:", range_start: "02", range_end: "05", order: :desc)
                 .to_a.map(&:first)
        expect(keys).to eq(%w[user:04 user:03 user:02])
      end
    end

    it "works inside a transaction over uncommitted writes" do
      SlateDb::Database.open(tmpdir) do |db|
        db.transaction do |txn|
          %w[01 02 03 04].each { |n| txn.put("user:#{n}", "v#{n}") }

          keys = txn.scan_prefix("user:", range_start: "02", range_end: "04").to_a.map(&:first)
          expect(keys).to eq(%w[user:02 user:03])
        end
      end
    end

    it "works from a snapshot" do
      SlateDb::Database.open(tmpdir) do |db|
        seed(db)

        db.snapshot do |snap|
          keys = snap.scan_prefix("user:", range_start: "02", range_end: "04").to_a.map(&:first)
          expect(keys).to eq(%w[user:02 user:03])
        end
      end
    end

    it "works from a reader" do
      Dir.mktmpdir("slatedb-reader-subrange") do |dir|
        url = "file://#{dir}/store"
        path = "reader_db"

        SlateDb::Database.open(path, url: url) do |db|
          %w[01 02 03 04 05].each { |n| db.put("user:#{n}", "v#{n}") }
          db.flush
        end

        SlateDb::Reader.open(path, url: url) do |reader|
          keys = reader.scan_prefix("user:", range_start: "02", range_end: "04").to_a.map(&:first)
          expect(keys).to eq(%w[user:02 user:03])
        end
      end
    end
  end
end
