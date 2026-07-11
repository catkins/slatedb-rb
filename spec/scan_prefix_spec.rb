# frozen_string_literal: true

require "securerandom"

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

  # Sub-range scanning within a prefix was added to SlateDB in 0.14.0. The
  # `from:`/`to:` bounds are key *suffixes* appended to the prefix: `from` is
  # inclusive, `to` is exclusive.
  describe "sub-range within a prefix (SlateDB >= 0.14.0)" do
    def seed(db)
      db.put("user:100", "a")
      db.put("user:101", "b")
      db.put("user:102", "c")
      db.put("user:103", "d")
      db.put("user:104", "e")
      db.put("other:1", "x")
    end

    describe "Database#scan_prefix" do
      it "restricts to [from, to) within the prefix" do
        SlateDb::Database.open(tmpdir) do |db|
          seed(db)
          keys = db.scan_prefix("user:", from: "101", to: "104").to_a.map { |k, _| k }
          expect(keys).to eq(["user:101", "user:102", "user:103"])
        end
      end

      it "honours an inclusive-only lower bound" do
        SlateDb::Database.open(tmpdir) do |db|
          seed(db)
          keys = db.scan_prefix("user:", from: "103").to_a.map { |k, _| k }
          expect(keys).to eq(["user:103", "user:104"])
        end
      end

      it "honours an exclusive-only upper bound" do
        SlateDb::Database.open(tmpdir) do |db|
          seed(db)
          keys = db.scan_prefix("user:", to: "102").to_a.map { |k, _| k }
          expect(keys).to eq(["user:100", "user:101"])
        end
      end

      it "never escapes the prefix even with a wide upper bound" do
        SlateDb::Database.open(tmpdir) do |db|
          seed(db)
          keys = db.scan_prefix("user:", from: "000", to: "999").to_a.map { |k, _| k }
          expect(keys).to all(start_with("user:"))
          expect(keys.length).to eq(5)
        end
      end

      it "returns nothing for an empty sub-range" do
        SlateDb::Database.open(tmpdir) do |db|
          seed(db)
          expect(db.scan_prefix("user:", from: "200").to_a).to be_empty
        end
      end

      it "combines a sub-range with descending order" do
        SlateDb::Database.open(tmpdir) do |db|
          seed(db)
          keys = db.scan_prefix("user:", from: "101", to: "104", order: :desc).to_a.map { |k, _| k }
          expect(keys).to eq(["user:103", "user:102", "user:101"])
        end
      end
    end

    describe "Transaction#scan_prefix" do
      it "restricts to [from, to) including uncommitted writes" do
        SlateDb::Database.open(tmpdir) do |db|
          seed(db)
          db.transaction do |txn|
            txn.put("user:102b", "new")
            keys = txn.scan_prefix("user:", from: "102", to: "103").to_a.map { |k, _| k }
            expect(keys).to eq(["user:102", "user:102b"])
          end
        end
      end
    end

    describe "Snapshot#scan_prefix" do
      it "restricts to [from, to) within the prefix" do
        SlateDb::Database.open(tmpdir) do |db|
          seed(db)
          db.snapshot do |snap|
            keys = snap.scan_prefix("user:", from: "101", to: "103").to_a.map { |k, _| k }
            expect(keys).to eq(["user:101", "user:102"])
          end
        end
      end
    end

    describe "Reader#scan_prefix" do
      it "restricts to [from, to) within the prefix" do
        Dir.mktmpdir("slatedb-reader-subrange") do |dir|
          url = "file://#{dir}/store"
          path = "reader_subrange_#{SecureRandom.hex(8)}"

          SlateDb::Database.open(path, url: url) do |db|
            seed(db)
            db.flush
          end

          SlateDb::Reader.open(path, url: url) do |reader|
            keys = reader.scan_prefix("user:", from: "101", to: "104").to_a.map { |k, _| k }
            expect(keys).to eq(["user:101", "user:102", "user:103"])
          end
        end
      end
    end
  end
end
