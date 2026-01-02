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
end
