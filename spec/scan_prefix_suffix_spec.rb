# frozen_string_literal: true

require "securerandom"

# Coverage for the `suffix:` sub-range option added to `scan_prefix` in
# SlateDB 0.14.0. Bounds are key suffixes relative to the prefix; a bound `s`
# selects the full key `prefix + s`.
RSpec.describe "scan_prefix suffix range" do
  let(:tmpdir) { Dir.mktmpdir("slatedb-test") }

  after do
    FileUtils.rm_rf(tmpdir)
  end

  def keys(iter)
    iter.map(&:first)
  end

  describe "Database#scan_prefix with suffix:" do
    def seed(db)
      %w[2022 2023 2024 2025 2026].each { |y| db.put("event:#{y}", "v#{y}") }
      db.put("eventz", "leak") # shares no "event:" prefix boundary
      db.put("other:1", "x")
    end

    it "applies an exclusive-end suffix range (...)" do
      SlateDb::Database.open(tmpdir) do |db|
        seed(db)
        result = keys(db.scan_prefix("event:", suffix: "2023"..."2025"))
        expect(result).to eq(["event:2023", "event:2024"])
      end
    end

    it "applies an inclusive-end suffix range (..)" do
      SlateDb::Database.open(tmpdir) do |db|
        seed(db)
        result = keys(db.scan_prefix("event:", suffix: "2023".."2025"))
        expect(result).to eq(["event:2023", "event:2024", "event:2025"])
      end
    end

    it "supports an endless suffix range" do
      SlateDb::Database.open(tmpdir) do |db|
        seed(db)
        result = keys(db.scan_prefix("event:", suffix: "2024"..))
        expect(result).to eq(["event:2024", "event:2025", "event:2026"])
      end
    end

    it "supports a beginless suffix range" do
      SlateDb::Database.open(tmpdir) do |db|
        seed(db)
        result = keys(db.scan_prefix("event:", suffix: ..."2024"))
        expect(result).to eq(["event:2022", "event:2023"])
      end
    end

    it "supports a single-point inclusive suffix range" do
      SlateDb::Database.open(tmpdir) do |db|
        seed(db)
        result = keys(db.scan_prefix("event:", suffix: "2024".."2024"))
        expect(result).to eq(["event:2024"])
      end
    end

    it "still confines results to the prefix" do
      SlateDb::Database.open(tmpdir) do |db|
        seed(db)
        result = keys(db.scan_prefix("event:", suffix: "2000"..))
        expect(result).to all(start_with("event:"))
        expect(result).not_to include("eventz", "other:1")
      end
    end

    it "yields to a block when given" do
      SlateDb::Database.open(tmpdir) do |db|
        seed(db)
        collected = []
        db.scan_prefix("event:", suffix: "2023"..."2025") { |k, _v| collected << k }
        expect(collected).to eq(["event:2023", "event:2024"])
      end
    end

    it "composes with descending order" do
      SlateDb::Database.open(tmpdir) do |db|
        seed(db)
        result = keys(db.scan_prefix("event:", suffix: "2023".."2025", order: :desc))
        expect(result).to eq(["event:2025", "event:2024", "event:2023"])
      end
    end

    it "raises for an empty exclusive range" do
      SlateDb::Database.open(tmpdir) do |db|
        expect do
          db.scan_prefix("event:", suffix: "2024"..."2024")
        end.to raise_error(SlateDb::InvalidArgumentError, /empty/)
      end
    end

    it "raises for a reversed inclusive range" do
      SlateDb::Database.open(tmpdir) do |db|
        expect do
          db.scan_prefix("event:", suffix: "2025".."2024")
        end.to raise_error(SlateDb::InvalidArgumentError, /empty/)
      end
    end

    it "raises for an empty-string bound" do
      SlateDb::Database.open(tmpdir) do |db|
        expect do
          db.scan_prefix("event:", suffix: ""..."2024")
        end.to raise_error(SlateDb::InvalidArgumentError, /empty/)
      end
    end

    it "raises when suffix is not a Range" do
      SlateDb::Database.open(tmpdir) do |db|
        expect do
          db.scan_prefix("event:", suffix: "2024")
        end.to raise_error(ArgumentError, /Range/)
      end
    end
  end

  describe "Snapshot#scan_prefix with suffix:" do
    it "restricts the snapshot scan to the suffix range" do
      SlateDb::Database.open(tmpdir) do |db|
        %w[a b c d].each { |s| db.put("k:#{s}", s) }
        db.snapshot do |snap|
          expect(keys(snap.scan_prefix("k:", suffix: "b"..."d"))).to eq(["k:b", "k:c"])
        end
      end
    end
  end

  describe "Transaction#scan_prefix with suffix:" do
    it "restricts the transaction scan to the suffix range" do
      SlateDb::Database.open(tmpdir) do |db|
        %w[a b c d].each { |s| db.put("k:#{s}", s) }
        db.transaction do |txn|
          txn.put("k:e", "e")
          expect(keys(txn.scan_prefix("k:", suffix: "c"..))).to eq(["k:c", "k:d", "k:e"])
        end
      end
    end
  end

  describe "Reader#scan_prefix with suffix:" do
    around do |example|
      Dir.mktmpdir("slatedb-reader-suffix") do |dir|
        @url = "file://#{dir}/store"
        @path = "reader_db_#{SecureRandom.hex(8)}"
        example.run
      end
    end

    it "restricts the reader scan to the suffix range" do
      SlateDb::Database.open(@path, url: @url) do |db|
        %w[a b c d].each { |s| db.put("k:#{s}", s) }
        db.flush
      end

      SlateDb::Reader.open(@path, url: @url) do |reader|
        expect(keys(reader.scan_prefix("k:", suffix: "b".."c"))).to eq(["k:b", "k:c"])
      end
    end
  end

  describe "SlateDb.suffix_range_options" do
    it "returns an empty hash for nil" do
      expect(SlateDb.suffix_range_options(nil)).to eq({})
    end

    it "decomposes an exclusive range" do
      expect(SlateDb.suffix_range_options("a"..."z"))
        .to eq({ start: "a", end: "z", end_inclusive: false })
    end

    it "decomposes an inclusive range" do
      expect(SlateDb.suffix_range_options("a".."z"))
        .to eq({ start: "a", end: "z", end_inclusive: true })
    end

    it "decomposes an endless range" do
      expect(SlateDb.suffix_range_options("a"..)).to eq({ start: "a" })
    end

    it "decomposes a beginless range" do
      expect(SlateDb.suffix_range_options(..."z")).to eq({ end: "z", end_inclusive: false })
    end

    it "raises for a non-range" do
      expect { SlateDb.suffix_range_options("a") }.to raise_error(ArgumentError, /Range/)
    end
  end
end
