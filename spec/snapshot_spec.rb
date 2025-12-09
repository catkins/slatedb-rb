# frozen_string_literal: true

RSpec.describe SlateDb::Snapshot do
  let(:tmpdir) { Dir.mktmpdir("slatedb-test") }

  after do
    FileUtils.rm_rf(tmpdir)
  end

  describe "#get" do
    it "reads values from the snapshot" do
      SlateDb::Database.open(tmpdir) do |db|
        db.put("key", "value")

        snapshot = db.snapshot
        expect(snapshot.get("key")).to eq("value")
        snapshot.close
      end
    end

    it "returns nil for missing keys" do
      SlateDb::Database.open(tmpdir) do |db|
        snapshot = db.snapshot
        expect(snapshot.get("nonexistent")).to be_nil
        snapshot.close
      end
    end

    it "provides point-in-time consistency" do
      SlateDb::Database.open(tmpdir) do |db|
        db.put("key", "original")

        snapshot = db.snapshot

        # Modify after snapshot
        db.put("key", "modified")

        # Snapshot still sees original value
        expect(snapshot.get("key")).to eq("original")
        # Database sees new value
        expect(db.get("key")).to eq("modified")

        snapshot.close
      end
    end
  end

  describe "#scan" do
    it "scans keys from the snapshot" do
      SlateDb::Database.open(tmpdir) do |db|
        db.put("a", "1")
        db.put("b", "2")
        db.put("c", "3")

        snapshot = db.snapshot
        entries = snapshot.scan("a").to_a

        expect(entries).to eq([%w[a 1], %w[b 2], %w[c 3]])
        snapshot.close
      end
    end

    it "provides point-in-time consistency for scans" do
      SlateDb::Database.open(tmpdir) do |db|
        db.put("a", "1")
        db.put("b", "2")

        snapshot = db.snapshot

        # Add more data after snapshot
        db.put("c", "3")

        # Snapshot only sees original data
        entries = snapshot.scan("a").to_a
        expect(entries).to eq([%w[a 1], %w[b 2]])

        snapshot.close
      end
    end

    it "supports block form" do
      SlateDb::Database.open(tmpdir) do |db|
        db.put("x", "1")
        db.put("y", "2")

        snapshot = db.snapshot
        results = []
        snapshot.scan("x") { |k, v| results << [k, v] }

        expect(results).to eq([%w[x 1], %w[y 2]])
        snapshot.close
      end
    end
  end

  describe "#close" do
    it "closes the snapshot" do
      SlateDb::Database.open(tmpdir) do |db|
        snapshot = db.snapshot
        expect(snapshot.closed?).to be false
        snapshot.close
        expect(snapshot.closed?).to be true
      end
    end

    it "raises error when accessing closed snapshot" do
      SlateDb::Database.open(tmpdir) do |db|
        db.put("key", "value")
        snapshot = db.snapshot
        snapshot.close

        expect { snapshot.get("key") }.to raise_error(SlateDb::ClosedError)
      end
    end
  end
end

RSpec.describe "Database#snapshot" do
  let(:tmpdir) { Dir.mktmpdir("slatedb-test") }

  after do
    FileUtils.rm_rf(tmpdir)
  end

  describe "block form" do
    it "auto-closes on block exit" do
      SlateDb::Database.open(tmpdir) do |db|
        db.put("key", "value")

        snapshot_ref = nil
        db.snapshot do |snap|
          snapshot_ref = snap
          expect(snap.get("key")).to eq("value")
        end

        expect(snapshot_ref.closed?).to be true
      end
    end

    it "returns block result" do
      SlateDb::Database.open(tmpdir) do |db|
        db.put("key", "value")

        result = db.snapshot do |snap|
          snap.get("key")
        end

        expect(result).to eq("value")
      end
    end

    it "closes snapshot even on exception" do
      SlateDb::Database.open(tmpdir) do |db|
        snapshot_ref = nil

        expect do
          db.snapshot do |snap|
            snapshot_ref = snap
            raise "oops"
          end
        end.to raise_error("oops")

        expect(snapshot_ref.closed?).to be true
      end
    end
  end
end
