# frozen_string_literal: true

RSpec.describe SlateDb::Iterator do
  let(:tmpdir) { Dir.mktmpdir("slatedb-test") }

  after do
    FileUtils.rm_rf(tmpdir)
  end

  describe "#next_entry" do
    it "returns key-value pairs in sorted order" do
      SlateDb::Database.open(tmpdir) do |db|
        db.put("c", "3")
        db.put("a", "1")
        db.put("b", "2")

        iter = db.scan("a")
        entries = []
        while (entry = iter.next_entry)
          entries << entry
        end

        expect(entries).to eq([%w[a 1], %w[b 2], %w[c 3]])
      end
    end

    it "returns nil when iteration is complete" do
      SlateDb::Database.open(tmpdir) do |db|
        db.put("key", "value")

        iter = db.scan("key")
        expect(iter.next_entry).to eq(%w[key value])
        expect(iter.next_entry).to be_nil
      end
    end
  end

  describe "#next_entry_bytes" do
    it "returns key-value pairs as byte arrays" do
      SlateDb::Database.open(tmpdir) do |db|
        db.put("key", "value")

        iter = db.scan("key")
        entry = iter.next_entry_bytes

        expect(entry[0]).to eq("key".bytes)
        expect(entry[1]).to eq("value".bytes)
      end
    end
  end

  describe "#seek" do
    it "positions iterator at the specified key" do
      SlateDb::Database.open(tmpdir) do |db|
        db.put("a", "1")
        db.put("b", "2")
        db.put("c", "3")
        db.put("d", "4")

        iter = db.scan("a")
        iter.seek("c")

        expect(iter.next_entry).to eq(%w[c 3])
        expect(iter.next_entry).to eq(%w[d 4])
        expect(iter.next_entry).to be_nil
      end
    end

    it "raises InvalidArgumentError for empty key" do
      SlateDb::Database.open(tmpdir) do |db|
        db.put("key", "value")
        iter = db.scan("key")

        expect { iter.seek("") }.to raise_error(SlateDb::InvalidArgumentError)
      end
    end
  end

  describe "#each" do
    it "yields key-value pairs to block" do
      SlateDb::Database.open(tmpdir) do |db|
        db.put("a", "1")
        db.put("b", "2")

        iter = db.scan("a")
        entries = iter.map { |entry| entry }

        expect(entries).to eq([%w[a 1], %w[b 2]])
      end
    end

    it "returns an Enumerator when no block given" do
      SlateDb::Database.open(tmpdir) do |db|
        db.put("a", "1")
        db.put("b", "2")

        iter = db.scan("a")
        enum = iter.each

        expect(enum).to be_a(Enumerator)
        expect(enum.to_a).to eq([%w[a 1], %w[b 2]])
      end
    end
  end

  describe "Enumerable methods" do
    it "supports map" do
      SlateDb::Database.open(tmpdir) do |db|
        db.put("a", "1")
        db.put("b", "2")

        iter = db.scan("a")
        result = iter.map { |k, v| "#{k}=#{v}" }

        expect(result).to eq(["a=1", "b=2"])
      end
    end

    it "supports select" do
      SlateDb::Database.open(tmpdir) do |db|
        db.put("a", "1")
        db.put("b", "2")
        db.put("c", "3")

        iter = db.scan("a")
        result = iter.select { |k, _v| k == "a" || k == "c" }

        expect(result).to eq([%w[a 1], %w[c 3]])
      end
    end

    it "supports first" do
      SlateDb::Database.open(tmpdir) do |db|
        db.put("a", "1")
        db.put("b", "2")

        iter = db.scan("a")
        expect(iter.first).to eq(%w[a 1])
      end
    end

    it "supports to_a" do
      SlateDb::Database.open(tmpdir) do |db|
        db.put("x", "24")
        db.put("y", "25")
        db.put("z", "26")

        iter = db.scan("x")
        expect(iter.to_a).to eq([%w[x 24], %w[y 25], %w[z 26]])
      end
    end
  end

  describe "#close" do
    it "closes the iterator" do
      SlateDb::Database.open(tmpdir) do |db|
        db.put("key", "value")
        iter = db.scan("key")
        expect { iter.close }.not_to raise_error
      end
    end

    it "raises error when accessing closed iterator" do
      SlateDb::Database.open(tmpdir) do |db|
        db.put("key", "value")
        iter = db.scan("key")
        iter.close

        expect { iter.next_entry }.to raise_error(SlateDb::InternalError)
      end
    end
  end
end

RSpec.describe "Database#scan" do
  let(:tmpdir) { Dir.mktmpdir("slatedb-test") }

  after do
    FileUtils.rm_rf(tmpdir)
  end

  describe "with range" do
    it "scans from start to end key" do
      SlateDb::Database.open(tmpdir) do |db|
        db.put("a", "1")
        db.put("b", "2")
        db.put("c", "3")
        db.put("d", "4")

        iter = db.scan("b", "d")
        entries = iter.to_a

        expect(entries).to eq([%w[b 2], %w[c 3]])
      end
    end

    it "scans from start to end (open ended)" do
      SlateDb::Database.open(tmpdir) do |db|
        db.put("a", "1")
        db.put("b", "2")
        db.put("c", "3")

        iter = db.scan("b")
        entries = iter.to_a

        expect(entries).to eq([%w[b 2], %w[c 3]])
      end
    end
  end

  describe "with block" do
    it "yields entries to block" do
      SlateDb::Database.open(tmpdir) do |db|
        db.put("a", "1")
        db.put("b", "2")

        entries = []
        db.scan("a") { |k, v| entries << [k, v] }

        expect(entries).to eq([%w[a 1], %w[b 2]])
      end
    end
  end

  describe "raises errors" do
    it "raises InvalidArgumentError for empty start key" do
      SlateDb::Database.open(tmpdir) do |db|
        expect { db.scan("") }.to raise_error(SlateDb::InvalidArgumentError)
      end
    end
  end
end
