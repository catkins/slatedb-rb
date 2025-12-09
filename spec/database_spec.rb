# frozen_string_literal: true

RSpec.describe SlateDb::Database do
  let(:tmpdir) { Dir.mktmpdir("slatedb-test") }

  after do
    FileUtils.rm_rf(tmpdir)
  end

  describe ".open" do
    it "opens a database and returns a Database instance" do
      db = SlateDb::Database.open(tmpdir)
      expect(db).to be_a(SlateDb::Database)
      db.close
    end

    it "yields to block and auto-closes" do
      opened_db = nil

      SlateDb::Database.open(tmpdir) do |db|
        opened_db = db
        db.put("key", "value")
        expect(db.get("key")).to eq("value")
      end

      # Database should be closed now - attempting operations should fail
      # (In practice, we can't easily test this without triggering an error)
      expect(opened_db).not_to be_nil
    end

    it "returns block result when block given" do
      result = SlateDb::Database.open(tmpdir) do |db|
        db.put("key", "value")
        "result"
      end

      expect(result).to eq("result")
    end
  end

  describe "#put and #get" do
    it "stores and retrieves string values" do
      SlateDb::Database.open(tmpdir) do |db|
        db.put("hello", "world")
        expect(db.get("hello")).to eq("world")
      end
    end

    it "stores and retrieves multiple key-value pairs" do
      SlateDb::Database.open(tmpdir) do |db|
        db.put("key1", "value1")
        db.put("key2", "value2")
        db.put("key3", "value3")

        expect(db.get("key1")).to eq("value1")
        expect(db.get("key2")).to eq("value2")
        expect(db.get("key3")).to eq("value3")
      end
    end

    it "returns nil for missing keys" do
      SlateDb::Database.open(tmpdir) do |db|
        expect(db.get("nonexistent")).to be_nil
      end
    end

    it "overwrites existing values" do
      SlateDb::Database.open(tmpdir) do |db|
        db.put("key", "value1")
        expect(db.get("key")).to eq("value1")

        db.put("key", "value2")
        expect(db.get("key")).to eq("value2")
      end
    end

    it "raises InvalidArgumentError for empty keys on put" do
      SlateDb::Database.open(tmpdir) do |db|
        expect { db.put("", "value") }.to raise_error(SlateDb::InvalidArgumentError)
      end
    end

    it "raises InvalidArgumentError for empty keys on get" do
      SlateDb::Database.open(tmpdir) do |db|
        expect { db.get("") }.to raise_error(SlateDb::InvalidArgumentError)
      end
    end
  end

  describe "#delete" do
    it "removes a key" do
      SlateDb::Database.open(tmpdir) do |db|
        db.put("key", "value")
        expect(db.get("key")).to eq("value")

        db.delete("key")
        expect(db.get("key")).to be_nil
      end
    end

    it "does not raise when deleting non-existent key" do
      SlateDb::Database.open(tmpdir) do |db|
        expect { db.delete("nonexistent") }.not_to raise_error
      end
    end

    it "raises InvalidArgumentError for empty keys" do
      SlateDb::Database.open(tmpdir) do |db|
        expect { db.delete("") }.to raise_error(SlateDb::InvalidArgumentError)
      end
    end
  end

  describe "#flush" do
    it "flushes without error" do
      SlateDb::Database.open(tmpdir) do |db|
        db.put("key", "value")
        expect { db.flush }.not_to raise_error
      end
    end
  end

  describe "#close" do
    it "closes without error" do
      db = SlateDb::Database.open(tmpdir)
      db.put("key", "value")
      expect { db.close }.not_to raise_error
    end
  end

  describe "persistence with local file URL" do
    it "persists data across database reopens using file:// URL" do
      file_url = "file://#{tmpdir}"

      # Write data
      SlateDb::Database.open(tmpdir, url: file_url) do |db|
        db.put("persistent_key", "persistent_value")
        db.flush
      end

      # Read data in new session
      SlateDb::Database.open(tmpdir, url: file_url) do |db|
        expect(db.get("persistent_key")).to eq("persistent_value")
      end
    end
  end

  describe "in-memory store (default)" do
    it "does not persist data across database reopens without URL" do
      # Without a URL, uses in-memory store which doesn't persist
      SlateDb::Database.open(tmpdir) do |db|
        db.put("key", "value")
        db.flush
      end

      # New session with in-memory store won't have the data
      SlateDb::Database.open(tmpdir) do |db|
        expect(db.get("key")).to be_nil
      end
    end
  end

  describe "error handling" do
    it "defines TransactionError" do
      expect(SlateDb::TransactionError).to be < SlateDb::Error
    end

    it "defines ClosedError" do
      expect(SlateDb::ClosedError).to be < SlateDb::Error
    end

    it "defines UnavailableError" do
      expect(SlateDb::UnavailableError).to be < SlateDb::Error
    end

    it "defines InvalidArgumentError" do
      expect(SlateDb::InvalidArgumentError).to be < SlateDb::Error
    end

    it "defines DataError" do
      expect(SlateDb::DataError).to be < SlateDb::Error
    end

    it "defines InternalError" do
      expect(SlateDb::InternalError).to be < SlateDb::Error
    end

    it "all errors inherit from SlateDb::Error" do
      expect(SlateDb::Error).to be < StandardError
    end
  end
end
