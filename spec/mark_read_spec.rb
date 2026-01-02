# frozen_string_literal: true

RSpec.describe "Transaction#mark_read" do
  let(:tmpdir) { Dir.mktmpdir("slatedb-test") }

  after do
    FileUtils.rm_rf(tmpdir)
  end

  it "marks keys as read without raising an error" do
    SlateDb::Database.open(tmpdir) do |db|
      db.put("key1", "value1")
      db.put("key2", "value2")

      db.transaction(isolation: :serializable) do |txn|
        txn.mark_read(%w[key1 key2])
        txn.put("key3", "value3")
      end

      expect(db.get("key3")).to eq("value3")
    end
  end

  it "accepts a single key wrapped in array" do
    SlateDb::Database.open(tmpdir) do |db|
      db.put("key1", "value1")

      db.transaction(isolation: :serializable) do |txn|
        txn.mark_read(["key1"])
        txn.put("key2", "value2")
      end

      expect(db.get("key2")).to eq("value2")
    end
  end

  it "coerces a single key to array" do
    SlateDb::Database.open(tmpdir) do |db|
      db.put("key1", "value1")

      # The Ruby wrapper uses Array() so a single string should work too
      db.transaction(isolation: :serializable) do |txn|
        txn.mark_read("key1")
        txn.put("key2", "value2")
      end

      expect(db.get("key2")).to eq("value2")
    end
  end
end
