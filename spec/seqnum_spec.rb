# frozen_string_literal: true

RSpec.describe "User-defined sequence numbers" do
  let(:tmpdir) { Dir.mktmpdir("slatedb-test") }

  after do
    FileUtils.rm_rf(tmpdir)
  end

  describe "Database#put with :seqnum" do
    it "uses the provided sequence number for the write" do
      SlateDb::Database.open(tmpdir) do |db|
        db.put("key", "value", seqnum: 100)

        entry = db.get_key_value("key")
        expect(entry[:value]).to eq("value")
        expect(entry[:seq]).to eq(100)
      end
    end

    it "raises when the sequence number is not greater than the current max" do
      SlateDb::Database.open(tmpdir) do |db|
        db.put("key", "value", seqnum: 100)

        expect { db.put("other", "value", seqnum: 50) }
          .to raise_error(SlateDb::InvalidArgumentError, /sequence number/)
      end
    end

    it "auto-assigns a sequence number when :seqnum is not given" do
      SlateDb::Database.open(tmpdir) do |db|
        db.put("key", "value")
        entry = db.get_key_value("key")
        expect(entry[:seq]).to be_a(Integer)
      end
    end
  end

  describe "Database#delete with :seqnum" do
    it "accepts an explicit sequence number" do
      SlateDb::Database.open(tmpdir) do |db|
        db.put("key", "value", seqnum: 10)
        db.delete("key", seqnum: 20)
        expect(db.get("key")).to be_nil
      end
    end
  end

  describe "Database#merge with :seqnum" do
    it "accepts an explicit sequence number" do
      SlateDb::Database.open(tmpdir, merge_operator: :string_concat) do |db|
        db.merge("key", "a", seqnum: 10)
        db.merge("key", "b", seqnum: 20)
        expect(db.get("key")).to eq("ab")
      end
    end
  end

  describe "Database#write with :seqnum" do
    it "applies the sequence number to a batch" do
      SlateDb::Database.open(tmpdir) do |db|
        batch = SlateDb::WriteBatch.new
        batch.put("a", "1")
        batch.put("b", "2")
        db.write(batch, seqnum: 500)

        expect(db.get_key_value("b")[:seq]).to eq(500)
      end
    end
  end

  describe "Database#batch with :seqnum" do
    it "applies the sequence number to the block-built batch" do
      SlateDb::Database.open(tmpdir) do |db|
        db.batch(seqnum: 600) do |b|
          b.put("a", "1")
        end

        expect(db.get_key_value("a")[:seq]).to eq(600)
      end
    end
  end

  describe "Transaction#commit with :seqnum" do
    it "applies the sequence number to the committed batch" do
      SlateDb::Database.open(tmpdir) do |db|
        txn = db.begin_transaction
        txn.put("key", "value")
        txn.commit(seqnum: 700)

        expect(db.get_key_value("key")[:seq]).to eq(700)
      end
    end
  end
end
