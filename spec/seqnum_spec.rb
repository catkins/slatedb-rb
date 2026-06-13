# frozen_string_literal: true

# Coverage for user-supplied sequence numbers (SlateDB >= 0.13.0).
#
# When a non-zero `seqnum:` is supplied to a write, SlateDB uses it instead of
# the internally generated sequence number. The supplied value must be strictly
# greater than the current maximum sequence number or the write is rejected.
RSpec.describe "User-supplied sequence numbers" do
  let(:tmpdir) { Dir.mktmpdir("slatedb-test") }

  after do
    FileUtils.rm_rf(tmpdir)
  end

  # The current maximum sequence number after a single auto-assigned write.
  def current_max_seq(db)
    db.put("__seed__", "0")
    db.get_key_value("__seed__")[:seq]
  end

  describe "Database#put" do
    it "uses the supplied sequence number for the write" do
      SlateDb::Database.open(tmpdir) do |db|
        target = current_max_seq(db) + 100

        db.put("key", "value", seqnum: target)

        entry = db.get_key_value("key")
        expect(entry[:value]).to eq("value")
        expect(entry[:seq]).to eq(target)
      end
    end

    it "rejects a sequence number that is not greater than the current maximum" do
      SlateDb::Database.open(tmpdir) do |db|
        current_max_seq(db)

        expect { db.put("key", "value", seqnum: 1) }
          .to raise_error(SlateDb::InvalidArgumentError, /sequence number/)
      end
    end

    it "auto-assigns a sequence number when none is supplied" do
      SlateDb::Database.open(tmpdir) do |db|
        db.put("a", "1")
        db.put("b", "2")

        expect(db.get_key_value("b")[:seq]).to be > db.get_key_value("a")[:seq]
      end
    end
  end

  describe "Database#delete" do
    it "uses the supplied sequence number for the delete" do
      SlateDb::Database.open(tmpdir) do |db|
        db.put("key", "value")
        target = current_max_seq(db) + 100

        expect { db.delete("key", seqnum: target) }.not_to raise_error
        expect(db.get("key")).to be_nil
      end
    end
  end

  describe "Database#merge" do
    it "uses the supplied sequence number for the merge" do
      SlateDb::Database.open(tmpdir, merge_operator: :string_concat) do |db|
        target = current_max_seq(db) + 100

        db.merge("key", "value", seqnum: target)

        expect(db.get_key_value("key")[:seq]).to eq(target)
      end
    end
  end

  describe "Database#write" do
    it "applies the supplied sequence number to the batch" do
      SlateDb::Database.open(tmpdir) do |db|
        target = current_max_seq(db) + 100

        batch = SlateDb::WriteBatch.new
        batch.put("x", "10")
        batch.put("y", "20")
        db.write(batch, seqnum: target)

        # The final entry in the batch carries the supplied sequence number.
        expect(db.get_key_value("y")[:seq]).to eq(target)
      end
    end
  end

  describe "Database#batch" do
    it "applies the supplied sequence number to the batch block" do
      SlateDb::Database.open(tmpdir) do |db|
        target = current_max_seq(db) + 100

        db.batch(seqnum: target) do |b|
          b.put("p", "1")
          b.put("q", "2")
        end

        expect(db.get_key_value("q")[:seq]).to eq(target)
      end
    end
  end

  describe "Transaction#commit" do
    it "uses the supplied sequence number for the commit" do
      SlateDb::Database.open(tmpdir) do |db|
        target = current_max_seq(db) + 100

        txn = db.begin_transaction
        txn.put("t", "99")
        txn.commit(seqnum: target)

        expect(db.get_key_value("t")[:seq]).to eq(target)
      end
    end

    it "commits normally when no sequence number is supplied" do
      SlateDb::Database.open(tmpdir) do |db|
        txn = db.begin_transaction
        txn.put("t", "1")
        expect { txn.commit }.not_to raise_error
        expect(db.get("t")).to eq("1")
      end
    end
  end
end
