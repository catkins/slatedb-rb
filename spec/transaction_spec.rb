# frozen_string_literal: true

RSpec.describe SlateDb::Transaction do
  let(:tmpdir) { Dir.mktmpdir("slatedb-test") }

  after do
    FileUtils.rm_rf(tmpdir)
  end

  describe "#get and #put" do
    it "reads and writes within a transaction" do
      SlateDb::Database.open(tmpdir) do |db|
        txn = db.begin_transaction
        txn.put("key", "value")
        expect(txn.get("key")).to eq("value")
        txn.commit
      end
    end

    it "makes changes visible after commit" do
      SlateDb::Database.open(tmpdir) do |db|
        txn = db.begin_transaction
        txn.put("key", "value")
        txn.commit

        expect(db.get("key")).to eq("value")
      end
    end

    it "returns nil for missing keys" do
      SlateDb::Database.open(tmpdir) do |db|
        txn = db.begin_transaction
        expect(txn.get("nonexistent")).to be_nil
        txn.rollback
      end
    end
  end

  describe "#delete" do
    it "deletes keys within transaction" do
      SlateDb::Database.open(tmpdir) do |db|
        db.put("key", "value")

        txn = db.begin_transaction
        txn.delete("key")
        expect(txn.get("key")).to be_nil
        txn.commit

        expect(db.get("key")).to be_nil
      end
    end
  end

  describe "#scan" do
    it "scans keys within transaction" do
      SlateDb::Database.open(tmpdir) do |db|
        txn = db.begin_transaction
        txn.put("a", "1")
        txn.put("b", "2")
        txn.put("c", "3")

        entries = txn.scan("a").to_a
        expect(entries).to eq([%w[a 1], %w[b 2], %w[c 3]])

        txn.commit
      end
    end
  end

  describe "#commit" do
    it "persists changes" do
      SlateDb::Database.open(tmpdir) do |db|
        txn = db.begin_transaction
        txn.put("key", "value")
        txn.commit

        expect(db.get("key")).to eq("value")
      end
    end

    it "marks transaction as closed" do
      SlateDb::Database.open(tmpdir) do |db|
        txn = db.begin_transaction
        txn.put("key", "value")
        expect(txn.closed?).to be false
        txn.commit
        expect(txn.closed?).to be true
      end
    end

    it "raises error when committing closed transaction" do
      SlateDb::Database.open(tmpdir) do |db|
        txn = db.begin_transaction
        txn.commit

        expect { txn.commit }.to raise_error(SlateDb::ClosedError)
      end
    end
  end

  describe "#rollback" do
    it "discards changes" do
      SlateDb::Database.open(tmpdir) do |db|
        db.put("key", "original")

        txn = db.begin_transaction
        txn.put("key", "modified")
        txn.rollback

        expect(db.get("key")).to eq("original")
      end
    end

    it "marks transaction as closed" do
      SlateDb::Database.open(tmpdir) do |db|
        txn = db.begin_transaction
        expect(txn.closed?).to be false
        txn.rollback
        expect(txn.closed?).to be true
      end
    end
  end

  describe "isolation levels" do
    it "supports snapshot isolation (default)" do
      SlateDb::Database.open(tmpdir) do |db|
        txn = db.begin_transaction(isolation: :snapshot)
        txn.put("key", "value")
        txn.commit
        expect(db.get("key")).to eq("value")
      end
    end

    it "supports serializable isolation" do
      SlateDb::Database.open(tmpdir) do |db|
        txn = db.begin_transaction(isolation: :serializable)
        txn.put("key", "value")
        txn.commit
        expect(db.get("key")).to eq("value")
      end
    end

    it "raises error for invalid isolation level" do
      SlateDb::Database.open(tmpdir) do |db|
        expect { db.begin_transaction(isolation: :invalid) }
          .to raise_error(SlateDb::InvalidArgumentError)
      end
    end
  end
end

RSpec.describe "Database#transaction" do
  let(:tmpdir) { Dir.mktmpdir("slatedb-test") }

  after do
    FileUtils.rm_rf(tmpdir)
  end

  describe "block form" do
    it "auto-commits on success" do
      SlateDb::Database.open(tmpdir) do |db|
        db.transaction do |txn|
          txn.put("key", "value")
        end

        expect(db.get("key")).to eq("value")
      end
    end

    it "returns block result" do
      SlateDb::Database.open(tmpdir) do |db|
        result = db.transaction do |txn|
          txn.put("key", "value")
          "result"
        end

        expect(result).to eq("result")
      end
    end

    it "auto-rollbacks on exception" do
      SlateDb::Database.open(tmpdir) do |db|
        db.put("key", "original")

        expect do
          db.transaction do |txn|
            txn.put("key", "modified")
            raise "oops"
          end
        end.to raise_error("oops")

        expect(db.get("key")).to eq("original")
      end
    end

    it "re-raises the original exception" do
      SlateDb::Database.open(tmpdir) do |db|
        expect do
          db.transaction do |_txn|
            raise ArgumentError, "test error"
          end
        end.to raise_error(ArgumentError, "test error")
      end
    end
  end
end
