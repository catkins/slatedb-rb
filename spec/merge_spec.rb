# frozen_string_literal: true

RSpec.describe "Merge Operations" do
  let(:tmpdir) { Dir.mktmpdir("slatedb-test") }

  after do
    FileUtils.rm_rf(tmpdir)
  end

  describe "Database#merge" do
    context "with string_concat merge operator" do
      it "concatenates values for the same key" do
        SlateDb::Database.open(tmpdir, merge_operator: :string_concat) do |db|
          db.merge("key", "hello")
          db.merge("key", " world")

          expect(db.get("key")).to eq("hello world")
        end
      end

      it "creates the key if it does not exist" do
        SlateDb::Database.open(tmpdir, merge_operator: :string_concat) do |db|
          db.merge("new_key", "value")

          expect(db.get("new_key")).to eq("value")
        end
      end

      it "appends to existing put value" do
        SlateDb::Database.open(tmpdir, merge_operator: :string_concat) do |db|
          db.put("key", "initial")
          db.merge("key", "-appended")

          expect(db.get("key")).to eq("initial-appended")
        end
      end

      it "supports multiple merges in sequence" do
        SlateDb::Database.open(tmpdir, merge_operator: :string_concat) do |db|
          db.merge("key", "a")
          db.merge("key", "b")
          db.merge("key", "c")
          db.merge("key", "d")

          expect(db.get("key")).to eq("abcd")
        end
      end

      it "supports concat as alias for string_concat" do
        SlateDb::Database.open(tmpdir, merge_operator: :concat) do |db|
          db.merge("key", "foo")
          db.merge("key", "bar")

          expect(db.get("key")).to eq("foobar")
        end
      end
    end

    context "with options" do
      it "accepts await_durable option" do
        SlateDb::Database.open(tmpdir, merge_operator: :string_concat) do |db|
          db.merge("key", "value", await_durable: false)

          expect(db.get("key")).to eq("value")
        end
      end

      it "accepts ttl option" do
        SlateDb::Database.open(tmpdir, merge_operator: :string_concat) do |db|
          db.merge("key", "value", ttl: 60_000)

          expect(db.get("key")).to eq("value")
        end
      end
    end

    context "error handling" do
      it "raises InvalidArgumentError for empty keys" do
        SlateDb::Database.open(tmpdir, merge_operator: :string_concat) do |db|
          expect { db.merge("", "value") }.to raise_error(SlateDb::InvalidArgumentError)
        end
      end

      it "raises InvalidArgumentError for invalid merge_operator" do
        expect do
          SlateDb::Database.open(tmpdir, merge_operator: :invalid_operator)
        end.to raise_error(SlateDb::InvalidArgumentError, /invalid merge_operator/)
      end
    end
  end

  describe "Transaction#merge" do
    context "with string_concat merge operator" do
      it "concatenates values within a transaction" do
        SlateDb::Database.open(tmpdir, merge_operator: :string_concat) do |db|
          db.transaction do |txn|
            txn.merge("key", "hello")
            txn.merge("key", " world")

            expect(txn.get("key")).to eq("hello world")
          end

          expect(db.get("key")).to eq("hello world")
        end
      end

      it "merges are visible within the transaction" do
        SlateDb::Database.open(tmpdir, merge_operator: :string_concat) do |db|
          db.put("key", "before")

          db.transaction do |txn|
            txn.merge("key", "-during")
            expect(txn.get("key")).to eq("before-during")
          end
        end
      end

      it "merges are discarded on rollback" do
        SlateDb::Database.open(tmpdir, merge_operator: :string_concat) do |db|
          db.put("key", "original")

          txn = db.begin_transaction
          txn.merge("key", "-modified")
          txn.rollback

          expect(db.get("key")).to eq("original")
        end
      end
    end

    context "with options" do
      it "accepts ttl option" do
        SlateDb::Database.open(tmpdir, merge_operator: :string_concat) do |db|
          db.transaction do |txn|
            txn.merge("key", "value", ttl: 60_000)
          end

          expect(db.get("key")).to eq("value")
        end
      end
    end

    context "error handling" do
      it "raises InvalidArgumentError for empty keys" do
        SlateDb::Database.open(tmpdir, merge_operator: :string_concat) do |db|
          txn = db.begin_transaction
          expect { txn.merge("", "value") }.to raise_error(SlateDb::InvalidArgumentError)
          txn.rollback
        end
      end

      it "raises ClosedError when merging on closed transaction" do
        SlateDb::Database.open(tmpdir, merge_operator: :string_concat) do |db|
          txn = db.begin_transaction
          txn.commit

          expect { txn.merge("key", "value") }.to raise_error(SlateDb::ClosedError)
        end
      end
    end
  end

  describe "WriteBatch#merge" do
    context "with string_concat merge operator" do
      it "batches merge operations" do
        SlateDb::Database.open(tmpdir, merge_operator: :string_concat) do |db|
          batch = SlateDb::WriteBatch.new
          batch.merge("key", "hello")
          batch.merge("key", " world")

          db.write(batch)

          expect(db.get("key")).to eq("hello world")
        end
      end

      it "returns self for method chaining" do
        batch = SlateDb::WriteBatch.new
        result = batch.merge("key", "value")

        expect(result).to be(batch)
      end

      it "supports method chaining" do
        SlateDb::Database.open(tmpdir, merge_operator: :string_concat) do |db|
          batch = SlateDb::WriteBatch.new
                                     .merge("key", "a")
                                     .merge("key", "b")
                                     .merge("key", "c")

          db.write(batch)

          expect(db.get("key")).to eq("abc")
        end
      end

      it "can mix put, merge, and delete in same batch" do
        SlateDb::Database.open(tmpdir, merge_operator: :string_concat) do |db|
          db.put("to_delete", "will_be_deleted")
          db.put("to_merge", "start")

          batch = SlateDb::WriteBatch.new
                                     .put("new_key", "new_value")
                                     .merge("to_merge", "-end")
                                     .delete("to_delete")

          db.write(batch)

          expect(db.get("new_key")).to eq("new_value")
          expect(db.get("to_merge")).to eq("start-end")
          expect(db.get("to_delete")).to be_nil
        end
      end
    end

    context "with options" do
      it "accepts ttl option" do
        SlateDb::Database.open(tmpdir, merge_operator: :string_concat) do |db|
          batch = SlateDb::WriteBatch.new
          batch.merge("key", "value", ttl: 60_000)

          db.write(batch)

          expect(db.get("key")).to eq("value")
        end
      end
    end

    context "with Database#batch block" do
      it "supports merge in batch block" do
        SlateDb::Database.open(tmpdir, merge_operator: :string_concat) do |db|
          db.batch do |b|
            b.merge("key", "hello")
            b.merge("key", " world")
          end

          expect(db.get("key")).to eq("hello world")
        end
      end
    end

    context "error handling" do
      it "raises InvalidArgumentError for empty keys" do
        batch = SlateDb::WriteBatch.new

        expect { batch.merge("", "value") }.to raise_error(SlateDb::InvalidArgumentError)
      end
    end
  end

  describe "without merge operator" do
    it "raises error when reading merged values without merge_operator configured" do
      SlateDb::Database.open(tmpdir) do |db|
        # Merge succeeds without an operator
        db.merge("key", "value")

        # But reading the merged value fails
        expect { db.get("key") }.to raise_error(SlateDb::InvalidArgumentError, /merge operator missing/)
      end
    end
  end
end
