# frozen_string_literal: true

RSpec.describe SlateDb::WriteBatch do
  let(:tmpdir) { Dir.mktmpdir("slatedb-test") }

  after do
    FileUtils.rm_rf(tmpdir)
  end

  describe ".new" do
    it "creates an empty batch" do
      batch = SlateDb::WriteBatch.new
      expect(batch).to be_a(SlateDb::WriteBatch)
    end
  end

  describe "#put" do
    it "adds put operations to the batch" do
      SlateDb::Database.open(tmpdir) do |db|
        batch = SlateDb::WriteBatch.new
        batch.put("key1", "value1")
        batch.put("key2", "value2")

        db.write(batch)

        expect(db.get("key1")).to eq("value1")
        expect(db.get("key2")).to eq("value2")
      end
    end

    it "returns self for method chaining" do
      batch = SlateDb::WriteBatch.new
      result = batch.put("key", "value")
      expect(result).to be(batch)
    end

    it "supports method chaining" do
      SlateDb::Database.open(tmpdir) do |db|
        batch = SlateDb::WriteBatch.new
                                   .put("a", "1")
                                   .put("b", "2")
                                   .put("c", "3")

        db.write(batch)

        expect(db.get("a")).to eq("1")
        expect(db.get("b")).to eq("2")
        expect(db.get("c")).to eq("3")
      end
    end

    it "raises InvalidArgumentError for empty keys" do
      batch = SlateDb::WriteBatch.new
      expect { batch.put("", "value") }.to raise_error(SlateDb::InvalidArgumentError)
    end
  end

  describe "#delete" do
    it "adds delete operations to the batch" do
      SlateDb::Database.open(tmpdir) do |db|
        db.put("key1", "value1")
        db.put("key2", "value2")

        batch = SlateDb::WriteBatch.new
        batch.delete("key1")

        db.write(batch)

        expect(db.get("key1")).to be_nil
        expect(db.get("key2")).to eq("value2")
      end
    end

    it "returns self for method chaining" do
      batch = SlateDb::WriteBatch.new
      result = batch.delete("key")
      expect(result).to be(batch)
    end

    it "raises InvalidArgumentError for empty keys" do
      batch = SlateDb::WriteBatch.new
      expect { batch.delete("") }.to raise_error(SlateDb::InvalidArgumentError)
    end
  end

  describe "mixed operations" do
    it "handles puts and deletes in same batch" do
      SlateDb::Database.open(tmpdir) do |db|
        db.put("existing", "old_value")

        batch = SlateDb::WriteBatch.new
                                   .put("new_key", "new_value")
                                   .put("existing", "updated_value")
                                   .delete("to_delete")

        db.put("to_delete", "will_be_deleted")
        db.write(batch)

        expect(db.get("new_key")).to eq("new_value")
        expect(db.get("existing")).to eq("updated_value")
        expect(db.get("to_delete")).to be_nil
      end
    end
  end
end

RSpec.describe "Database#write" do
  let(:tmpdir) { Dir.mktmpdir("slatedb-test") }

  after do
    FileUtils.rm_rf(tmpdir)
  end

  describe "#write" do
    it "writes a batch atomically" do
      SlateDb::Database.open(tmpdir) do |db|
        batch = SlateDb::WriteBatch.new
        batch.put("key1", "value1")
        batch.put("key2", "value2")

        db.write(batch)

        expect(db.get("key1")).to eq("value1")
        expect(db.get("key2")).to eq("value2")
      end
    end
  end

  describe "#batch" do
    it "creates and writes a batch using block" do
      SlateDb::Database.open(tmpdir) do |db|
        db.batch do |b|
          b.put("key1", "value1")
          b.put("key2", "value2")
        end

        expect(db.get("key1")).to eq("value1")
        expect(db.get("key2")).to eq("value2")
      end
    end

    it "supports method chaining in block" do
      SlateDb::Database.open(tmpdir) do |db|
        db.put("old_key", "old_value")

        db.batch do |b|
          b.put("a", "1")
           .put("b", "2")
           .delete("old_key")
        end

        expect(db.get("a")).to eq("1")
        expect(db.get("b")).to eq("2")
        expect(db.get("old_key")).to be_nil
      end
    end
  end
end
