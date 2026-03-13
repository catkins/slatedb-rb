# frozen_string_literal: true

RSpec.describe "WriteHandle metadata" do
  let(:tmpdir) { Dir.mktmpdir("slatedb-test") }

  after do
    FileUtils.rm_rf(tmpdir)
  end

  describe "Database#put!" do
    it "returns a hash with :seqnum and :create_ts" do
      SlateDb::Database.open(tmpdir) do |db|
        result = db.put!("key", "value")
        expect(result).to be_a(Hash)
        expect(result).to have_key(:seqnum)
        expect(result).to have_key(:create_ts)
        expect(result[:seqnum]).to be_a(Integer)
        expect(result[:create_ts]).to be_a(Integer)
      end
    end

    it "returns increasing sequence numbers" do
      SlateDb::Database.open(tmpdir) do |db|
        r1 = db.put!("key1", "value1")
        r2 = db.put!("key2", "value2")
        expect(r2[:seqnum]).to be > r1[:seqnum]
      end
    end

    it "accepts ttl option" do
      SlateDb::Database.open(tmpdir) do |db|
        result = db.put!("key", "value", ttl: 60_000)
        expect(result).to have_key(:seqnum)
      end
    end

    it "accepts await_durable option" do
      SlateDb::Database.open(tmpdir) do |db|
        result = db.put!("key", "value", await_durable: false)
        expect(result).to have_key(:seqnum)
      end
    end
  end

  describe "Database#put" do
    it "returns nil for backward compatibility" do
      SlateDb::Database.open(tmpdir) do |db|
        result = db.put("key", "value")
        expect(result).to be_nil
      end
    end
  end

  describe "Database#delete!" do
    it "returns a hash with :seqnum and :create_ts" do
      SlateDb::Database.open(tmpdir) do |db|
        db.put("key", "value")
        result = db.delete!("key")
        expect(result).to be_a(Hash)
        expect(result).to have_key(:seqnum)
        expect(result).to have_key(:create_ts)
      end
    end

    it "accepts await_durable option" do
      SlateDb::Database.open(tmpdir) do |db|
        db.put("key", "value")
        result = db.delete!("key", await_durable: false)
        expect(result).to have_key(:seqnum)
      end
    end
  end

  describe "Database#delete" do
    it "returns nil for backward compatibility" do
      SlateDb::Database.open(tmpdir) do |db|
        db.put("key", "value")
        result = db.delete("key")
        expect(result).to be_nil
      end
    end
  end

  describe "Database#write!" do
    it "returns a hash with :seqnum and :create_ts" do
      SlateDb::Database.open(tmpdir) do |db|
        batch = SlateDb::WriteBatch.new
        batch.put("key1", "value1")
        batch.put("key2", "value2")
        result = db.write!(batch)
        expect(result).to be_a(Hash)
        expect(result).to have_key(:seqnum)
        expect(result).to have_key(:create_ts)
      end
    end

    it "accepts await_durable option" do
      SlateDb::Database.open(tmpdir) do |db|
        batch = SlateDb::WriteBatch.new
        batch.put("key", "value")
        result = db.write!(batch, await_durable: false)
        expect(result).to have_key(:seqnum)
      end
    end
  end

  describe "Database#write" do
    it "returns nil for backward compatibility" do
      SlateDb::Database.open(tmpdir) do |db|
        batch = SlateDb::WriteBatch.new
        batch.put("key", "value")
        result = db.write(batch)
        expect(result).to be_nil
      end
    end
  end

  describe "Database#merge!" do
    it "returns a hash with :seqnum and :create_ts" do
      SlateDb::Database.open(tmpdir, merge_operator: :string_concat) do |db|
        result = db.merge!("key", "value")
        expect(result).to be_a(Hash)
        expect(result).to have_key(:seqnum)
        expect(result).to have_key(:create_ts)
      end
    end

    it "accepts ttl option" do
      SlateDb::Database.open(tmpdir, merge_operator: :string_concat) do |db|
        result = db.merge!("key", "value", ttl: 60_000)
        expect(result).to have_key(:seqnum)
      end
    end
  end

  describe "Database#merge" do
    it "returns nil for backward compatibility" do
      SlateDb::Database.open(tmpdir, merge_operator: :string_concat) do |db|
        result = db.merge("key", "value")
        expect(result).to be_nil
      end
    end
  end
end
