# frozen_string_literal: true

RSpec.describe "create_checkpoint" do
  let(:tmpdir) { Dir.mktmpdir("slatedb-test") }

  after do
    FileUtils.rm_rf(tmpdir)
  end

  describe "Database#create_checkpoint" do
    it "creates a checkpoint and returns id and manifest_id" do
      SlateDb::Database.open(tmpdir) do |db|
        db.put("key", "value")
        db.flush

        checkpoint = db.create_checkpoint
        expect(checkpoint).to be_a(Hash)
        expect(checkpoint[:id]).to be_a(String)
        expect(checkpoint[:id]).to match(/\A[0-9a-f-]{36}\z/) # UUID format
        expect(checkpoint[:manifest_id]).to be_a(Integer)
      end
    end

    it "creates a checkpoint with a name" do
      SlateDb::Database.open(tmpdir) do |db|
        db.put("key", "value")
        db.flush

        checkpoint = db.create_checkpoint(name: "my-checkpoint")
        expect(checkpoint[:id]).to be_a(String)
      end
    end

    it "creates a checkpoint with lifetime" do
      SlateDb::Database.open(tmpdir) do |db|
        db.put("key", "value")
        db.flush

        checkpoint = db.create_checkpoint(lifetime: 3_600_000) # 1 hour
        expect(checkpoint[:id]).to be_a(String)
      end
    end
  end
end
