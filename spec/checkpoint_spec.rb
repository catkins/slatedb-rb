# frozen_string_literal: true

RSpec.describe "create_checkpoint" do
  let(:tmpdir) { Dir.mktmpdir("slatedb-test") }

  after do
    FileUtils.rm_rf(tmpdir)
  end

  describe "Database#create_checkpoint" do
    it "includes pending writes with scope: :all" do
      url = "file://#{tmpdir}"
      SlateDb::Database.open("db", url: url) do |db|
        db.put("key", "pending", await_durable: false)
        checkpoint = db.create_checkpoint(scope: :all)
        db.put("key", "later")

        SlateDb::Reader.open("db", url: url, checkpoint_id: checkpoint[:id]) do |reader|
          expect(reader.get("key")).to eq("pending")
        end
      end
    end

    it "rejects unknown scopes before starting a checkpoint" do
      SlateDb::Database.open(tmpdir) do |db|
        expect { db.create_checkpoint(scope: :unknown) }
          .to raise_error(SlateDb::InvalidArgumentError, /invalid scope/)
        expect { db.begin_checkpoint(scope: :unknown) }
          .to raise_error(SlateDb::InvalidArgumentError, /invalid scope/)
      end
    end

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

  describe "Database#begin_checkpoint" do
    it "captures the boundary before later writes and returns a consumable handle" do
      url = "file://#{tmpdir}"
      SlateDb::Database.open("db", url: url) do |db|
        db.put("key", "before", await_durable: false)
        handle = db.begin_checkpoint(scope: "all", name: "boundary", lifetime: 60_000)
        expect(handle).to be_a(SlateDb::CheckpointHandle)
        id = handle.id
        db.put("key", "after")
        db.put("later", "excluded")
        result = handle.wait
        expect(result).to include(id: id, manifest_id: an_instance_of(Integer))
        expect(handle.id).to eq(id)
        expect { handle.wait }.to raise_error(SlateDb::ClosedError)

        SlateDb::Reader.open("db", url: url, checkpoint_id: id) do |reader|
          expect(reader.get("key")).to eq("before")
          expect(reader.get("later")).to be_nil
        end
        checkpoint = SlateDb::Admin.new("db", url: url).list_checkpoints(name: "boundary").first
        expect(checkpoint).to include(id: id, name: "boundary")
        expect(checkpoint[:expire_time]).not_to be_nil
      end
    end

    it "finishes a checkpoint even when its handle is discarded" do
      url = "file://#{tmpdir}"
      SlateDb::Database.open("db", url: url) do |db|
        db.put("key", "value")
        db.begin_checkpoint(name: "discarded")
        GC.start
      end

      checkpoints = SlateDb::Admin.new("db", url: url).list_checkpoints(name: "discarded")
      expect(checkpoints.length).to eq(1)
      SlateDb::Reader.open("db", url: url, checkpoint_id: checkpoints.first[:id]) do |reader|
        expect(reader.get("key")).to eq("value")
      end
    end
  end
end
