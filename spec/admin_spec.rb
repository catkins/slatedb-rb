# frozen_string_literal: true

require "spec_helper"
require "securerandom"
require "json"

RSpec.describe SlateDb::Admin do
  let(:db_path) { "test_admin_#{SecureRandom.hex(8)}" }

  after do
    # Clean up any open handles
    GC.start
  end

  describe ".new" do
    it "creates an admin handle" do
      admin = SlateDb::Admin.new(db_path)
      expect(admin).to be_a(SlateDb::Admin)
    end
  end

  describe "#read_manifest" do
    it "returns nil when no manifests exist" do
      admin = SlateDb::Admin.new("nonexistent_#{SecureRandom.hex(8)}")
      result = admin.read_manifest
      expect(result).to be_nil
    end

    it "accepts an optional manifest id parameter" do
      admin = SlateDb::Admin.new(db_path)
      # Should not raise error
      expect { admin.read_manifest(nil) }.not_to raise_error
      expect { admin.read_manifest(1) }.not_to raise_error
    end
  end

  describe "#list_manifests" do
    it "returns a JSON string" do
      admin = SlateDb::Admin.new(db_path)
      json = admin.list_manifests

      expect(json).to be_a(String)
      parsed = JSON.parse(json)
      expect(parsed).to be_a(Array)
    end

    it "supports range filtering" do
      admin = SlateDb::Admin.new(db_path)

      # All these should work without error
      expect { admin.list_manifests }.not_to raise_error
      expect { admin.list_manifests(start: 0) }.not_to raise_error
      expect { admin.list_manifests(end_id: 100) }.not_to raise_error
      expect { admin.list_manifests(start: 0, end_id: 100) }.not_to raise_error
    end
  end

  describe "#list_checkpoints" do
    it "requires an initialized manifest" do
      admin = SlateDb::Admin.new(db_path)
      # list_checkpoints requires an existing manifest
      expect { admin.list_checkpoints }.to raise_error(RuntimeError, /manifest/)
    end

    it "accepts name parameter" do
      # Verify the method signature accepts the name parameter
      admin = SlateDb::Admin.new(db_path)
      # Will fail due to no manifest, but method signature should be correct
      expect { admin.list_checkpoints(name: "test") }.to raise_error(RuntimeError, /manifest/)
    end
  end

  describe "#refresh_checkpoint" do
    it "raises error for invalid UUID" do
      admin = SlateDb::Admin.new(db_path)
      expect { admin.refresh_checkpoint("invalid-uuid") }.to raise_error(SlateDb::InvalidArgumentError)
    end

    it "accepts valid UUID format" do
      admin = SlateDb::Admin.new(db_path)
      valid_uuid = "00000000-0000-0000-0000-000000000000"
      # This will fail because checkpoint doesn't exist, but UUID parsing should work
      expect { admin.refresh_checkpoint(valid_uuid) }.to raise_error(SlateDb::Error)
    end
  end

  describe "#delete_checkpoint" do
    it "raises error for invalid UUID" do
      admin = SlateDb::Admin.new(db_path)
      expect { admin.delete_checkpoint("invalid-uuid") }.to raise_error(SlateDb::InvalidArgumentError)
    end

    it "accepts valid UUID format" do
      admin = SlateDb::Admin.new(db_path)
      valid_uuid = "00000000-0000-0000-0000-000000000000"
      # This will fail because checkpoint doesn't exist, but UUID parsing should work
      expect { admin.delete_checkpoint(valid_uuid) }.to raise_error(SlateDb::Error)
    end
  end

  describe "#run_gc" do
    it "runs without error on empty database" do
      admin = SlateDb::Admin.new(db_path)
      # GC on empty/new database should work
      expect { admin.run_gc }.not_to raise_error
    end
  end

  describe "#delete_db" do
    # delete_db needs a persistent object store so the Admin handle sees the
    # objects a separate Database handle wrote.
    around do |example|
      Dir.mktmpdir("slatedb-delete-db-test") do |dir|
        @url = "file://#{dir}/store"
        @path = "delete_db_#{SecureRandom.hex(8)}"
        example.run
      end
    end

    before do
      SlateDb::Database.open(@path, url: @url) do |db|
        db.put("keep", "me")
        db.flush
      end
    end

    it "returns the objects it would delete without deleting on a dry run" do
      admin = SlateDb::Admin.new(@path, url: @url)
      would_delete = admin.delete_db

      expect(would_delete).to be_a(Array)
      expect(would_delete).not_to be_empty
      expect(would_delete).to all(be_a(String))

      # The database is untouched by a dry run.
      SlateDb::Database.open(@path, url: @url) do |db|
        expect(db.get("keep")).to eq("me")
      end
    end

    it "defaults to a dry run when confirm is not given" do
      admin = SlateDb::Admin.new(@path, url: @url)
      expect { admin.delete_db }.not_to raise_error

      SlateDb::Database.open(@path, url: @url) do |db|
        expect(db.get("keep")).to eq("me")
      end
    end

    it "deletes the database when confirm: true" do
      admin = SlateDb::Admin.new(@path, url: @url)
      deleted = admin.delete_db(confirm: true)

      expect(deleted).to be_a(Array)
      expect(deleted).not_to be_empty

      # Reopening initialises a fresh database; the old data is gone.
      SlateDb::Database.open(@path, url: @url) do |db|
        expect(db.get("keep")).to be_nil
      end
    end

    it "is idempotent for repeated confirmed deletes" do
      admin = SlateDb::Admin.new(@path, url: @url)
      admin.delete_db(confirm: true)
      expect { admin.delete_db(confirm: true) }.not_to raise_error
    end
  end

  describe "API structure" do
    it "has the expected instance methods" do
      expect(SlateDb::Admin.instance_methods).to include(:read_manifest)
      expect(SlateDb::Admin.instance_methods).to include(:list_manifests)
      expect(SlateDb::Admin.instance_methods).to include(:create_checkpoint)
      expect(SlateDb::Admin.instance_methods).to include(:list_checkpoints)
      expect(SlateDb::Admin.instance_methods).to include(:refresh_checkpoint)
      expect(SlateDb::Admin.instance_methods).to include(:delete_checkpoint)
      expect(SlateDb::Admin.instance_methods).to include(:run_gc)
      expect(SlateDb::Admin.instance_methods).to include(:delete_db)
    end
  end
end
