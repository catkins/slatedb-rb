# frozen_string_literal: true

RSpec.describe "Database#status" do
  let(:tmpdir) { Dir.mktmpdir("slatedb-test") }

  after do
    FileUtils.rm_rf(tmpdir)
  end

  it "returns true for a healthy database" do
    SlateDb::Database.open(tmpdir) do |db|
      expect(db.status).to eq(true)
    end
  end

  it "returns true after write operations" do
    SlateDb::Database.open(tmpdir) do |db|
      db.put("key", "value")
      db.flush
      expect(db.status).to eq(true)
    end
  end
end
