# frozen_string_literal: true

RSpec.describe "metrics" do
  let(:tmpdir) { Dir.mktmpdir("slatedb-test") }

  after do
    FileUtils.rm_rf(tmpdir)
  end

  it "exposes metric names and values" do
    SlateDb::Database.open(tmpdir) do |db|
      db.put("key", "value")

      metrics = db.metrics
      expect(metrics).to be_a(SlateDb::Metrics)

      names = metrics.names
      expect(names).to be_an(Array)
      expect(names).not_to be_empty

      first_name = names.first
      expect(metrics.get(first_name)).to be_a(Integer)
      expect(metrics[first_name]).to be_a(Integer)
      expect(metrics["missing.metric"]).to be_nil

      metrics_hash = metrics.to_h
      expect(metrics_hash).to be_a(Hash)
      expect(metrics_hash).to include(first_name => metrics.get(first_name))
    end
  end
end
