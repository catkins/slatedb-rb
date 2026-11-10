# frozen_string_literal: true

RSpec.describe "point-read concurrency" do
  it "preserves recency, tombstones, merges, and snapshot isolation across persisted sources" do
    Dir.mktmpdir("slatedb-reads") do |dir|
      url = "file://#{dir}"
      SlateDb::Database.open("db", url: url, merge_operator: :concat) do |db|
        db.put("old", "oldest")
        db.put("updated", "original")
        db.put("deleted", "removed")
        db.put("merged", "base")
        db.flush(flush_type: :memtable)
        db.put("updated", "newest")
        db.delete("deleted")
        db.merge("merged", "-operand")
        db.flush(flush_type: :memtable)
        db.put("other", "newest-source")
        db.flush(flush_type: :memtable)

        db.snapshot do |snapshot|
          db.transaction do |txn|
            SlateDb::Reader.open("db", url: url, merge_operator: :concat) do |reader|
              [db, snapshot, txn, reader].each do |source|
                [0, 1, 4].each do |limit|
                  options = { max_concurrent_sources: limit, cache_blocks: false }
                  expect(source.get("old", **options)).to eq("oldest")
                  expect(source.get("updated", **options)).to eq("newest")
                  expect(source.get("deleted", **options)).to be_nil
                  expect(source.get("merged", **options)).to eq("base-operand")
                  expect(source.get("missing", **options)).to be_nil
                end
                expect { source.get("old", max_concurrent_sources: -1) }.to raise_error(RangeError)
              end
            end

            db.put("updated", "after-snapshot")
            expect(snapshot.get("updated", max_concurrent_sources: 4)).to eq("newest")
            expect(txn.get("updated", max_concurrent_sources: 4)).to eq("newest")
            expect(db.get_entry("updated", max_concurrent_sources: 4)).to include(value: "after-snapshot")
            expect { db.get_entry("updated", max_concurrent_sources: -1) }.to raise_error(RangeError)
          end
        end
      end
    end
  end
end
