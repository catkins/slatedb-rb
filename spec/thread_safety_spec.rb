# frozen_string_literal: true

require "spec_helper"
require "securerandom"

RSpec.describe "Thread Safety" do
  describe "concurrent access" do
    it "handles multiple threads reading and writing concurrently" do
      SlateDb::Database.open("/tmp/thread_test_#{SecureRandom.hex(4)}") do |db|
        # Track any errors from threads
        errors = []
        mutex = Mutex.new

        threads = 5.times.map do |i|
          Thread.new do
            20.times do |j|
              key = "thread_#{i}_key_#{j}"
              db.put(key, "value_#{j}")
              value = db.get(key)

              unless value == "value_#{j}"
                mutex.synchronize { errors << "Expected value_#{j}, got #{value.inspect}" }
              end
            end
          rescue => e
            mutex.synchronize { errors << "Thread #{i} error: #{e.message}" }
          end
        end

        threads.each(&:join)

        expect(errors).to be_empty, "Thread errors: #{errors.join(', ')}"

        # Verify all keys are present
        5.times do |i|
          20.times do |j|
            key = "thread_#{i}_key_#{j}"
            expect(db.get(key)).to eq("value_#{j}")
          end
        end
      end
    end

    it "handles concurrent scans" do
      SlateDb::Database.open("/tmp/thread_scan_#{SecureRandom.hex(4)}") do |db|
        # Write test data
        100.times { |i| db.put("key_#{i.to_s.rjust(3, '0')}", "value_#{i}") }

        errors = []
        mutex = Mutex.new

        threads = 5.times.map do |i|
          Thread.new do
            3.times do
              count = 0
              db.scan("key_").each do |key, value|
                count += 1
              end

              unless count == 100
                mutex.synchronize { errors << "Thread #{i} scan returned #{count} items, expected 100" }
              end
            end
          rescue => e
            mutex.synchronize { errors << "Thread #{i} error: #{e.message}" }
          end
        end

        threads.each(&:join)
        expect(errors).to be_empty, "Thread errors: #{errors.join(', ')}"
      end
    end

    it "handles concurrent transactions" do
      SlateDb::Database.open("/tmp/thread_txn_#{SecureRandom.hex(4)}") do |db|
        # Initialize counter
        db.put("counter", "0")

        successful_commits = 0
        conflicts = 0
        errors = []
        mutex = Mutex.new

        # Run concurrent increment transactions
        # Some may fail due to serialization conflicts, which is expected
        threads = 5.times.map do |i|
          Thread.new do
            3.times do |j|
              begin
                db.transaction(isolation: :serializable) do |txn|
                  val = txn.get("counter").to_i
                  txn.put("counter", (val + 1).to_s)
                end
                mutex.synchronize { successful_commits += 1 }
              rescue => e
                if e.message.include?("transaction conflict") || e.message.include?("Transaction")
                  # Expected - serialization conflict
                  mutex.synchronize { conflicts += 1 }
                else
                  mutex.synchronize { errors << "Thread #{i} error: #{e.class}: #{e.message}" }
                end
              end
            end
          end
        end

        threads.each(&:join)
        expect(errors).to be_empty, "Unexpected errors: #{errors.join(', ')}"

        # Some transactions should succeed
        expect(successful_commits).to be > 0

        # Counter should match successful commits
        final_value = db.get("counter").to_i
        expect(final_value).to eq(successful_commits)
      end
    end
  end
end
