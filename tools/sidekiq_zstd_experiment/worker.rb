# frozen_string_literal: true

# Sidekiq models a queue as a single Redis list:
#   - enqueue  = LPUSH queue:default <json-job>   (producer, Worker.perform_async)
#   - dequeue  = BRPOP queue:default ...          (worker)
#
# Every enqueued element is a Sidekiq job hash - a JSON blob whose structure is
# nearly identical across jobs (only class/args/jid/created_at/enqueued_at vary).
# That structural repetition is exactly what Dragonfly's per-thread ZSTD
# dictionary exploits once the list grows past ~2 KiB into a multi-node QList.
#
# This file is loaded BOTH by the producer (require_relative "worker") and by the
# real worker process (`sidekiq -r ./worker.rb`), so it configures redis for both
# the client and server roles and defines the job class + payload builder.

require "sidekiq"
require "securerandom"

# Dragonfly speaks the Redis protocol, so point Sidekiq straight at it.
REDIS_URL = ENV.fetch("REDIS_URL", "redis://localhost:6379/0")

Sidekiq.configure_server do |config|
  config.redis = { url: REDIS_URL }
end

Sidekiq.configure_client do |config|
  config.redis = { url: REDIS_URL }
end

WAREHOUSES = ["us-east-1", "us-west-2", "eu-central-1", "ap-south-1"].freeze
STATUSES = ["pending", "processing", "shipped", "backordered"].freeze

# Structured job payload. With n_items > 0 it carries a list of records with a
# fixed schema but realistically varying values (incl. a random per-record id),
# like a real batch job - compressible via the shared structure, not artificially
# so (the random line_id ids stop it compressing too well).
def make_payload(i, n_items)
  p = { "user_id" => i % 10_000, "action" => "process", "retries" => 0,
        "queue" => "my_queue", "attempt" => 1 }
  if n_items.positive?
    p["items"] = Array.new(n_items) do |k|
      { "sku" => format("SKU-%07d", (i * 7 + k * 13) % 1_000_000),
        "qty" => (k % 9) + 1,
        "price" => format("%.2f", ((i + k) % 5000) / 100.0),
        "warehouse" => WAREHOUSES[(i + k) % 4],
        "status" => STATUSES[(i + k) % 4],
        "line_id" => SecureRandom.uuid }
    end
  end
  p
end

# Trivial job body - only its envelope matters for this experiment. retry: false
# keeps memory clean (no retry sorted-set on the rare error).
class HardWorker
  include Sidekiq::Job

  sidekiq_options queue: "default", retry: false

  def perform(job_id, _payload)
    job_id
  end
end
