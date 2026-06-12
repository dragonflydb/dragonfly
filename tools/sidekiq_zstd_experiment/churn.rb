#!/usr/bin/env ruby
# frozen_string_literal: true

# Fully-real continuous spike-and-drain load for the ZSTD-dict peak-memory
# experiment, driven from one Ruby process:
#
#   * producer (thread): bursts of real HardWorker.perform_bulk -> LPUSH
#     queue:default, then pauses, repeat.
#   * consumer (thread): drives a REAL `sidekiq` worker subprocess. The worker is
#     paused with SIGSTOP so the queue builds, then every --drain-every seconds it
#     is resumed (SIGCONT) for a --drain-window burst during which it actually
#     BRPOPs + executes jobs (decompressing nodes on read), then paused again.
#   * monitor (thread): prints LLEN(queue:default) + used_memory.
#
# Runs until Ctrl-C. The thread-local ZSTD dictionary stays trained across drains,
# so each rebuild compresses immediately - realistic steady-state behavior.
#
# Usage:
#   bundle exec ruby churn.rb --produce-burst 20000 --produce-pause 5 \
#                             --payload-items 50 --drain-every 20 --drain-window 8

require_relative "worker"
require "optparse"
require "timeout"

$stdout.sync = true

WORKER_LOG = "/tmp/sidekiq_zstd_worker.log"

opts = {
  queue: "default",
  produce_burst: 20_000,
  produce_pause: 5.0,
  payload_items: 0,
  drain_every: 20.0,
  drain_window: 8.0,
  worker_boot: 5.0,
  concurrency: 10,
  monitor_every: 2.0,
}

OptionParser.new do |o|
  o.banner = "Usage: churn.rb [options]"
  o.on("--queue NAME", String, "Sidekiq queue name (list key is queue:NAME)") { |v| opts[:queue] = v }
  o.on("--produce-burst N", Integer, "jobs per producer spike") { |v| opts[:produce_burst] = v }
  o.on("--produce-pause SECS", Float, "pause between spikes (0 = continuous)") { |v| opts[:produce_pause] = v }
  o.on("--payload-items N", Integer,
       "structured records per job: 0=~1KB, 50=~12KB, 100=~22KB each") { |v| opts[:payload_items] = v }
  o.on("--drain-every SECS", Float, "build-up seconds between worker wake-ups") { |v| opts[:drain_every] = v }
  o.on("--drain-window SECS", Float, "seconds the worker drains on each wake-up") { |v| opts[:drain_window] = v }
  o.on("--worker-boot SECS", Float, "seconds to let the worker boot before first pause") { |v| opts[:worker_boot] = v }
  o.on("--concurrency N", Integer, "sidekiq worker threads (drain speed)") { |v| opts[:concurrency] = v }
  o.on("--monitor-every SECS", Float, "seconds between monitor prints") { |v| opts[:monitor_every] = v }
end.parse!

QUEUE_KEY = "queue:#{opts[:queue]}"

# --- cooperative stop, with snappy Ctrl-C via a self-pipe -------------------
@mutex = Mutex.new
@cond = ConditionVariable.new
@stop = false

def stop?
  @mutex.synchronize { @stop }
end

def signal_stop
  @mutex.synchronize do
    @stop = true
    @cond.broadcast
  end
end

# Sleep up to `secs`, but wake immediately once stop is set. Returns true if stop
# was signalled (so callers can `break if sleep_or_stop(...)`).
def sleep_or_stop(secs)
  deadline = Time.now + secs
  @mutex.synchronize do
    until @stop
      remaining = deadline - Time.now
      break if remaining <= 0

      @cond.wait(@mutex, remaining)
    end
    @stop
  end
end

def signal_worker(pid, sig)
  Process.kill(sig, pid)
rescue Errno::ESRCH
  # worker already gone
end

def spawn_worker(opts)
  log = File.open(WORKER_LOG, "w")
  cmd = ["bundle", "exec", "sidekiq",
         "-r", File.join(__dir__, "worker.rb"),
         "-q", opts[:queue],
         "-c", opts[:concurrency].to_s]
  launch = lambda do
    Process.spawn(
      { "REDIS_URL" => REDIS_URL }, *cmd,
      pgroup: true,         # own process group: shield from terminal Ctrl-C; we drive it
      out: log, err: log, chdir: __dir__
    )
  end
  # If we were started via `bundle exec`, spawn the child with a clean bundler env
  # so it resolves this Gemfile rather than inheriting a nested context.
  defined?(Bundler) ? Bundler.with_unbundled_env(&launch) : launch.call
end

def producer_loop(opts)
  i = 0
  until stop?
    produced = 0
    while produced < opts[:produce_burst] && !stop?
      chunk = [1_000, opts[:produce_burst] - produced].min
      batch = Array.new(chunk) do |j|
        idx = i + j
        ["b3e4b923-8a77-4053-aff0-#{100_000 + idx}", make_payload(idx, opts[:payload_items])]
      end
      HardWorker.perform_bulk(batch)
      i += chunk
      produced += chunk
    end
    puts format("[produce] spike of %d", produced)
    break if sleep_or_stop(opts[:produce_pause]) # pause between spikes
  end
end

def consumer_loop(worker_pid, opts)
  sleep_or_stop(opts[:worker_boot]) # let the worker connect first
  return if stop?

  signal_worker(worker_pid, "STOP")
  puts "[worker] paused; queue will build"
  until stop?
    break if sleep_or_stop(opts[:drain_every]) # build-up phase

    signal_worker(worker_pid, "CONT")
    puts "[worker] resumed - draining"
    break if sleep_or_stop(opts[:drain_window])

    signal_worker(worker_pid, "STOP")
    puts "[worker] paused"
  end
end

def monitor_loop(opts, queue_key)
  until stop?
    llen = Sidekiq.redis { |c| c.call("LLEN", queue_key) }
    info = Sidekiq.redis { |c| c.call("INFO", "memory") }
    used = (info[/used_memory:(\d+)/, 1] || 0).to_i
    puts format("[mon]  LLEN(%s)=%9d  used_memory=%6.1f MiB", queue_key, llen, used / 1024.0 / 1024.0)
    break if sleep_or_stop(opts[:monitor_every])
  end
end

# --- main -------------------------------------------------------------------
self_read, self_write = IO.pipe
%w[INT TERM].each { |sig| trap(sig) { self_write.puts(sig) } }

puts format(
  "real Sidekiq: producer spikes of %d (pause %ss); worker drains %ss every %ss. " \
  "Worker log: %s. Ctrl-C to stop.",
  opts[:produce_burst], opts[:produce_pause], opts[:drain_window], opts[:drain_every], WORKER_LOG
)

worker_pid = spawn_worker(opts)
threads = [
  Thread.new { producer_loop(opts) },
  Thread.new { consumer_loop(worker_pid, opts) },
  Thread.new { monitor_loop(opts, QUEUE_KEY) },
]

# Block until a signal arrives (or all workers finish on their own).
watcher = Thread.new do
  self_read.gets
  signal_stop
end
threads.each(&:join)
signal_stop
watcher.kill

# Resume (so it can act on TERM), then shut the worker down cleanly.
signal_worker(worker_pid, "CONT")
signal_worker(worker_pid, "TERM")
begin
  Timeout.timeout(15) { Process.wait(worker_pid) }
rescue Timeout::Error
  signal_worker(worker_pid, "KILL")
  Process.wait(worker_pid)
rescue Errno::ECHILD
  # already reaped
end
puts "stopped."
