require "test_helper"

# Property test: random interleavings of enqueues, claims, rollbacks, deadline
# expiries, kill-switch flips, and below-floor arrivals must always drain to
# exactly-once delivery. Reproduce a failure with DRAIN_SEED=<seed> and raise
# the workload with DRAIN_ITERATIONS/DRAIN_OPS.
class SolidQueue::ClaimCursorsDrainTest < ActiveSupport::TestCase
  PROCESS_ID = 42
  QUEUES = %w[ drain_one drain_two ].freeze

  setup do
    skip "Claim cursors require PostgreSQL" unless SolidQueue::Record.connection_db_config.adapter.match?(/postg/i)
    SolidQueue::ReadyExecution.claim_cursors.reset!
    @original_discovery_interval = SolidQueue.claim_cursors_discovery_interval
    @original_full_discovery_interval = SolidQueue.claim_cursors_full_discovery_interval
    SolidQueue.claim_cursors_discovery_interval = 10.minutes
    SolidQueue.claim_cursors_full_discovery_interval = 10.minutes
  end

  teardown do
    SolidQueue::ReadyExecution.claim_cursors.reset!
    SolidQueue.claim_cursors = true
    SolidQueue.claim_cursors_discovery_interval = @original_discovery_interval if @original_discovery_interval
    SolidQueue.claim_cursors_full_discovery_interval = @original_full_discovery_interval if @original_full_discovery_interval
  end

  test "random interleavings always drain to exactly-once delivery" do
    iterations = Integer(ENV.fetch("DRAIN_ITERATIONS", "25"))
    ops_per_iteration = Integer(ENV.fetch("DRAIN_OPS", "40"))

    iterations.times do |iteration|
      seed = ENV["DRAIN_SEED"]&.to_i || Random.new_seed
      rng = Random.new(seed)
      context = "iteration #{iteration}, DRAIN_SEED=#{seed}"

      enqueued, planted, claimed_log = run_random_ops(rng, ops_per_iteration)

      surface_drained = drain(full: false)

      all_claimed = claimed_ids(claimed_log, surface_drained)
      missing_surface = (enqueued - planted) - all_claimed
      assert_empty missing_surface,
        "jobs above the floor not drained by floored+fast passes alone (#{context}): #{missing_surface.inspect}"

      all_claimed += claimed_ids([ drain(full: true) ])
      missing = enqueued - all_claimed
      assert_empty missing, "jobs stranded after full passes (#{context}): #{missing.inspect}"

      duplicates = all_claimed.tally.select { |_, count| count > 1 }
      assert_empty duplicates, "jobs claimed more than once (#{context}): #{duplicates.inspect}"

      assert_equal 0, SolidQueue::ReadyExecution.count, "ready executions left behind (#{context})"

      SolidQueue::ClaimedExecution.delete_all
      SolidQueue::Job.delete_all
      SolidQueue::ReadyExecution.claim_cursors.reset!
      SolidQueue.claim_cursors = true
    end
  end

  private
    def run_random_ops(rng, count)
      enqueued = []
      planted = []
      claimed_log = []
      low_id_pool = reserve_low_ids(rng)

      count.times do
        case rng.rand(100)
        when 0...35 # enqueue a small batch at random priority and queue
          rng.rand(1..4).times do
            job = AddToBufferJob.set(queue: QUEUES.sample(random: rng), priority: rng.rand(0..3)).perform_later("op")
            enqueued << job.provider_job_id
          end
        when 35...65 # claim from a random scope
          claimed_log << claim(rng.rand(1..4), queues: [ QUEUES.sample(random: rng), "*" ].sample(random: rng))
        when 65...75 # a claim that rolls back: rows must become claimable again,
          # though possibly only by the full pass, the designed healing bound
          SolidQueue::Record.transaction do
            rolled_back = claim(rng.rand(1..3), queues: "*")
            planted.concat(claimed_ids([ rolled_back ]))
            raise ActiveRecord::Rollback
          end
        when 75...85 # deadline expiries in random combinations
          key = [ :all, *QUEUES ].sample(random: rng)
          rng.rand(2).zero? ? cursors.expire_discovery!(key) : cursors.expire_full_discovery!(key)
        when 85...93 # a row arrives below the floor, like a late commit
          if (slot = low_id_pool.pop)
            SolidQueue::ReadyExecution.insert_all!([ { id: slot[:ready_id], job_id: slot[:job_id], queue_name: slot[:queue_name], priority: 0, created_at: Time.current } ])
            planted << slot[:job_id]
            enqueued << slot[:job_id]
          end
        else # kill switch flip: one classic claim, then back on
          SolidQueue.claim_cursors = false
          claimed_log << claim(rng.rand(1..3), queues: "*")
          SolidQueue.claim_cursors = true
        end
      end

      [ enqueued, planted, claimed_log ]
    end

    # Jobs whose ready rows are deleted up front, freeing ids below every
    # future floor for the late-commit op to reuse
    def reserve_low_ids(rng)
      Array.new(3) do
        job = AddToBufferJob.set(queue: QUEUES.sample(random: rng)).perform_later("reserved")
        execution = SolidQueue::ReadyExecution.find_by!(job_id: job.provider_job_id)
        slot = { ready_id: execution.id, job_id: job.provider_job_id, queue_name: execution.queue_name }
        execution.delete
        slot
      end
    end

    # full: false drains with floored+fast passes only -- everything above the
    # floor must be reachable without an unbounded scan
    def drain(full:)
      claimed = []
      50.times do
        [ :all, *QUEUES ].each { |key| full ? cursors.expire_full_discovery!(key) : cursors.expire_discovery!(key) }
        batch = claim(10, queues: "*")
        claimed << batch
        break if batch.empty? && SolidQueue::ReadyExecution.count.zero?
      end
      claimed
    end

    def claimed_ids(*logs)
      logs.flatten.map { |execution| execution.try(:job_id) }.compact
    end

    def claim(limit, queues:)
      SolidQueue::ReadyExecution.claim(queues, limit, PROCESS_ID)
    end

    def cursors
      SolidQueue::ReadyExecution.claim_cursors
    end
end
