require "test_helper"

class SolidQueue::ClaimCursorsTest < ActiveSupport::TestCase
  PROCESS_ID = 42

  setup do
    skip "Claim cursors require PostgreSQL" unless SolidQueue::Record.connection_db_config.adapter.match?(/postg/i)
    SolidQueue::ReadyExecution.claim_cursors.reset!
    # The dummy app shortens the interval for integration latency; these tests
    # control discovery explicitly via expire_discovery!
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

  test "advance is monotonic per key" do
    cursors.advance(:all, [ 0, 10 ])
    cursors.advance(:all, [ 0, 5 ])
    assert_equal [ 0, 10 ], cursors.position(:all)

    cursors.advance(:all, [ 1, 1 ])
    assert_equal [ 1, 1 ], cursors.position(:all)
  end

  test "clear only removes the position it observed" do
    cursors.advance(:all, [ 0, 10 ])

    cursors.clear(:all, [ 0, 5 ]) # stale observation from a slower thread
    assert_equal [ 0, 10 ], cursors.position(:all)

    cursors.clear(:all, [ 0, 10 ])
    assert_nil cursors.position(:all)
  end

  test "reset clears the observed floor source" do
    cursors.observe(42)
    assert_equal 42, cursors.observed_max_id

    cursors.reset!
    assert_nil cursors.observed_max_id
  end

  test "observations are shared across queue keys" do
    AddToBufferJob.set(queue: :first).perform_later("a")
    claim(1, queues: "first") # observes an id

    AddToBufferJob.set(queue: :second).perform_later("b")
    cursors.expire_full_discovery!("second")
    claim(1, queues: "second")

    # Ids share one table-wide namespace, so the second queue's full pass
    # records a floor from the first queue's observation stream
    assert_not_nil cursors.floor("second")
  end

  test "a row inserted with an anomalous low id heals on the next full pass" do
    seed_cursor_and_floor

    job = SolidQueue::Job.create!(queue_name: "default", class_name: "AddToBufferJob", arguments: "{}", priority: 0)
    SolidQueue::ReadyExecution.where(job_id: job.id).delete_all
    low_id = cursors.floor(:all) - 1
    SolidQueue::ReadyExecution.insert_all!([ { id: low_id, job_id: job.id, queue_name: "default", priority: 0, created_at: Time.current } ])

    cursors.expire_discovery!(:all)
    assert_empty claim(1) # hidden below the floor, like a setval rewind would be

    cursors.expire_full_discovery!(:all)
    assert_equal 1, claim(1).size
  end

  test "cursors are tracked independently per queue" do
    AddToBufferJob.set(queue: :first).perform_later("a")
    AddToBufferJob.set(queue: :second).perform_later("b")

    claim(1, queues: "first")
    first_position = cursors.position("first")
    assert_not_nil first_position
    assert_nil cursors.position("second")
    assert_equal 1, SolidQueue::ReadyExecution.count

    claim(1, queues: "second")
    assert_not_nil cursors.position("second")
    assert_equal first_position, cursors.position("first")
  end

  test "an empty discovery suppresses claim queries until the next one is due" do
    assert_empty claim(1) # discovery against an empty queue records a nil position

    queries = capture_candidate_queries do
      3.times { assert_empty claim(1) }
    end
    assert_empty queries
  end

  test "discovery seeds a single lexicographic position" do
    2.times { |i| AddToBufferJob.perform_later(i) }
    executions = SolidQueue::ReadyExecution.ordered.to_a

    assert_equal 1, claim(1).size
    assert_equal position_of(executions.first), cursors.position(:all)

    assert_equal 1, claim(1).size
    assert_equal position_of(executions.second), cursors.position(:all)
  end

  test "fast path claims across priorities with one cursor query" do
    AddToBufferJob.set(priority: 1).perform_later("seed")
    claim(1)

    AddToBufferJob.set(priority: 1).perform_later("high")
    AddToBufferJob.set(priority: 5).perform_later("low")

    queries = capture_candidate_queries do
      claimed_jobs = claim(2).sort_by(&:id).map { |execution| SolidQueue::Job.find(execution.job_id) }
      assert_equal [ 1, 5 ], claimed_jobs.map(&:priority)
    end

    assert_equal 1, queries.size
    assert_includes queries.sole, "(priority, id) > ("
  end

  test "fast path immediately finds new work at the current priority" do
    AddToBufferJob.set(priority: 3).perform_later("seed")
    claim(1)

    AddToBufferJob.set(priority: 3).perform_later("next")

    assert_equal 1, claim(1).size
    assert_equal 0, SolidQueue::ReadyExecution.count
  end

  test "empty seek clears the position and skips cursor queries until discovery is due" do
    AddToBufferJob.perform_later("seed")
    claim(1)

    queries = capture_candidate_queries { assert_empty claim(1) }
    assert_equal 1, queries.size
    assert_nil cursors.position(:all)
    assert_not cursors.discovery_due?(:all)

    queries = capture_candidate_queries do
      3.times { assert_empty claim(1) }
    end
    assert_empty queries
  end

  test "higher priority arrivals are picked up once discovery is due" do
    AddToBufferJob.set(priority: 5).perform_later("seed")
    claim(1)

    AddToBufferJob.set(priority: 1).perform_later("higher")

    assert_empty claim(1)
    assert_nil cursors.position(:all)

    cursors.expire_discovery!(:all)
    claimed = claim(1)

    assert_equal 1, claimed.size
    assert_equal 1, SolidQueue::Job.find(claimed.sole.job_id).priority
  end

  test "discovery heals an overshot position" do
    AddToBufferJob.perform_later("seed")
    claim(1)

    AddToBufferJob.perform_later("stranded")
    execution = SolidQueue::ReadyExecution.sole
    cursors.advance(:all, [ execution.priority, execution.id + 1000 ])

    assert_empty claim(1)
    assert_nil cursors.position(:all)

    cursors.expire_discovery!(:all)
    claimed = claim(1)

    assert_equal 1, claimed.size
    assert_equal "stranded", SolidQueue::Job.find(claimed.sole.job_id).arguments.dig("arguments").first
  end

  test "within a priority, claims follow readiness order, not enqueue order" do
    scheduled = AddToBufferJob.set(wait: 5.minutes).perform_later("scheduled first")
    immediate = AddToBufferJob.perform_later("enqueued second")

    travel_to(6.minutes.from_now) { SolidQueue::ScheduledExecution.dispatch_next_batch(10) }

    claimed_jobs = claim(2).sort_by(&:id).map { |execution| SolidQueue::Job.find(execution.job_id) }
    assert_equal [ immediate.job_id, scheduled.job_id ], claimed_jobs.map(&:active_job_id)
  end

  test "discovery is unbounded until a pass has observed a floor" do
    AddToBufferJob.perform_later("seed")

    queries = capture_candidate_queries { assert_equal 1, claim(1).size }

    assert_equal 1, queries.size
    assert_not_includes queries.sole, "id > "
    assert_nil cursors.floor(:all) # nothing observed before the first pass

    AddToBufferJob.perform_later("next")
    cursors.expire_full_discovery!(:all)
    claim(1)

    assert_not_nil cursors.floor(:all)
  end

  test "a floored pass sweeps below the cursor and seeks above it in one poll" do
    seed_cursor_and_floor

    AddToBufferJob.perform_later("next")
    cursors.expire_discovery!(:all)

    selects = nil
    queries = capture_candidate_queries do
      selects = capture_ready_selects { assert_equal 1, claim(1).size }
    end

    # The arrival sits above the cursor, so the empty in-memory pick issues no
    # below-cursor lock at all; the window read and the above-cursor seek remain
    assert_equal 1, queries.size
    assert_includes queries.sole, "(priority, id) > ("
    assert selects.any? { |sql| sql.include?("id > ") && sql.exclude?("FOR UPDATE") }
  end

  test "a floored pass finds a higher priority arrival without scanning below the watermark" do
    seed_cursor_and_floor(priority: 5)
    position = cursors.position(:all)

    AddToBufferJob.set(priority: 1).perform_later("higher")
    cursors.expire_discovery!(:all)

    selects = nil
    queries = capture_candidate_queries do
      selects = capture_ready_selects do
        claimed = claim(1)
        assert_equal 1, SolidQueue::Job.find(claimed.sole.job_id).priority
      end
    end

    assert_equal 1, queries.size
    assert_match(/"id" (=|IN)/, queries.sole)

    # The window is ordered by id alone and the lock query carries no ORDER BY,
    # so no plan has an ordering reason to walk an index past the graveyard
    window = selects.find { |sql| sql.include?("id > ") && sql.exclude?("FOR UPDATE") }
    assert_includes window, %(ORDER BY "solid_queue_ready_executions"."id" ASC)
    assert_no_match(/ORDER BY.*priority/im, window)
    assert_no_match(/ORDER BY/i, queries.sole)

    # A pass that only sees part of the index must not move the cursor: the rows
    # its floor hid would fall below it and out of every other query
    assert_equal position, cursors.position(:all)
  end

  test "successive floored passes reach every arrival below the cursor" do
    AddToBufferJob.set(priority: 9).perform_later("seed")
    claim(1)

    # The higher priority arrival takes the higher id, so a watermark that
    # followed the claims would jump over the one left behind
    lower = AddToBufferJob.set(priority: 5).perform_later("lower priority, lower id")
    higher = AddToBufferJob.set(priority: 1).perform_later("higher priority, higher id")

    cursors.expire_discovery!(:all)
    assert_equal higher.job_id, claimed_active_job_ids(1).sole

    cursors.expire_discovery!(:all)
    assert_equal lower.job_id, claimed_active_job_ids(1).sole
  end

  test "a limit-filling sweep keeps discovery due until the region below the cursor drains" do
    AddToBufferJob.set(priority: 9).perform_later("seed")
    claim(1)

    AddToBufferJob.set(priority: 1).perform_later("highest")
    AddToBufferJob.set(priority: 5).perform_later("middle")
    AddToBufferJob.set(priority: 10).perform_later("lowest")

    cursors.expire_discovery!(:all)
    first = claim(1)
    assert_equal 1, SolidQueue::Job.find(first.sole.job_id).priority

    # The sweep filled its limit, so without any deadline expiring the next
    # poll must continue below the cursor, not claim the lower-priority row
    # sitting above it
    second = claim(1)
    assert_equal 5, SolidQueue::Job.find(second.sole.job_id).priority

    third = claim(1)
    assert_equal 10, SolidQueue::Job.find(third.sole.job_id).priority
  end

  test "a region larger than the window keeps priority order only within it until it slides" do
    seed_cursor_and_floor(priority: 9)

    now = Time.current
    filler_ids = SolidQueue::Job.insert_all!(
      Array.new(SolidQueue::ReadyExecution::FLOORED_WINDOW) { { queue_name: "default", class_name: "AddToBufferJob", arguments: "{}", priority: 10, created_at: now, updated_at: now } },
      returning: [ :id ]
    ).rows.flatten
    high_id = SolidQueue::Job.insert_all!(
      [ { queue_name: "default", class_name: "AddToBufferJob", arguments: "{}", priority: 1, created_at: now, updated_at: now } ],
      returning: [ :id ]
    ).rows.flatten.sole
    SolidQueue::ReadyExecution.insert_all!(
      filler_ids.map { |id| { job_id: id, queue_name: "default", priority: 10, created_at: now } } +
        [ { job_id: high_id, queue_name: "default", priority: 1, created_at: now } ]
    )

    cursors.expire_discovery!(:all)

    # The high-priority row's id lies beyond the window, so a lower-priority
    # claim precedes it: the documented slack of a region larger than the window
    slack = claim(1)
    assert_equal 10, SolidQueue::Job.find(slack.sole.job_id).priority

    # Consuming a row slides the window over it, and priority order resumes
    healed = claim(1)
    assert_equal 1, SolidQueue::Job.find(healed.sole.job_id).priority
  end

  test "a floored pass with no cursor spans every priority and seeds one" do
    seed_cursor_and_floor(priority: 5)
    assert_empty claim(1)
    assert_nil cursors.position(:all)

    AddToBufferJob.set(priority: 1).perform_later("higher")
    cursors.expire_discovery!(:all)

    queries = capture_candidate_queries { assert_equal 1, claim(1).size }

    assert_equal 1, queries.size
    assert_match(/"id" (=|IN)/, queries.sole)
    assert_not_includes queries.sole, "(priority, id) <= ("
    assert_not_nil cursors.position(:all)
  end

  test "a row below both the cursor and the watermark waits for the unbounded pass" do
    AddToBufferJob.set(priority: 9).perform_later("seed")
    claim(1)
    seed_position = cursors.position(:all)

    AddToBufferJob.set(priority: 1).perform_later("stranded")
    stranded = SolidQueue::ReadyExecution.sole

    # What a rolled back claim or a commit landing after the watermark was read
    # leaves behind: a row below the cursor that the floor also hides
    cursors.record_full_discovery(:all, seed_position, stranded.id)

    cursors.expire_discovery!(:all)
    assert_empty claim(1)

    cursors.expire_full_discovery!(:all)
    assert_equal 1, claim(1).size
  end

  test "the unbounded pass runs on its own cadence" do
    AddToBufferJob.perform_later("seed")
    claim(1)

    assert_not cursors.discovery_due?(:all)
    assert_not cursors.full_discovery_due?(:all)

    cursors.expire_discovery!(:all)
    assert cursors.discovery_due?(:all)
    assert_not cursors.full_discovery_due?(:all)

    AddToBufferJob.perform_later("next")
    cursors.expire_full_discovery!(:all)
    queries = capture_candidate_queries { assert_equal 1, claim(1).size }

    assert_equal 1, queries.size
    assert_not_includes queries.sole, "id > "
    assert_not_includes queries.sole, "(priority, id)"
  end

  test "a zero full discovery interval keeps every discovery pass unbounded" do
    SolidQueue.claim_cursors_full_discovery_interval = 0
    AddToBufferJob.perform_later("seed")
    claim(1)

    AddToBufferJob.perform_later("next")
    cursors.expire_discovery!(:all)

    queries = capture_candidate_queries { assert_equal 1, claim(1).size }

    assert_equal 1, queries.size
    assert_not_includes queries.sole, "id > "
  end

  test "jitter only schedules the unbounded pass earlier, never later" do
    now = 0.0
    clocked = SolidQueue::ReadyExecution::ClaimCursors.new(clock: -> { now })

    3.times do
      clocked.record_full_discovery(:all, [ 0, 1 ], 1)

      now += 0.7 * SolidQueue.claim_cursors_full_discovery_interval
      assert_not clocked.full_discovery_due?(:all)

      now += 0.3 * SolidQueue.claim_cursors_full_discovery_interval
      assert clocked.full_discovery_due?(:all)
    end
  end

  test "cursor state is scoped to the datastore" do
    cursors.advance(:all, [ 0, 10 ])

    # The pool is the datastore's identity: two datastores never share one
    SolidQueue::ReadyExecution.stubs(:connection_pool).returns(Object.new)
    other_cursors = SolidQueue::ReadyExecution.claim_cursors

    assert_nil other_cursors.position(:all)
    other_cursors.advance(:all, [ 0, 99 ])

    SolidQueue::ReadyExecution.unstub(:connection_pool)
    assert_equal [ 0, 10 ], cursors.position(:all)
  ensure
    other_cursors&.reset!
  end

  test "disabling claim cursors uses the classic path without changing cursor state" do
    SolidQueue.claim_cursors = false
    AddToBufferJob.perform_later("classic")

    queries = capture_candidate_queries { assert_equal 1, claim(1).size }

    assert_equal 1, queries.size
    assert_not_includes queries.sole, "(priority, id) > ("
    assert_nil cursors.position(:all)
    assert cursors.discovery_due?(:all)
  end

  private
    def claim(limit, queues: "*")
      SolidQueue::ReadyExecution.claim(queues, limit, PROCESS_ID)
    end

    def cursors
      SolidQueue::ReadyExecution.claim_cursors
    end

    def position_of(execution)
      [ execution.priority, execution.id ]
    end

    def seed_cursor_and_floor(priority: 0)
      AddToBufferJob.set(priority: priority).perform_later("floor seed")
      claim(1) # observes the seed id
      AddToBufferJob.set(priority: priority).perform_later("cursor seed")
      cursors.expire_full_discovery!(:all)
      claim(1) # a full pass that records the observed floor and the position
    end

    def claimed_active_job_ids(limit)
      claim(limit).map { |execution| SolidQueue::Job.find(execution.job_id).active_job_id }
    end

    def capture_ready_selects
      queries = []
      subscriber = ActiveSupport::Notifications.subscribe("sql.active_record") do |event|
        sql = event.payload[:sql]
        queries << sql if sql.start_with?("SELECT") && sql.include?("solid_queue_ready_executions")
      end

      yield
      queries
    ensure
      ActiveSupport::Notifications.unsubscribe(subscriber) if subscriber
    end

    def capture_candidate_queries
      queries = []
      subscriber = ActiveSupport::Notifications.subscribe("sql.active_record") do |event|
        sql = event.payload[:sql]
        queries << sql if sql.include?("solid_queue_ready_executions") && sql.include?("FOR UPDATE SKIP LOCKED")
      end

      yield
      queries
    ensure
      ActiveSupport::Notifications.unsubscribe(subscriber) if subscriber
    end
end

# Uses a second database connection, so rows must be committed and visible
# across connections
class SolidQueue::ClaimCursorsContentionTest < ActiveSupport::TestCase
  self.use_transactional_tests = false

  PROCESS_ID = 42

  setup do
    skip "Claim cursors require PostgreSQL" unless SolidQueue::Record.connection_db_config.adapter.match?(/postg/i)
    SolidQueue::ReadyExecution.claim_cursors.reset!
    @original_discovery_interval = SolidQueue.claim_cursors_discovery_interval
    SolidQueue.claim_cursors_discovery_interval = 10.minutes
  end

  teardown do
    SolidQueue::ReadyExecution.claim_cursors.reset!
    SolidQueue.claim_cursors_discovery_interval = @original_discovery_interval if @original_discovery_interval
  end

  test "rows skipped as locked elsewhere are rediscovered after the peer rolls back" do
    AddToBufferJob.perform_later("seed")
    claim(1) # discovery seeds the cursor

    AddToBufferJob.perform_later("contended")
    execution = SolidQueue::ReadyExecution.sole

    peer = SolidQueue::Record.connection_pool.checkout
    peer.execute("BEGIN")
    peer.execute("SELECT id FROM solid_queue_ready_executions WHERE id = #{execution.id} FOR UPDATE")

    assert_empty claim(1) # the only row is locked, so the seek looks empty
    assert_nil SolidQueue::ReadyExecution.claim_cursors.position(:all)

    peer.execute("ROLLBACK")

    SolidQueue::ReadyExecution.claim_cursors.expire_discovery!(:all)
    claimed = claim(1)

    assert_equal 1, claimed.size
    assert_equal "contended", SolidQueue::Job.find(claimed.sole.job_id).arguments.dig("arguments").first
  ensure
    if peer
      peer.execute("ROLLBACK") rescue nil
      SolidQueue::Record.connection_pool.checkin(peer)
    end
  end

  private
    def claim(limit)
      SolidQueue::ReadyExecution.claim("*", limit, PROCESS_ID)
    end
end
