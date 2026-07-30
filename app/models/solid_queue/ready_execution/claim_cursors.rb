# frozen_string_literal: true

module SolidQueue
  class ReadyExecution
    # Process-local claim cursors: for each queue key, the last (priority, id)
    # position this process observed. Cursor-guided queries can descend the
    # polling index past dead tuples instead of scanning them.
    #
    # A cursor is not a lower bound for live work. A competing claim below it
    # can roll back (its own transaction or an enclosing one), a row can
    # receive a lower sequence id but commit after the cursor advances, and an
    # empty SKIP LOCKED seek can mean the remaining rows were locked elsewhere
    # rather than gone. Discovery is therefore fundamental to finding work, not
    # merely defensive healing, and it comes in two forms. A floored pass runs
    # on the short cadence, bounded below by the id watermark the last full
    # pass recorded, which every row allocated since then exceeds. The
    # unbounded full pass runs on the long cadence; it is the only pass that
    # reaches rows at or below that watermark, and so the only one allowed to
    # move a cursor.
    class ClaimCursors
      # Deadlines land up to this fraction BELOW the interval: worker processes
      # that booted together don't scan the graveyard in lockstep, and the
      # interval stays an upper bound on how long healing waits
      FULL_DISCOVERY_JITTER = 0.25

      def initialize(clock: -> { ::Process.clock_gettime(::Process::CLOCK_MONOTONIC) })
        @clock = clock
        @mutex = Mutex.new
        @positions = {}
        @floors = {}
        @observed_max_id = nil
        @next_discovery_at = {}
        @next_full_discovery_at = {}
      end

      # The highest ready execution id this process has seen anywhere. Ids are
      # allocated in increasing order, so it is a floor above which all newer
      # rows land; anything that breaks that (sequence caching, setval) only
      # delays a row until the next full pass, like a late commit.
      def observe(id)
        @mutex.synchronize do
          @observed_max_id = id if id && (@observed_max_id.nil? || id > @observed_max_id)
        end
      end

      def observed_max_id
        @mutex.synchronize { @observed_max_id }
      end

      def state(key)
        @mutex.synchronize { [ discovery_without_lock(key), @floors[key], @positions[key]&.dup ] }
      end

      def position(key)
        @mutex.synchronize { @positions[key]&.dup }
      end

      def floor(key)
        @mutex.synchronize { @floors[key] }
      end

      def advance(key, position)
        @mutex.synchronize do
          current = @positions[key]
          @positions[key] = position.dup if current.nil? || (current <=> position).negative?
        end
      end

      def clear(key, expected_position)
        @mutex.synchronize do
          @positions.delete(key) if @positions[key] == expected_position
        end
      end

      def discovery_due?(key)
        @mutex.synchronize { due_without_lock?(@next_discovery_at, key) }
      end

      def full_discovery_due?(key)
        @mutex.synchronize { due_without_lock?(@next_full_discovery_at, key) }
      end

      def record_full_discovery(key, position, floor)
        @mutex.synchronize do
          @positions[key] = position&.dup
          @floors[key] = floor
          @next_discovery_at[key] = monotonic_time + SolidQueue.claim_cursors_discovery_interval
          @next_full_discovery_at[key] = monotonic_time + jittered_full_discovery_interval
        end
      end

      # A floored pass only sees the part of the index above its floor, so it may
      # seed a missing position but must never advance or clear one: the rows its
      # floor hid would end up below the cursor, out of reach of every query but
      # the next full pass.
      def seed(key, position)
        @mutex.synchronize do
          @positions[key] = position.dup if @positions[key].nil?
        end
      end

      def record_floored_discovery(key)
        @mutex.synchronize do
          @next_discovery_at[key] = monotonic_time + SolidQueue.claim_cursors_discovery_interval
        end
      end

      def expire_discovery!(key)
        @mutex.synchronize { @next_discovery_at[key] = monotonic_time }
      end

      def expire_full_discovery!(key)
        @mutex.synchronize do
          @next_discovery_at[key] = monotonic_time
          @next_full_discovery_at[key] = monotonic_time
        end
      end

      def reset!
        @mutex.synchronize do
          @positions.clear
          @floors.clear
          @observed_max_id = nil
          @next_discovery_at.clear
          @next_full_discovery_at.clear
        end
      end

      private
        def discovery_without_lock(key)
          return nil unless due_without_lock?(@next_discovery_at, key)
          return :full if @floors[key].nil? || due_without_lock?(@next_full_discovery_at, key)

          :floored
        end

        def due_without_lock?(deadlines, key)
          deadline = deadlines[key]
          deadline.nil? || monotonic_time >= deadline
        end

        def jittered_full_discovery_interval
          interval = SolidQueue.claim_cursors_full_discovery_interval
          interval - rand * FULL_DISCOVERY_JITTER * interval
        end

        def monotonic_time
          @clock.call
        end
    end
  end
end
