# frozen_string_literal: true

module SolidQueue
  class ReadyExecution < Execution
    scope :queued_as, ->(queue_name) { where(queue_name: queue_name) }

    # Within a priority, claims follow the order rows became ready (id) rather
    # than enqueue order (job_id), matching the claim cursor's index position.
    scope :ordered, -> { order(priority: :asc, id: :asc) }

    assumes_attributes_from_job

    FLOORED_WINDOW = 500

    class << self
      def claim(queue_list, limit, process_id)
        QueueSelector.new(queue_list, self).relations_by_queue.flat_map do |key, queue_relation|
          select_and_lock(key, queue_relation, process_id, limit).tap do |locked|
            limit -= locked.size
          end
        end
      end

      def aggregated_count_across(queue_list)
        QueueSelector.new(queue_list, self).scoped_relations.map(&:count).sum
      end

      # Cursors, floors and discovery deadlines are positions in one table's id
      # space, so a process polling several databases keeps a registry per
      # datastore. The pool is the datastore's identity: config names can
      # collide (every raw hash or URL config resolves to "primary"), pools
      # cannot.
      def claim_cursors
        claim_cursors_registry.compute_if_absent(connection_pool) { ClaimCursors.new }
      end

      private
        # Keyed to the current pid so forked processes always cold-start:
        # inherited cursors, floors, and deadlines belong to the parent
        def claim_cursors_registry
          if @claim_cursors_pid != ::Process.pid
            @claim_cursors_pid = ::Process.pid
            @claim_cursors_registry = Concurrent::Map.new
          end

          @claim_cursors_registry
        end

        def select_and_lock(key, queue_relation, process_id, limit)
          return [] if limit <= 0

          unless claim_cursors_enabled?
            return claim_classically(queue_relation, process_id, limit)
          end

          discovery, floor, position = claim_cursors.state(key)

          case discovery
          when :full    then claim_discovering(key, queue_relation, process_id, limit)
          when :floored then claim_discovering_above(key, floor, position, queue_relation, process_id, limit)
          else
            position ? claim_along_cursor(key, position, queue_relation, process_id, limit) : []
          end
        end

        def claim_classically(queue_relation, process_id, limit)
          _candidates, claimed = claim_candidates(queue_relation, process_id, limit)
          claimed
        end

        # Cursor-free claim in full (priority, id) order. The one pass that reaches
        # rows a floored pass cannot see, so the one that may move the cursor
        # anywhere. Its floor is the highest id observed before the pass, so
        # every row that arrives after it is above it.
        def claim_discovering(key, queue_relation, process_id, limit)
          floor = claim_cursors.observed_max_id
          candidates, claimed = claim_candidates(queue_relation, process_id, limit)

          claim_cursors.observe(candidates.map(&:id).max)
          claim_cursors.record_full_discovery(key, position_of(candidates.last), floor)

          claimed
        end

        # Discovery bounded below by the floor: the highest id observed before
        # the last full pass, which every row arriving since then exceeds. The
        # database is never asked to order this pass: the window of region rows
        # is fetched in id order, the (priority, id) ordering and the pick
        # happen in memory, and the picked rows are locked by bare id equality
        # with no ORDER BY -- so no plan has an ordering reason to walk an index
        # through the graveyard. A stale window row just misses its lock. One
        # transaction for both claim halves, so a failure above the cursor
        # cannot strand committed below-cursor claims; the deadline is recorded
        # after it returns, and only once the region is drained, or rows below
        # the cursor would wait on lower-priority work above it. A region larger
        # than the window trades strict priority order for boundedness inside
        # the window until it drains or the next full pass, a documented slack.
        def claim_discovering_above(key, floor, position, queue_relation, process_id, limit)
          drained = seed = nil

          claimed = transaction(requires_new: true) do
            # The window is read, not consumed: observing it would raise the
            # floor over rows merely beyond this pick, demoting them to
            # full-pass healing. The floor follows claimed candidates only.
            window = queue_relation.where("id > ?", floor).order(:id).limit(FLOORED_WINDOW)
                                   .select(:id, :job_id, :priority).to_a
            region = position ? window.select { |row| (position_of(row) <=> position) <= 0 } : window
            picked = region.sort_by { |row| position_of(row) }.first(limit)

            below_claimed = lock_picked(picked, queue_relation, process_id)
            drained = window.size < FLOORED_WINDOW && picked.size < limit
            seed = position_of(picked.last) unless position

            if position && below_claimed.size < limit
              below_claimed + claim_along_cursor(key, position, queue_relation, process_id, limit - below_claimed.size)
            else
              below_claimed
            end
          end

          claim_cursors.seed(key, seed) if seed
          claim_cursors.record_floored_discovery(key) if drained

          claimed
        end

        # Bare id equality and no ORDER BY: SKIP LOCKED may still lose rows to
        # peers, and the claim order is the in-memory (priority, id) pick order
        def lock_picked(picked, queue_relation, process_id)
          return [] if picked.empty?

          relocked = queue_relation.where(id: picked.map(&:id))
                                   .non_blocking_lock.select(:id, :job_id, :priority).to_a.index_by(&:id)
          candidates = picked.filter_map { |row| relocked[row.id] }

          Array(lock_candidates(candidates, process_id))
        end

        # One lexicographic index seek across every priority in the queue key.
        def claim_along_cursor(key, position, queue_relation, process_id, limit)
          candidates, claimed = claim_candidates(
            queue_relation.where("(priority, id) > (?, ?)", *position), process_id, limit
          )

          if candidates.any?
            claim_cursors.observe(candidates.map(&:id).max)
            claim_cursors.advance(key, position_of(candidates.last))
          else
            # Under SKIP LOCKED, empty can also mean every remaining row was
            # locked elsewhere; either way the next discovery re-checks
            claim_cursors.clear(key, position)
          end

          claimed
        end

        def claim_candidates(relation, process_id, limit)
          candidates = nil

          claimed = transaction do
            candidates = select_candidates(relation, limit)
            Array(lock_candidates(candidates, process_id))
          end

          [ candidates, claimed ]
        end

        # Row-constructor index seeks and the motivating dead-tuple pathology
        # are PostgreSQL-specific.
        def claim_cursors_enabled?
          SolidQueue.claim_cursors && connection_db_config.adapter.match?(/postg/i)
        end

        def select_candidates(relation, limit)
          # Force query execution here with #to_a to avoid unintended FOR UPDATE query executions
          relation.ordered.limit(limit).non_blocking_lock.select(:id, :job_id, :priority).to_a
        end

        def lock_candidates(executions, process_id)
          return [] if executions.none?

          SolidQueue::ClaimedExecution.claiming(executions.map(&:job_id), process_id) do |claimed|
            ids_to_delete = executions.index_by(&:job_id).values_at(*claimed.map(&:job_id)).map(&:id)
            where(id: ids_to_delete).delete_all
          end
        end

        def position_of(execution)
          [ execution.priority, execution.id ] if execution
        end

        def discard_jobs(job_ids)
          Job.release_all_concurrency_locks Job.where(id: job_ids)
          super
        end
    end
  end
end
