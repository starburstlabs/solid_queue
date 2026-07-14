# frozen_string_literal: true

module SolidQueue
  class Job
    module ConcurrencyControls
      extend ActiveSupport::Concern

      included do
        has_one :blocked_execution

        delegate :concurrency_limit, :concurrency_duration, :concurrency_max_blocked, to: :job_class

        before_destroy :unblock_next_blocked_job, if: -> { concurrency_limited? && ready? }
      end

      class_methods do
        def release_all_concurrency_locks(jobs)
          Semaphore.signal_all(jobs.select(&:concurrency_limited?))
        end
      end

      def unblock_next_blocked_job
        promote_next_blocked_job if release_concurrency_permit
      end

      # Return this job's concurrency permit. Kept separate from promotion so the
      # permit can be returned inside the claim transaction while promoting the
      # next blocked job happens after that transaction commits.
      def release_concurrency_permit
        release_concurrency_lock
      end

      def promote_next_blocked_job
        release_next_blocked_job
      end

      def concurrency_limited?
        concurrency_key.present? && job_class.present?
      end

      def blocked?
        blocked_execution.present?
      end

      private
        def concurrency_on_conflict
          job_class.concurrency_on_conflict.to_s.inquiry
        end

        def acquire_concurrency_lock
          return true unless concurrency_limited?

          Semaphore.wait(self)
        end

        def release_concurrency_lock
          return false unless concurrency_limited?

          Semaphore.signal(self)
        end

        def handle_concurrency_conflict
          if concurrency_on_conflict.discard?
            destroy
          else
            block
          end
        end

        def block
          return BlockedExecution.create_or_find_by!(job_id: id) unless concurrency_max_blocked

          transaction do
            # Lock the semaphore row to serialize concurrent enqueues racing to fill the cap.
            Semaphore.lock.find_by(key: concurrency_key)

            if BlockedExecution.where(concurrency_key: concurrency_key).count < concurrency_max_blocked
              BlockedExecution.create_or_find_by!(job_id: id)
            else
              destroy
            end
          end
        end

        def release_next_blocked_job
          BlockedExecution.release_one(concurrency_key)
        end

        def job_class
          @job_class ||= class_name.safe_constantize
        end

        def execution
          super || blocked_execution
        end
    end
  end
end
