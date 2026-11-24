module SolidQueue
  module Supervisor::Maintenance
    extend ActiveSupport::Concern

    included do
      after_boot :fail_orphaned_executions
    end

    private
      def launch_maintenance_task
        @maintenance_task = Concurrent::TimerTask.new(run_now: true, execution_interval: SolidQueue.process_alive_threshold) do
          prune_dead_processes
        end

        @maintenance_task.add_observer do |_, _, error|
          handle_thread_error(error) if error
        end

        @maintenance_task.execute
      end

      def stop_maintenance_task
        @maintenance_task&.shutdown
      end

      def prune_dead_processes
        wrap_in_app_executor { SolidQueue::Process.prune(excluding: process) }
      end

      def fail_orphaned_executions
        wrap_in_app_executor do
          ClaimedExecution.orphaned.fail_all_with(Processes::ProcessMissingError.new)
        end
      end

      # When a supervised process crashes or exits we need to mark all the
      # executions it had claimed as failed so that they can be retried
      # by some other worker.
      def release_claimed_jobs_by(process_name, with_error:)
        wrap_in_app_executor do
          if registered_process = SolidQueue::Process.find_by(name: process_name)
            registered_process.fail_all_claimed_executions_with(with_error)
            registered_process.deregister(pruned: true)
          end
        end
      end

      def attempt_to_release_claimed_jobs_by(terminated_process, with_error:)
        pending_cleanup[terminated_process.name] ||= with_error
        attempt_pending_cleanup(terminated_process.name)
      end

      def retry_pending_cleanup
        pending_cleanup.keys.each { |name| attempt_pending_cleanup(name) }
      end

      def attempt_pending_cleanup(name)
        release_claimed_jobs_by(name, with_error: pending_cleanup.fetch(name))
        pending_cleanup.delete(name)
      rescue StandardError => error
        handle_thread_error(error)
      end

      def pending_cleanup
        @pending_cleanup ||= {}
      end

      def deregister
        # Retain the stopped parent so pruning can fail pending claims once its
        # heartbeat expires. Normal deregistration would release those claims.
        super if pending_cleanup.empty?
      end
  end
end
