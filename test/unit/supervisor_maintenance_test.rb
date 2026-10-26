require "test_helper"

class SupervisorMaintenanceTest < ActiveSupport::TestCase
  self.use_transactional_tests = false

  setup do
    configuration = SolidQueue::Configuration.new(mode: :fork, standalone: false, workers: [], dispatchers: [], skip_recurring: true)
    @supervisor = SolidQueue::ForkSupervisor.new(configuration)
    @supervisor.send(:register)
    @parent = SolidQueue::Process.find(@supervisor.process_id)
    @worker = register_worker
    @claim = claim_job(@worker)
    @exit_error = SolidQueue::Processes::ThreadTerminatedError.new(@worker.name)
    @supervisor.stubs(:handle_thread_error)
  end

  test "confirmed death fails claims and removes the worker registration" do
    release_worker

    assert_failed_claim(@claim)
    assert_not SolidQueue::Process.exists?(@worker.id), "Reaped worker registration must be removed"
  end

  test "the supervisor retries after a lookup failure while protecting an unreaped sibling" do
    sibling = register_worker
    sibling_claim = claim_job(sibling)
    [ @worker, sibling ].each { |worker| worker.update!(last_heartbeat_at: 10.minutes.ago) }

    fail_worker_lookup { release_worker }
    SolidQueue::Process.prune(excluding: @parent)
    assert SolidQueue::ClaimedExecution.exists?(@claim.id)
    assert SolidQueue::ClaimedExecution.exists?(sibling_claim.id)

    supervise_once

    assert_failed_claim(@claim)
    assert_equal @exit_error.class.name, SolidQueue::FailedExecution.find_by!(job_id: @claim.job_id).error["exception_class"]
    assert_not SolidQueue::Process.exists?(@worker.id)
    assert SolidQueue::Process.exists?(sibling.id)
    assert SolidQueue::ClaimedExecution.exists?(sibling_claim.id)
  end

  test "shutdown preserves pending claims until the stopped parent becomes stale" do
    @supervisor.send(:launch_heartbeat)
    heartbeat = @supervisor.instance_variable_get(:@heartbeat_task)
    fail_worker_lookup { release_worker }

    @supervisor.send(:shutdown)

    assert_not heartbeat.running?
    assert SolidQueue::Process.exists?(@parent.id), "Pending cleanup must retain the stopped parent"
    assert SolidQueue::Process.exists?(@worker.id)
    assert SolidQueue::ClaimedExecution.exists?(@claim.id)
    assert_not SolidQueue::ReadyExecution.exists?(job_id: @claim.job_id)

    @parent.update!(last_heartbeat_at: 10.minutes.ago)
    @worker.update!(last_heartbeat_at: 10.minutes.ago)
    replacement = SolidQueue::Process.register(kind: "Supervisor(fork)", pid: 900003, name: SecureRandom.hex)
    SolidQueue::Process.prune(excluding: replacement)

    assert_failed_claim(@claim)
    assert_not SolidQueue::Process.exists?(@parent.id)
    assert_not SolidQueue::Process.exists?(@worker.id)
    assert SolidQueue::Process.exists?(replacement.id)
  ensure
    heartbeat&.shutdown
  end

  test "heartbeat stops before a clean supervisor deregisters" do
    @supervisor.send(:launch_heartbeat)
    heartbeat = @supervisor.instance_variable_get(:@heartbeat_task)
    parent_id, test_case = @parent.id, self
    check_heartbeat = ->(record) { test_case.assert_not heartbeat.running? if record.id == parent_id }
    SolidQueue::Process.set_callback(:destroy, :before, check_heartbeat)

    @supervisor.send(:shutdown)

    assert_not SolidQueue::Process.exists?(@parent.id)
    assert SolidQueue::ReadyExecution.exists?(job_id: @claim.job_id)
  ensure
    SolidQueue::Process.skip_callback(:destroy, :before, check_heartbeat) if check_heartbeat
    heartbeat&.shutdown
  end

  test "partial cleanup retries remaining claims without failing a job twice" do
    second_claim = claim_job(@worker)
    fail_second = ->(failure) { raise ActiveRecord::ConnectionNotEstablished, "connection is closed" if failure.job_id == second_claim.job_id }
    SolidQueue::FailedExecution.set_callback(:create, :before, fail_second)

    release_worker

    assert_failed_claim(@claim)
    assert SolidQueue::ClaimedExecution.exists?(second_claim.id)
    first_failure_id = SolidQueue::FailedExecution.find_by!(job_id: @claim.job_id).id
    SolidQueue::FailedExecution.skip_callback(:create, :before, fail_second)
    fail_second = nil

    supervise_once

    assert_failed_claim(second_claim)
    release_worker
    assert_equal first_failure_id, SolidQueue::FailedExecution.find_by!(job_id: @claim.job_id).id
    assert_equal 2, SolidQueue::FailedExecution.count
    assert_not SolidQueue::Process.exists?(@worker.id)
  ensure
    SolidQueue::FailedExecution.skip_callback(:create, :before, fail_second) if fail_second
  end

  test "a failed deregistration retries without repeating claim finalization" do
    worker_id = @worker.id
    fail_destroy = ->(record) { raise ActiveRecord::ConnectionNotEstablished, "connection is closed" if record.id == worker_id }
    SolidQueue::Process.set_callback(:destroy, :before, fail_destroy)

    release_worker
    assert_failed_claim(@claim)
    failure_id = SolidQueue::FailedExecution.find_by!(job_id: @claim.job_id).id
    assert SolidQueue::Process.exists?(@worker.id)
    SolidQueue::Process.skip_callback(:destroy, :before, fail_destroy)
    fail_destroy = nil

    supervise_once

    assert_not SolidQueue::Process.exists?(@worker.id)
    assert_equal [ failure_id ], SolidQueue::FailedExecution.pluck(:id)
  ensure
    SolidQueue::Process.skip_callback(:destroy, :before, fail_destroy) if fail_destroy
  end

  test "cleanup tolerates an already removed worker" do
    release_worker
    release_worker

    assert_equal 1, SolidQueue::FailedExecution.where(job_id: @claim.job_id).count
    assert_not SolidQueue::Process.exists?(@worker.id)
  end

  test "cleanup removes a dead nonworker registration" do
    dispatcher = SolidQueue::Process.register(kind: "Dispatcher", pid: 900004, name: SecureRandom.hex, supervisor_id: @parent.id)

    @supervisor.send(:attempt_to_release_claimed_jobs_by, dispatcher, with_error: @exit_error)

    assert_not SolidQueue::Process.exists?(dispatcher.id)
    assert SolidQueue::ClaimedExecution.exists?(@claim.id)
  end

  test "abnormal shutdown reap fails claims and removes the dead worker" do
    reaped_worker = stub(name: @worker.name, mark_as_reaped: true)
    @supervisor.send(:process_instances)[42] = reaped_worker
    status = stub(exited?: false, exitstatus: nil, pid: 42, signaled?: true, stopsig: nil, termsig: 9)
    ::Process.stubs(:waitpid2).returns([ 42, status ], nil)

    @supervisor.send(:reap_terminated_forks)

    assert_failed_claim(@claim)
    assert_not SolidQueue::Process.exists?(@worker.id)
  end

  test "a clean shutdown reap keeps normal claim release semantics" do
    reaped_worker = stub(name: @worker.name, mark_as_reaped: true)
    @supervisor.send(:process_instances)[42] = reaped_worker
    status = stub(exited?: true, exitstatus: 0)
    ::Process.stubs(:waitpid2).returns([ 42, status ], nil)

    @supervisor.send(:reap_terminated_forks)
    @supervisor.send(:shutdown)

    assert_not SolidQueue::FailedExecution.exists?(job_id: @claim.job_id)
    assert SolidQueue::ReadyExecution.exists?(job_id: @claim.job_id)
  end

  test "retry after committed finalization does not promote another blocked job" do
    result = JobResult.create!(queue_name: "default", status: "")
    3.times { |i| NonOverlappingUpdateResultJob.perform_later(result, name: i.to_s) }
    SolidQueue::ReadyExecution.claim("*", 1, @worker.id)
    claim = @worker.claimed_executions.where.not(id: @claim.id).sole
    worker_id = @worker.id
    fail_destroy = ->(record) { raise ActiveRecord::ConnectionNotEstablished, "connection is closed" if record.id == worker_id }
    SolidQueue::Process.set_callback(:destroy, :before, fail_destroy)

    release_worker

    assert_failed_claim(claim)
    assert_equal 1, SolidQueue::ReadyExecution.count
    assert_equal 1, SolidQueue::BlockedExecution.count
    SolidQueue::Process.skip_callback(:destroy, :before, fail_destroy)
    fail_destroy = nil

    supervise_once
    release_worker

    assert_equal 1, SolidQueue::ReadyExecution.count
    assert_equal 1, SolidQueue::BlockedExecution.count
    assert_equal 0, SolidQueue::Semaphore.find_by!(key: claim.job.concurrency_key).value
    assert_not SolidQueue::Process.exists?(@worker.id)
  ensure
    SolidQueue::Process.skip_callback(:destroy, :before, fail_destroy) if fail_destroy
  end

  test "shutdown stops an embedded async heartbeat while retaining pending cleanup" do
    configuration = SolidQueue::Configuration.new(mode: :async, standalone: false, workers: [], dispatchers: [], skip_recurring: true)
    @supervisor = SolidQueue::AsyncSupervisor.new(configuration)
    @supervisor.stubs(:handle_thread_error)
    @supervisor.send(:process=, @parent)
    @supervisor.send(:launch_heartbeat)
    heartbeat = @supervisor.instance_variable_get(:@heartbeat_task)
    fail_worker_lookup { release_worker }

    @supervisor.send(:shutdown)

    assert_not heartbeat.running?
    assert SolidQueue::Process.exists?(@parent.id)
    assert SolidQueue::ClaimedExecution.exists?(@claim.id)
    assert_not SolidQueue::ReadyExecution.exists?(job_id: @claim.job_id)
  ensure
    heartbeat&.shutdown
  end

  private
    def register_worker
      SolidQueue::Process.register(kind: "Worker", pid: 900002, name: SecureRandom.hex, supervisor_id: @parent.id)
    end

    def claim_job(worker)
      job = StoreResultJob.perform_later(42)
      SolidQueue::ReadyExecution.claim("*", 1, worker.id)
      SolidQueue::ClaimedExecution.find_by!(job_id: job.provider_job_id)
    end

    def release_worker
      @supervisor.send(:attempt_to_release_claimed_jobs_by, @worker, with_error: @exit_error)
    end

    def fail_worker_lookup
      SolidQueue::Process.stubs(:find_by).with(name: @worker.name).raises(ActiveRecord::ConnectionNotEstablished.new("connection is closed"))
      yield
    ensure
      SolidQueue::Process.unstub(:find_by)
    end

    def supervise_once
      @supervisor.stubs(:time_to_stop?).returns(false, true)
      @supervisor.stubs(:check_and_replace_terminated_processes)
      @supervisor.stubs(:interruptible_sleep)
      @supervisor.stubs(:shutdown)
      @supervisor.send(:supervise)
    end

    def assert_failed_claim(claim)
      assert SolidQueue::FailedExecution.exists?(job_id: claim.job_id), "Claimed job must be failed"
      assert_not SolidQueue::ClaimedExecution.exists?(claim.id)
      assert_not SolidQueue::ReadyExecution.exists?(job_id: claim.job_id)
    end
end
