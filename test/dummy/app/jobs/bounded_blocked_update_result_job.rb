class BoundedBlockedUpdateResultJob < UpdateResultJob
  limits_concurrency key: ->(job_result, **) { job_result }, on_conflict: :block, max_blocked: 1
end
