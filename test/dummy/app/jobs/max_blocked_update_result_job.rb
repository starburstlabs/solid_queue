class MaxBlockedUpdateResultJob < UpdateResultJob
  limits_concurrency key: ->(job_result, **) { job_result }, max_blocked: 1
end
