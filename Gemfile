source "https://rubygems.org"
git_source(:github) { |repo| "https://github.com/#{repo}.git" }

# Specify your gem's dependencies in solid_queue.gemspec.
gemspec

# Security floor (Dependabot advisories on this fork):
# Rails 7.2.3.1 patches activesupport (DoS/ReDoS/XSS) and actionview (XSS).
# No backport exists in the 7.1.x line.
gem "railties", "~> 7.2.3", ">= 7.2.3.1"
