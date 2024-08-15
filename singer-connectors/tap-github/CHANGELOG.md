1.1.0 (2023-09-13)
------------------
- Update GHA
- add additional field merge_commit_hash

1.0.3 (2021-12-10)
------------------
- Fixed pagination issue while fetching comments.
- Lock versions required by test packages.

1.0.2 (2021-08-17)
------------------
- Skipping repos with size 0 on list all repos
- Fixing wrong order of include/exclude params
- Improving rate throttling logs
- Added 1 minute more to wait on rate limit reset time when rate limit exceed 

1.0.1 (2021-08-09)
------------------
- Do not do rate throtting during discovery

1.0.0 (2021-07-19)
------------------

- This is a fork of https://github.com/singer-io/tap-github v1.10.0. 
- Add `organization`, `repos_include`, `repos_exclude`, `include_archived` and `include_disabled` options.
- Add `max_rate_limit_wait_seconds` option to wait if you hit the github api limit.
