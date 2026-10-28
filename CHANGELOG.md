# CHANGELOG


## v0.1.0 (2026-10-28)

### Bug Fixes

- Address Slurm job monitoring and subscription review findings
  ([`afcf5fb`](https://github.com/bec-project/slurm_manager/commit/afcf5fbe65c5293ed60a129cc5e4eba2eb04d492))

- Cleanup
  ([`b44b216`](https://github.com/bec-project/slurm_manager/commit/b44b21661214574d9c6351e89c9a02722f961cc1))

- Fix vscode settings and formatting
  ([`3db18d6`](https://github.com/bec-project/slurm_manager/commit/3db18d6583bfdbed1c518a3ed71cf0707bed3b47))

- Formatting
  ([`b9ad35a`](https://github.com/bec-project/slurm_manager/commit/b9ad35a51d5895cddc63b5bac790aa5e2e2163d2))

- Route clustered jobs and serialize callback registration
  ([`24a8fa8`](https://github.com/bec-project/slurm_manager/commit/24a8fa889458ace13b80191bb088db3181a9e6d2))

### Chores

- Add .gitignore file with common Python ignores
  ([`2ebc5f2`](https://github.com/bec-project/slurm_manager/commit/2ebc5f273d746d130404e05a3038a11037f2e1a2))

- Add venv to gitignore
  ([`0f3e1e0`](https://github.com/bec-project/slurm_manager/commit/0f3e1e0e40e22935a6761ddeacc16af770649930))

### Continuous Integration

- Add ci actions
  ([`fd54d2b`](https://github.com/bec-project/slurm_manager/commit/fd54d2b1a8ea88b80e1a8723c6bf4d2071babb1d))

- Add semantic release workflow
  ([`36c858a`](https://github.com/bec-project/slurm_manager/commit/36c858aa43baffad0ccaac79a78627606629f0f2))

Closes #5.

- Separate unit tests and cover Python 3.12 through 3.14
  ([`63c58ce`](https://github.com/bec-project/slurm_manager/commit/63c58ce824c87fe127d13c85b24a9782e47da5be))

### Features

- Add redis_pubsub_client
  ([`b685051`](https://github.com/bec-project/slurm_manager/commit/b685051d8cc01f540191073fd209565fbbd643f7))

- Bash wrapper for job
  ([`4ef8646`](https://github.com/bec-project/slurm_manager/commit/4ef8646ca775d4ce5137805ada38f3082d3b4129))

- Initial commit
  ([`0b9782d`](https://github.com/bec-project/slurm_manager/commit/0b9782d751218cf3abe3f7247a4251b385c14fad))

- Slurmmanager, JobFuture and RedisSubClient initial implementation
  ([`0164768`](https://github.com/bec-project/slurm_manager/commit/01647688e93acb1bbd0baa306ae8d081c68eb4a6))

- **job-future**: Add JobFuture implementation for monitoring running slurm jobs.
  ([`aa863c8`](https://github.com/bec-project/slurm_manager/commit/aa863c80f3b6272cfd638deef8fa467ffe48b727))

- **slurm-manager**: First imlementation of SlurmManager
  ([`3019d6e`](https://github.com/bec-project/slurm_manager/commit/3019d6e50534781ee3585e27cf3922da09121574))

### Refactoring

- Add slurm job id to SubscriptionInfo and to metadata
  ([`aa2b3e5`](https://github.com/bec-project/slurm_manager/commit/aa2b3e5389ff0254e6f7248dc9c04f2071d9e039))

- Address PR comments
  ([`5d7b9d4`](https://github.com/bec-project/slurm_manager/commit/5d7b9d4034c98f3b6a2e7781ea1895ef5012a521))

- Fix pyproject and add readme
  ([`bb6e43f`](https://github.com/bec-project/slurm_manager/commit/bb6e43fe7473ee4981f8966c3083519a6b3f8ae6))

- Simplify redis_worker.sh and use jq for json bash parsing
  ([`1685e88`](https://github.com/bec-project/slurm_manager/commit/1685e88a1af49de96969c1ddf624ed5f08125519))

- **slurm-manager**: Refactor job_info to pydantic, and cleanup
  ([`a64bb84`](https://github.com/bec-project/slurm_manager/commit/a64bb843b95516344e23fae5f48640bd86d299c2))

### Testing

- Fix test for test_redis_pubsub_client
  ([`86d3a9e`](https://github.com/bec-project/slurm_manager/commit/86d3a9ee14234d1b80dcbae90dab1097930554ca))
