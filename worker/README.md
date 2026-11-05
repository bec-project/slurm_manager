# bec-slurm-worker

Compute-node executable for Slurm Manager. It runs one uploaded Bash workload
and forwards stdout, stderr, lifecycle status, and heartbeats to Redis.
It is launched by Slurm Manager, not as a system service.

The worker reads `job_id`, `script`, and `activation` from a JSON object on stdin.
The job ID is kept out of process arguments. The ID remains in the submitted
Slurm batch script, which must be protected from other users.

Activation and workload scripts receive the job ID as `SLURM_WORKER_JOBID` in
their environment, including child processes. The worker does not log the payload
or print this variable automatically. Scripts that print their environment expose
the ID in Slurm output and Redis logs.

## Builds

With Go 1.27.3 or later installed, `make build` creates `../dist/bec-slurm-worker-linux-amd64`.
Use `TARGET_ARCH=arm64` for ARM64.

With GoReleaser 2.18.0 installed, `make package VERSION=0.2.0` creates static Linux
archives, RPMs for x86_64 and aarch64, and SHA-256 checksums in `dist/`.
The RPM installs `/usr/bin/bec-slurm-worker` and requires Bash and `getent` for
compute-node account lookup. No Python or Go runtime is required on the node.

## CI and Gitea

Every CI run tests the worker and builds both architectures and RPMs. GitHub Actions
retains them in the `bec-slurm-worker-packages` artifact. Branch and PR builds use a
development version; version tags use the matching release version.

After all CI jobs pass for a pushed `v*` tag, CI publishes the archives, RPMs,
and checksums to the Gitea generic package `bec/bec-slurm-worker`, and indexes
the RPMs in the BEC RPM registry, following CAGO's packaging approach.
`GITEA_REGISTRY_TOKEN` must be available to this repository as an Actions secret,
with the value `username:token` and permission to publish BEC packages.

Generic downloads:
`https://gitea.psi.ch/api/packages/bec/generic/bec-slurm-worker/VERSION/FILENAME`

The publisher requires both architectures and verifies the complete checksum manifest
before uploading. Reruns compare existing remote files byte-for-byte and continue
only when they match; conflicting files are never overwritten. Upload failures are
reported, and a later run can resume missing generic files and RPM indexing.
No packages are uploaded for PRs or untagged branch builds.
