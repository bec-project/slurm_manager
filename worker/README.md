# bec-slurm-worker

Compute-node executable for Slurm Manager. It runs one uploaded Bash workload
and forwards stdout, stderr, lifecycle status, and heartbeats to Redis.
It is launched by Slurm Manager, not as a system service.

## Builds

With Go installed, `make build` creates `../dist/bec-slurm-worker-linux-amd64`.
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

The publisher refuses incomplete artifact sets and propagates upload failures.
Existing package files are not overwritten. No packages are uploaded for PRs or
untagged branch builds.
