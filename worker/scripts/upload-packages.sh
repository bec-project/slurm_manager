#!/usr/bin/env bash
# Match CAGO's Gitea authentication: username:token in GITEA_REGISTRY_TOKEN.
set -euo pipefail

if (( $# < 1 || $# > 2 )); then
  echo 'Usage: upload-packages.sh VERSION [ARTIFACT_DIRECTORY]' >&2
  exit 1
fi
: "${GITEA_REGISTRY_TOKEN:?Set GITEA_REGISTRY_TOKEN to username:token}"
version=$1
if [[ ! "$version" =~ ^[0-9]+\.[0-9]+\.[0-9]+(-[0-9A-Za-z.-]+)?(\+[0-9A-Za-z.-]+)?$ ]]; then
  echo 'Expected a semantic version without a leading v.' >&2
  exit 1
fi
artifact_directory=${2:-worker/dist}
shopt -s nullglob
archives=("$artifact_directory"/bec-slurm-worker_*.tar.gz)
rpms=("$artifact_directory"/bec-slurm-worker-*.rpm)
checksums="$artifact_directory/checksums.txt"
if (( ${#archives[@]} == 0 || ${#rpms[@]} == 0 )) || [[ ! -s "$checksums" ]]; then
  echo 'Linux archives, RPMs, and checksums are required; refusing an incomplete upload.' >&2
  exit 1
fi

# Keep a downloadable release bundle in the generic registry, including RPMs so
# every entry in checksums.txt can be downloaded from the same package version.
for package in "${archives[@]}" "${rpms[@]}" "$checksums"; do
  file_name=$(basename "$package")
  echo "Uploading $file_name to the BEC generic registry"
  curl --fail-with-body --silent --show-error \
    --connect-timeout 15 --max-time 300 \
    --user "$GITEA_REGISTRY_TOKEN" \
    --upload-file "$package" \
    "https://gitea.psi.ch/api/packages/bec/generic/bec-slurm-worker/$version/$file_name"
done

# Also index RPMs in the existing BEC RPM repository for Puppet/dnf deployment.
for package in "${rpms[@]}"; do
  echo "Uploading $(basename "$package") to the BEC RPM registry"
  curl --fail-with-body --silent --show-error \
    --connect-timeout 15 --max-time 300 \
    --user "$GITEA_REGISTRY_TOKEN" \
    --upload-file "$package" \
    'https://gitea.psi.ch/api/packages/bec/rpm/upload'
done
