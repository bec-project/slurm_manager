#!/usr/bin/env bash
# Match CAGO's Gitea authentication: username:token in GITEA_REGISTRY_TOKEN.
set -euo pipefail

fail() {
  echo "$*" >&2
  exit 1
}

if (( $# < 1 || $# > 2 )); then
  fail 'Usage: upload-packages.sh VERSION [ARTIFACT_DIRECTORY]'
fi
: "${GITEA_REGISTRY_TOKEN:?Set GITEA_REGISTRY_TOKEN to username:token}"
version=$1
if [[ ! "$version" =~ ^[0-9]+\.[0-9]+\.[0-9]+(-[0-9A-Za-z.-]+)?(\+[0-9A-Za-z.-]+)?$ ]]; then
  fail 'Expected a semantic version without a leading v.'
fi
artifact_directory=${2:-worker/dist}
shopt -s nullglob
archives=("$artifact_directory"/bec-slurm-worker_*.tar.gz)
rpms=("$artifact_directory"/bec-slurm-worker-*.rpm)
x86_rpms=("$artifact_directory"/bec-slurm-worker-*.x86_64.rpm)
arm_rpms=("$artifact_directory"/bec-slurm-worker-*.aarch64.rpm)
checksums="$artifact_directory/checksums.txt"
if (( ${#archives[@]} != 2 || ${#rpms[@]} != 2 || ${#x86_rpms[@]} != 1 || ${#arm_rpms[@]} != 1 )) ||
   [[ ! -s "$checksums" ||
      ! -s "$artifact_directory/bec-slurm-worker_${version}_linux_amd64.tar.gz" ||
      ! -s "$artifact_directory/bec-slurm-worker_${version}_linux_arm64.tar.gz" ]]; then
  fail 'Both Linux architectures, their RPMs, and checksums are required; refusing an incomplete upload.'
fi

temporary=$(mktemp -d)
trap 'rm -rf "$temporary"' EXIT
: > "$temporary/manifest-names"
: > "$temporary/expected-names"
for package in "${archives[@]}" "${rpms[@]}"; do
  file_name=$(basename "$package")
  [[ "$file_name" =~ ^[A-Za-z0-9._+-]+$ ]] || fail "Invalid package filename: $file_name"
  printf '%s\n' "$file_name" >> "$temporary/expected-names"
done
while read -r digest file_name extra || [[ -n "$digest$file_name$extra" ]]; do
  [[ "$digest" =~ ^[[:xdigit:]]{64}$ && "$file_name" =~ ^[A-Za-z0-9._+-]+$ && -z "$extra" ]] ||
    fail 'Invalid checksum manifest entry.'
  printf '%s\n' "$file_name" >> "$temporary/manifest-names"
done < "$checksums"
sort "$temporary/expected-names" > "$temporary/expected-sorted"
sort "$temporary/manifest-names" > "$temporary/manifest-sorted"
cmp -s "$temporary/expected-sorted" "$temporary/manifest-sorted" ||
  fail 'Checksum manifest must list each of the four packages exactly once.'
# Validate all local bytes before making any registry requests.
if command -v sha256sum >/dev/null 2>&1; then
  (cd "$artifact_directory" && sha256sum --check checksums.txt)
else
  (cd "$artifact_directory" && shasum -a 256 --check checksums.txt)
fi

upload_package() {
  local package=$1 upload_url=$2 download_url=$3 status
  echo "Publishing $(basename "$package")"
  # Inspect HTTP status explicitly: 409 is safe only after comparing existing bytes.
  status=$(curl --silent --show-error \
    --connect-timeout 15 --max-time 300 \
    --user "$GITEA_REGISTRY_TOKEN" \
    --output "$temporary/response" --write-out '%{http_code}' \
    --upload-file "$package" "$upload_url")
  case "$status" in
    201) return ;;
    409)
      curl --fail-with-body --silent --show-error \
        --connect-timeout 15 --max-time 300 \
        --user "$GITEA_REGISTRY_TOKEN" \
        --output "$temporary/existing" "$download_url"
      cmp -s "$package" "$temporary/existing" ||
        fail "Conflicting remote package: $(basename "$package"); refusing to overwrite."
      echo 'Identical package already published; continuing.'
      ;;
    *) fail "Package upload failed (HTTP $status): $(basename "$package")" ;;
  esac
}

# Keep the complete downloadable bundle together in the generic registry.
for package in "${archives[@]}" "${rpms[@]}" "$checksums"; do
  file_name=$(basename "$package")
  url="https://gitea.psi.ch/api/packages/bec/generic/bec-slurm-worker/$version/$file_name"
  upload_package "$package" "$url" "$url"
done

# Gitea indexes RPMs by name, version-release, and architecture. Undo the filename's
# tilde substitution to recover the RPM version used by Gitea's download endpoint.
for package in "${rpms[@]}"; do
  rpm_name=$(basename "$package" .rpm)
  architecture=${rpm_name##*.}
  rpm_version=${rpm_name%.*}
  rpm_version=${rpm_version#bec-slurm-worker-}
  rpm_version=${rpm_version//_/\~}
  download_url="https://gitea.psi.ch/api/packages/bec/rpm/package/bec-slurm-worker/$rpm_version/$architecture"
  upload_package "$package" 'https://gitea.psi.ch/api/packages/bec/rpm/upload' "$download_url"
done
