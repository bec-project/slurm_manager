#!/usr/bin/env bash
set -euo pipefail

module load redis

fifo=$1
hash_name=$2

exec {fifo_fd}<"$fifo"

redis_host="bec-slurm.psi.ch"
status="finished"

publish_payload() {
  local topic=$1
  local payload=$2
  redis-cli -h "$redis_host" PUBLISH "$topic" "$payload"
}

timestamp=$(date +%s)
publish_payload \
  "info/$hash_name/status" \
  "$(jq -cn --arg status "running" '{msg_type:"status", status:$status}')"

heartbeat() {
  while :; do
    local timestamp
    timestamp=$(date +%s)
    publish_payload \
      "info/$hash_name/heartbeat" \
      "$(jq -cn --argjson timestamp "$timestamp" '{msg_type:"heartbeat", timestamp:$timestamp}')" \
      || true
    sleep 1
  done
}

heartbeat &
hb_pid=$!

cleanup() {
  if kill -0 "$hb_pid" 2>/dev/null; then
    kill "$hb_pid" 2>/dev/null || true
    wait "$hb_pid" 2>/dev/null || true
  fi
}
trap cleanup EXIT

while IFS= read -r line <&"$fifo_fd"; do
  case "$line" in
    stop)
      status="finished"
      break
      ;;
    error:*)
      status="error:${line#error:}"
      break
      ;;
    log:*)
      msg="${line#log:}"
      publish_payload \
        "info/$hash_name/log" \
        "$(jq -cn --arg log "$msg" '{msg_type:"log", log:$log}')"
      ;;
    *)
      :
      ;;
  esac
done

publish_payload \
  "info/$hash_name/status" \
  "$(jq -cn --arg status "$status" '{msg_type:"status", status:$status}')"
