#!/usr/bin/env bash
set -euo pipefail

module load redis

fifo=$1
hash_name=$2

exec {fifo_fd}<"$fifo"

redis_host="bec-slurm.psi.ch"
status="finished"

json_escape() {
  local value=${1-}
  value=${value//\\/\\\\}
  value=${value//\"/\\\"}
  value=${value//$'\n'/\\n}
  value=${value//$'\r'/\\r}
  value=${value//$'\t'/\\t}
  value=${value//$'\f'/\\f}
  value=${value//$'\b'/\\b}
  printf '%s' "$value"
}

build_heartbeat_payload() {
  local timestamp=$1
  printf '{"msg_type":"heartbeat","timestamp":%s}' "$timestamp"
}

build_status_payload() {
  local status_value=$1
  printf '{"msg_type":"status","status":"%s"}' "$(json_escape "$status_value")"
}

build_log_payload() {
  local log_value=$1
  printf '{"msg_type":"log","log":"%s"}' "$(json_escape "$log_value")"
}

publish_payload() {
  local topic=$1
  local payload=$2
  redis-cli -h "$redis_host" PUBLISH "$topic" "$payload"
}

timestamp=$(date +%s)
publish_payload "info/$hash_name/status" "$(build_status_payload "running")"

heartbeat() {
  while :; do
      local timestamp
      timestamp=$(date +%s)
      publish_payload "info/$hash_name/heartbeat" "$(build_heartbeat_payload "$timestamp")" || true
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
        publish_payload "info/$hash_name/log" "$(build_log_payload "$msg")"
        ;;
      *)
        :
        ;;
  esac
done

publish_payload "info/$hash_name/status" "$(build_status_payload "$status")"
