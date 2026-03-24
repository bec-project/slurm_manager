#!/usr/bin/env bash
set -euo pipefail

module load redis

fifo=$1
hash_name=$2

exec {fifo_fd}<"$fifo"

status="finished"

redis-cli -h bec-slurm.psi.ch PUBLISH "info/$hash_name/event" start

heartbeat() {
  while :; do
      redis-cli -h bec-slurm.psi.ch PUBLISH "info/$hash_name/heartbeat" "$(date +%s)" || true
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
        redis-cli -h bec-slurm.psi.ch PUBLISH "info/$hash_name/log" "$msg"
        ;;
      *)
        :
        ;;
  esac
done

redis-cli -h bec-slurm.psi.ch PUBLISH "info/$hash_name/event" "$status"
