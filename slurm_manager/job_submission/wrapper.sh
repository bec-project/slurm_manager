#!/usr/bin/env bash

set -o pipefail

env_path=$1
script_path=$2
hash_name=$3
have_out_stream=$4
have_out_file=$5


fifo=$(mktemp -u)
mkfifo "$fifo"

./redis_worker.sh "$fifo" "$hash_name" >/dev/null &
worker_pid=$!

trap 'rm -f "$fifo"' EXIT

exec {fifo_fd}>"$fifo"

source "$env_path"

{
    stdbuf -o0 -e0 "$script_path" 2>&1
} | while IFS= read -r line; do

    if [ "$have_out_stream" -eq 1 ]; then
        printf 'log:%s\n' "$line" >&"$fifo_fd"
    fi

    if [ "$have_out_file" -eq 1 ]; then
        printf '%s\n' "$line"
    fi

done


exit_code=${PIPESTATUS[0]}

if [ "$exit_code" -ne 0 ]; then
  printf 'error:%s\n' "$exit_code" >&"$fifo_fd"
fi

printf 'stop\n' >&"$fifo_fd"
exec {fifo_fd}>&-

wait $worker_pid

exit $exit_code
