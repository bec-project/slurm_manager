#!/bin/bash
for second in {1..10}; do
    printf 'stdout-test %02d/10\n' "$second"
    sleep 1
done
