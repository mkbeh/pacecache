#!/usr/bin/env bash

set -euo pipefail

root_dir="$(git rev-parse --show-toplevel)"
cd "${root_dir}"

echo "capacity,entries,segments,alloc_bytes,total_alloc_bytes,bytes_per_entry"

for capacity in \
    1000 \
    10000 \
    25000 \
    100000 \
    1000000
do
    go run ./benchmarks/performance/memory \
        -capacity "${capacity}"
done