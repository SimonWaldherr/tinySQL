#!/usr/bin/env bash
set -euo pipefail
cd "$(dirname "${BASH_SOURCE[0]}")/.."

go_bin=${GO:-go}
base=$(git rev-parse --verify --end-of-options "${BASE:-HEAD}^{commit}")
package=${BENCH_PKG:-./internal/engine}
filter=${BENCH_FILTER:-.}
count=${BENCH_COUNT:-3}
duration=${BENCH_TIME:-1s}
cpu=${BENCH_CPU:-2}
if [[ ! "$count" =~ ^[1-9][0-9]*$ || ! "$cpu" =~ ^[1-9][0-9]*$ ]]; then
    echo 'BENCH_COUNT and BENCH_CPU must be positive integers.' >&2
    exit 2
fi

root="$PWD"
scratch=$(mktemp -d "${TMPDIR:-/tmp}/tinysql-bench-build.XXXXXX")
trap 'rm -rf "$scratch"' EXIT
results=$(mktemp -d "${TMPDIR:-/tmp}/tinysql-bench-results.XXXXXX")
echo "Benchmark results: $results"
mkdir "$scratch/base"
git archive --format=tar "$base" | tar -xf - -C "$scratch/base"

baseline_dir="$scratch/base/$package"
working_dir="$root/$package"
{
    printf 'baseline: %s\nworking tree HEAD: %s\n' "$base" "$(git rev-parse HEAD)"
    printf 'package: %s\nfilter: %s\nsamples: %s\ntime: %s\nGOMAXPROCS: %s\n' "$package" "$filter" "$count" "$duration" "$cpu"
    printf 'baseline Go: %s\nworking tree Go: %s\n' \
        "$(cd "$baseline_dir" && "$go_bin" version)" "$(cd "$working_dir" && "$go_bin" version)"
    git status --short
} > "$results/config.txt"

(cd "$baseline_dir" && "$go_bin" test -c -o "$scratch/before.test" .)
(cd "$working_dir" && "$go_bin" test -c -o "$scratch/after.test" .)

measure() {
    local version=$1 directory=$2
    (cd "$directory" && "$scratch/$version.test" -test.run='^$' -test.bench="$filter" \
        -test.benchmem -test.count=1 -test.benchtime="$duration" -test.cpu="$cpu") \
        | tee -a "$results/$version.txt"
}

for ((sample = 1; sample <= count; sample++)); do
    printf '\nSample %d/%d\n' "$sample" "$count"
    if ((sample % 2)); then
        measure before "$baseline_dir"
        measure after "$working_dir"
    else
        measure after "$working_dir"
        measure before "$baseline_dir"
    fi
done

for version in before after; do
    awk '/^Benchmark[^ ]+[[:space:]]+[0-9]+[[:space:]]/ { print $1 }' "$results/$version.txt" \
        | LC_ALL=C sort -u > "$scratch/$version.names"
    if [[ ! -s "$scratch/$version.names" ]]; then
        printf 'No benchmarks matched %s in %s.\n' "$filter" "$version" >&2
        exit 1
    fi
done

if ! diff -u "$scratch/before.names" "$scratch/after.names"; then
    echo 'The benchmark sets differ. Compare only benchmarks present in both runs.' >&2
fi

if command -v benchstat >/dev/null; then
    (cd "$results" && benchstat before.txt after.txt) | tee "$results/comparison.txt"
else
    printf 'Compare the captures with: benchstat %q %q\n' "$results/before.txt" "$results/after.txt"
fi
