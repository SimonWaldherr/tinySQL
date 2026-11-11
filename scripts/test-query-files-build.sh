#!/usr/bin/env bash
# Check incremental/failed builds without compiling the full WASM engine.
set -euo pipefail
ROOT="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." >/dev/null && pwd -P)"
TMP="$(mktemp -d "${TMPDIR:-/tmp}/tinysql-wasm-build-test.XXXXXX")"
trap 'rm -rf "$TMP"' EXIT
DEMO="$TMP/repo/cmd/query_files_wasm"
mkdir -p "$DEMO" "$TMP/bin" "$TMP/goroot/lib/wasm"
cp "$ROOT/cmd/query_files_wasm/build.sh" "$DEMO/"
printf '\000asm\001\000\000\000' > "$TMP/module.wasm"
printf '// Go runtime fixture\n' > "$TMP/goroot/lib/wasm/wasm_exec.js"
printf 'initial build ID\n' > "$TMP/build-id"
printf 'go version go-test js/wasm\n' > "$TMP/go-version"
printf 'optimizer v1\n' > "$TMP/optimizer-version"
export WASM_BUILD_TEST_DIR="$TMP"
export WASM_BUILD_TEST_GZIP="$(command -v gzip)"
cat > "$TMP/bin/go" <<'GO'
#!/usr/bin/env bash
set -euo pipefail
case "$1" in
    version) cat "$WASM_BUILD_TEST_DIR/go-version" ;;
    env) printf '%s/goroot\n' "$WASM_BUILD_TEST_DIR" ;;
    list) [ ! -e "$WASM_BUILD_TEST_DIR/fail-list" ]; cat "$WASM_BUILD_TEST_DIR/build-id" ;;
    build)
        echo build >> "$WASM_BUILD_TEST_DIR/builds"
        while [ "$#" -gt 0 ]; do
            if [ "$1" = -o ]; then cp "$WASM_BUILD_TEST_DIR/module.wasm" "$2"; exit; fi
            shift
        done
        exit 1 ;;
    *) exit 2 ;;
esac
GO
cat > "$TMP/bin/wasm-opt" <<'OPT'
#!/usr/bin/env bash
if [ "$1" = --version ]; then cat "$WASM_BUILD_TEST_DIR/optimizer-version"; exit; fi
# An optional optimizer failure must preserve the valid Go output.
exit 1
OPT
cat > "$TMP/bin/gzip" <<'GZIP'
#!/usr/bin/env bash
if [ -e "$WASM_BUILD_TEST_DIR/fail-gzip" ]; then exit 1; fi
exec "$WASM_BUILD_TEST_GZIP" "$@"
GZIP
chmod +x "$TMP/bin/"*
export PATH="$TMP/bin:$PATH"
run_build() { "$DEMO/build.sh" --if-needed --build-only >> "$TMP/build.log" 2>&1; }
builds() { wc -l < "$TMP/builds" | tr -d ' '; }
unchanged() { cmp -s "$TMP/saved.wasm" "$DEMO/query_files.wasm" && cmp -s "$TMP/saved.gz" "$DEMO/query_files.wasm.gz" && cmp -s "$TMP/saved.fingerprint" "$DEMO/.build-fingerprint"; }

run_build
[ "$(builds)" = 1 ]
run_build
[ "$(builds)" = 1 ]
grep -q 'skipping rebuild' "$TMP/build.log"
cp "$DEMO/query_files.wasm.gz" "$TMP/deterministic.gz"
printf 'changed build ID\n' > "$TMP/build-id"
touch -t 200001010000 "$TMP/build-id"
run_build
[ "$(builds)" = 2 ]
cmp -s "$TMP/deterministic.gz" "$DEMO/query_files.wasm.gz"
printf '// runtime changed\n' >> "$TMP/goroot/lib/wasm/wasm_exec.js"
run_build
[ "$(builds)" = 3 ]
printf 'optimizer v2\n' > "$TMP/optimizer-version"
run_build
[ "$(builds)" = 4 ]
GOFLAGS='-gcflags=all=-l' run_build
[ "$(builds)" = 5 ]
GOFLAGS='-gcflags=all=-l' run_build
[ "$(builds)" = 5 ]
printf 'go version go-test-next js/wasm\n' > "$TMP/go-version"
GOFLAGS='-gcflags=all=-l' run_build
[ "$(builds)" = 6 ]
printf 'corrupted output\n' > "$DEMO/query_files.wasm"
GOFLAGS='-gcflags=all=-l' run_build
[ "$(builds)" = 7 ]

cp "$DEMO/query_files.wasm" "$TMP/saved.wasm"
cp "$DEMO/query_files.wasm.gz" "$TMP/saved.gz"
cp "$DEMO/.build-fingerprint" "$TMP/saved.fingerprint"
touch "$TMP/fail-list"
if run_build; then echo 'go list failure was ignored' >&2; exit 1; fi
unchanged
rm "$TMP/fail-list"
printf 'another build ID\n' > "$TMP/build-id"
touch "$TMP/fail-gzip"
if run_build; then echo 'gzip failure was ignored' >&2; exit 1; fi
unchanged
rm "$TMP/fail-gzip"
run_build
[ "$(builds)" = 9 ]
"$DEMO/build.sh" --clean > /dev/null
[ ! -e "$DEMO/.build-fingerprint" ] && [ ! -e "$DEMO/query_files.wasm" ]
echo 'query-files build cache and atomic output checks passed'
