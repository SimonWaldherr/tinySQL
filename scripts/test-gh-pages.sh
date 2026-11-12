#!/usr/bin/env bash
# Exercise publishing guards with tiny assets and isolated local repositories.
set -euo pipefail
ROOT="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." >/dev/null && pwd -P)"
TMP="$(mktemp -d "${TMPDIR:-/tmp}/tinysql-pages-test.XXXXXX")"
trap 'rm -rf "$TMP"' EXIT
REPO="$TMP/source"
WORKTREE="$TMP/pages"
mkdir -p "$REPO/scripts" "$REPO/cmd/query_files_wasm"
cp "$ROOT/scripts/gh-pages.sh" "$REPO/scripts/"
cp "$ROOT/cmd/query_files_wasm/pages-assets.txt" "$REPO/cmd/query_files_wasm/"
ASSETS="$REPO/cmd/query_files_wasm"
while IFS= read -r file; do
    case "$file" in
        *.gz) continue ;;
        *.wasm) printf '\000asm\001\000\000\000' > "$ASSETS/$file" ;;
        *.html) printf '<!DOCTYPE html><html lang="en"><head><title>Test</title></head><body>Demo</body></html>\n' > "$ASSETS/$file" ;;
        *.js) printf 'const example = true;\n' > "$ASSETS/$file" ;;
        *) printf 'fixture\n' > "$ASSETS/$file" ;;
    esac
done < "$ASSETS/pages-assets.txt"
gzip -n -c "$ASSETS/query_files.wasm" > "$ASSETS/query_files.wasm.gz"
git init -q -b main "$REPO"
git -C "$REPO" config user.name 'Pages test'
git -C "$REPO" config user.email pages-test@example.invalid
git -C "$REPO" add .
git -C "$REPO" commit -qm source
git -C "$REPO" checkout -q --orphan gh-pages
git -C "$REPO" rm -qrf .
printf 'keep the separate dataset\n' > "$REPO/bavaria-snapshot.b64"
git -C "$REPO" add .
git -C "$REPO" commit -qm dataset
git -C "$REPO" checkout -q main
git clone -q --bare "$REPO" "$TMP/remote.git"
git -C "$REPO" remote add origin "$TMP/remote.git"
PAGES="$REPO/scripts/gh-pages.sh"

expect_failure() {
    if "$@" > "$TMP/failure.log" 2>&1; then
        echo "expected failure: $*" >&2
        exit 1
    fi
}

# The Pages build must generate both demo assets before validating them.
cat > "$TMP/fake-go" <<'GO'
#!/usr/bin/env bash
set -euo pipefail
[ "$*" = 'run -tags=sqliteimport ./cmd/mbtilesdemo' ]
echo generated >> "$PAGES_TEST_LOG"
[ ! -e "$PAGES_TEST_FAIL" ]
GO
cat > "$TMP/fake-wasm-build" <<'WASM'
#!/usr/bin/env bash
set -euo pipefail
[ "$*" = '--if-needed --build-only' ]
echo wasm >> "$PAGES_TEST_LOG"
WASM
cat > "$TMP/fake-pages" <<'PAGES'
#!/usr/bin/env bash
set -euo pipefail
[ "$*" = check ]
echo checked >> "$PAGES_TEST_LOG"
PAGES
chmod +x "$TMP/fake-go" "$TMP/fake-wasm-build" "$TMP/fake-pages"
export PAGES_TEST_LOG="$TMP/pages-build.log" PAGES_TEST_FAIL="$TMP/fail-generator"
pages_build() {
    make -s -C "$ROOT" build-gh-pages-demo GO="$TMP/fake-go" \
        QUERY_FILES_WASM_SCRIPT="$TMP/fake-wasm-build" GH_PAGES_SCRIPT="$TMP/fake-pages" > /dev/null
}
pages_build
printf 'wasm\ngenerated\nchecked\n' > "$TMP/expected-build.log"
cmp "$TMP/expected-build.log" "$PAGES_TEST_LOG"
: > "$PAGES_TEST_LOG"
touch "$PAGES_TEST_FAIL"
expect_failure pages_build
printf 'wasm\ngenerated\n' > "$TMP/expected-build.log"
cmp "$TMP/expected-build.log" "$PAGES_TEST_LOG"

"$PAGES" stage "$TMP/staged"
[ -f "$TMP/staged/.nojekyll" ]
printf 'protected\n' > "$TMP/protected"
mkdir "$TMP/symlinked"
ln -s "$TMP/protected" "$TMP/symlinked/index.html"
expect_failure "$PAGES" stage "$TMP/symlinked"
[ "$(cat "$TMP/protected")" = protected ]
printf 'invalid javascript (\n' > "$ASSETS/tiles-demo-bavaria.js"
expect_failure "$PAGES" check
git -C "$REPO" restore cmd/query_files_wasm/tiles-demo-bavaria.js
if command -v tidy >/dev/null 2>&1; then
    printf '<!DOCTYPE html><html><head><title>Test</title></head><body><not-a-real-element></body></html>' > "$ASSETS/tiles-demo-bavaria.html"
    expect_failure "$PAGES" check
    git -C "$REPO" restore cmd/query_files_wasm/tiles-demo-bavaria.html
fi
printf 'different\n' | gzip -n -c > "$ASSETS/query_files.wasm.gz"
expect_failure "$PAGES" check
git -C "$REPO" restore cmd/query_files_wasm/query_files.wasm.gz

"$PAGES" update "$WORKTREE" gh-pages 'Update demo' > "$TMP/update.log" 2>&1
[ "$(cat "$WORKTREE/bavaria-snapshot.b64")" = 'keep the separate dataset' ]
head="$(git -C "$WORKTREE" rev-parse HEAD)"
"$PAGES" update "$WORKTREE" gh-pages 'Unnecessary commit' >> "$TMP/update.log" 2>&1
[ "$(git -C "$WORKTREE" rev-parse HEAD)" = "$head" ]
printf 'uncommitted\n' >> "$WORKTREE/index.html"
expect_failure "$PAGES" update "$WORKTREE" gh-pages 'Overwrite dirty files'
grep -q uncommitted "$WORKTREE/index.html"
git -C "$WORKTREE" restore index.html

git init -q -b gh-pages "$TMP/unrelated"
printf 'unrelated\n' > "$TMP/unrelated/index.html"
expect_failure "$PAGES" update "$TMP/unrelated" gh-pages 'Wrong repository'
[ "$(cat "$TMP/unrelated/index.html")" = unrelated ]

# A remote-only dataset update must be incorporated before generating a commit.
git -C "$WORKTREE" push -q origin gh-pages
git clone -q --branch gh-pages "$TMP/remote.git" "$TMP/writer"
git -C "$TMP/writer" config user.name 'Dataset writer'
git -C "$TMP/writer" config user.email dataset-test@example.invalid
printf 'new dataset\n' > "$TMP/writer/bavaria-snapshot.b64"
git -C "$TMP/writer" commit -qam 'Refresh dataset'
git -C "$TMP/writer" push -q origin gh-pages
"$PAGES" update "$WORKTREE" gh-pages 'Update after fetch' >> "$TMP/update.log" 2>&1
[ "$(cat "$WORKTREE/bavaria-snapshot.b64")" = 'new dataset' ]

# Conflicting histories remain intact instead of being reset or auto-rebased.
printf 'local\n' > "$WORKTREE/local.txt"
git -C "$WORKTREE" add local.txt
git -C "$WORKTREE" commit -qm local
head="$(git -C "$WORKTREE" rev-parse HEAD)"
printf 'remote\n' > "$TMP/writer/remote.txt"
git -C "$TMP/writer" add remote.txt
git -C "$TMP/writer" commit -qm remote
git -C "$TMP/writer" push -q origin gh-pages
expect_failure "$PAGES" update "$WORKTREE" gh-pages 'Diverged histories'
[ "$(git -C "$WORKTREE" rev-parse HEAD)" = "$head" ]
[ -z "$(git -C "$WORKTREE" status --porcelain)" ]
echo 'gh-pages staging and worktree checks passed'
