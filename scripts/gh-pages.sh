#!/usr/bin/env bash
# Validate, stage or update the static demo without touching unrelated assets.
set -euo pipefail

REPO_ROOT="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." >/dev/null && pwd -P)"
SOURCE_DIR="${GH_PAGES_SOURCE_DIR:-$REPO_ROOT/cmd/query_files_wasm}"
MANIFEST="${GH_PAGES_MANIFEST:-$REPO_ROOT/cmd/query_files_wasm/pages-assets.txt}"
REMOTE="${GH_PAGES_REMOTE:-origin}"

die() { echo "gh-pages: $*" >&2; exit 1; }

ASSETS=()
while IFS= read -r asset || [ -n "$asset" ]; do
    case "$asset" in ''|'#'*) continue ;; esac
    case "$asset" in *..*|*[!a-zA-Z0-9._-]*) die "invalid asset filename: $asset" ;; esac
    ASSETS+=("$asset")
done < "$MANIFEST"
[ "${#ASSETS[@]}" -gt 0 ] || die "empty asset manifest: $MANIFEST"

check_assets() {
    local directory="$1" asset output
    for asset in "${ASSETS[@]}"; do
        [ -f "$directory/$asset" ] && [ -s "$directory/$asset" ] && [ ! -L "$directory/$asset" ] || die "missing, empty or symlinked asset: $directory/$asset"
    done
    if command -v gzip >/dev/null 2>&1; then
        gzip -t "$directory/query_files.wasm.gz"
        gzip -cd "$directory/query_files.wasm.gz" | cmp -s "$directory/query_files.wasm" - || die "compressed WASM does not match query_files.wasm"
    fi
    if command -v node >/dev/null 2>&1; then
        for asset in "${ASSETS[@]}"; do
            case "$asset" in *.js) node --check "$directory/$asset" ;; esac
        done
        node -e 'if (!WebAssembly.validate(require("fs").readFileSync(process.argv[1]))) process.exit(1)' "$directory/query_files.wasm"
    else
        echo "gh-pages: Node unavailable; skipping JavaScript/WASM validation" >&2
    fi
    if command -v tidy >/dev/null 2>&1; then
        output="$(mktemp "${TMPDIR:-/tmp}/tinysql-html.XXXXXX")"
        for asset in "${ASSETS[@]}"; do
            case "$asset" in *.html)
                tidy -q -e "$directory/$asset" > "$output" 2>&1 || true
                if grep -qi 'Error:' "$output"; then cat "$output" >&2; rm -f "$output"; return 1; fi
            ;; esac
        done
        rm -f "$output"
    elif command -v xmllint >/dev/null 2>&1; then
        for asset in "${ASSETS[@]}"; do
            case "$asset" in *.html) xmllint --html --noout "$directory/$asset" 2>/dev/null ;; esac
        done
    else
        echo "gh-pages: HTML validator unavailable; skipping HTML validation" >&2
    fi
}

copy_assets() {
    local destination="$1" asset
    [ ! -L "$destination" ] || die "refusing symlinked destination: $destination"
    mkdir -p "$destination"
    for asset in "${ASSETS[@]}"; do
        [ ! -L "$destination/$asset" ] || die "refusing to overwrite symlink: $destination/$asset"
        mkdir -p "$(dirname "$destination/$asset")"
        if ! cmp -s "$SOURCE_DIR/$asset" "$destination/$asset"; then
            cp "$SOURCE_DIR/$asset" "$destination/$asset"
        fi
    done
    [ ! -L "$destination/.nojekyll" ] || die "refusing to overwrite symlink: $destination/.nojekyll"
    touch "$destination/.nojekyll"
}

common_dir() {
    local directory
    directory="$(git -C "$1" rev-parse --git-common-dir)" || return 1
    (cd "$1" >/dev/null && cd "$directory" >/dev/null && pwd -P)
}

update_branch() {
    local worktree="$1" branch="$2" message="$3" repository head remote_ref
    case "$worktree" in /*) ;; *) worktree="$PWD/$worktree" ;; esac
    check_assets "$SOURCE_DIR"
    repository="$(common_dir "$REPO_ROOT")"
    git check-ref-format --branch "$branch" >/dev/null || die "invalid branch: $branch"
    if [ -e "$worktree" ]; then
        [ "$(common_dir "$worktree")" = "$repository" ] || die "$worktree belongs to a different repository"
        [ "$(git -C "$worktree" rev-parse --show-toplevel)" = "$(cd "$worktree" >/dev/null && pwd -P)" ] || die "$worktree is not a worktree root"
        [ "$(cd "$worktree" >/dev/null && pwd -P)" != "$REPO_ROOT" ] || die "use a separate gh-pages worktree"
        [ "$(git -C "$worktree" branch --show-current)" = "$branch" ] || die "$worktree is not on branch $branch"
        [ -z "$(git -C "$worktree" status --porcelain)" ] || die "$worktree has uncommitted changes"
    fi
    # Refresh only the requested remote branch. Do not reset or rebase local
    # commits; a diverged branch needs an explicit decision from its owner.
    remote_ref="refs/remotes/$REMOTE/$branch"
    git -C "$REPO_ROOT" fetch --no-tags "$REMOTE" "refs/heads/$branch:$remote_ref"
    if [ ! -e "$worktree" ]; then
        if git -C "$REPO_ROOT" show-ref --verify --quiet "refs/heads/$branch"; then
            git -C "$REPO_ROOT" worktree add "$worktree" "$branch"
        else
            git -C "$REPO_ROOT" worktree add -b "$branch" "$worktree" "$remote_ref"
        fi
    fi
    head="$(git -C "$worktree" rev-parse HEAD)"
    if git -C "$worktree" merge-base --is-ancestor "$head" "$remote_ref"; then
        git -C "$worktree" merge --ff-only "$remote_ref"
    elif ! git -C "$worktree" merge-base --is-ancestor "$remote_ref" "$head"; then
        die "$branch has diverged from $REMOTE/$branch; refusing to overwrite local commits"
    fi
    copy_assets "$worktree"
    git -C "$worktree" add -- .nojekyll "${ASSETS[@]}"
    if git -C "$worktree" diff --cached --quiet; then
        echo "gh-pages: $branch already up to date"
    else
        git -C "$worktree" commit -m "$message"
    fi
}

case "${1:-}" in
    check) [ "$#" -le 2 ] || die "usage: $0 check [directory]"; check_assets "${2:-$SOURCE_DIR}" ;;
    stage) [ "$#" -eq 2 ] || die "usage: $0 stage directory"; check_assets "$SOURCE_DIR"; copy_assets "$2"; check_assets "$2" ;;
    update) [ "$#" -eq 4 ] || die "usage: $0 update worktree branch message"; update_branch "$2" "$3" "$4" ;;
    *) die "usage: $0 {check [directory]|stage directory|update worktree branch message}" ;;
esac
