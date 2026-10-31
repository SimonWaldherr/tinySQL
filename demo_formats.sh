#!/usr/bin/env bash
# Compatibility entry point; the implementation lives with the other scripts.
set -euo pipefail
exec bash "$(dirname -- "${BASH_SOURCE[0]}")/scripts/demo-formats.sh" "$@"
