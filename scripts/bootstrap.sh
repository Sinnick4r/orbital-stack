#!/usr/bin/env bash
#
# orbital-stack bootstrap — reproducible setup on a clean machine.
# Supports Ubuntu 24.x and recent macOS. No Docker (see ADR-002).
#
# Usage:
#   bash scripts/bootstrap.sh
#

set -euo pipefail

log()  { printf '\033[1;34m[..]\033[0m %s\n' "$*"; }
ok()   { printf '\033[1;32m[ok]\033[0m %s\n' "$*"; }
fail() { printf '\033[1;31m[!!]\033[0m %s\n' "$*" >&2; exit 1; }

detect_os() {
    case "$(uname -s)" in
        Linux*)  echo "linux"  ;;
        Darwin*) echo "macos"  ;;
        *)       fail "Unsupported OS: $(uname -s). Use Ubuntu 24.x or macOS." ;;
    esac
}

ensure_uv() {
    if command -v uv >/dev/null 2>&1; then
        ok "uv already installed: $(uv --version)"
        return
    fi
    log "installing uv from astral.sh"
    curl -LsSf https://astral.sh/uv/install.sh | sh
    # Make uv available in the current shell session.
    export PATH="${HOME}/.local/bin:${HOME}/.cargo/bin:${PATH}"
    command -v uv >/dev/null 2>&1 || fail "uv installed but not on PATH; open a new shell and re-run."
    ok "uv installed: $(uv --version)"
}

sync_deps() {
    log "syncing runtime + dev dependencies (this creates .venv)"
    uv sync --all-extras
    ok "dependencies synced"
}

install_hooks() {
    log "installing pre-commit hooks"
    uv run pre-commit install
    uv run pre-commit install --hook-type commit-msg
    ok "pre-commit hooks installed"
}

run_tests() {
    log "running tests to verify the setup"
    if ! make test; then
        fail "tests failed — review output above"
    fi
    ok "tests passed"
}

main() {
    local os
    os="$(detect_os)"
    log "orbital-stack bootstrap on ${os}"

    ensure_uv
    sync_deps
    install_hooks
    run_tests

    cat <<'EOF'

------------------------------------------------------------------
[ok] bootstrap complete.

Next steps:
  1. Copy .env.example to .env and fill in any required secrets
     (Space-Track credentials, Backblaze B2 keys, etc.).
  2. Run `make lint typecheck` and confirm your editor uses the
     project's .venv for ruff / mypy / pytest.
  3. Run `make scrape` to pull a local UNOOSA snapshot.
  4. Run `make pipeline` to execute the weekly ingest flow locally.
  5. See docs/PLAN.md for the full workflow and docs/adrs/ for
     architecture decisions.
------------------------------------------------------------------
EOF
}

main "$@"
