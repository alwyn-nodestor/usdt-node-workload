#!/usr/bin/env bash
# blockline-usdt one-command setup.  Clone the repo and run:
#
#     git clone <repo> blockline-usdt && cd blockline-usdt && sudo ./setup.sh
#
# What it does, in order:
#   1. bootstrap    packages, usdt user, data root, systemd units (needs root)
#   2. venv         Python venv with pyarrow and pycryptodome for the ETL
#   3. install      JDK, java-tron, ClickHouse binary (pinned by sha256)
#   4. up           bring the full stack up: ClickHouse → snapshot → node → sync → ETL → dashboard
#
# Idempotent: re-running after an interruption continues where it left off.
# The snapshot (~37 h) and the node run under systemd, so this script can be stopped and restarted safely.
set -euo pipefail
here=$(cd "$(dirname "$0")" && pwd)
# shellcheck source=lib/compat.sh
. "$here/lib/compat.sh"

# ── must run as root (the bootstrap needs it; later steps drop to usdt) ───────
[ "$(id -u)" -eq 0 ] || { echo "run with sudo:  sudo $0 $*"; exit 1; }

# ── 1. bootstrap ──────────────────────────────────────────────────────────────
echo; echo "===== 1/4 bootstrap (packages, user, systemd units) ====="
bash "$here/bootstrap/usdt-bootstrap.sh"

# After the bootstrap, ~usdt/blockline-usdt exists as a clone.
# Use that path for everything below so the usdt user owns the working tree.
REPO=$(eval echo ~usdt)/blockline-usdt

# ── 2. Python venv for the ETL ────────────────────────────────────────────────
echo; echo "===== 2/4 Python venv ====="
if [ -x "$REPO/.venv/bin/python" ]; then
  echo "   venv already exists at $REPO/.venv"
else
  # install uv if not present (single-binary Python package manager)
  if ! run_as usdt bash -c 'command -v uv >/dev/null 2>&1 || [ -x ~/.local/bin/uv ]'; then
    echo "   installing uv..."
    run_as usdt bash -c 'curl -LsSf https://astral.sh/uv/install.sh | sh' || {
      echo "   uv install failed; install it manually: https://docs.astral.sh/uv/getting-started/installation/"
      exit 1
    }
  fi
  echo "   creating venv and installing dependencies..."
  run_as usdt bash -c 'export PATH="$HOME/.local/bin:$PATH"; cd '"$REPO"' && uv venv --python /usr/bin/python3 .venv && uv pip install --python .venv/bin/python pyarrow==25.0.1 pycryptodome==3.23.0'
  echo "   venv ready at $REPO/.venv"
fi

# ── 3. install runtime (JDK, java-tron, ClickHouse) ──────────────────────────
echo; echo "===== 3/4 runtime install (JDK, java-tron, ClickHouse) ====="
run_as usdt "$REPO/bin/blockline-usdt" install

# ── 4. bring the stack up ────────────────────────────────────────────────────
echo; echo "===== 4/4 bringing the stack up ====="
run_as usdt "$REPO/bin/usdt-up" "$@"
