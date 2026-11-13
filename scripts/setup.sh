#!/usr/bin/env bash
# One-shot dev setup. Safe to re-run.
set -euo pipefail
cd "$(dirname "$0")/.."

if [ ! -f vendor/aiomoqt/pyproject.toml ]; then
  echo "vendor/aiomoqt is empty -- run: git submodule update --init --recursive"
  exit 1
fi

# The default coder (ISA-L) needs the system library.
if ! ldconfig -p 2>/dev/null | grep -q libisal; then
  echo "ISA-L not found: sudo apt install libisal-dev"
  exit 1
fi

python3 -m venv .venv
. .venv/bin/activate
pip install -q --upgrade pip

# Editable so edits under vendor/aiomoqt take effect without reinstalling.
# aiopquic, certifi and sortedcontainers come from PyPI as its dependencies.
pip install -e "./vendor/aiomoqt[test,examples]"
pip install -e ".[dev]"

echo
echo "Ready. Activate with: . .venv/bin/activate"
echo "Smoke test (no relay needed):"
echo "  python -m aiomoqt.tools.loopback_bench -s 4096 -P 4 -t 20"
