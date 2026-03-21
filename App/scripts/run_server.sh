#!/usr/bin/env bash
set -euo pipefail

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
PROJECT_ROOT="$(cd "${SCRIPT_DIR}/../.." && pwd)"

if [[ -x "${PROJECT_ROOT}/.venv/bin/python" ]]; then
  PYTHON_BIN="${PROJECT_ROOT}/.venv/bin/python"
else
  PYTHON_BIN="python3"
fi

cd "${PROJECT_ROOT}"
exec "${PYTHON_BIN}" -m uvicorn App.main:app --host 127.0.0.1 --port "${SPEC_EXTRACTION_WEB_PORT:-8010}"
