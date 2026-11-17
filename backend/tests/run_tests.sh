#!/usr/bin/env bash
# 费用批次补传引擎全量测试（零第三方依赖，仅需 Python 3.11+ 与 SQLite）。
set -euo pipefail
cd "$(dirname "$0")/.."
python3 -m unittest discover -t . -s tests -v "$@"
