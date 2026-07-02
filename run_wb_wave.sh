#!/usr/bin/env bash
set -euo pipefail

cd "$(dirname "$0")"

MODE="${1:-prod}"
ARTICLES="979947681,964018818,918342224"
SMOKE_DIR="logs"
MIN_LIVE_BUNDLES=3
BUNDLES=(01 02 03 04 05 06 07)
LIVE_BUNDLES=()

if [[ -x ".venv/bin/python" ]]; then
  PYTHON_BIN=".venv/bin/python"
else
  PYTHON_BIN="${PYTHON_BIN:-python}"
fi

if ! command -v "$PYTHON_BIN" >/dev/null 2>&1; then
  echo "Python interpreter not found: $PYTHON_BIN" >&2
  exit 1
fi

if [[ ! -f ".env" ]]; then
  echo ".env file not found in project root" >&2
  exit 1
fi

mkdir -p "$SMOKE_DIR"

load_bundle_env() {
  local bundle_index="$1"
  eval "$(
    "$PYTHON_BIN" - "$bundle_index" <<'PY'
from __future__ import annotations

import shlex
import sys
from dotenv import dotenv_values

bundle_index = sys.argv[1]
data = dotenv_values(".env")

proxy_url = str(data.get(f"WB_PROXY_{bundle_index}_URL", "") or "")
cookie = str(data.get(f"WB_PROXY_{bundle_index}_COOKIE", "") or "")
device_id = str(data.get(f"WB_PROXY_{bundle_index}_DEVICE_ID", "") or "")

print(f"export WB_PROXY_URL={shlex.quote(proxy_url)}")
print(f"export WB_COOKIE={shlex.quote(cookie)}")
print(f"export WB_DEVICE_ID={shlex.quote(device_id)}")
print("export WB_AUTHORIZATION=''")
PY
  )"
}

export_live_bundles() {
  eval "$(
    "$PYTHON_BIN" - "${LIVE_BUNDLES[@]}" <<'PY'
from __future__ import annotations

import shlex
import sys
from dotenv import dotenv_values

selected = list(sys.argv[1:])
data = dotenv_values(".env")

for idx in range(1, 100):
    slot = f"{idx:02d}"
    print(f"unset WB_PROXY_{slot}_URL")
    print(f"unset WB_PROXY_{slot}_COOKIE")
    print(f"unset WB_PROXY_{slot}_DEVICE_ID")

for new_pos, old_slot in enumerate(selected, start=1):
    new_slot = f"{new_pos:02d}"
    proxy_url = str(data.get(f"WB_PROXY_{old_slot}_URL", "") or "")
    cookie = str(data.get(f"WB_PROXY_{old_slot}_COOKIE", "") or "")
    device_id = str(data.get(f"WB_PROXY_{old_slot}_DEVICE_ID", "") or "")
    print(f"export WB_PROXY_{new_slot}_URL={shlex.quote(proxy_url)}")
    print(f"export WB_PROXY_{new_slot}_COOKIE={shlex.quote(cookie)}")
    print(f"export WB_PROXY_{new_slot}_DEVICE_ID={shlex.quote(device_id)}")
PY
  )"
}

echo "[1/4] Smoke check for proxy bundles"
for bundle in "${BUNDLES[@]}"; do
  echo "===== proxy_${bundle} ====="
  load_bundle_env "$bundle"
  "$PYTHON_BIN" scripts/poc_wb_internal_detail.py \
    --articles "$ARTICLES" \
    --limit 3 \
    --profiles K \
    --endpoints u_card_v4 \
    --verbose \
    > "${SMOKE_DIR}/poc_bundle_${bundle}_u_card_smoke_fresh.txt"
done

echo
echo "[2/4] Smoke summaries"
for bundle in "${BUNDLES[@]}"; do
  echo "===== proxy_${bundle} ====="
  cat "${SMOKE_DIR}/poc_bundle_${bundle}_u_card_smoke_fresh.txt"
done

echo
echo "[3/4] Selecting live bundles"
for bundle in "${BUNDLES[@]}"; do
  if grep -Fq "| K       | u_card_v4/detail_params | 3        | 3         | 0         | 0         |" \
    "${SMOKE_DIR}/poc_bundle_${bundle}_u_card_smoke_fresh.txt"; then
    LIVE_BUNDLES+=("$bundle")
    echo "proxy_${bundle} = LIVE"
  else
    echo "proxy_${bundle} = DEAD"
  fi
done

if [[ "${#LIVE_BUNDLES[@]}" -eq 0 ]]; then
  echo
  echo "Smoke failed: no live bundles found. Production wave cancelled."
  exit 1
fi

echo "Live bundles: ${LIVE_BUNDLES[*]}"
echo "Live bundle count: ${#LIVE_BUNDLES[@]}"

if [[ "${#LIVE_BUNDLES[@]}" -lt "$MIN_LIVE_BUNDLES" ]]; then
  echo
  echo "Smoke failed: live bundle count is below minimum ${MIN_LIVE_BUNDLES}. Production wave cancelled."
  exit 1
fi

if [[ "${MODE}" == "smoke" ]]; then
  echo
  echo "Smoke finished successfully. Production wave skipped by mode=smoke."
  exit 0
fi

echo
echo "[4/4] Starting production wave"
export_live_bundles

export WB_AUTHORIZATION=""
export WB_PROXY_URL=""
export WB_COOKIE=""
export WB_DEVICE_ID=""
export WB_DETAIL_ENDPOINT_MODE="u_card_v4"
export WB_SKIP_SIMILAR_STAGE="True"
export WB_ALLOW_MISSING_PRICE="True"
export WB_ALLOW_MISSING_PRODUCT="True"
export WB_COOKIE_ENABLED="True"
export WB_PROXY_BUNDLES_ENABLED="True"
export WB_SESSION_ROTATION_ENABLED="True"
export WB_SESSION_ROTATE_EVERY="50"
export WB_SESSION_ROTATION_SCOPE="detail"
export CONCURRENT_REQUESTS_LIMIT="2"
export WB_DETAIL_SUBMIT_DELAY="0.5"
export CLICKHOUSE_WRITE_ENABLED="True"
export WB_PROXY_BUNDLE_POOL="$(IFS=,; echo "${LIVE_BUNDLES[*]}")"
export WB_BATCH_FORBIDDEN_STOP_LOSS_ENABLED="True"
export WB_BATCH_FORBIDDEN_STOP_LOSS_RATIO="0.35"
export WB_BATCH_FORBIDDEN_STOP_LOSS_MIN_BATCH_SIZE="20"
export WB_ALL_BUNDLES_498_COOLDOWN_ENABLED="True"
export WB_ALL_BUNDLES_498_COOLDOWN_SECONDS="300"
export CHECKPOINT_FILE="processing_checkpoint_full_fresh_7proxy.json"
unset WB_ROLLOUT_ARTICLES_LIMIT

"$PYTHON_BIN" -m src.main
