#!/usr/bin/env bash
set -euo pipefail

EXP_NAME="_extract_hrrr_vars_at_gauges"
SRC="/playpen-hdd/levi/UNITES-backup-10-29-25/tianlong-chen-lab/nws-lv-precip-forecasting/ccrfcd-gauge-grids/data"
DST="data"

python scripts/add_hrr_env_params_v2.py > "$EXP_NAME.out" 2>&1 &

# ---------------------------------------
exit