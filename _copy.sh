#!/usr/bin/env bash
set -euo pipefail

SRC="/playpen-hdd/levi/UNITES-backup-10-29-25/tianlong-chen-lab/nws-lv-precip-forecasting/ccrfcd-gauge-grids/data"
DST="data"

rsync -a --progress "$SRC"/ "$DST"/