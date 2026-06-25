#!/usr/bin/env bash
# Full-year pipeline runner. Run from the repo root. On macOS, wrap in caffeinate
# so the machine can't sleep mid-run:
#   caffeinate -dimsu bash run_pipeline.sh
# Outputs are idempotent (mode=overwrite), so if a step fails you can re-run from there.
set -euo pipefail

# Resolve repo root from this script's location, then load .env (for PROCESSED_DATA_DIR).
ROOT="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
cd "$ROOT"
[ -f .env ] && set -a && . ./.env && set +a

LOG() { echo "=== [$(date '+%m-%d %H:%M:%S')] $* ==="; }
SUB="docker compose exec -T spark-master /opt/spark/bin/spark-submit --master spark://spark-master:7077"
# Tolerate transient external-disk slowness instead of declaring the executor dead.
CONF="--conf spark.network.timeout=1200s --conf spark.executor.heartbeatInterval=60s"

step() {
  local name="$1"; shift
  LOG "START $name"
  $SUB $CONF "$@"
  local rc=$?
  LOG "END $name rc=$rc"
  if [ "$rc" -ne 0 ]; then LOG "PIPELINE ABORTED at $name (rc=$rc)"; exit "$rc"; fi
}

# Clear any leftover Spark spill from a previous run.
rm -rf "${PROCESSED_DATA_DIR:?set PROCESSED_DATA_DIR in .env}/_spill"/* 2>/dev/null || true
LOG "PIPELINE START (full-year)"

step 01_load      /workspace/src/01_load_ais.py --skip-dedup --executor-memory 28g --executor-cores 12
step 02_aggregate /workspace/src/02_aggregate_hotspots.py --executor-memory 28g --executor-cores 12 --executor-instances 1 --total-executor-cores 12 --executor-memory-overhead 4g --shuffle-partitions 2000
step 03_activity  --executor-memory 28g --executor-cores 12 --total-executor-cores 12 /workspace/src/03_prepare_ais_activity.py --shuffle-partitions 2000
step 04_mrv       --executor-memory 8g  /workspace/src/04_prepare_mrv_emissions.py
step 05_match     --executor-memory 8g  /workspace/src/05_match_ais_mrv.py
step 06_export    --executor-memory 8g  /workspace/src/06_export_dashboard_data.py
step 07_hotspot   --executor-memory 8g  /workspace/src/07_export_hotspot_assets.py

LOG "PIPELINE COMPLETE — full year processed"
