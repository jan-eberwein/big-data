#!/usr/bin/env bash
# Resume the pipeline from step 03 (use when steps 01+02 are already complete).
set -euo pipefail
ROOT="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
cd "$ROOT"
[ -f .env ] && set -a && . ./.env && set +a
LOG(){ echo "=== [$(date '+%m-%d %H:%M:%S')] $* ==="; }
SUB="docker compose exec -T spark-master /opt/spark/bin/spark-submit --master spark://spark-master:7077"
CONF="--conf spark.network.timeout=1200s --conf spark.executor.heartbeatInterval=60s"
step(){ local name="$1"; shift; LOG "START $name"; $SUB $CONF "$@"; local rc=$?; LOG "END $name rc=$rc"; [ "$rc" -eq 0 ] || { LOG "ABORTED at $name rc=$rc"; exit "$rc"; }; sleep 25; }
rm -rf "${PROCESSED_DATA_DIR:?set PROCESSED_DATA_DIR in .env}/_spill"/* 2>/dev/null || true
LOG "RESUME from 03 (01+02 already complete)"
step 03_activity  --executor-memory 28g --executor-cores 12 --total-executor-cores 12 /workspace/src/03_prepare_ais_activity.py --shuffle-partitions 2000
step 04_mrv       --executor-memory 8g  /workspace/src/04_prepare_mrv_emissions.py
step 05_match     --executor-memory 8g  /workspace/src/05_match_ais_mrv.py
step 06_export    --executor-memory 8g  /workspace/src/06_export_dashboard_data.py
step 07_hotspot   --executor-memory 8g  /workspace/src/07_export_hotspot_assets.py
LOG "RESUME COMPLETE — full year done"
