#!/usr/bin/env bash
# Download all NOAA Marine Cadastre AIS "csv2" daily files for 2025 (.csv.zst).
# Source: https://noaaocm.blob.core.windows.net/ais/csv2/csv2025/ais-YYYY-MM-DD.csv.zst
# These are already in the project's column layout (mmsi,base_date_time,longitude,latitude,...)
# and Spark reads .csv.zst directly, so no extraction/transform is needed.
#
# Usage: ./download_ais_2025.sh [DEST_DIR] [PARALLEL]
# DEST_DIR defaults to $RAW_DATA_DIR from .env (falls back to ./data/raw/ais).
set -euo pipefail

ROOT="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
[ -f "$ROOT/.env" ] && set -a && . "$ROOT/.env" && set +a

DEST="${1:-${RAW_DATA_DIR:-$ROOT/data/raw/ais}}"
PAR="${2:-4}"
BASE="https://noaaocm.blob.core.windows.net/ais/csv2/csv2025"

mkdir -p "$DEST"
echo "Destination: $DEST"
echo "Parallel:    $PAR"

# All 2025 dates -> a temp list
DATES="$(mktemp)"
python3 - > "$DATES" <<'PY'
import datetime
d, end = datetime.date(2025, 1, 1), datetime.date(2025, 12, 31)
while d <= end:
    print(d.isoformat()); d += datetime.timedelta(days=1)
PY

dl() {
  local date="$1" dest="$2" base="$3"
  local f="ais-${date}.csv.zst"
  if [ -s "${dest}/${f}" ]; then echo "skip ${f}"; return 0; fi
  if curl -fsS --retry 4 --retry-delay 3 -o "${dest}/${f}.part" "${base}/${f}"; then
    mv "${dest}/${f}.part" "${dest}/${f}"; echo "ok   ${f}"
  else
    rm -f "${dest}/${f}.part"; echo "FAIL ${f}"
  fi
}
export -f dl

# shellcheck disable=SC2002
cat "$DATES" | xargs -P "$PAR" -I{} bash -c 'dl "$1" "$2" "$3"' _ {} "$DEST" "$BASE"
rm -f "$DATES"

echo "----"
echo "Files:  $(ls "$DEST"/ais-2025-*.csv.zst 2>/dev/null | wc -l | tr -d ' ') / 365"
echo "Size:   $(du -sh "$DEST" 2>/dev/null | cut -f1)"
FAILS=$(ls "$DEST"/*.part 2>/dev/null | wc -l | tr -d ' ')
echo "Partial/failed leftovers: ${FAILS} (re-run the script to retry missing days)"
