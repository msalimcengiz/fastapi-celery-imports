#!/usr/bin/env bash
# Import throughput benchmark (no faults injected).
#
# Uploads IMPORTS files of ROWS rows each (1% invalid) as fast as the API accepts them,
# then measures, from the database clock, how long it took until every import was finished:
# first import created -> last import finished. Repeated with 1, 2 and 4 import-worker
# containers (4 Celery processes each) to see how chunk processing scales with workers.
set -euo pipefail
cd "$(dirname "$0")/.."

ROWS=${ROWS:-100000}
IMPORTS=${IMPORTS:-20}
SCALES=${SCALES:-"1 2 4"}
RESULTS="$PWD/bench/results"
mkdir -p "$RESULTS"
export FAULT_RATE=0

sql() { docker compose exec -T postgres psql -U imports -d imports -tAc "$1"; }

python3 - "$RESULTS/import.csv" "$ROWS" <<'PY'
import sys
path, rows = sys.argv[1], int(sys.argv[2])
with open(path, "w") as f:
    f.write("sku,name,price,currency,stock\n")
    for n in range(rows):
        price = "n/a" if n % 100 == 0 else f"{(n % 9000) + 1}.99"  # 1% invalid
        f.write(f"{{p}}{n},Ürün {n},{price},TRY,{n % 300}\n")
PY

rows_md=""
for workers in $SCALES; do
  docker compose down -v --remove-orphans >/dev/null 2>&1 || true
  docker compose up -d --build --wait --scale worker-imports="$workers" >/dev/null
  echo "$workers import worker(s): uploading $IMPORTS × $ROWS rows"
  for i in $(seq 1 "$IMPORTS"); do
    sed "s/{p}/W$workers-I$i-/" "$RESULTS/import.csv" > "$RESULTS/upload.csv"
    curl -sf -XPOST localhost:8001/v1/imports -H 'Authorization: Bearer dev-key' -H "Idempotency-Key: bench-$workers-$i" \
      -F "file=@$RESULTS/upload.csv" >/dev/null
  done
  for _ in $(seq 1 600); do
    [ "$(sql "SELECT count(*) FROM imports WHERE status IN ('QUEUED', 'PROCESSING')")" = 0 ] && break
    sleep 1
  done
  read -r n total ok err secs p50 max not_ok <<<"$(sql "
    SELECT count(*), sum(total_rows), sum(ok_rows), sum(error_rows),
           round(extract(epoch FROM max(finished_at) - min(created_at))::numeric, 1),
           round(percentile_cont(0.5) WITHIN GROUP (ORDER BY extract(epoch FROM finished_at - created_at))::numeric, 1),
           round(max(extract(epoch FROM finished_at - created_at))::numeric, 1),
           count(*) FILTER (WHERE status <> 'COMPLETED_WITH_ERRORS')
    FROM imports" | tr '|' ' ')"
  products=$(sql "SELECT count(*) FROM products")
  rate=$(python3 -c "print(f'{$total / $secs:,.0f}')")
  echo "  $total rows in ${secs}s = $rate rows/s"
  rows_md+="| $workers × 4 | $n | $(printf "%'d" "$total") | $(printf "%'d" "$ok") | $(printf "%'d" "$err") | $(printf "%'d" "$products") | $secs | **$rate** | $p50 | $max | $not_ok |"$'\n'
done

{
  echo "No faults injected. Each run: $IMPORTS imports of $(printf "%'d" "$ROWS") rows (1% invalid), all uploaded back to back, 1,000-row chunks."
  echo "Machine: $(sysctl -n machdep.cpu.brand_string 2>/dev/null || uname -m), Docker VM with $(docker info --format '{{.NCPU}}') CPUs and $(docker info --format '{{.MemTotal}}' | awk '{printf "%.0f GiB", $1/1073741824}') shared by every container (API, workers, RabbitMQ, Postgres)."
  echo
  echo "| import workers × processes | imports | rows | valid rows | invalid rows (reported by line) | products stored | seconds (first upload → last finished) | rows/s | per-import p50 s | per-import max s | imports not in the expected state |"
  echo "|---|---:|---:|---:|---:|---:|---:|---:|---:|---:|---:|"
  printf '%s' "$rows_md"
} | tee "$RESULTS/summary.md"
