#!/usr/bin/env bash
# Chaos test: imports keep arriving (one per second) while import workers are SIGKILLed
# every few seconds and RabbitMQ is killed once, with 2% of chunk attempts failing on purpose.
# Passes only if every import finishes and every number adds up exactly:
#   - no chunk left PENDING, no import FAILED
#   - ok_rows + error_rows = total_rows for every import
#   - products in the catalog = ok_rows, rows in the error table = error_rows
#     (a chunk applied twice, or lost, would break these)
set -euo pipefail
cd "$(dirname "$0")/.."

ROWS=${ROWS:-20000}
DURATION=${DURATION:-60}
RESULTS="$PWD/chaos/results"
mkdir -p "$RESULTS"
export STUCK_AFTER_SECONDS=20 FAULT_RATE=${FAULT_RATE:-0.02}

sql() { docker compose exec -T postgres psql -U imports -d imports -tAc "$1"; }

docker compose down -v --remove-orphans >/dev/null 2>&1 || true
docker compose up -d --build --wait >/dev/null
echo "stack up; one $ROWS-row import per second, FAULT_RATE=$FAULT_RATE"

python3 - "$RESULTS/import.csv" "$ROWS" <<'PY'
import sys
path, rows = sys.argv[1], int(sys.argv[2])
with open(path, "w") as f:
    f.write("sku,name,price,currency,stock\n")
    for n in range(rows):
        price = "n/a" if n % 100 == 0 else f"{(n % 9000) + 1}.99"  # 1% invalid
        f.write(f"{{p}}{n},Ürün {n},{price},TRY,{n % 300}\n")
PY

# Uploads one import per second for the whole run; each gets its own SKU prefix.
uploader() {
  local i=0
  while [ $SECONDS -lt $1 ]; do
    i=$((i + 1))
    sed "s/{p}/I$i-/" "$RESULTS/import.csv" > "$RESULTS/upload.csv"
    curl -sf -XPOST localhost:8001/v1/imports -H 'Authorization: Bearer dev-key' -H "Idempotency-Key: chaos-$i" \
      -F "file=@$RESULTS/upload.csv" >/dev/null || i=$((i - 1))  # API briefly down: try the same key again
    sleep 1
  done
  echo "$i" > "$RESULTS/uploaded.txt"
}

kills=0
rabbit_killed=no
end=$((SECONDS + DURATION))
uploader "$end" &
echo "uploading and killing workers for ${DURATION}s"
while [ $SECONDS -lt $end ]; do
  sleep 3
  ids=($(docker compose ps -q worker-imports))
  victim=${ids[$((RANDOM % ${#ids[@]}))]}
  docker kill -s KILL "$victim" >/dev/null && kills=$((kills + 1))
  ( sleep 2; docker start "$victim" >/dev/null 2>&1 || true ) &
  if [ "$rabbit_killed" = no ] && [ $SECONDS -gt $((end - DURATION / 2)) ]; then
    echo "killing RabbitMQ"
    docker compose kill -s KILL rabbitmq >/dev/null
    sleep 5
    docker compose start rabbitmq >/dev/null
    rabbit_killed=yes
  fi
done
wait
IMPORTS=$(cat "$RESULTS/uploaded.txt")
docker compose up -d --wait >/dev/null
echo "killed $kills worker processes and RabbitMQ once; waiting for imports to finish"

for _ in $(seq 1 300); do
  open=$(sql "SELECT count(*) FROM imports WHERE status IN ('QUEUED', 'PROCESSING')")
  [ "$open" = "0" ] && break
  sleep 2
done

expected_ok=$((ROWS - (ROWS + 99) / 100))
expected_err=$(((ROWS + 99) / 100))
open=$(sql "SELECT count(*) FROM imports WHERE status IN ('QUEUED', 'PROCESSING')")
failed=$(sql "SELECT count(*) FROM imports WHERE status = 'FAILED'")
pending_chunks=$(sql "SELECT count(*) FROM import_chunks WHERE status <> 'DONE'")
bad_totals=$(sql "SELECT count(*) FROM imports WHERE ok_rows + error_rows <> total_rows OR ok_rows <> $expected_ok OR error_rows <> $expected_err")
products_off=0
errors_off=0
for i in $(seq 1 "$IMPORTS"); do
  p=$(sql "SELECT count(*) FROM products WHERE sku LIKE 'I$i-%'")
  e=$(sql "SELECT count(*) FROM import_errors e JOIN imports i ON i.id = e.import_id WHERE i.idempotency_key = 'chaos-$i'")
  [ "$p" = "$expected_ok" ] || products_off=$((products_off + 1))
  [ "$e" = "$expected_err" ] || errors_off=$((errors_off + 1))
done
chunks=$(sql "SELECT count(*) FROM import_chunks")
rerun=$(sql "SELECT count(*) FROM import_chunks WHERE attempts > 1")
sweeps=$(docker compose logs worker-webhooks 2>/dev/null | grep -c "re-planning stuck import" || true)

ok=yes
for v in "$open" "$failed" "$pending_chunks" "$bad_totals" "$products_off" "$errors_off"; do [ "$v" = "0" ] || ok=no; done

{
  echo "$IMPORTS imports of $(printf "%'d" "$ROWS") rows (1% invalid), one uploaded per second = $(printf "%'d" $((IMPORTS * ROWS))) rows in $chunks chunks. SIGKILLed $kills import-worker containers (every 3 s) and RabbitMQ once; $FAULT_RATE of chunk attempts failed on purpose."
  echo
  echo "| check | violations |"
  echo "|---|---:|"
  echo "| imports not finished | **$open** |"
  echo "| imports FAILED | **$failed** |"
  echo "| chunks not DONE | **$pending_chunks** |"
  echo "| imports whose ok + error rows ≠ total, or ≠ the expected $expected_ok / $expected_err | **$bad_totals** |"
  echo "| imports whose product count ≠ ok rows (lost or double-applied chunk) | **$products_off** |"
  echo "| imports whose error-table count ≠ error rows | **$errors_off** |"
  echo
  echo "Chunks that ran more than once (redelivered after a kill, or retried): **$rerun** of $chunks. Imports re-planned by the sweeper: $sweeps."
  echo
  [ "$ok" = yes ] && echo "**PASS**" || echo "**FAIL**"
} | tee "$RESULTS/chaos.md"
[ "$ok" = yes ]
