#!/usr/bin/env bash
# Guided demo: starts the stack, walks through the main behaviours and checks each one.
# Needs only Docker. Takes about two minutes on the first run (image builds).
set -euo pipefail
cd "$(dirname "$0")/.."

STATE="$PWD/.demo-state"   # inside the project: Docker Desktop/Colima only share the home directory
rm -rf "$STATE" && mkdir -p "$STATE" && echo 1 > "$STATE/next_step"
py() {
  docker run --rm --network imports_default -v "$PWD/demo:/demo:ro" -v "$STATE:/tmp" -w /demo \
    -e DEMO_STEP="$(cat "$STATE/next_step")" python:3.13-alpine python -u demo.py "$1"
}
quiet() { "$@" >/dev/null 2>&1; }

echo "Starting the stack (API, 2 import workers, webhook worker, beat, RabbitMQ, Postgres, Redis)…"
quiet docker compose up -d --build --wait
py basics
py webhook_retry
py big_upload
for c in $(docker compose ps -q worker-imports); do docker kill -s KILL "$c" >/dev/null; done
echo -e "   \033[2mkilled every import worker with SIGKILL; restarting them\033[0m"
sleep 2
quiet docker compose up -d --wait worker-imports
py big_check
py cancel
echo -e "\033[2mCelery queues in RabbitMQ (all quorum):\033[0m"
docker compose exec -T rabbitmq rabbitmqctl list_queues -q name type messages 2>/dev/null | grep -E "^(default|imports|webhooks)\s" | sed 's/^/   /'
rm -rf "$STATE"
echo "Explore: API docs http://localhost:8001/docs · RabbitMQ http://localhost:15673 (imports/imports) · stop → make down"
