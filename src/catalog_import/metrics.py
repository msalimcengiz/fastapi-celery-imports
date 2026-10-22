from prometheus_client import Counter

IMPORTS = Counter("catalog_imports_finished_total", "Imports that reached a final state.", ["status"])
CHUNKS = Counter("catalog_import_chunks_total", "Chunk task outcomes.", ["outcome"])
ROWS = Counter("catalog_import_rows_total", "Rows imported or rejected.", ["result"])
WEBHOOKS = Counter("catalog_webhook_attempts_total", "Webhook delivery attempts by resulting state.", ["state"])
SWEEPS = Counter("catalog_import_sweeps_total", "Stuck imports re-planned by the sweeper.")
ENQUEUE_FAILURES = Counter(
    "catalog_import_enqueue_failures_total", "Imports stored but not enqueued; the sweeper will pick them up."
)
