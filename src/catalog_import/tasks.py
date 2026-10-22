"""The import pipeline.

    plan_import ──► chord( process_chunk × N ) ──► finalize_import ──► deliver_webhook

Every task may run more than once: acks_late redelivers tasks from killed workers,
retries re-run them, and the sweeper re-launches work that looks stuck. Each task
therefore checks the database state first and makes its change in one transaction,
so a repeated run is a no-op.
"""

import json
import random
import uuid
from datetime import datetime
from typing import Any

import httpx
import structlog
from celery import chord
from celery.exceptions import SoftTimeLimitExceeded
from sqlalchemy import Connection, text
from sqlalchemy.exc import OperationalError

from . import csvfile, metrics, storage, webhooks
from .celery_app import app
from .config import settings
from .db import transaction

log = structlog.get_logger()

FINAL = ("COMPLETED", "COMPLETED_WITH_ERRORS", "FAILED", "CANCELLED")


class TransientError(Exception):
    """Worth retrying: the database or a dependency was briefly unavailable."""


def _maybe_fail(where: str) -> None:
    if settings().fault_rate and random.random() < settings().fault_rate:
        raise TransientError(f"injected failure in {where}")


# ---------------------------------------------------------------- planning


@app.task(bind=True, acks_late=True, max_retries=10)
def plan_import(self: Any, import_id: str) -> None:
    """Splits the file into chunks (once) and launches the chord over pending chunks."""
    try:
        with transaction() as conn:
            row = conn.execute(
                text("SELECT status, chunks_total FROM imports WHERE id = :id FOR UPDATE"), {"id": import_id}
            ).one_or_none()
            if row is None or row.status in FINAL:
                return
            rejected = False
            delivery: str | None = None
            if row.chunks_total is None:
                try:
                    chunks, total = csvfile.split(storage.read(conn, import_id), settings().chunk_rows)
                    _store_chunks(conn, import_id, chunks, total)
                except csvfile.InvalidFile as e:
                    rejected = True
                    delivery = _finish(conn, import_id, "FAILED", error=str(e))
            pending = (
                conn.execute(
                    text(
                        "SELECT chunk_no FROM import_chunks WHERE import_id = :id AND status = 'PENDING' ORDER BY chunk_no"
                    ),
                    {"id": import_id},
                )
                .scalars()
                .all()
            )
    except OperationalError as e:
        raise self.retry(exc=e, countdown=webhooks.backoff_seconds(self.request.retries, base=2, cap=60)) from e

    if rejected:
        if delivery:
            deliver_webhook.delay(delivery)
        return
    # Launched after the commit: workers must be able to see the chunks they are given.
    # If this process dies right here, the sweeper re-runs plan_import, which finds the
    # chunks already stored and launches the chord for whatever is still pending.
    if pending:
        chord(process_chunk.si(import_id, n) for n in pending)(finalize_import.si(import_id))
    else:
        finalize_import.delay(import_id)


def _store_chunks(conn: Connection, import_id: str, chunks: list[csvfile.Chunk], total: int) -> None:
    conn.execute(
        text("""INSERT INTO import_chunks (import_id, chunk_no, first_line, payload)
                VALUES (:id, :n, :first, :payload) ON CONFLICT DO NOTHING"""),
        [{"id": import_id, "n": c.number, "first": c.first_line, "payload": c.payload} for c in chunks],
    )
    conn.execute(
        text("""UPDATE imports SET status = 'PROCESSING', chunks_total = :n, total_rows = :rows,
                updated_at = now() WHERE id = :id"""),
        {"id": import_id, "n": len(chunks), "rows": total},
    )


# ---------------------------------------------------------------- chunks


@app.task(bind=True, acks_late=True, max_retries=8)
def process_chunk(self: Any, import_id: str, chunk_no: int) -> str:
    """Validates and upserts one chunk. The chunk row is locked for the whole transaction
    and moves to DONE in that same transaction, so a chunk's rows and counters are
    applied exactly once no matter how often the task runs."""
    try:
        # Counted in its own transaction so it survives the work being rolled back: it
        # shows how often a chunk was redelivered (killed worker) or retried.
        with transaction() as conn:
            conn.execute(
                text("UPDATE import_chunks SET attempts = attempts + 1 WHERE import_id = :id AND chunk_no = :n"),
                {"id": import_id, "n": chunk_no},
            )
        with transaction() as conn:
            chunk = conn.execute(
                text("""SELECT c.status, c.first_line, c.payload, i.seller_id, i.status AS import_status, i.created_at
                        FROM import_chunks c JOIN imports i ON i.id = c.import_id
                        WHERE c.import_id = :id AND c.chunk_no = :n FOR UPDATE OF c"""),
                {"id": import_id, "n": chunk_no},
            ).one_or_none()
            if chunk is None or chunk.status != "PENDING":
                metrics.CHUNKS.labels("duplicate").inc()
                return "already-done"
            if chunk.import_status == "CANCELLED":
                conn.execute(
                    text(
                        "UPDATE import_chunks SET status = 'SKIPPED', updated_at = now() WHERE import_id = :id AND chunk_no = :n"
                    ),
                    {"id": import_id, "n": chunk_no},
                )
                return "skipped"

            rows, errors = csvfile.parse_chunk(chunk.payload)
            _maybe_fail("process_chunk")
            _upsert_products(conn, chunk.seller_id, import_id, chunk.created_at, rows)
            if errors:
                conn.execute(
                    text("""INSERT INTO import_errors (import_id, chunk_no, line_no, sku, error)
                            VALUES (:id, :n, :line, :sku, :error)"""),
                    [
                        {"id": import_id, "n": chunk_no, "line": e.line_no, "sku": e.sku, "error": e.error}
                        for e in errors
                    ],
                )
            _complete_chunk(conn, import_id, chunk_no, "DONE", ok=len(rows), failed=len(errors))
        metrics.ROWS.labels("ok").inc(len(rows))
        metrics.ROWS.labels("error").inc(len(errors))
        metrics.CHUNKS.labels("done").inc()
        return "done"
    except (TransientError, OperationalError, SoftTimeLimitExceeded) as e:
        if self.request.retries < self.max_retries:
            metrics.CHUNKS.labels("retry").inc()
            raise self.retry(exc=e, countdown=webhooks.backoff_seconds(self.request.retries, base=1, cap=60)) from e
        # Out of retries: record the failure instead of raising. A raising task would break
        # the chord and finalize_import would never run.
        with transaction() as conn:
            _complete_chunk(conn, import_id, chunk_no, "FAILED", ok=0, failed=0, error=str(e))
        metrics.CHUNKS.labels("failed").inc()
        log.error("chunk failed permanently", import_id=import_id, chunk_no=chunk_no, error=str(e))
        return "failed"


def _upsert_products(
    conn: Connection, seller_id: str, import_id: str, imported_at: datetime, rows: list[csvfile.ProductRow]
) -> None:
    if not rows:
        return
    # Rows from a newer import win. If two imports for one seller overlap, the older
    # one's late chunks cannot overwrite what the newer one already wrote.
    conn.execute(
        text("""
            INSERT INTO products (seller_id, sku, name, price, currency, stock, barcode, source_import_id, source_imported_at)
            VALUES (:seller, :sku, :name, :price, :currency, :stock, :barcode, :import_id, :imported_at)
            ON CONFLICT (seller_id, sku) DO UPDATE SET
                name = EXCLUDED.name, price = EXCLUDED.price, currency = EXCLUDED.currency,
                stock = EXCLUDED.stock, barcode = EXCLUDED.barcode,
                source_import_id = EXCLUDED.source_import_id,
                source_imported_at = EXCLUDED.source_imported_at, updated_at = now()
            WHERE products.source_imported_at <= EXCLUDED.source_imported_at"""),
        [
            {
                "seller": seller_id,
                "sku": r.sku,
                "name": r.name,
                "price": r.price,
                "currency": r.currency,
                "stock": r.stock,
                "barcode": r.barcode,
                "import_id": import_id,
                "imported_at": imported_at,
            }
            for r in rows
        ],
    )


def _complete_chunk(
    conn: Connection, import_id: str, chunk_no: int, status: str, ok: int, failed: int, error: str | None = None
) -> None:
    updated = conn.execute(
        text("""UPDATE import_chunks SET status = :status, ok_rows = :ok, error_rows = :failed, error = :error,
                updated_at = now() WHERE import_id = :id AND chunk_no = :n AND status = 'PENDING'"""),
        {"id": import_id, "n": chunk_no, "status": status, "ok": ok, "failed": failed, "error": error},
    ).rowcount
    if updated:
        conn.execute(
            text("""UPDATE imports SET chunks_done = chunks_done + 1, ok_rows = ok_rows + :ok,
                    error_rows = error_rows + :failed, updated_at = now() WHERE id = :id"""),
            {"id": import_id, "ok": ok, "failed": failed},
        )


# ---------------------------------------------------------------- finishing


@app.task(bind=True, acks_late=True, max_retries=10)
def finalize_import(self: Any, import_id: str) -> None:
    try:
        with transaction() as conn:
            imp = conn.execute(
                text("SELECT status, chunks_total, callback_url FROM imports WHERE id = :id FOR UPDATE"),
                {"id": import_id},
            ).one_or_none()
            if imp is None or imp.status != "PROCESSING":
                return
            counts: dict[str, int] = dict(
                conn.execute(
                    text("SELECT status, count(*) FROM import_chunks WHERE import_id = :id GROUP BY status"),
                    {"id": import_id},
                )
                .tuples()
                .all()
            )
            if counts.get("PENDING"):
                return  # a chunk is still running or was lost; the sweeper will re-plan it
            if counts.get("FAILED"):
                status = "FAILED"
            elif conn.execute(text("SELECT error_rows FROM imports WHERE id = :id"), {"id": import_id}).scalar_one():
                status = "COMPLETED_WITH_ERRORS"
            else:
                status = "COMPLETED"
            delivery_id = _finish(conn, import_id, status)
    except OperationalError as e:
        raise self.retry(exc=e, countdown=webhooks.backoff_seconds(self.request.retries, base=2, cap=60)) from e
    if delivery_id:
        deliver_webhook.delay(delivery_id)


def _finish(conn: Connection, import_id: str, status: str, error: str | None = None) -> str | None:
    """Moves an import to a final state and, in the same transaction, schedules its webhook."""
    conn.execute(
        text(
            "UPDATE imports SET status = :status, error = :error, finished_at = now(), updated_at = now() WHERE id = :id"
        ),
        {"id": import_id, "status": status, "error": error},
    )
    metrics.IMPORTS.labels(status).inc()
    url = conn.execute(text("SELECT callback_url FROM imports WHERE id = :id"), {"id": import_id}).scalar_one()
    if not url:
        return None
    delivery_id = conn.execute(
        text("""INSERT INTO webhook_deliveries (id, import_id, url) VALUES (:did, :id, :url)
                ON CONFLICT (import_id) DO UPDATE SET import_id = EXCLUDED.import_id RETURNING id"""),
        {"did": str(uuid.uuid4()), "id": import_id, "url": url},
    ).scalar_one()
    return str(delivery_id)


# ---------------------------------------------------------------- webhooks


@app.task(bind=True, acks_late=True, max_retries=None)
def deliver_webhook(self: Any, delivery_id: str) -> str:
    with transaction() as conn:
        d = conn.execute(
            text("""SELECT d.status, d.url, d.attempts, i.id AS import_id, i.status AS import_status, i.total_rows,
                           i.ok_rows, i.error_rows, i.error, i.finished_at
                    FROM webhook_deliveries d JOIN imports i ON i.id = d.import_id WHERE d.id = :id"""),
            {"id": delivery_id},
        ).one_or_none()
    if d is None or d.status != "PENDING":
        return "nothing-to-do"

    body = json.dumps(
        {
            "type": "import.finished",
            "delivery_id": delivery_id,
            "import": {
                "id": str(d.import_id),
                "status": d.import_status,
                "total_rows": d.total_rows,
                "ok_rows": d.ok_rows,
                "error_rows": d.error_rows,
                "error": d.error,
                "finished_at": d.finished_at.isoformat() if d.finished_at else None,
            },
        },
        separators=(",", ":"),
    ).encode()
    headers = {
        "Content-Type": "application/json",
        webhooks.SIGNATURE_HEADER: webhooks.sign(settings().webhook_secret, body),
        # Receivers dedupe on this: retries of one delivery carry the same id.
        "X-Delivery-Id": delivery_id,
    }

    attempt = d.attempts + 1
    code: int | None = None
    error: str | None = None
    try:
        code = httpx.post(d.url, content=body, headers=headers, timeout=10).status_code
    except httpx.HTTPError as e:
        error = f"{type(e).__name__}: {e}"

    delivered = code is not None and 200 <= code < 300
    retry = (
        not delivered
        and (code is None or webhooks.retryable_status(code))
        and attempt < settings().webhook_max_attempts
    )
    status = "DELIVERED" if delivered else ("PENDING" if retry else "FAILED")
    countdown = webhooks.backoff_seconds(attempt) if retry else 0
    with transaction() as conn:
        # next_attempt_at lets the sweeper notice a retry that never arrived.
        conn.execute(
            text("""UPDATE webhook_deliveries SET status = :status, attempts = :attempt, last_status_code = :code,
                    last_error = :error, delivered_at = CASE WHEN :status = 'DELIVERED' THEN now() END,
                    next_attempt_at = now() + make_interval(secs => :countdown) WHERE id = :id"""),
            {
                "id": delivery_id,
                "status": status,
                "attempt": attempt,
                "code": code,
                "error": error,
                "countdown": countdown,
            },
        )
    metrics.WEBHOOKS.labels(status.lower()).inc()
    if retry:
        # Native delayed delivery: the retry waits in RabbitMQ, not in a worker.
        raise self.retry(countdown=countdown)
    return status


# ---------------------------------------------------------------- housekeeping


@app.task
def sweep_stuck_imports() -> int:
    """Re-plans imports that stopped moving: the enqueue after the API commit was lost,
    a plan_import worker died before launching the chord, or a chunk task vanished.
    plan_import only launches PENDING chunks, so finished work is never redone."""
    with transaction() as conn:
        stuck = (
            conn.execute(
                text("""SELECT id FROM imports WHERE status IN ('QUEUED', 'PROCESSING')
                    AND updated_at < now() - make_interval(secs => :secs)
                    ORDER BY updated_at LIMIT 100 FOR UPDATE SKIP LOCKED"""),
                {"secs": settings().stuck_after_seconds},
            )
            .scalars()
            .all()
        )
        for import_id in stuck:
            conn.execute(text("UPDATE imports SET updated_at = now() WHERE id = :id"), {"id": import_id})
    for import_id in stuck:
        log.warning("re-planning stuck import", import_id=str(import_id))
        metrics.SWEEPS.inc()
        plan_import.delay(str(import_id))

    # Webhook deliveries whose scheduled retry never ran (the retry message was lost):
    # re-enqueue them. A duplicate attempt is harmless; receivers dedupe on X-Delivery-Id.
    with transaction() as conn:
        late = (
            conn.execute(
                text("""SELECT id FROM webhook_deliveries WHERE status = 'PENDING'
                    AND next_attempt_at < now() - make_interval(secs => :secs)
                    ORDER BY next_attempt_at LIMIT 100 FOR UPDATE SKIP LOCKED"""),
                {"secs": settings().stuck_after_seconds},
            )
            .scalars()
            .all()
        )
        for delivery_id in late:
            conn.execute(
                text("UPDATE webhook_deliveries SET next_attempt_at = now() WHERE id = :id"), {"id": delivery_id}
            )
    for delivery_id in late:
        log.warning("re-enqueueing stalled webhook delivery", delivery_id=str(delivery_id))
        metrics.SWEEPS.inc()
        deliver_webhook.delay(str(delivery_id))
    return len(stuck) + len(late)


@app.task
def cleanup() -> None:
    """Uploaded files and chunk payloads are only needed until the import finishes."""
    with transaction() as conn:
        conn.execute(
            text("""DELETE FROM import_files f USING imports i
                             WHERE f.import_id = i.id AND i.finished_at < now() - interval '7 days'""")
        )
        conn.execute(
            text("""UPDATE import_chunks c SET payload = '' FROM imports i
                             WHERE c.import_id = i.id AND i.finished_at < now() - interval '7 days' AND c.payload <> ''""")
        )
