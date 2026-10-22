import hashlib
import hmac
import os
import uuid
from typing import Annotated, Any

import structlog
from fastapi import Depends, FastAPI, File, Form, Header, HTTPException, Request, Response, UploadFile, status
from fastapi.responses import PlainTextResponse, StreamingResponse
from prometheus_client import CONTENT_TYPE_LATEST, REGISTRY, CollectorRegistry, generate_latest, multiprocess
from pydantic import AnyHttpUrl
from sqlalchemy import text

from . import metrics, storage
from .config import settings
from .db import engine, transaction
from .tasks import plan_import

log = structlog.get_logger()
app = FastAPI(title="catalog-import")


def seller(authorization: Annotated[str | None, Header()] = None) -> str:
    token = (authorization or "").removeprefix("Bearer ").strip()
    for key, seller_id in settings().sellers_by_key().items():
        if token and hmac.compare_digest(token, key):
            return seller_id
    raise HTTPException(status.HTTP_401_UNAUTHORIZED, "missing or invalid API key")


Seller = Annotated[str, Depends(seller)]


@app.post("/v1/imports", status_code=status.HTTP_202_ACCEPTED)
async def create_import(
    response: Response,
    seller_id: Seller,
    file: Annotated[UploadFile, File()],
    idempotency_key: Annotated[str, Header(min_length=1, max_length=128)],
    callback_url: Annotated[AnyHttpUrl | None, Form()] = None,
) -> dict[str, Any]:
    content = await file.read(settings().max_upload_bytes + 1)
    if len(content) > settings().max_upload_bytes:
        raise HTTPException(status.HTTP_413_REQUEST_ENTITY_TOO_LARGE, "file too large")
    if not content:
        raise HTTPException(status.HTTP_422_UNPROCESSABLE_ENTITY, "file is empty")

    import_id = str(uuid.uuid4())
    sha = hashlib.sha256(content).hexdigest()
    with transaction() as conn:
        inserted = conn.execute(
            text("""INSERT INTO imports (id, seller_id, idempotency_key, status, callback_url, file_sha256)
                    VALUES (:id, :seller, :key, 'QUEUED', :cb, :sha)
                    ON CONFLICT (seller_id, idempotency_key) DO NOTHING RETURNING id"""),
            {
                "id": import_id,
                "seller": seller_id,
                "key": idempotency_key,
                "cb": str(callback_url) if callback_url else None,
                "sha": sha,
            },
        ).scalar_one_or_none()
        if inserted is None:
            existing = conn.execute(
                text("SELECT id, file_sha256 FROM imports WHERE seller_id = :s AND idempotency_key = :k"),
                {"s": seller_id, "k": idempotency_key},
            ).one()
            if existing.file_sha256 != sha:
                raise HTTPException(status.HTTP_409_CONFLICT, "Idempotency-Key was already used for a different file")
            response.status_code = status.HTTP_200_OK
            return _summary(str(existing.id))
        storage.write(conn, import_id, content)

    # Enqueued after the commit, so the worker always finds the row. If the broker is
    # down right now the import stays QUEUED and the sweeper enqueues it later; the
    # client already has its 202.
    try:
        plan_import.delay(import_id)
    except Exception:
        metrics.ENQUEUE_FAILURES.inc()
        log.warning("enqueue failed; sweeper will retry", import_id=import_id, exc_info=True)
    return _summary(import_id)


@app.get("/v1/imports/{import_id}")
def get_import(import_id: uuid.UUID, seller_id: Seller) -> dict[str, Any]:
    _owned(str(import_id), seller_id)
    return _summary(str(import_id))


@app.post("/v1/imports/{import_id}/cancel")
def cancel_import(import_id: uuid.UUID, seller_id: Seller) -> dict[str, Any]:
    with transaction() as conn:
        updated = conn.execute(
            text("""UPDATE imports SET status = 'CANCELLED', finished_at = now(), updated_at = now()
                    WHERE id = :id AND seller_id = :s AND status IN ('QUEUED', 'PROCESSING')"""),
            {"id": str(import_id), "s": seller_id},
        ).rowcount
    if not updated:
        _owned(str(import_id), seller_id)
        raise HTTPException(status.HTTP_409_CONFLICT, "import already finished")
    return _summary(str(import_id))


@app.get("/v1/imports/{import_id}/errors.csv")
def import_errors(import_id: uuid.UUID, seller_id: Seller) -> StreamingResponse:
    _owned(str(import_id), seller_id)

    def rows() -> Any:
        yield "line,sku,error\n"
        with engine().connect() as conn:
            result = conn.execution_options(stream_results=True).execute(
                text("SELECT line_no, sku, error FROM import_errors WHERE import_id = :id ORDER BY line_no"),
                {"id": str(import_id)},
            )
            for r in result:
                yield f'{r.line_no},{r.sku or ""},"{r.error.replace(chr(34), chr(39))}"\n'

    return StreamingResponse(rows(), media_type="text/csv")


@app.get("/healthz")
def healthz() -> dict[str, str]:
    return {"status": "ok"}


@app.get("/readyz")
def readyz() -> dict[str, str]:
    with engine().connect() as conn:
        conn.execute(text("SELECT 1"))
    return {"status": "ready"}


@app.get("/metrics")
def prometheus(_: Request) -> PlainTextResponse:
    registry = REGISTRY
    if os.environ.get("PROMETHEUS_MULTIPROC_DIR"):
        registry = CollectorRegistry()
        multiprocess.MultiProcessCollector(registry)
    return PlainTextResponse(generate_latest(registry), media_type=CONTENT_TYPE_LATEST)


def _owned(import_id: str, seller_id: str) -> None:
    with engine().connect() as conn:
        found = conn.execute(
            text("SELECT 1 FROM imports WHERE id = :id AND seller_id = :s"), {"id": import_id, "s": seller_id}
        ).first()
    if not found:
        # Same answer for "missing" and "someone else's": no information leaks.
        raise HTTPException(status.HTTP_404_NOT_FOUND, "import not found")


def _summary(import_id: str) -> dict[str, Any]:
    with engine().connect() as conn:
        r = conn.execute(
            text("""SELECT i.id, i.status, i.total_rows, i.ok_rows, i.error_rows, i.chunks_total, i.chunks_done,
                           i.error, i.created_at, i.finished_at, d.status AS webhook_status, d.attempts AS webhook_attempts
                    FROM imports i LEFT JOIN webhook_deliveries d ON d.import_id = i.id WHERE i.id = :id"""),
            {"id": import_id},
        ).one()
    progress = round(100 * r.chunks_done / r.chunks_total, 1) if r.chunks_total else 0.0
    return {
        "id": str(r.id),
        "status": r.status,
        "progress_pct": progress,
        "total_rows": r.total_rows,
        "ok_rows": r.ok_rows,
        "error_rows": r.error_rows,
        "error": r.error,
        "created_at": r.created_at,
        "finished_at": r.finished_at,
        "webhook": {"status": r.webhook_status, "attempts": r.webhook_attempts} if r.webhook_status else None,
    }
