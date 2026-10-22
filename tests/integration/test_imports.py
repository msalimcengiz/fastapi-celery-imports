import time
import uuid
from collections.abc import Callable
from typing import Any

import httpx
import pytest

from catalog_import import webhooks
from catalog_import.tasks import process_chunk, sweep_stuck_imports

from .conftest import API, AUTH, Receiver, make_csv, upload, wait_for

pytestmark = pytest.mark.integration


def test_imports_valid_rows_and_reports_invalid_ones_by_line(sql: Callable[..., Any]) -> None:
    r = upload(make_csv(3_500, bad_every=100, prefix="HAPPY-"))
    assert r.status_code == 202
    done = wait_for(r.json()["id"])

    assert done["status"] == "COMPLETED_WITH_ERRORS"
    assert (done["total_rows"], done["ok_rows"], done["error_rows"]) == (3_500, 3_465, 35)
    assert sql("SELECT count(*) FROM products WHERE sku LIKE 'HAPPY-%'").scalar_one() == 3_465

    errors = httpx.get(f"{API}/v1/imports/{done['id']}/errors.csv", headers=AUTH).text.splitlines()
    assert errors[0] == "line,sku,error"
    assert errors[1].startswith("2,HAPPY-0,") and errors[2].startswith("102,HAPPY-100,")


def test_idempotency_key_returns_the_same_import() -> None:
    key = f"upload-{uuid.uuid4()}"
    content = make_csv(10, prefix="IDEM-")
    first = upload(content, key=key)
    second = upload(content, key=key)
    assert (first.status_code, second.status_code) == (202, 200)
    assert first.json()["id"] == second.json()["id"]
    # Same key with a different file is a client bug, not a retry.
    assert upload(make_csv(11, prefix="IDEM-"), key=key).status_code == 409


def test_sellers_cannot_see_each_others_imports() -> None:
    import_id = upload(make_csv(5, prefix="T-")).json()["id"]
    other = {"Authorization": "Bearer other-key"}
    assert httpx.get(f"{API}/v1/imports/{import_id}", headers=other).status_code == 404
    assert httpx.get(f"{API}/v1/imports/{uuid.uuid4()}", headers=AUTH).status_code == 404
    assert httpx.get(f"{API}/v1/imports/{import_id}", headers={"Authorization": "Bearer nope"}).status_code == 401


def test_file_without_required_columns_fails_with_a_reason() -> None:
    r = upload(b"sku,title\nA,b\n")
    done = wait_for(r.json()["id"])
    assert done["status"] == "FAILED"
    assert "missing columns" in done["error"]


def test_running_a_chunk_twice_applies_it_once(sql: Callable[..., Any]) -> None:
    # What acks_late redelivery looks like: the same task body runs again.
    import_id = upload(make_csv(1_000, bad_every=10, prefix="TWICE-")).json()["id"]
    first = wait_for(import_id)
    assert process_chunk.run(import_id, 0) == "already-done"
    again = wait_for(import_id)
    assert (again["ok_rows"], again["error_rows"]) == (first["ok_rows"], first["error_rows"]) == (900, 100)
    assert sql("SELECT count(*) FROM import_errors WHERE import_id = :id", id=import_id).scalar_one() == 100


def test_a_newer_import_wins_even_if_an_older_one_finishes_later(sql: Callable[..., Any]) -> None:
    old = upload(make_csv(1, prefix="ORDER-", price="1.00")).json()["id"]
    wait_for(old)
    new = upload(make_csv(1, prefix="ORDER-", price="2.00")).json()["id"]
    wait_for(new)
    # Replay the older import's chunk after the newer one finished.
    sql("UPDATE import_chunks SET status = 'PENDING' WHERE import_id = :id", id=old)
    process_chunk.run(old, 0)
    assert str(sql("SELECT price FROM products WHERE sku = 'ORDER-0'").scalar_one()) == "2.00"


def test_cancelled_import_stops_processing(sql: Callable[..., Any]) -> None:
    import_id = upload(make_csv(30_000, prefix="CANCEL-")).json()["id"]
    r = httpx.post(f"{API}/v1/imports/{import_id}/cancel", headers=AUTH)
    assert r.status_code in (200, 409)  # 409 only if it already finished
    time.sleep(5)
    status = httpx.get(f"{API}/v1/imports/{import_id}", headers=AUTH).json()["status"]
    if r.status_code == 200:
        assert status == "CANCELLED"
        skipped = sql(
            "SELECT count(*) FROM import_chunks WHERE import_id = :id AND status = 'PENDING'", id=import_id
        ).scalar_one()
        assert skipped == 0  # every chunk was either finished or skipped, none left hanging


def test_webhook_is_signed_and_retried_until_delivered(receiver: Receiver) -> None:
    receiver.reset([503, 503])
    import_id = upload(make_csv(20, prefix="HOOK-"), callback="http://tests:9999/hook").json()["id"]
    done = wait_for(import_id, lambda r: (r["webhook"] or {}).get("status") == "DELIVERED", timeout=90)

    assert done["webhook"]["attempts"] == 3
    assert len(receiver.calls) == 3
    ids = {c["headers"]["X-Delivery-Id"] for c in receiver.calls}
    assert len(ids) == 1  # every retry carries the same delivery id for receiver-side dedupe
    last = receiver.calls[-1]
    assert webhooks.verify("dev-webhook-secret", last["body"], last["headers"]["X-Signature"])
    assert last["json"]["import"] | {"finished_at": None} == {
        "id": import_id,
        "status": "COMPLETED",
        "total_rows": 20,
        "ok_rows": 20,
        "error_rows": 0,
        "error": None,
        "finished_at": None,
    }


def test_webhook_client_errors_are_not_retried(receiver: Receiver) -> None:
    receiver.reset([400])
    import_id = upload(make_csv(5, prefix="HOOK400-"), callback="http://tests:9999/hook").json()["id"]
    done = wait_for(import_id, lambda r: (r["webhook"] or {}).get("status") == "FAILED", timeout=30)
    assert done["webhook"]["attempts"] == 1


def test_sweeper_recovers_an_import_whose_enqueue_was_lost(sql: Callable[..., Any]) -> None:
    # The API committed the import, then the broker publish was lost.
    import_id = str(uuid.uuid4())
    sql(
        """INSERT INTO imports (id, seller_id, idempotency_key, status, file_sha256, updated_at)
           VALUES (:id, 'seller-1', :key, 'QUEUED', 'x', now() - interval '1 hour')""",
        id=import_id,
        key=f"lost-{import_id}",
    )
    sql("INSERT INTO import_files (import_id, content) VALUES (:id, :c)", id=import_id, c=make_csv(50, prefix="LOST-"))

    sweep_stuck_imports.delay()
    done = wait_for(import_id)
    assert (done["status"], done["ok_rows"]) == ("COMPLETED", 50)


def test_sweeper_re_enqueues_a_webhook_whose_retry_was_lost(sql: Callable[..., Any], receiver: Receiver) -> None:
    receiver.reset([])
    import_id = upload(make_csv(3, prefix="LOSTHOOK-"), callback="http://tests:9999/hook").json()["id"]
    wait_for(import_id, lambda r: (r["webhook"] or {}).get("status") == "DELIVERED", timeout=30)
    # Simulate a retry message that vanished: the delivery is PENDING and long overdue.
    sql(
        """UPDATE webhook_deliveries SET status = 'PENDING', delivered_at = NULL,
           next_attempt_at = now() - interval '1 hour' WHERE import_id = :id""",
        id=import_id,
    )
    receiver.reset([])
    sweep_stuck_imports.delay()
    done = wait_for(import_id, lambda r: (r["webhook"] or {}).get("status") == "DELIVERED", timeout=30)
    assert done["webhook"]["attempts"] == 2
    assert len(receiver.calls) == 1
