"""Guided demo of fastapi-celery-imports. Run with `make demo`; demo/run.sh kills the
import workers in the middle of a large import."""

import sys
import time
import uuid

from lib import done, http, next_step, note, ok, show, step, wait_until

API = "http://api:8000/v1"
RECEIVER = "http://webhook-receiver:9000"
SELLER1 = {"Authorization": "Bearer dev-key"}
SELLER2 = {"Authorization": "Bearer other-key"}
FINAL = ("COMPLETED", "COMPLETED_WITH_ERRORS", "FAILED", "CANCELLED")


def csv(rows: int, prefix: str, bad_every: int = 0) -> bytes:
    lines = ["sku,name,price,currency,stock"]
    for i in range(rows):
        price = "fiyat-yok" if bad_every and i % bad_every == 7 else f'"{(i % 900) + 10},{i % 100:02d}"'
        lines.append(f"{prefix}{i},Ürün {i},{price},TRY,{i % 40}")
    return ("\n".join(lines) + "\n").encode()


def upload(content: bytes, key: str, auth: dict = SELLER1, callback: str | None = None):
    boundary = uuid.uuid4().hex
    parts = [
        f'--{boundary}\r\nContent-Disposition: form-data; name="file"; filename="products.csv"\r\n'
        f"Content-Type: text/csv\r\n\r\n".encode()
        + content
        + b"\r\n"
    ]
    if callback:
        parts.append(
            f'--{boundary}\r\nContent-Disposition: form-data; name="callback_url"\r\n\r\n{callback}\r\n'.encode()
        )
    body = b"".join(parts) + f"--{boundary}--\r\n".encode()
    return http(
        "POST",
        f"{API}/imports",
        raw=body,
        headers={**auth, "Idempotency-Key": key, "Content-Type": f"multipart/form-data; boundary={boundary}"},
    )


def get(import_id: str, auth: dict = SELLER1) -> dict:
    return http("GET", f"{API}/imports/{import_id}", headers=auth).json()


def finished(import_id: str, timeout: float = 120) -> dict:
    return wait_until(lambda: get(import_id), lambda r: r["status"] in FINAL, timeout=timeout)


def basics() -> None:
    step("A 5,000-row CSV with Turkish decimal commas and 1% bad rows is imported in parallel chunks")
    r = upload(csv(5000, "DEMO-", bad_every=100), key=f"feed-{uuid.uuid4()}", callback=f"{RECEIVER}/hooks")
    show(r)
    ok(r.status == 202 and r.json()["status"] == "QUEUED", "202: stored, then enqueued after the commit")
    res = finished(r.json()["id"])
    note(
        f"status {res['status']} · {res['ok_rows']} imported · {res['error_rows']} rejected · progress {res['progress_pct']}%"
    )
    ok((res["ok_rows"], res["error_rows"]) == (4950, 50), "4,950 rows imported, 50 rejected")

    step("Every rejected row is reported with its line number in the original file and the reason")
    errors = http("GET", f"{API}/imports/{r.json()['id']}/errors.csv", headers=SELLER1).body.decode().splitlines()
    for line in errors[:3]:
        note(line[:110])
    ok(errors[1].startswith("9,DEMO-7,"), "line 9 of the file is DEMO-7 (header is line 1)")

    step("The seller's endpoint got a signed webhook")
    hook = wait_until(
        lambda: http("GET", f"{RECEIVER}/received").json(),
        lambda rs: any(x["body"]["import"]["id"] == r.json()["id"] for x in rs),
        timeout=30,
    )
    h = next(x for x in hook if x["body"]["import"]["id"] == r.json()["id"])
    note(f"delivery {h['delivery_id'][:8]}… body: {h['body']['import']}")
    ok(h["signature_valid"], "HMAC-SHA256 signature verified by the receiver")

    step("The same Idempotency-Key returns the same import; reusing it for a different file is refused")
    key = f"feed-{uuid.uuid4()}"
    first = upload(csv(10, "IDEM-"), key)
    again = upload(csv(10, "IDEM-"), key)
    other = upload(csv(11, "IDEM-"), key)
    ok(again.status == 200 and again.json()["id"] == first.json()["id"], "200 with the original import")
    ok(other.status == 409, "409: same key, different file content")

    step("Sellers cannot see each other's imports")
    ok(
        http("GET", f"{API}/imports/{first.json()['id']}", headers=SELLER2).status == 404,
        "404 for another seller (same as missing)",
    )
    ok(http("GET", f"{API}/imports/{first.json()['id']}").status == 401, "401 without an API key")


def webhook_retry() -> None:
    step("The seller's endpoint is down (503 twice): the webhook is retried with jittered backoff")
    http("POST", f"{RECEIVER}/control", {"fail": 2})
    r = upload(csv(20, "HOOK-"), key=f"feed-{uuid.uuid4()}", callback=f"{RECEIVER}/hooks")
    res = wait_until(
        lambda: get(r.json()["id"]), lambda x: (x["webhook"] or {}).get("status") == "DELIVERED", timeout=90, every=1
    )
    calls = [x for x in http("GET", f"{RECEIVER}/received").json() if x["body"]["import"]["id"] == r.json()["id"]]
    note(f"receiver answers: {[c['answered'] for c in calls]}")
    ok(
        res["webhook"]["attempts"] == 3 and len({c["delivery_id"] for c in calls}) == 1,
        "delivered on attempt 3, all attempts carry the same X-Delivery-Id",
    )


def big_upload() -> None:
    step("A 200,000-row import is uploaded, and the import workers are SIGKILLed while it runs")
    r = upload(csv(200_000, "BIG-", bad_every=100), key=f"feed-{uuid.uuid4()}")
    ok(r.status == 202, "202 Accepted")
    at = wait_until(lambda: get(r.json()["id"]), lambda x: x["progress_pct"] > 0, timeout=60, every=0.2)
    note(f"progress when the workers are killed: {at['progress_pct']}% ({at['ok_rows']} rows done)")
    ok(at["status"] == "PROCESSING", "the import is mid-flight")
    open("/tmp/big", "w").write(r.json()["id"])


def big_check() -> None:
    step("Unacknowledged chunks were redelivered to the restarted workers; every number adds up")
    res = finished(open("/tmp/big").read(), timeout=300)
    note(f"{res['ok_rows']} imported + {res['error_rows']} rejected of {res['total_rows']}")
    ok(
        res["status"] == "COMPLETED_WITH_ERRORS" and (res["ok_rows"], res["error_rows"]) == (198_000, 2_000),
        "exactly 198,000 + 2,000: no chunk lost, none applied twice",
    )


def cancel() -> None:
    step("A running import can be cancelled; remaining chunks are skipped")
    r = upload(csv(100_000, "CANCEL-"), key=f"feed-{uuid.uuid4()}")
    c = http("POST", f"{API}/imports/{r.json()['id']}/cancel", headers=SELLER1)
    show(c, 200)
    ok(c.status in (200, 409), "cancel accepted (409 only if it had already finished)")
    time.sleep(3)
    after = get(r.json()["id"])
    expected = "CANCELLED" if c.status == 200 else "COMPLETED"
    ok(after["status"] == expected, f"status {after['status']}; its queued chunk tasks are skipped as they drain")
    done(
        "chunked parallel import, line-accurate errors, signed webhooks with retries, idempotency, isolation, worker kills, cancel."
    )


{"basics": basics, "webhook_retry": webhook_retry, "big_upload": big_upload, "big_check": big_check, "cancel": cancel}[
    sys.argv[1]
]()
with open("/tmp/next_step", "w") as f:
    f.write(str(next_step()))
