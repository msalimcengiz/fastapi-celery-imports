"""Integration tests run against the docker compose stack (API, workers, beat, RabbitMQ,
Postgres, Redis) from a container on the same network; see `make test-integration`."""

import json
import os
import threading
import time
import uuid
from collections.abc import Callable, Iterator
from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer
from typing import Any

import httpx
import pytest
from sqlalchemy import create_engine, text

API = os.environ.get("API_URL", "http://api:8000")
AUTH = {"Authorization": "Bearer dev-key"}


@pytest.fixture(scope="session")
def db() -> Any:
    return create_engine(os.environ["DATABASE_URL"])


def make_csv(rows: int, *, bad_every: int = 0, prefix: str = "S", price: str = "10.00") -> bytes:
    lines = ["sku,name,price,currency,stock"]
    for i in range(rows):
        p = "not-a-price" if bad_every and i % bad_every == 0 else price
        lines.append(f"{prefix}{i},Product {i},{p},TRY,{i % 50}")
    return ("\n".join(lines) + "\n").encode()


def upload(
    content: bytes, key: str | None = None, callback: str | None = None, auth: dict[str, str] = AUTH
) -> httpx.Response:
    data = {"callback_url": callback} if callback else {}
    return httpx.post(
        f"{API}/v1/imports",
        headers={**auth, "Idempotency-Key": key or str(uuid.uuid4())},
        files={"file": ("products.csv", content, "text/csv")},
        data=data,
        timeout=30,
    )


def wait_for(
    import_id: str, done: Callable[[dict[str, Any]], bool] | None = None, timeout: float = 60
) -> dict[str, Any]:
    check = done or (lambda r: r["status"] not in ("QUEUED", "PROCESSING"))
    deadline = time.time() + timeout
    while time.time() < deadline:
        r = httpx.get(f"{API}/v1/imports/{import_id}", headers=AUTH).json()
        if check(r):
            return r
        time.sleep(0.3)
    raise AssertionError(f"import {import_id} did not finish: {r}")


class Receiver:
    """Webhook endpoint. Answers with the queued status codes first, then 200."""

    def __init__(self) -> None:
        self.calls: list[dict[str, Any]] = []
        self.script: list[int] = []
        self.lock = threading.Lock()

    def reset(self, script: list[int]) -> None:
        with self.lock:
            self.calls.clear()
            self.script = list(script)


@pytest.fixture(scope="session")
def receiver() -> Iterator[Receiver]:
    rec = Receiver()

    class Handler(BaseHTTPRequestHandler):
        def do_POST(self) -> None:
            body = self.rfile.read(int(self.headers["Content-Length"]))
            with rec.lock:
                rec.calls.append({"headers": dict(self.headers), "body": body, "json": json.loads(body)})
                code = rec.script.pop(0) if rec.script else 200
            self.send_response(code)
            self.end_headers()

        def log_message(self, *args: object) -> None:
            pass

    server = ThreadingHTTPServer(("0.0.0.0", 9999), Handler)  # noqa: S104
    threading.Thread(target=server.serve_forever, daemon=True).start()
    yield rec
    server.shutdown()


@pytest.fixture
def sql(db: Any) -> Callable[..., Any]:
    def run(query: str, **params: Any) -> Any:
        with db.begin() as conn:
            return conn.execute(text(query), params)

    return run
