"""A webhook receiver for the demo, the way a seller's system would implement one:
verify the signature, dedupe on X-Delivery-Id, answer 2xx. POST /control {"fail": N}
makes it answer 503 to the next N deliveries. GET /received lists what arrived."""

import hashlib
import hmac
import json
import os
import threading
import time
from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer

SECRET = os.environ.get("WEBHOOK_SECRET", "dev-webhook-secret").encode()
lock = threading.Lock()
received: list[dict] = []
seen: set[str] = set()
fail_next = 0


def valid(body: bytes, header: str) -> bool:
    try:
        parts = dict(p.split("=", 1) for p in header.split(","))
        ts, given = int(parts["t"]), parts["v1"]
    except (KeyError, ValueError):
        return False
    if abs(time.time() - ts) > 300:
        return False
    expected = hmac.new(SECRET, f"{ts}.".encode() + body, hashlib.sha256).hexdigest()
    return hmac.compare_digest(expected, given)


class Handler(BaseHTTPRequestHandler):
    def _json(self, code: int, obj: object) -> None:
        body = json.dumps(obj).encode()
        self.send_response(code)
        self.send_header("Content-Type", "application/json")
        self.end_headers()
        self.wfile.write(body)

    def do_POST(self) -> None:
        global fail_next
        body = self.rfile.read(int(self.headers.get("Content-Length", 0)))
        if self.path == "/control":
            with lock:
                fail_next = json.loads(body)["fail"]
            return self._json(200, {"fail_next": fail_next})
        with lock:
            delivery = self.headers.get("X-Delivery-Id", "")
            entry = {
                "delivery_id": delivery,
                "signature_valid": valid(body, self.headers.get("X-Signature", "")),
                "duplicate": delivery in seen,
                "body": json.loads(body),
                "answered": 200,
            }
            if fail_next > 0:
                fail_next -= 1
                entry["answered"] = 503
            else:
                seen.add(delivery)
            received.append(entry)
        self._json(entry["answered"], {"ok": entry["answered"] == 200})

    def do_GET(self) -> None:
        with lock:
            self._json(200, received)

    def log_message(self, *args: object) -> None:
        pass


ThreadingHTTPServer(("0.0.0.0", 9000), Handler).serve_forever()  # noqa: S104
