"""Webhook signing. Same scheme as Stripe: the receiver recomputes the HMAC over
"timestamp.body" and rejects stale timestamps, which stops forged and replayed calls."""

import hashlib
import hmac
import random
import time

SIGNATURE_HEADER = "X-Signature"
TOLERANCE_SECONDS = 300


def sign(secret: str, body: bytes, timestamp: int | None = None) -> str:
    ts = int(time.time()) if timestamp is None else timestamp
    mac = hmac.new(secret.encode(), f"{ts}.".encode() + body, hashlib.sha256).hexdigest()
    return f"t={ts},v1={mac}"


def verify(secret: str, body: bytes, header: str, now: int | None = None) -> bool:
    try:
        parts = dict(p.split("=", 1) for p in header.split(","))
        ts = int(parts["t"])
        given = parts["v1"]
    except (KeyError, ValueError):
        return False
    current = int(time.time()) if now is None else now
    if abs(current - ts) > TOLERANCE_SECONDS:
        return False
    expected = hmac.new(secret.encode(), f"{ts}.".encode() + body, hashlib.sha256).hexdigest()
    return hmac.compare_digest(expected, given)


def backoff_seconds(attempt: int, base: float = 5, cap: float = 3600) -> int:
    """Exponential backoff with full jitter: spreads retries so a recovering receiver
    is not hit by every sender at the same moment.

    Whole seconds, at least 1: Celery's native delayed delivery works in whole seconds and
    rejects a countdown that rounds to 0, and a rejected retry is dropped, not requeued."""
    return max(1, round(random.uniform(1, max(1.0, min(cap, base * 2**attempt)))))


def retryable_status(code: int) -> bool:
    """408, 425, 429 and 5xx may succeed later; other 4xx mean the request itself is wrong."""
    return code in (408, 425, 429) or code >= 500
