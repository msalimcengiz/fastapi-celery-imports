from catalog_import import webhooks
from catalog_import.config import Settings


def test_signature_round_trip_and_tampering() -> None:
    body = b'{"type":"import.finished"}'
    header = webhooks.sign("secret", body, timestamp=1_000)
    assert webhooks.verify("secret", body, header, now=1_010)
    assert not webhooks.verify("other", body, header, now=1_010)
    assert not webhooks.verify("secret", body + b" ", header, now=1_010)
    assert not webhooks.verify("secret", body, "garbage", now=1_010)


def test_old_signatures_are_rejected_to_stop_replays() -> None:
    body = b"{}"
    header = webhooks.sign("secret", body, timestamp=1_000)
    assert not webhooks.verify("secret", body, header, now=1_000 + webhooks.TOLERANCE_SECONDS + 1)


def test_backoff_grows_has_jitter_and_is_capped() -> None:
    samples = [webhooks.backoff_seconds(3, base=5, cap=3600) for _ in range(500)]
    assert all(1 <= s <= 40 for s in samples)
    assert len(set(samples)) > 20  # jittered, not a fixed delay
    assert all(webhooks.backoff_seconds(30, base=5, cap=60) <= 60 for _ in range(50))


def test_backoff_is_never_below_one_whole_second() -> None:
    # Celery rejects (and drops) a retry whose countdown rounds to 0: this lost webhooks once.
    samples = [webhooks.backoff_seconds(0, base=1, cap=60) for _ in range(1000)]
    assert all(isinstance(s, int) and s >= 1 for s in samples)


def test_which_responses_are_worth_retrying() -> None:
    assert all(webhooks.retryable_status(c) for c in (408, 425, 429, 500, 502, 503))
    assert not any(webhooks.retryable_status(c) for c in (400, 401, 403, 404, 410, 422))


def test_api_keys_map_to_sellers() -> None:
    s = Settings(api_keys="k1:seller-1, k2:seller-2,broken")
    assert s.sellers_by_key() == {"k1": "seller-1", "k2": "seller-2"}
