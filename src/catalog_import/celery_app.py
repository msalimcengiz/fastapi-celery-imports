"""Celery configuration. Each setting below fixes a specific way background jobs get
lost or duplicated in production; the comments say which."""

import os

from celery import Celery
from celery.signals import worker_init, worker_ready
from kombu import Exchange, Queue
from prometheus_client import CollectorRegistry, multiprocess, start_http_server

from .config import settings

app = Celery("catalog_import", broker=settings().broker_url, backend=settings().result_backend)

exchange = Exchange("catalog", type="topic")

app.conf.update(
    # Durable, replicated RabbitMQ queues. Quorum queues also make RabbitMQ track how
    # often a message was redelivered, and enable native delayed delivery for retries,
    # so a retry with countdown does not hold a worker slot while it waits.
    task_default_queue_type="quorum",
    task_default_exchange="catalog",
    task_default_exchange_type="topic",
    task_default_queue="default",
    task_default_routing_key="default",
    task_queues=(
        Queue("default", exchange, routing_key="default", queue_arguments={"x-queue-type": "quorum"}),
        Queue("imports", exchange, routing_key="imports", queue_arguments={"x-queue-type": "quorum"}),
        Queue("webhooks", exchange, routing_key="webhooks", queue_arguments={"x-queue-type": "quorum"}),
    ),
    # Heavy chunk work and slow outbound HTTP get separate queues and separate worker
    # pools, so a slow webhook receiver can never starve imports (or the reverse).
    task_routes={
        "catalog_import.tasks.plan_import": {"queue": "imports", "routing_key": "imports"},
        "catalog_import.tasks.process_chunk": {"queue": "imports", "routing_key": "imports"},
        "catalog_import.tasks.finalize_import": {"queue": "imports", "routing_key": "imports"},
        "catalog_import.tasks.deliver_webhook": {"queue": "webhooks", "routing_key": "webhooks"},
    },
    # Publisher confirms: .delay() returns only after RabbitMQ has the message.
    broker_transport_options={"confirm_publish": True},
    broker_connection_retry_on_startup=True,
    # Acknowledge after the task finishes, not when it is received. If the worker is
    # killed mid-task, RabbitMQ redelivers it to another worker. Tasks are written to be
    # idempotent because of this.
    task_acks_late=True,
    task_reject_on_worker_lost=True,
    # Take one message at a time; with acks_late a large prefetch would hide queued work
    # inside one busy worker while others sit idle.
    worker_prefetch_multiplier=1,
    # A hung task cannot block a worker forever. Soft limit raises inside the task first.
    task_soft_time_limit=240,
    task_time_limit=300,
    # Recycle worker processes to contain slow memory growth.
    worker_max_tasks_per_child=500,
    task_serializer="json",
    accept_content=["json"],
    result_expires=24 * 3600,
    task_track_started=True,
    worker_send_task_events=True,
    task_send_sent_event=True,
    timezone="UTC",
    beat_schedule={
        "sweep-stuck-imports": {"task": "catalog_import.tasks.sweep_stuck_imports", "schedule": 30.0},
        "cleanup": {"task": "catalog_import.tasks.cleanup", "schedule": 3600.0},
    },
)

app.autodiscover_tasks(["catalog_import"], related_name="tasks", force=True)


@worker_init.connect
def _declare_all_queues(**_: object) -> None:
    """Declare every queue before the worker starts consuming. A worker started with
    `-Q imports` would otherwise only declare its own queue, and binding the other queues
    to RabbitMQ's delayed-delivery exchanges (used for retries with a countdown) fails
    until some other worker has created them. Declaring is idempotent."""
    with app.connection_for_write() as conn:
        for queue in app.conf.task_queues:
            queue.bind(conn.default_channel).declare()


@worker_ready.connect
def _serve_metrics(**_: object) -> None:
    """Prefork workers run tasks in child processes; prometheus_client's multiprocess
    mode (PROMETHEUS_MULTIPROC_DIR) collects their counters, served from the parent."""
    port = os.environ.get("METRICS_PORT")
    if port and os.environ.get("PROMETHEUS_MULTIPROC_DIR"):
        registry = CollectorRegistry()
        multiprocess.MultiProcessCollector(registry)
        start_http_server(int(port), registry=registry)
