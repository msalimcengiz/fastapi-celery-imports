from collections.abc import Iterator
from contextlib import contextmanager
from functools import lru_cache

from sqlalchemy import Connection, Engine, create_engine

from .config import settings


@lru_cache
def engine() -> Engine:
    # pool_pre_ping: a worker that sat idle while Postgres restarted gets a fresh connection.
    return create_engine(settings().database_url, pool_size=10, max_overflow=10, pool_pre_ping=True)


@contextmanager
def transaction() -> Iterator[Connection]:
    with engine().begin() as conn:
        yield conn
