FROM python:3.13-slim AS base
COPY --from=ghcr.io/astral-sh/uv:0.9 /uv /usr/local/bin/uv
ENV UV_COMPILE_BYTECODE=1 UV_LINK_MODE=copy PYTHONUNBUFFERED=1
WORKDIR /app
COPY pyproject.toml uv.lock ./
RUN uv sync --frozen --no-dev --no-install-project
COPY src ./src
COPY migrations ./migrations
COPY alembic.ini ./
RUN uv sync --frozen --no-dev

FROM base AS dev
RUN uv sync --frozen

FROM base AS prod
RUN useradd --system --uid 10001 app && mkdir -p /tmp/metrics && chown app /tmp/metrics
USER app
ENV PATH="/app/.venv/bin:$PATH" PROMETHEUS_MULTIPROC_DIR=/tmp/metrics
EXPOSE 8000
CMD ["uvicorn", "catalog_import.api:app", "--host", "0.0.0.0", "--port", "8000"]
