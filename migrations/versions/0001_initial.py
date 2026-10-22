"""Initial schema.

Revision ID: 0001
"""

from alembic import op

revision = "0001"
down_revision = None


def upgrade() -> None:
    op.execute("""
    CREATE TABLE imports (
        id              uuid        PRIMARY KEY,
        seller_id       text        NOT NULL,
        idempotency_key text        NOT NULL,
        status          text        NOT NULL CHECK (status IN (
                            'QUEUED', 'PROCESSING', 'COMPLETED', 'COMPLETED_WITH_ERRORS', 'FAILED', 'CANCELLED')),
        callback_url    text,
        file_sha256     text        NOT NULL,
        total_rows      int,
        ok_rows         int         NOT NULL DEFAULT 0,
        error_rows      int         NOT NULL DEFAULT 0,
        chunks_total    int,
        chunks_done     int         NOT NULL DEFAULT 0,
        error           text,
        created_at      timestamptz NOT NULL DEFAULT now(),
        updated_at      timestamptz NOT NULL DEFAULT now(),
        finished_at     timestamptz,
        UNIQUE (seller_id, idempotency_key)
    );
    CREATE INDEX imports_open_idx ON imports (updated_at) WHERE status IN ('QUEUED', 'PROCESSING');

    -- The uploaded file. In production this would be object storage (S3); the
    -- code reads and writes it in one place (storage.py).
    CREATE TABLE import_files (
        import_id uuid  PRIMARY KEY REFERENCES imports(id) ON DELETE CASCADE,
        content   bytea NOT NULL
    );

    CREATE TABLE import_chunks (
        import_id  uuid        NOT NULL REFERENCES imports(id) ON DELETE CASCADE,
        chunk_no   int         NOT NULL,
        first_line int         NOT NULL,
        payload    text        NOT NULL,
        status     text        NOT NULL DEFAULT 'PENDING' CHECK (status IN ('PENDING', 'DONE', 'FAILED', 'SKIPPED')),
        ok_rows    int,
        error_rows int,
        attempts   int         NOT NULL DEFAULT 0,
        error      text,
        updated_at timestamptz NOT NULL DEFAULT now(),
        PRIMARY KEY (import_id, chunk_no)
    );

    CREATE TABLE import_errors (
        import_id uuid NOT NULL,
        chunk_no  int  NOT NULL,
        line_no   int  NOT NULL,
        sku       text,
        error     text NOT NULL,
        PRIMARY KEY (import_id, line_no),
        FOREIGN KEY (import_id, chunk_no) REFERENCES import_chunks (import_id, chunk_no) ON DELETE CASCADE
    );

    -- The catalog. Rows from a newer import win over an older one, even if the
    -- older import's chunks are processed later.
    CREATE TABLE products (
        seller_id          text          NOT NULL,
        sku                text          NOT NULL,
        name               text          NOT NULL,
        price              numeric(12,2) NOT NULL CHECK (price >= 0),
        currency           text          NOT NULL,
        stock              int           NOT NULL CHECK (stock >= 0),
        barcode            text,
        source_import_id   uuid          NOT NULL,
        source_imported_at timestamptz   NOT NULL,
        updated_at         timestamptz   NOT NULL DEFAULT now(),
        PRIMARY KEY (seller_id, sku)
    );

    CREATE TABLE webhook_deliveries (
        id               uuid        PRIMARY KEY,
        import_id        uuid        NOT NULL UNIQUE REFERENCES imports(id) ON DELETE CASCADE,
        url              text        NOT NULL,
        status           text        NOT NULL DEFAULT 'PENDING' CHECK (status IN ('PENDING', 'DELIVERED', 'FAILED')),
        attempts         int         NOT NULL DEFAULT 0,
        last_status_code int,
        last_error       text,
        created_at       timestamptz NOT NULL DEFAULT now(),
        delivered_at     timestamptz
    );
    """)


def downgrade() -> None:
    op.execute("DROP TABLE webhook_deliveries, products, import_errors, import_chunks, import_files, imports")
