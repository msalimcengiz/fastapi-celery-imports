"""Track when a webhook delivery is due, so the sweeper can find lost retries.

Revision ID: 0002
"""

from alembic import op

revision = "0002"
down_revision = "0001"


def upgrade() -> None:
    op.execute("ALTER TABLE webhook_deliveries ADD COLUMN next_attempt_at timestamptz NOT NULL DEFAULT now()")
    op.execute(
        "CREATE INDEX webhook_deliveries_due_idx ON webhook_deliveries (next_attempt_at) WHERE status = 'PENDING'"
    )


def downgrade() -> None:
    op.execute("ALTER TABLE webhook_deliveries DROP COLUMN next_attempt_at")
