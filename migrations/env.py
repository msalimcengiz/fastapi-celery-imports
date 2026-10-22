from alembic import context
from sqlalchemy import create_engine

from catalog_import.config import settings

connectable = create_engine(settings().database_url)
with connectable.connect() as connection:
    context.configure(connection=connection)
    with context.begin_transaction():
        context.run_migrations()
