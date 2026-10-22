"""Where uploaded files live. Postgres here, so the demo needs no extra service; swapping
in S3 means changing these two functions (the import id becomes the object key)."""

from sqlalchemy import Connection, text


def write(conn: Connection, import_id: str, content: bytes) -> None:
    conn.execute(
        text("INSERT INTO import_files (import_id, content) VALUES (:id, :content)"),
        {"id": import_id, "content": content},
    )


def read(conn: Connection, import_id: str) -> bytes:
    content: bytes = conn.execute(
        text("SELECT content FROM import_files WHERE import_id = :id"), {"id": import_id}
    ).scalar_one()
    return bytes(content)
