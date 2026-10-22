"""CSV parsing, row validation and chunking. Pure functions, no I/O."""

import csv
import io
from dataclasses import dataclass
from decimal import Decimal

from pydantic import BaseModel, ConfigDict, Field, ValidationError, field_validator

REQUIRED_COLUMNS = ("sku", "name", "price", "currency", "stock")
OPTIONAL_COLUMNS = ("barcode",)
CURRENCIES = {"TRY", "USD", "EUR"}


class ProductRow(BaseModel):
    model_config = ConfigDict(str_strip_whitespace=True, extra="ignore")

    sku: str = Field(min_length=1, max_length=64, pattern=r"^[A-Za-z0-9._-]+$")
    name: str = Field(min_length=1, max_length=255)
    price: Decimal = Field(ge=0, max_digits=12, decimal_places=2)
    currency: str
    stock: int = Field(ge=0, le=10_000_000)
    barcode: str | None = Field(default=None, max_length=32)

    @field_validator("price", mode="before")
    @classmethod
    def accept_decimal_comma(cls, v: object) -> object:
        # Turkish exports often write 1.234,56
        if isinstance(v, str) and "," in v:
            return v.replace(".", "").replace(",", ".")
        return v

    @field_validator("currency")
    @classmethod
    def known_currency(cls, v: str) -> str:
        v = v.upper()
        if v not in CURRENCIES:
            raise ValueError(f"currency must be one of {sorted(CURRENCIES)}")
        return v

    @field_validator("barcode", mode="before")
    @classmethod
    def empty_is_none(cls, v: object) -> object:
        return None if v == "" else v


class InvalidFile(ValueError):
    """The file as a whole cannot be imported (encoding, missing columns, empty)."""


LINE_COLUMN = "__line"


@dataclass(frozen=True)
class Chunk:
    number: int
    first_line: int  # 1-based line number in the original file of the chunk's first data row
    # CSV text with a header row. Each row starts with its original line number, so errors
    # point at the right line even when the file has blank lines or quoted multi-line cells.
    payload: str


@dataclass(frozen=True)
class RowError:
    line_no: int
    sku: str | None
    error: str


def decode(content: bytes) -> str:
    try:
        text = content.decode("utf-8-sig")  # tolerate the BOM Excel adds
    except UnicodeDecodeError as e:
        raise InvalidFile("file must be UTF-8 encoded") from e
    if not text.strip():
        raise InvalidFile("file is empty")
    return text


def split(content: bytes, rows_per_chunk: int) -> tuple[list[Chunk], int]:
    """Validates the header and splits the data rows into chunks. Returns (chunks, total_rows)."""
    reader = csv.reader(io.StringIO(decode(content)))
    try:
        header = [h.strip().lower() for h in next(reader)]
    except StopIteration as e:
        raise InvalidFile("file has no header row") from e
    missing = [c for c in REQUIRED_COLUMNS if c not in header]
    if missing:
        raise InvalidFile(f"missing columns: {', '.join(missing)}")

    chunks: list[Chunk] = []
    buf: list[list[str]] = []
    first_line = 2
    total = 0
    line = reader.line_num
    for row in reader:
        start, line = line + 1, reader.line_num
        if not any(cell.strip() for cell in row):
            continue
        if not buf:
            first_line = start
        buf.append([str(start), *row])
        total += 1
        if len(buf) == rows_per_chunk:
            chunks.append(_chunk(len(chunks), first_line, header, buf))
            buf = []
    if buf:
        chunks.append(_chunk(len(chunks), first_line, header, buf))
    if total == 0:
        raise InvalidFile("file has no data rows")
    return chunks, total


def _chunk(number: int, first_line: int, header: list[str], rows: list[list[str]]) -> Chunk:
    out = io.StringIO()
    w = csv.writer(out, lineterminator="\n")
    w.writerow([LINE_COLUMN, *header])
    w.writerows(rows)
    return Chunk(number=number, first_line=first_line, payload=out.getvalue())


def parse_chunk(chunk_payload: str) -> tuple[list[ProductRow], list[RowError]]:
    """Validates every row. Duplicate SKUs inside one file: the last occurrence wins."""
    reader = csv.DictReader(io.StringIO(chunk_payload))
    valid: dict[str, ProductRow] = {}
    errors: list[RowError] = []
    for raw in reader:
        line_no = int(raw.pop(LINE_COLUMN))
        try:
            row = ProductRow.model_validate(raw)
        except ValidationError as e:
            msg = "; ".join(f"{'.'.join(str(p) for p in err['loc'])}: {err['msg']}" for err in e.errors())
            errors.append(RowError(line_no=line_no, sku=(raw.get("sku") or None), error=msg))
            continue
        valid[row.sku] = row
    return list(valid.values()), errors
