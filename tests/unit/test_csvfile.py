from decimal import Decimal

import pytest

from catalog_import import csvfile

HEADER = "sku,name,price,currency,stock,barcode\n"


def test_splits_into_chunks_and_keeps_original_line_numbers() -> None:
    body = "".join(f"S{i},Name {i},10.00,TRY,1,\n" for i in range(25))
    chunks, total = csvfile.split((HEADER + body).encode(), rows_per_chunk=10)
    assert total == 25
    assert [c.first_line for c in chunks] == [2, 12, 22]
    assert all(c.payload.startswith("__line,sku,name,price") for c in chunks)
    rows, errors = csvfile.parse_chunk(chunks[2].payload)
    assert [r.sku for r in rows] == ["S20", "S21", "S22", "S23", "S24"] and errors == []


def test_blank_lines_are_skipped_and_line_numbers_stay_true() -> None:
    content = (HEADER + "A,a,1,TRY,1,\n\nB,b,x,TRY,1,\n").encode()
    chunks, total = csvfile.split(content, 100)
    assert total == 2
    _, errors = csvfile.parse_chunk(chunks[0].payload)
    assert [(e.line_no, e.sku) for e in errors] == [(4, "B")]


def test_line_numbers_survive_quoted_multiline_cells() -> None:
    content = (HEADER + 'A,"two\nlines",1,TRY,1,\nB,b,x,TRY,1,\n').encode()
    chunks, _ = csvfile.split(content, 100)
    _, errors = csvfile.parse_chunk(chunks[0].payload)
    assert [(e.line_no, e.sku) for e in errors] == [(4, "B")]


def test_accepts_bom_turkish_decimal_comma_and_lowercase_currency() -> None:
    content = ("﻿" + HEADER + 'A,Çay Bardağı,"1.234,50",try,5,8690000000001\n').encode()
    chunks, _ = csvfile.split(content, 100)
    rows, errors = csvfile.parse_chunk(chunks[0].payload)
    assert errors == []
    assert rows[0].price == Decimal("1234.50")
    assert rows[0].currency == "TRY"
    assert rows[0].name == "Çay Bardağı"


def test_reports_each_invalid_row_with_its_line_and_reason() -> None:
    content = (HEADER + "OK1,x,1,TRY,1,\nBAD SKU,x,1,TRY,1,\nB2,x,-5,TRY,1,\nB3,x,1,GBP,1,\nB4,x,1,TRY,-1,\n").encode()
    chunks, _ = csvfile.split(content, 100)
    rows, errors = csvfile.parse_chunk(chunks[0].payload)
    assert [r.sku for r in rows] == ["OK1"]
    assert [(e.line_no, e.sku) for e in errors] == [(3, "BAD SKU"), (4, "B2"), (5, "B3"), (6, "B4")]
    assert "sku" in errors[0].error and "price" in errors[1].error
    assert "currency" in errors[2].error and "stock" in errors[3].error


def test_duplicate_sku_in_one_file_last_row_wins() -> None:
    chunks, _ = csvfile.split((HEADER + "A,first,1,TRY,1,\nA,second,2,TRY,1,\n").encode(), 100)
    rows, _ = csvfile.parse_chunk(chunks[0].payload)
    assert [(r.sku, r.name) for r in rows] == [("A", "second")]


@pytest.mark.parametrize(
    ("content", "message"),
    [
        (b"", "empty"),
        (b"sku,name,price\nA,a,1\n", "missing columns: currency, stock"),
        (HEADER.encode(), "no data rows"),
        ("sku,name,price,currency,stock\n".encode("utf-16"), "UTF-8"),
    ],
)
def test_rejects_files_that_cannot_be_imported(content: bytes, message: str) -> None:
    with pytest.raises(csvfile.InvalidFile, match=message):
        csvfile.split(content, 100)
