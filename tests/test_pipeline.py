"""Pytest unit tests for price sanitization, discount math, CDC hashing, and SCD Type 2."""

from __future__ import annotations

import asyncio
from datetime import datetime, timedelta, timezone
from decimal import Decimal
from pathlib import Path

import pytest
from pydantic import ValidationError

from src.models import ProductPriceRecord, sanitize_price_value
from src.warehouse import PricingWarehouse


def test_dirty_price_string_parsing() -> None:
    assert sanitize_price_value("₹ 1,299.00") == Decimal("1299.00")
    assert sanitize_price_value("$ 2,499.50") == Decimal("2499.50")
    assert sanitize_price_value(" € 89.95 ") == Decimal("89.95")
    assert sanitize_price_value("") is None
    assert sanitize_price_value("Unavailable") is None
    assert sanitize_price_value(None) is None

    record = ProductPriceRecord(
        product_id="SKU-100",
        title="Noise Cancelling Earbuds",
        currency="INR",
        current_price="₹ 1,299.00",  # type: ignore[arg-type]
        original_price="₹ 2,598.00",  # type: ignore[arg-type]
        in_stock=True,
        source_url="https://example.com/sku-100",
    )
    assert record.current_price == Decimal("1299.00")
    assert record.original_price == Decimal("2598.00")
    assert record.discount_pct == 50.0


def test_current_price_rejects_invalid_or_non_positive() -> None:
    with pytest.raises(ValidationError):
        ProductPriceRecord(
            product_id="SKU-BAD",
            title="Broken Item",
            current_price="Not A Price",  # type: ignore[arg-type]
            in_stock=True,
            source_url="https://example.com/bad",
        )

    with pytest.raises(ValidationError):
        ProductPriceRecord(
            product_id="SKU-ZERO",
            title="Zero Price Item",
            current_price="₹ 0.00",  # type: ignore[arg-type]
            in_stock=True,
            source_url="https://example.com/zero",
        )


def test_missing_and_empty_discount_fields() -> None:
    record_missing = ProductPriceRecord(
        product_id="SKU-200",
        title="USB-C Cable",
        current_price="₹ 499.00",  # type: ignore[arg-type]
        original_price="",  # type: ignore[arg-type]
        discount_pct="",  # type: ignore[arg-type]
        in_stock=True,
        source_url="https://example.com/sku-200",
    )
    assert record_missing.original_price is None
    assert record_missing.discount_pct is None

    record_no_discount = ProductPriceRecord(
        product_id="SKU-201",
        title="Desk Mat",
        current_price="₹ 799.00",  # type: ignore[arg-type]
        original_price="₹ 799.00",  # type: ignore[arg-type]
        in_stock=True,
        source_url="https://example.com/sku-201",
    )
    assert record_no_discount.discount_pct is None


def test_sha256_hash_consistency() -> None:
    r1 = ProductPriceRecord(
        product_id="SKU-HASH",
        title="Monitor Stand",
        current_price="₹ 1,500.00",  # type: ignore[arg-type]
        in_stock=True,
        source_url="https://example.com/stand-a",
        scraped_at=datetime(2026, 1, 1, 10, 0, tzinfo=timezone.utc),
    )
    r2 = ProductPriceRecord(
        product_id="SKU-HASH",
        title="Monitor Stand Updated Title",
        current_price=Decimal("1500"),
        in_stock=True,
        source_url="https://example.com/stand-b",
        scraped_at=datetime(2026, 1, 2, 10, 0, tzinfo=timezone.utc),
    )
    r3 = ProductPriceRecord(
        product_id="SKU-HASH",
        title="Monitor Stand",
        current_price=Decimal("1399.00"),
        in_stock=True,
        source_url="https://example.com/stand-a",
    )
    r4 = ProductPriceRecord(
        product_id="SKU-HASH",
        title="Monitor Stand",
        current_price=Decimal("1500.00"),
        in_stock=False,
        source_url="https://example.com/stand-a",
    )

    assert len(r1.record_hash) == 64
    assert r1.record_hash == r2.record_hash
    assert r1.record_hash != r3.record_hash
    assert r1.record_hash != r4.record_hash


def test_scd_type_2_version_updates(tmp_path: Path) -> None:
    async def _exercise_warehouse() -> None:
        db_file = tmp_path / "test_warehouse.db"
        warehouse = PricingWarehouse(db_path=db_file)
        await warehouse.initialize()

        t1 = datetime(2026, 5, 1, 8, 0, tzinfo=timezone.utc)
        t2 = t1 + timedelta(hours=1)
        t3 = t1 + timedelta(hours=2)

        initial_record = ProductPriceRecord(
            product_id="SKU-SCD-1",
            title="Ergonomic Mouse",
            current_price="₹ 2,499.00",  # type: ignore[arg-type]
            original_price="₹ 3,499.00",  # type: ignore[arg-type]
            in_stock=True,
            source_url="file:///tmp/mouse.html",
            scraped_at=t1,
        )
        stats1 = await warehouse.upsert_price_records([initial_record])
        assert stats1.inserted == 1
        assert stats1.updated == 0
        assert stats1.unchanged == 0

        # Identical price & stock at later timestamp -> idempotent no-op in SCD2 table
        same_record = ProductPriceRecord(
            product_id="SKU-SCD-1",
            title="Ergonomic Mouse",
            current_price="₹ 2,499.00",  # type: ignore[arg-type]
            original_price="₹ 3,499.00",  # type: ignore[arg-type]
            in_stock=True,
            source_url="file:///tmp/mouse.html",
            scraped_at=t2,
        )
        stats2 = await warehouse.upsert_price_records([same_record])
        assert stats2.inserted == 0
        assert stats2.updated == 0
        assert stats2.unchanged == 1

        # Price drops -> closes prior version and opens new active version
        changed_record = ProductPriceRecord(
            product_id="SKU-SCD-1",
            title="Ergonomic Mouse",
            current_price="₹ 1,999.00",  # type: ignore[arg-type]
            original_price="₹ 3,499.00",  # type: ignore[arg-type]
            in_stock=True,
            source_url="file:///tmp/mouse.html",
            scraped_at=t3,
        )
        stats3 = await warehouse.upsert_price_records([changed_record])
        assert stats3.inserted == 0
        assert stats3.updated == 1
        assert stats3.raw_appended == 1

        history = await warehouse.fetch_scd_history()
        assert len(history) == 2

        closed_row, active_row = history[0], history[1]
        assert closed_row["is_current"] == 0
        assert closed_row["current_price"] == "2499.00"
        assert closed_row["valid_from"] == t1.isoformat()
        assert closed_row["valid_to"] == t3.isoformat()

        assert active_row["is_current"] == 1
        assert active_row["current_price"] == "1999.00"
        assert active_row["valid_from"] == t3.isoformat()
        assert active_row["valid_to"] is None

    asyncio.run(_exercise_warehouse())
