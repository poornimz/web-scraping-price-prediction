"""SQLite Data Warehouse layer with append-only landing and SCD Type 2 history."""

from __future__ import annotations

from dataclasses import dataclass
from datetime import datetime, timezone
from pathlib import Path
from typing import Any, Dict, List

import aiosqlite

from src.models import ProductPriceRecord


@dataclass(frozen=True)
class UpsertStats:
    """Summary counters for a warehouse ingestion batch."""

    raw_appended: int
    inserted: int
    updated: int
    unchanged: int


class PricingWarehouse:
    """Async SQLite warehouse implementing raw snapshots and SCD Type 2 pricing history."""

    def __init__(self, db_path: Path = Path("pricing_warehouse.db")) -> None:
        self.db_path = Path(db_path)

    async def initialize(self) -> None:
        """Create warehouse tables and indexes idempotently."""
        self.db_path.parent.mkdir(parents=True, exist_ok=True)
        async with aiosqlite.connect(self.db_path) as db:
            await db.execute("PRAGMA journal_mode=WAL;")
            await db.execute(
                """
                CREATE TABLE IF NOT EXISTS raw_price_snapshots (
                    snapshot_id INTEGER PRIMARY KEY AUTOINCREMENT,
                    product_id TEXT NOT NULL,
                    title TEXT NOT NULL,
                    currency TEXT NOT NULL,
                    current_price TEXT NOT NULL,
                    original_price TEXT,
                    discount_pct REAL,
                    in_stock INTEGER NOT NULL,
                    source_url TEXT NOT NULL,
                    scraped_at TEXT NOT NULL,
                    record_hash TEXT NOT NULL,
                    ingested_at TEXT NOT NULL
                );
                """
            )
            await db.execute(
                """
                CREATE TABLE IF NOT EXISTS dim_product_pricing (
                    surrogate_key INTEGER PRIMARY KEY AUTOINCREMENT,
                    product_id TEXT NOT NULL,
                    title TEXT NOT NULL,
                    currency TEXT NOT NULL,
                    current_price TEXT NOT NULL,
                    original_price TEXT,
                    discount_pct REAL,
                    in_stock INTEGER NOT NULL,
                    source_url TEXT NOT NULL,
                    record_hash TEXT NOT NULL,
                    valid_from TEXT NOT NULL,
                    valid_to TEXT,
                    is_current INTEGER NOT NULL CHECK (is_current IN (0, 1))
                );
                """
            )
            await db.execute(
                """
                CREATE INDEX IF NOT EXISTS idx_dim_product_current
                ON dim_product_pricing (product_id, is_current);
                """
            )
            await db.commit()

    async def upsert_price_records(self, records: List[ProductPriceRecord]) -> UpsertStats:
        """Append raw snapshots and apply idempotent SCD Type 2 merges into dim_product_pricing."""
        if not records:
            return UpsertStats(raw_appended=0, inserted=0, updated=0, unchanged=0)

        raw_appended = 0
        inserted = 0
        updated = 0
        unchanged = 0
        ingested_at = datetime.now(timezone.utc).isoformat()

        async with aiosqlite.connect(self.db_path) as db:
            db.row_factory = aiosqlite.Row
            await db.execute("BEGIN IMMEDIATE;")
            try:
                for record in records:
                    scraped_iso = record.scraped_at.isoformat()
                    current_price_str = f"{record.current_price:.2f}"
                    original_price_str = (
                        f"{record.original_price:.2f}"
                        if record.original_price is not None
                        else None
                    )
                    in_stock_int = int(record.in_stock)

                    await db.execute(
                        """
                        INSERT INTO raw_price_snapshots (
                            product_id, title, currency, current_price, original_price,
                            discount_pct, in_stock, source_url, scraped_at, record_hash, ingested_at
                        ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?);
                        """,
                        (
                            record.product_id,
                            record.title,
                            record.currency,
                            current_price_str,
                            original_price_str,
                            record.discount_pct,
                            in_stock_int,
                            record.source_url,
                            scraped_iso,
                            record.record_hash,
                            ingested_at,
                        ),
                    )
                    raw_appended += 1

                    cursor = await db.execute(
                        """
                        SELECT surrogate_key, record_hash
                        FROM dim_product_pricing
                        WHERE product_id = ? AND is_current = 1
                        ORDER BY surrogate_key DESC
                        LIMIT 1;
                        """,
                        (record.product_id,),
                    )
                    active_row = await cursor.fetchone()
                    await cursor.close()

                    if active_row is None:
                        await db.execute(
                            """
                            INSERT INTO dim_product_pricing (
                                product_id, title, currency, current_price, original_price,
                                discount_pct, in_stock, source_url, record_hash,
                                valid_from, valid_to, is_current
                            ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, NULL, 1);
                            """,
                            (
                                record.product_id,
                                record.title,
                                record.currency,
                                current_price_str,
                                original_price_str,
                                record.discount_pct,
                                in_stock_int,
                                record.source_url,
                                record.record_hash,
                                scraped_iso,
                            ),
                        )
                        inserted += 1
                    elif active_row["record_hash"] == record.record_hash:
                        unchanged += 1
                    else:
                        await db.execute(
                            """
                            UPDATE dim_product_pricing
                            SET valid_to = ?, is_current = 0
                            WHERE surrogate_key = ?;
                            """,
                            (scraped_iso, active_row["surrogate_key"]),
                        )
                        await db.execute(
                            """
                            INSERT INTO dim_product_pricing (
                                product_id, title, currency, current_price, original_price,
                                discount_pct, in_stock, source_url, record_hash,
                                valid_from, valid_to, is_current
                            ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, NULL, 1);
                            """,
                            (
                                record.product_id,
                                record.title,
                                record.currency,
                                current_price_str,
                                original_price_str,
                                record.discount_pct,
                                in_stock_int,
                                record.source_url,
                                record.record_hash,
                                scraped_iso,
                            ),
                        )
                        updated += 1

                await db.commit()
            except Exception:
                await db.rollback()
                raise

        return UpsertStats(
            raw_appended=raw_appended,
            inserted=inserted,
            updated=updated,
            unchanged=unchanged,
        )

    async def fetch_scd_history(self) -> List[Dict[str, Any]]:
        """Return all SCD Type 2 rows ordered by product_id and surrogate_key."""
        async with aiosqlite.connect(self.db_path) as db:
            db.row_factory = aiosqlite.Row
            cursor = await db.execute(
                """
                SELECT surrogate_key, product_id, title, currency, current_price,
                       original_price, discount_pct, in_stock, source_url,
                       record_hash, valid_from, valid_to, is_current
                FROM dim_product_pricing
                ORDER BY product_id ASC, surrogate_key ASC;
                """
            )
            rows = await cursor.fetchall()
            await cursor.close()
            return [dict(row) for row in rows]
