"""End-to-end orchestrator demonstrating offline scraping and SCD Type 2 tracking."""

from __future__ import annotations

import asyncio
import logging
import tempfile
from pathlib import Path
from typing import Dict, List

from src.scraper import AsyncProductScraper
from src.warehouse import PricingWarehouse, UpsertStats

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s | %(levelname)s | %(name)s | %(message)s",
)
logger = logging.getLogger("price_tracker.main")


def _write_mock_product_html(
    path: Path,
    title: str,
    current_price: str,
    original_price: str,
    stock_label: str,
    in_stock_attr: str,
) -> str:
    html = f"""<!DOCTYPE html>
<html lang="en">
<head><meta charset="utf-8"><title>{title}</title></head>
<body>
  <main>
    <h1 class="product-title">{title}</h1>
    <div class="pricing">
      <span class="current-price">{current_price}</span>
      <span class="original-price">{original_price}</span>
    </div>
    <p class="stock-status" data-in-stock="{in_stock_attr}">{stock_label}</p>
  </main>
</body>
</html>
"""
    path.write_text(html, encoding="utf-8")
    return path.resolve().as_uri()


def _print_summary(run_label: str, stats: UpsertStats, dlq_count: int) -> None:
    print(f"\n=== {run_label} ===")
    print(
        f"Raw Appended: {stats.raw_appended} | "
        f"Inserted: {stats.inserted} | "
        f"Updated (SCD2): {stats.updated} | "
        f"Unchanged: {stats.unchanged} | "
        f"DLQ Failures: {dlq_count}"
    )


async def run_pipeline() -> None:
    db_path = Path("pricing_warehouse.db")
    dlq_path = Path("dlq_failed_urls.json")
    if db_path.exists():
        db_path.unlink()

    warehouse = PricingWarehouse(db_path=db_path)
    await warehouse.initialize()

    scraper = AsyncProductScraper(
        max_concurrency=3,
        max_retries=3,
        base_backoff_seconds=0.5,
        dlq_path=dlq_path,
    )

    with tempfile.TemporaryDirectory(prefix="price_tracker_mocks_") as tmp_dir:
        tmp_path = Path(tmp_dir)
        sku_a_path = tmp_path / "sku_headphones.html"
        sku_b_path = tmp_path / "sku_keyboard.html"

        sku_a_url = _write_mock_product_html(
            sku_a_path,
            title="Studio Wireless ANC Headphones",
            current_price="₹ 1,299.00",
            original_price="₹ 2,499.00",
            stock_label="In Stock",
            in_stock_attr="true",
        )
        sku_b_url = _write_mock_product_html(
            sku_b_path,
            title="Mechanical Tactile Keyboard 75%",
            current_price="₹ 4,499.00",
            original_price="₹ 5,999.00",
            stock_label="In Stock",
            in_stock_attr="true",
        )

        targets: List[Dict[str, str]] = [
            {"product_id": "SKU-HEADPHONES-01", "url": sku_a_url},
            {"product_id": "SKU-KEYBOARD-02", "url": sku_b_url},
        ]

        # Run 1: Initial extraction & SCD Type 2 load
        records_run_1, dlq_run_1 = await scraper.scrape_targets(targets)
        stats_run_1 = await warehouse.upsert_price_records(records_run_1)
        _print_summary("Run 1: Initial Baseline Load", stats_run_1, len(dlq_run_1))

        # Mutate price on SKU-HEADPHONES-01; leave SKU-KEYBOARD-02 unchanged
        _write_mock_product_html(
            sku_a_path,
            title="Studio Wireless ANC Headphones",
            current_price="₹ 1,099.00",
            original_price="₹ 2,499.00",
            stock_label="In Stock",
            in_stock_attr="true",
        )

        # Run 2: Change Data Capture detects price drop on SKU-HEADPHONES-01 only
        records_run_2, dlq_run_2 = await scraper.scrape_targets(targets)
        stats_run_2 = await warehouse.upsert_price_records(records_run_2)
        _print_summary("Run 2: Post-Price-Change CDC Load", stats_run_2, len(dlq_run_2))

    history = await warehouse.fetch_scd_history()
    print("\n=== dim_product_pricing (SCD Type 2 History) ===")
    for row in history:
        print(
            f"SK={row['surrogate_key']} | {row['product_id']} | "
            f"{row['currency']} {row['current_price']} | disc={row['discount_pct']}% | "
            f"is_current={row['is_current']} | valid_from={row['valid_from']} | "
            f"valid_to={row['valid_to']} | hash={row['record_hash'][:12]}..."
        )


if __name__ == "__main__":
    asyncio.run(run_pipeline())
