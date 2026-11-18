"""Resilient async Playwright web scraper with bounded concurrency, retries, and DLQ."""

from __future__ import annotations

import asyncio
import json
import logging
from datetime import datetime, timezone
from pathlib import Path
from typing import Any, Dict, List, Optional, Tuple

from playwright.async_api import (
    Browser,
    Error as PlaywrightError,
    Page,
    TimeoutError as PlaywrightTimeoutError,
    async_playwright,
)
from pydantic import ValidationError

from src.models import ProductPriceRecord

logger = logging.getLogger(__name__)

_CURRENCY_SYMBOL_MAP: Dict[str, str] = {
    "₹": "INR",
    "$": "USD",
    "€": "EUR",
    "£": "GBP",
}


def _detect_currency(raw_price: Optional[str], fallback: str = "INR") -> str:
    if not raw_price:
        return fallback
    for symbol, code in _CURRENCY_SYMBOL_MAP.items():
        if symbol in raw_price:
            return code
    return fallback


def _parse_stock_status(raw_stock: Optional[str], data_attr: Optional[str]) -> bool:
    if data_attr is not None:
        normalized_attr = data_attr.strip().lower()
        if normalized_attr in {"true", "1", "yes", "in_stock"}:
            return True
        if normalized_attr in {"false", "0", "no", "out_of_stock"}:
            return False

    if not raw_stock:
        return True

    text = raw_stock.strip().lower()
    negative_markers = ("out of stock", "sold out", "unavailable", "currently unavailable")
    return not any(marker in text for marker in negative_markers)


class AsyncProductScraper:
    """Async Playwright scraper supporting live URLs, local file:// paths, retries, and DLQ."""

    def __init__(
        self,
        max_concurrency: int = 3,
        max_retries: int = 3,
        base_backoff_seconds: float = 1.0,
        timeout_ms: int = 15000,
        dlq_path: Path = Path("dlq_failed_urls.json"),
        headless: bool = True,
    ) -> None:
        if max_concurrency < 1:
            raise ValueError("max_concurrency must be >= 1")
        if max_retries < 1:
            raise ValueError("max_retries must be >= 1")

        self.max_concurrency = max_concurrency
        self.max_retries = max_retries
        self.base_backoff_seconds = base_backoff_seconds
        self.timeout_ms = timeout_ms
        self.dlq_path = Path(dlq_path)
        self.headless = headless

    async def scrape_targets(
        self, targets: List[Dict[str, str]]
    ) -> Tuple[List[ProductPriceRecord], List[Dict[str, Any]]]:
        """Scrape all targets concurrently using a semaphore and route failures to DLQ."""
        if not targets:
            return [], []

        semaphore = asyncio.Semaphore(self.max_concurrency)
        records: List[ProductPriceRecord] = []
        dlq_entries: List[Dict[str, Any]] = []

        async with async_playwright() as pw:
            browser = await pw.chromium.launch(headless=self.headless)
            try:
                tasks = [
                    self._scrape_with_semaphore(semaphore, browser, target)
                    for target in targets
                ]
                results = await asyncio.gather(*tasks)
            finally:
                await browser.close()

        for record, dlq_entry in results:
            if record is not None:
                records.append(record)
            if dlq_entry is not None:
                dlq_entries.append(dlq_entry)

        if dlq_entries:
            self._append_to_dlq(dlq_entries)

        return records, dlq_entries

    async def _scrape_with_semaphore(
        self,
        semaphore: asyncio.Semaphore,
        browser: Browser,
        target: Dict[str, str],
    ) -> Tuple[Optional[ProductPriceRecord], Optional[Dict[str, Any]]]:
        async with semaphore:
            return await self._scrape_single_with_retry(browser, target)

    async def _scrape_single_with_retry(
        self,
        browser: Browser,
        target: Dict[str, str],
    ) -> Tuple[Optional[ProductPriceRecord], Optional[Dict[str, Any]]]:
        product_id = str(target.get("product_id", "")).strip()
        url = str(target.get("url", "")).strip()
        if not product_id or not url:
            dlq_entry = self._build_dlq_entry(
                product_id=product_id or "UNKNOWN",
                url=url or "",
                attempts=0,
                error="Missing required 'product_id' or 'url' in target definition.",
            )
            return None, dlq_entry

        last_error: str = "Unknown scraping error"

        for attempt in range(1, self.max_retries + 1):
            context = await browser.new_context()
            page = await context.new_page()
            try:
                await page.goto(url, wait_until="domcontentloaded", timeout=self.timeout_ms)
                extracted = await self._extract_dom_fields(page)
                record = ProductPriceRecord(
                    product_id=product_id,
                    title=extracted["title"],
                    currency=_detect_currency(extracted["current_price_raw"]),
                    current_price=extracted["current_price_raw"],
                    original_price=extracted["original_price_raw"],
                    in_stock=extracted["in_stock"],
                    source_url=url,
                    scraped_at=datetime.now(timezone.utc),
                )
                return record, None
            except (PlaywrightTimeoutError, PlaywrightError, ValidationError, ValueError) as exc:
                last_error = f"{type(exc).__name__}: {exc}"
                logger.warning(
                    "Attempt %d/%d failed for product_id=%s url=%s: %s",
                    attempt,
                    self.max_retries,
                    product_id,
                    url,
                    last_error,
                )
                if attempt < self.max_retries:
                    backoff = self.base_backoff_seconds * (2 ** (attempt - 1))
                    await asyncio.sleep(backoff)
            finally:
                await context.close()

        return None, self._build_dlq_entry(
            product_id=product_id,
            url=url,
            attempts=self.max_retries,
            error=last_error,
        )

    async def _extract_dom_fields(self, page: Page) -> Dict[str, Any]:
        title = await self._first_text(
            page,
            [
                "[data-testid='product-title']",
                "#productTitle",
                ".product-title",
                "h1",
                "title",
            ],
        )
        current_price_raw = await self._first_text(
            page,
            [
                "[data-testid='current-price']",
                ".current-price",
                ".sale-price",
                ".price",
            ],
        )
        original_price_raw = await self._first_text(
            page,
            [
                "[data-testid='original-price']",
                ".original-price",
                ".mrp",
                ".list-price",
            ],
        )
        stock_text = await self._first_text(
            page,
            [
                "[data-testid='stock-status']",
                ".stock-status",
                "#availability",
            ],
        )
        stock_attr: Optional[str] = None
        stock_locator = page.locator("[data-in-stock]").first
        if await stock_locator.count() > 0:
            stock_attr = await stock_locator.get_attribute("data-in-stock")

        if not title:
            raise ValueError("Unable to extract product title from DOM")
        if not current_price_raw:
            raise ValueError("Unable to extract current price from DOM")

        return {
            "title": title,
            "current_price_raw": current_price_raw,
            "original_price_raw": original_price_raw,
            "in_stock": _parse_stock_status(stock_text, stock_attr),
        }

    async def _first_text(self, page: Page, selectors: List[str]) -> Optional[str]:
        for selector in selectors:
            locator = page.locator(selector).first
            if await locator.count() > 0:
                text = await locator.inner_text()
                cleaned = text.strip()
                if cleaned:
                    return cleaned
        return None

    @staticmethod
    def _build_dlq_entry(
        product_id: str,
        url: str,
        attempts: int,
        error: str,
    ) -> Dict[str, Any]:
        return {
            "product_id": product_id,
            "url": url,
            "attempts": attempts,
            "error_message": error,
            "failed_at_utc": datetime.now(timezone.utc).isoformat(),
        }

    def _append_to_dlq(self, entries: List[Dict[str, Any]]) -> None:
        existing: List[Dict[str, Any]] = []
        if self.dlq_path.exists():
            try:
                raw_content = self.dlq_path.read_text(encoding="utf-8").strip()
                if raw_content:
                    loaded = json.loads(raw_content)
                    if isinstance(loaded, list):
                        existing = loaded
            except (json.JSONDecodeError, OSError) as exc:
                logger.warning("Resetting unreadable DLQ file %s: %s", self.dlq_path, exc)

        existing.extend(entries)
        self.dlq_path.parent.mkdir(parents=True, exist_ok=True)
        self.dlq_path.write_text(json.dumps(existing, indent=2), encoding="utf-8")
