"""Pydantic domain models, dirty-price sanitizers, and CDC hashing for product pricing."""

from __future__ import annotations

import hashlib
import re
from datetime import datetime, timezone
from decimal import Decimal, InvalidOperation
from typing import Any, Optional

from pydantic import BaseModel, Field, field_validator, model_validator

_CURRENCY_CLEANUP_RE = re.compile(r"[₹$€£¥,\s]+")
_NUMERIC_EXTRACT_RE = re.compile(r"^-?\d+(?:\.\d+)?$")


def sanitize_price_value(raw_value: Any) -> Optional[Decimal]:
    """Strip currency symbols, commas, and whitespace and convert safely to Decimal.

    Returns None for empty, null, or unparseable values without raising.
    """
    if raw_value is None or isinstance(raw_value, bool):
        return None

    if isinstance(raw_value, Decimal):
        return raw_value if raw_value.is_finite() else None

    if isinstance(raw_value, (int, float)):
        try:
            converted = Decimal(str(raw_value))
            return converted if converted.is_finite() else None
        except (InvalidOperation, ValueError):
            return None

    if isinstance(raw_value, str):
        cleaned = _CURRENCY_CLEANUP_RE.sub("", raw_value.strip())
        if not cleaned or not _NUMERIC_EXTRACT_RE.match(cleaned):
            return None
        try:
            return Decimal(cleaned)
        except InvalidOperation:
            return None

    return None


class ProductPriceRecord(BaseModel):
    """Validated product pricing snapshot with auto-discount and CDC SHA-256 hash."""

    product_id: str = Field(..., min_length=1, max_length=128)
    title: str = Field(..., min_length=1, max_length=512)
    currency: str = Field(default="INR", min_length=1, max_length=8)
    current_price: Decimal = Field(..., gt=Decimal("0"))
    original_price: Optional[Decimal] = Field(default=None)
    discount_pct: Optional[float] = Field(default=None, ge=0.0, le=100.0)
    in_stock: bool
    source_url: str = Field(..., min_length=1)
    scraped_at: datetime = Field(default_factory=lambda: datetime.now(timezone.utc))
    record_hash: str = Field(default="")

    @field_validator("product_id", "title", "source_url", mode="before")
    @classmethod
    def _strip_required_strings(cls, value: Any) -> Any:
        if isinstance(value, str):
            return value.strip()
        return value

    @field_validator("currency", mode="before")
    @classmethod
    def _normalize_currency(cls, value: Any) -> str:
        if not isinstance(value, str) or not value.strip():
            return "INR"
        return value.strip().upper()

    @field_validator("current_price", "original_price", mode="before")
    @classmethod
    def _clean_price_fields(cls, value: Any) -> Optional[Decimal]:
        return sanitize_price_value(value)

    @field_validator("discount_pct", mode="before")
    @classmethod
    def _clean_discount_field(cls, value: Any) -> Optional[float]:
        if value is None or isinstance(value, bool):
            return None
        if isinstance(value, str):
            stripped = value.strip().rstrip("%").strip()
            if not stripped:
                return None
            try:
                return float(stripped)
            except ValueError:
                return None
        if isinstance(value, (int, float, Decimal)):
            return float(value)
        return None

    @field_validator("scraped_at", mode="before")
    @classmethod
    def _ensure_utc_datetime(cls, value: Any) -> datetime:
        if value is None:
            return datetime.now(timezone.utc)
        if isinstance(value, str):
            parsed = datetime.fromisoformat(value.replace("Z", "+00:00"))
        elif isinstance(value, datetime):
            parsed = value
        else:
            raise ValueError("scraped_at must be a datetime or ISO-8601 string")

        if parsed.tzinfo is None:
            return parsed.replace(tzinfo=timezone.utc)
        return parsed.astimezone(timezone.utc)

    @model_validator(mode="after")
    def _derive_discount_and_cdc_hash(self) -> "ProductPriceRecord":
        if self.original_price is not None and self.original_price <= Decimal("0"):
            self.original_price = None

        if (
            self.original_price is not None
            and self.original_price > self.current_price
        ):
            discount = (
                (self.original_price - self.current_price) / self.original_price
            ) * Decimal("100")
            self.discount_pct = round(float(discount), 2)
        elif self.discount_pct is None:
            self.discount_pct = None

        self.record_hash = self.compute_cdc_hash(
            product_id=self.product_id,
            current_price=self.current_price,
            in_stock=self.in_stock,
        )
        return self

    @staticmethod
    def compute_cdc_hash(
        product_id: str,
        current_price: Decimal,
        in_stock: bool,
    ) -> str:
        """Compute deterministic SHA-256 hash from product_id, current_price, and in_stock."""
        normalized_price = f"{current_price.quantize(Decimal('0.01'))}"
        payload = f"{product_id.strip()}|{normalized_price}|{int(bool(in_stock))}"
        return hashlib.sha256(payload.encode("utf-8")).hexdigest()
