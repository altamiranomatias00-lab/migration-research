"""
Scraper module: dataclasses, HTTP fetching, HTML caching, and structured logging.
"""
from __future__ import annotations

import hashlib
import json
import os
import re
import time
from dataclasses import dataclass, field, asdict
from datetime import datetime, timezone
from pathlib import Path
from typing import Any

import requests
from bs4 import BeautifulSoup

# Paths
BASE_DIR = Path(__file__).resolve().parent.parent
DATA_DIR = BASE_DIR / "data"
CACHE_DIR = BASE_DIR / "cache"
LOG_DIR = BASE_DIR / "logs"
LOG_FILE = LOG_DIR / "scrape_log.jsonl"

# Ensure dirs
for d in [DATA_DIR, CACHE_DIR, LOG_DIR]:
    d.mkdir(parents=True, exist_ok=True)


# ---------------------------------------------------------------------------
# Dataclasses
# ---------------------------------------------------------------------------

@dataclass
class Country:
    country_id: str
    country_name: str
    months_to_pr: int | None = None
    study_visa_months: int | None = None
    post_study_extension_months: int | None = None
    solvency_buffer_usd: float | None = None
    work_permit_allowed: bool | None = None
    max_hours_per_week: int | None = None
    embassy_in_peru: bool | None = None
    source_urls: dict = field(default_factory=dict)
    unverified_fields: list[str] = field(default_factory=list)


@dataclass
class Program:
    program_id: str
    program_name: str
    university: str
    city: str
    country_id: str
    faculty_or_department: str | None = None
    duration_months: int | None = None
    language_of_instruction: str = "English"
    full_tuition_usd: float | None = None
    program_url: str = ""
    scholarship_ids: list[str] = field(default_factory=list)
    max_coverage_pct: float | None = None
    max_stipend_usd: float | None = None
    scholarship_providers: list[str] = field(default_factory=list)
    unverified_fields: list[str] = field(default_factory=list)


@dataclass
class Scholarship:
    scholarship_id: str
    scholarship_name: str
    provider_organization: str
    candidate_type: list[str] = field(default_factory=list)
    coverage_pct: float | None = None
    monthly_stipend_usd: float | None = None
    covers_mobility_expenses: bool | None = None
    covers_medical_insurance: bool | None = None
    application_deadline: str | None = None
    applicable_program_ids: list[str] = field(default_factory=list)
    eligible_country_ids: list[str] = field(default_factory=list)
    peru_eligible: bool | None = None
    unverified_fields: list[str] = field(default_factory=list)


# ---------------------------------------------------------------------------
# Logging
# ---------------------------------------------------------------------------

def log_event(
    url: str,
    status_code: int | None,
    entity_type: str,
    entity_id: str,
    field_extracted: str,
    value: Any,
    unverified: bool = False,
    error: str | None = None,
):
    entry = {
        "ts": datetime.now(timezone.utc).isoformat(),
        "url": url,
        "status_code": status_code,
        "entity_type": entity_type,
        "entity_id": entity_id,
        "field_extracted": field_extracted,
        "value": value,
        "unverified": unverified,
    }
    if error:
        entry["error"] = error
    with open(LOG_FILE, "a") as f:
        f.write(json.dumps(entry, default=str) + "\n")


# ---------------------------------------------------------------------------
# Fetching + caching
# ---------------------------------------------------------------------------

HEADERS = {
    "User-Agent": (
        "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) "
        "AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36"
    )
}


def _cache_path(url: str) -> Path:
    from urllib.parse import urlparse
    parsed = urlparse(url)
    domain = parsed.netloc.replace(":", "_")
    url_hash = hashlib.md5(url.encode()).hexdigest()[:12]
    return CACHE_DIR / f"{domain}_{url_hash}.html"


def fetch_page(url: str, use_cache: bool = True) -> str | None:
    """Fetch a URL, cache the result, return HTML string or None on failure."""
    cache_file = _cache_path(url)
    if use_cache and cache_file.exists():
        return cache_file.read_text(encoding="utf-8", errors="replace")

    try:
        resp = requests.get(url, headers=HEADERS, timeout=30)
        resp.raise_for_status()
        html = resp.text
        cache_file.write_text(html, encoding="utf-8")
        return html
    except Exception as exc:
        log_event(
            url=url,
            status_code=getattr(getattr(exc, "response", None), "status_code", None),
            entity_type="fetch",
            entity_id="",
            field_extracted="",
            value=None,
            unverified=True,
            error=str(exc),
        )
        return None


def parse_soup(html: str) -> BeautifulSoup:
    return BeautifulSoup(html, "html.parser")


# ---------------------------------------------------------------------------
# Currency conversion
# ---------------------------------------------------------------------------

_exchange_rates: dict[str, float] = {}


def get_usd_rate(currency: str) -> float | None:
    """Return how many USD 1 unit of `currency` buys. Cached per session."""
    currency = currency.upper()
    if currency == "USD":
        return 1.0
    if currency in _exchange_rates:
        return _exchange_rates[currency]
    try:
        url = f"https://open.er-api.com/v6/latest/{currency}"
        html = fetch_page(url, use_cache=True)
        if html:
            data = json.loads(html)
            rate = data.get("rates", {}).get("USD")
            if rate:
                _exchange_rates[currency] = rate
                return rate
    except Exception:
        pass
    return None


def to_usd(amount: float, currency: str) -> float | None:
    rate = get_usd_rate(currency)
    if rate is None:
        return None
    return round(amount * rate, 2)


# ---------------------------------------------------------------------------
# Utility serialization
# ---------------------------------------------------------------------------

def save_json(data: list[Any], filename: str):
    """Save list of dataclass instances to data/ as JSON."""
    out = [asdict(item) if hasattr(item, "__dataclass_fields__") else item for item in data]
    path = DATA_DIR / filename
    path.write_text(json.dumps(out, indent=2, default=str), encoding="utf-8")
    return path
