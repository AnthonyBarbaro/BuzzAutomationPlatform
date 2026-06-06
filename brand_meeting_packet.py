#!/usr/bin/env python3
import argparse
import base64
import math
import mimetypes
import os
import re
import shutil
import subprocess
import sys
from collections import Counter, OrderedDict
from concurrent.futures import ThreadPoolExecutor, as_completed
from dataclasses import dataclass
from datetime import date, datetime, timedelta
from email.message import EmailMessage
from functools import lru_cache
from io import BytesIO
from pathlib import Path
from threading import Lock
from typing import Any, Callable, Dict, List, Optional, Sequence, Tuple
from xml.sax.saxutils import escape as xml_escape
from zoneinfo import ZoneInfo

import matplotlib
matplotlib.use("Agg")
import matplotlib.pyplot as plt
import numpy as np
import pandas as pd

from google.auth.transport.requests import Request
from google.oauth2.credentials import Credentials
from googleapiclient.discovery import build

from reportlab.lib import colors
from reportlab.lib.enums import TA_CENTER, TA_RIGHT
from reportlab.lib.styles import ParagraphStyle
from reportlab.lib.pagesizes import landscape, letter
from reportlab.lib.units import inch
from reportlab.platypus import (
    CondPageBreak,
    Image,
    KeepTogether,
    PageBreak,
    Paragraph,
    SimpleDocTemplate,
    Spacer,
    Table,
    TableStyle,
)

import owner_snapshot as osnap
from brand_credit_ledger import (
    export_credit_csv,
    ledger_to_dataframe,
    load_credit_ledger,
    summarize_credit_reconciliation,
)
from creditflow_api import fetch_creditflow_credits_for_brand, write_creditflow_cache
from brand_meeting_insights import (
    build_followup_text,
    generate_brand_action_items,
    generate_brand_health_score,
    generate_meeting_ask,
    load_monthly_reference,
)
from brand_meeting_targets import get_brand_targets, load_targets
from dutchie_api_reports import (
    DEFAULT_API_WORKERS,
    canonical_env_map as dutchie_canonical_env_map,
    create_session as dutchie_create_session,
    local_date_range_to_utc_strings as dutchie_local_date_range_to_utc_strings,
    request_json as dutchie_request_json,
    resolve_integrator_key as dutchie_resolve_integrator_key,
    resolve_store_keys as dutchie_resolve_store_keys,
    resolve_worker_count,
)
from getSalesReport import run_sales_report, store_abbr_map


# ---------------------------------------------------------------------------
# Config
# ---------------------------------------------------------------------------
REPORT_TZ = "America/Los_Angeles"
DEFAULT_DAYS = 60
DEFAULT_OUTPUT_ROOT = Path("reports/brand_packets").resolve()
DEFAULT_API_ENV_FILE = ".env"
DEFAULT_PACKET_API_WORKERS = DEFAULT_API_WORKERS
SALES_API_MAX_WINDOW_DAYS = 30
MIN_REPORTABLE_INVENTORY_UNITS = 4.0
ALL_STORE_SLOW_MOVER_REPORT_NAME = "_all_store_slow_movers"
OWNER_BRAND_ROLLUP_REPORT_NAME = "owner_rollups"
THIS_DIR = Path(__file__).resolve().parent
FILES_DIR = THIS_DIR / "files"

GMAIL_SCOPES = ["https://www.googleapis.com/auth/gmail.send"]
GMAIL_TOKEN = THIS_DIR / "token_gmail.json"
DEFAULT_REPORT_EMAIL = "anthony@buzzcannabis.com"
OWNER_BRAND_ROLLUP_EMAIL = "anthony@buzzcannabis.com"

SUPPLY_PRICE_BUCKET = 0.50
SUPPLY_COST_BUCKET = 0.25
SUPPLY_BASE_USE_VARIANT = False
STORE_DISPLAY_ORDER = ["MV", "LM", "SV", "LG", "NC", "WP"]
STORE_ORDER_RANK = {code: idx for idx, code in enumerate(STORE_DISPLAY_ORDER)}

NOISE_TOKENS = {
    "TAX", "DISCOUNT", "PROMO", "BUNDLE", "BUNDLED", "SPECIAL", "DEAL", "SALE", "FREE", "OFFER",
    "PACK", "PK", "EACH", "UNIT", "GRAM", "GRAMS", "MG", "G", "ML", "OZ",
}

VARIANT_KEYWORDS = [
    "LIVE RESIN",
    "LIVE ROSIN",
    "ROSIN",
    "DISTILLATE",
    "INFUSED",
    "DISPOSABLE",
    "CURED RESIN",
    "HASH",
    "BADDER",
    "BATTER",
    "SAUCE",
    "DIAMOND",
]

DEAL_TARGET_MARGIN = 0.35
DEAL_SCENARIOS = [
    {"label": "Current (No Deal, 30% Off)", "discount_pct": 0.30, "kickback_pct": 0.00},
    {"label": "40% Off + 20% Kickback", "discount_pct": 0.40, "kickback_pct": 0.20},
    {"label": "40% Off + 25% Kickback", "discount_pct": 0.40, "kickback_pct": 0.25},
    {"label": "40% Off + 30% Kickback", "discount_pct": 0.40, "kickback_pct": 0.30},
    {"label": "50% Off + 25% Kickback", "discount_pct": 0.50, "kickback_pct": 0.25},
    {"label": "50% Off + 30% Kickback", "discount_pct": 0.50, "kickback_pct": 0.30},
]


# ---------------------------------------------------------------------------
# Data classes
# ---------------------------------------------------------------------------
@dataclass
class PacketOptions:
    run_export: bool = False
    run_catalog_export: bool = True
    use_api: bool = False
    api_env_file: str = DEFAULT_API_ENV_FILE
    include_store_sections: bool = True
    include_product_appendix: bool = True
    include_charts: bool = True
    include_prior_window_data: bool = True
    # Default OFF: margins should be based on sales only (no kickback boosts).
    include_kickback_adjustments: bool = False
    email_results: bool = True
    generate_xlsx: bool = False
    top_n: int = 20
    force_refresh_data: bool = False
    api_workers: int = DEFAULT_PACKET_API_WORKERS
    include_credit_reconciliation: bool = True
    credit_ledger_path: str = "brand_credit_ledger.json"
    include_creditflow_credits: bool = True
    creditflow_base_url: str = "https://creditflow.replit.app/api/v1"
    target_margin: Optional[float] = None
    include_monthly_reference: bool = True
    packet_mode: str = "standard"
    generate_followup_notes: bool = True
    compact_pdf_mode: bool = True
    packet_layout: str = "classic"
    max_products: int = 20
    max_store_products: int = 10


@dataclass
class RunPaths:
    run_dir: Path
    raw_sales_dir: Path
    raw_catalog_dir: Path
    pdf_dir: Path
    cache_dir: Path


@dataclass
class PacketArtifacts:
    quick_pdf_path: Path
    detail_pdf_path: Path
    pdf_path: Path
    xlsx_path: Optional[Path]
    followup_notes_path: Optional[Path]
    run_paths: RunPaths
    missing_sales_stores: List[str]
    missing_catalog_stores: List[str]


@dataclass
class AllStoreSlowMoverArtifacts:
    pdf_path: Path
    xlsx_path: Path
    cache_dir: Path
    category_candidates: int
    product_candidates: int


@dataclass
class OwnerBrandRollupArtifacts:
    pdf_path: Path
    scorecard_csv_path: Path
    summary_csv_path: Path
    cache_dir: Path
    brand_count: int
    missing_sales_stores: List[str]
    missing_catalog_stores: List[str]


@dataclass
class ProductDecisionRow:
    product_name: str
    category: str
    net_sales: float
    units_sold: float
    units_per_day: float
    sales_vs_prior_pct: Optional[float]
    margin_pct: Optional[float]
    discount_pct: Optional[float]
    inventory_units: float
    inventory_value: float
    days_supply: Optional[float]
    sell_through_pct: Optional[float]
    stores_selling: int
    risk: str
    action: str
    recommendation: str


# ---------------------------------------------------------------------------
# Logging
# ---------------------------------------------------------------------------
def _default_logger(msg: str) -> None:
    print(msg)


def _log(msg: str, logger: Optional[Callable[[str], None]]) -> None:
    (logger or _default_logger)(msg)


def _short_terminal_text(value: Any, max_chars: int = 140) -> str:
    text = re.sub(r"\s+", " ", str(value or "")).strip()
    if len(text) <= max_chars:
        return text
    return text[: max_chars - 3].rstrip() + "..."


def _print_creditflow_pull_audit(brand: str, rows: List[Dict[str, Any]], meta: Dict[str, Any]) -> None:
    """Print CreditFlow rows directly to stdout so GUI runs are debuggable from the terminal."""
    print("")
    print("=" * 88)
    print(f"[CREDITFLOW PULLED CREDITS] Brand packet: {brand}")
    print(
        f"Raw credits pulled: {int(meta.get('raw_credits') or 0)} | "
        f"Matched to brand: {len(rows)} | "
        f"Brands loaded: {int(meta.get('brands') or 0)} | Stores loaded: {int(meta.get('stores') or 0)}"
    )
    target_ids = meta.get("target_brand_ids") or []
    target_codes = meta.get("target_vendor_codes") or []
    if target_ids or target_codes:
        print(
            f"CreditFlow brand filter: ids={', '.join(map(str, target_ids)) or 'N/A'} | "
            f"vendorCodes={', '.join(map(str, target_codes)) or 'N/A'} | "
            f"used={bool(meta.get('brand_filter_used'))}"
        )
    warning = str(meta.get("warning") or "").strip()
    if warning:
        print(f"Warning: {warning}")
    raw_brand_counts = meta.get("raw_brand_counts") or {}
    if isinstance(raw_brand_counts, dict) and raw_brand_counts:
        print("Raw CreditFlow brand counts:")
        for raw_brand, count in list(raw_brand_counts.items())[:50]:
            print(f"  - {raw_brand}: {count}")
        if len(raw_brand_counts) > 50:
            print(f"  ... {len(raw_brand_counts) - 50} more raw brands")
    matched_raw_brands = meta.get("matched_raw_brands") or {}
    if isinstance(matched_raw_brands, dict) and matched_raw_brands:
        print("Matched raw brand names:")
        for raw_brand, count in matched_raw_brands.items():
            print(f"  - {raw_brand}: {count}")
    if not rows:
        print("No CreditFlow credits matched this brand/date window.")
        print("=" * 88)
        print("")
        sys.stdout.flush()
        return
    print("Matched CreditFlow rows used by the report:")
    for idx, row in enumerate(rows, start=1):
        expected = float(row.get("expected_amount") or 0.0)
        received = float(row.get("received_amount") or 0.0)
        gap = max(expected - received, 0.0)
        print(
            f"  #{idx:02d} external_id={row.get('external_id') or row.get('id') or ''} "
            f"store={row.get('store_code') or 'All'} "
            f"brand={_short_terminal_text(row.get('brand'), 42)} "
            f"canonical={_short_terminal_text(row.get('canonical_brand'), 42)}"
        )
        print(
            f"      dates={row.get('start_date') or ''} -> {row.get('end_date') or ''} "
            f"type={row.get('credit_type') or ''} status={row.get('status') or ''} "
            f"expected={money0(expected)} received={money0(received)} gap={money0(gap)}"
        )
        scope_parts = []
        if row.get("category"):
            scope_parts.append(f"category={_short_terminal_text(row.get('category'), 55)}")
        if row.get("product"):
            scope_parts.append(f"product={_short_terminal_text(row.get('product'), 70)}")
        if row.get("invoice_reference"):
            scope_parts.append(f"invoice={_short_terminal_text(row.get('invoice_reference'), 36)}")
        if row.get("payment_reference"):
            scope_parts.append(f"payment={_short_terminal_text(row.get('payment_reference'), 36)}")
        if scope_parts:
            print("      " + " | ".join(scope_parts))
        if row.get("notes"):
            print(f"      notes={_short_terminal_text(row.get('notes'), 170)}")
    print("=" * 88)
    print("")
    sys.stdout.flush()


# ---------------------------------------------------------------------------
# Basic helpers
# ---------------------------------------------------------------------------
def canon(text: Any) -> str:
    return re.sub(r"[^a-z0-9]+", "", str(text or "").lower())


EXCLUDE_PRODUCT_GROUP_RE = re.compile(r"(\bPROMO\b|\bSAMPLE\b)", re.IGNORECASE)
EXCLUDE_ACCESSORY_CATEGORY_RE = re.compile(r"\bACCESSOR(?:Y|IES)?\b", re.IGNORECASE)


def _is_excluded_product_group_name(name: Any) -> bool:
    s = str(name or "").strip()
    if not s:
        return False
    return bool(EXCLUDE_PRODUCT_GROUP_RE.search(s))


def _is_excluded_accessory_category(name: Any) -> bool:
    s = normalize_text(name)
    if not s:
        return False
    return bool(EXCLUDE_ACCESSORY_CATEGORY_RE.search(s))


def _filter_product_group_rows(df: pd.DataFrame, *, exclude_accessories: bool = False) -> pd.DataFrame:
    if df is None or df.empty:
        return pd.DataFrame(columns=(df.columns if df is not None else []))
    out = df.copy()
    product_col = ""
    for c in ["_product_raw", "Product", "Product Name", "display_product", "product_group_display"]:
        if c in out.columns:
            product_col = c
            break
    if product_col:
        mask_keep = ~out[product_col].map(_is_excluded_product_group_name)
        out = out[mask_keep].copy()
    if exclude_accessories:
        for c in ["category_normalized", "Category", "Product Category", "Category Name"]:
            if c not in out.columns:
                continue
            out = out[~out[c].map(_is_excluded_accessory_category)].copy()
    return out


def safe_filename(s: str) -> str:
    return osnap.safe_filename(str(s))


def parse_iso_date(value: str) -> date:
    try:
        return datetime.strptime(value, "%Y-%m-%d").date()
    except ValueError as exc:
        raise argparse.ArgumentTypeError(f"Invalid date '{value}'. Use YYYY-MM-DD.") from exc


def compute_default_window(days: int, tz_name: str = REPORT_TZ) -> Tuple[date, date]:
    tz = ZoneInfo(tz_name)
    today = datetime.now(tz).date()
    end_day = today - timedelta(days=1)
    start_day = end_day - timedelta(days=max(days, 1) - 1)
    return start_day, end_day


def order_store_codes(codes: Sequence[str]) -> List[str]:
    uniq: List[str] = []
    seen: set = set()
    for c in codes:
        up = str(c or "").upper().strip()
        if not up or up in seen:
            continue
        seen.add(up)
        uniq.append(up)
    ordered = [c for c in STORE_DISPLAY_ORDER if c in seen]
    extras = sorted([c for c in uniq if c not in STORE_ORDER_RANK])
    return ordered + extras


def parse_store_codes_arg(stores_arg: Optional[str]) -> List[str]:
    all_codes = order_store_codes(list({abbr for abbr in store_abbr_map.values()}))
    if not stores_arg:
        return all_codes
    requested = [s.strip().upper() for s in stores_arg.split(",") if s.strip()]
    valid = [s for s in requested if s in all_codes]
    return order_store_codes(valid) or all_codes


def window_days(start_day: date, end_day: date) -> int:
    return (end_day - start_day).days + 1


def compute_prior_report_window(start_day: date, end_day: date) -> Tuple[date, date]:
    n_days = max(window_days(start_day, end_day), 1)
    prior_report_end = start_day - timedelta(days=1)
    prior_report_start = prior_report_end - timedelta(days=n_days - 1)
    return prior_report_start, prior_report_end


def money0(x: float) -> str:
    return osnap.money(float(x or 0.0))


def money2(x: float) -> str:
    return osnap.money2(float(x or 0.0))


def pct1(x: float) -> str:
    return osnap.pct1(float(x or 0.0))


def int0(x: float) -> str:
    try:
        return f"{int(round(float(x or 0.0))):,}"
    except Exception:
        return "0"


def inventory_value_with_units(value: float, units: float) -> str:
    return f"{money0(value)} / {int0(units)} units"


def _inventory_reporting_rows(df: Optional[pd.DataFrame], min_units: float = MIN_REPORTABLE_INVENTORY_UNITS) -> pd.DataFrame:
    if df is None or df.empty:
        return pd.DataFrame(columns=(df.columns if df is not None else []))

    out = df.copy()
    out["Available"] = osnap.to_number(out.get("Available", 0.0)).fillna(0.0).astype(float)
    return out[out["Available"] >= float(min_units)].copy()


def days1(x: Any) -> str:
    try:
        v = float(x)
    except Exception:
        return "n/a"
    if not np.isfinite(v) or v <= 0:
        return "n/a"
    return f"{v:,.1f}d"


def _safe_dos(units: float, per_day: float) -> float:
    try:
        u = float(units)
        p = float(per_day)
    except Exception:
        return float("nan")
    if p > 0 and u > 0:
        return float(u / p)
    return float("nan")


def safe_div(numerator: Any, denominator: Any, default: float = 0.0) -> float:
    try:
        n = float(numerator)
        d = float(denominator)
    except Exception:
        return default
    if not np.isfinite(n) or not np.isfinite(d) or d == 0:
        return default
    return n / d


def pct_change(current: Any, prior: Any) -> Optional[float]:
    try:
        cur = float(current)
        base = float(prior)
    except Exception:
        return None
    if not np.isfinite(cur) or not np.isfinite(base):
        return None
    if base == 0:
        return 0.0 if cur == 0 else None
    return (cur - base) / base


def pp_change(current_percent: Any, prior_percent: Any) -> float:
    try:
        cur = float(current_percent)
        base = float(prior_percent)
    except Exception:
        return 0.0
    if not np.isfinite(cur) or not np.isfinite(base):
        return 0.0
    return cur - base


def _owner_pct_change_label(value: Optional[float], current: Any = 0.0, prior: Any = 0.0) -> str:
    try:
        cur = float(current)
        base = float(prior)
    except Exception:
        cur = 0.0
        base = 0.0
    try:
        val = float(value) if value is not None else float("nan")
    except Exception:
        val = float("nan")
    if value is None or not np.isfinite(val):
        return "New" if cur > 0 and base == 0 else "n/a"
    return pct1(val)


def _owner_margin_gap_label(value: Any) -> str:
    try:
        v = float(value)
    except Exception:
        v = 0.0
    return osnap.pp1(v) if hasattr(osnap, "pp1") else f"{v * 100.0:+.1f}pp"


def owner_brand_status_action(row: Dict[str, Any], targets: Dict[str, float]) -> Tuple[str, str]:
    net = float(row.get("net_revenue", 0.0) or 0.0)
    sales_share = float(row.get("sales_share_pct", 0.0) or 0.0)
    margin = float(row.get("margin_real", 0.0) or 0.0)
    target_margin = float(targets.get("target_margin", 0.35) or 0.35)
    margin_gap = margin - target_margin
    discount = float(row.get("discount_rate", 0.0) or 0.0)
    max_discount = float(targets.get("max_discount_rate", 0.45) or 0.45)
    days_supply = float(row.get("days_supply", np.nan))
    max_days = float(targets.get("max_days_supply", 60.0) or 60.0)
    sell_through = float(row.get("sell_through_pct", 0.0) or 0.0)
    min_sell = float(targets.get("min_sell_through", 0.25) or 0.25)
    credit_gap = float(row.get("credit_gap", 0.0) or 0.0)
    credit_gap_pct = float(row.get("credit_gap_pct_sales", 0.0) or 0.0)
    sales_delta = row.get("sales_vs_prior_pct")
    sales_declining = isinstance(sales_delta, (int, float)) and np.isfinite(float(sales_delta)) and float(sales_delta) < -0.10
    sales_growing = isinstance(sales_delta, (int, float)) and np.isfinite(float(sales_delta)) and float(sales_delta) > 0.10
    high_days = np.isfinite(days_supply) and days_supply > max_days

    if sales_share < 0.01 and margin_gap < -0.05 and high_days:
        return "Exit / Reduce", "Exit or buyback"
    if credit_gap >= max(1000.0, net * 0.05) or credit_gap_pct >= 0.08:
        return "Fix", "Collect support"
    if margin_gap < -0.05:
        return "Fix", "Fix margin"
    if discount > max_discount and margin < target_margin:
        return "Fix", "Reduce discount"
    if sales_declining and high_days:
        return "Watch", "Reduce buys"
    if high_days or sell_through < min_sell:
        return "Watch", "Move inventory"
    if sales_growing and margin >= target_margin and discount <= max_discount:
        return "Grow", "Grow"
    if margin >= target_margin and discount <= max_discount:
        return "Good", "Maintain"
    return "Watch", "Review terms"


def _file_snapshot(folder: Path, suffixes: Tuple[str, ...]) -> Dict[str, Tuple[int, float]]:
    if not folder.exists():
        return {}
    out: Dict[str, Tuple[int, float]] = {}
    for p in folder.iterdir():
        if not p.is_file() or not p.name.lower().endswith(suffixes):
            continue
        try:
            st = p.stat()
            out[p.name] = (int(st.st_size), float(st.st_mtime))
        except OSError:
            continue
    return out


def _changed_files_after(folder: Path, before: Dict[str, Tuple[int, float]], suffixes: Tuple[str, ...]) -> List[Path]:
    after = _file_snapshot(folder, suffixes)
    changed: List[Path] = []
    for name, stat in after.items():
        if before.get(name) != stat:
            changed.append(folder / name)
    return sorted(changed, key=lambda p: p.stat().st_mtime, reverse=True)


def _store_name_from_abbr(abbr: str) -> str:
    for store_name, code in store_abbr_map.items():
        if code == abbr:
            return store_name
    return abbr


def _abbr_from_store_name(store_name: str) -> Optional[str]:
    if not store_name:
        return None
    s = str(store_name).strip().lower()
    for full, abbr in store_abbr_map.items():
        if s == full.strip().lower():
            return abbr
    for full, abbr in store_abbr_map.items():
        if s in full.strip().lower() or full.strip().lower() in s:
            return abbr
    return None


def _abbr_from_filename(path: Path) -> Optional[str]:
    stem = path.stem.upper()
    known = sorted({abbr for abbr in store_abbr_map.values()}, key=len, reverse=True)
    for abbr in known:
        if re.search(rf"(^|[^A-Z0-9]){re.escape(abbr)}([^A-Z0-9]|$)", stem):
            return abbr
    return None


@lru_cache(maxsize=4)
def _api_auth_bundle(env_file: str) -> Tuple[Dict[str, str], str]:
    env_path = str(Path(env_file).expanduser())
    env_map = dutchie_canonical_env_map(env_path)
    integrator_key = dutchie_resolve_integrator_key(env_map)
    return env_map, integrator_key


def _api_store_keys(env_file: str, selected_store_codes: Sequence[str]) -> Tuple[Dict[str, str], str]:
    env_map, integrator_key = _api_auth_bundle(env_file)
    store_codes = [str(code).upper().strip() for code in selected_store_codes if str(code).strip()]
    store_keys = dutchie_resolve_store_keys(env_map, store_codes)
    missing = [code for code in store_codes if code not in store_keys]
    if missing:
        raise RuntimeError(
            "Missing Dutchie API location key(s) for "
            f"{', '.join(missing)} in {env_file}. "
            "Expected names like DUTCHIE_API_KEY_MV or mv."
        )
    return store_keys, integrator_key


def _api_session_for_store(store_code: str, env_file: str, selected_store_codes: Sequence[str]) -> Any:
    store_keys, integrator_key = _api_store_keys(env_file, selected_store_codes)
    return dutchie_create_session(store_keys[store_code], integrator_key)


def _clean_flat_dataframe(df: pd.DataFrame) -> pd.DataFrame:
    if df is None or df.empty:
        return pd.DataFrame(columns=(df.columns if df is not None else []))
    out = df.loc[:, ~df.columns.astype(str).str.contains("^Unnamed", case=False, regex=True)].copy()
    out.columns = [str(c).strip() for c in out.columns]
    return out


def _to_float(value: Any) -> float:
    try:
        if value is None or value == "":
            return 0.0
        return float(value)
    except Exception:
        return 0.0


def _first_nonempty(*values: Any) -> Any:
    for value in values:
        if value is None:
            continue
        if isinstance(value, str):
            if value.strip():
                return value.strip()
            continue
        return value
    return ""


def _tag_names(tags: Any) -> str:
    if not isinstance(tags, list):
        return ""
    names: List[str] = []
    for item in tags:
        if isinstance(item, dict):
            name = str(item.get("tagName") or item.get("name") or "").strip()
            if name:
                names.append(name)
    return ", ".join(names)


def _product_lookup_by_id(products_payload: Any) -> Dict[int, Dict[str, Any]]:
    lookup: Dict[int, Dict[str, Any]] = {}
    if not isinstance(products_payload, list):
        return lookup
    for row in products_payload:
        if not isinstance(row, dict):
            continue
        product_id = row.get("productId")
        try:
            key = int(product_id)
        except Exception:
            continue
        lookup[key] = row
    return lookup


def _normalize_inventory_api_catalog_rows(inventory_payload: Any, store_code: str) -> pd.DataFrame:
    rows: List[Dict[str, Any]] = []
    store_name = _store_name_from_abbr(store_code)
    for item in inventory_payload or []:
        if not isinstance(item, dict):
            continue

        product_name = str(item.get("productName") or item.get("alternateName") or "").strip()
        category = _first_nonempty(item.get("category"), item.get("masterCategory"), "Unknown")
        unit_price = _to_float(_first_nonempty(item.get("unitPrice"), item.get("recUnitPrice"), item.get("medUnitPrice")))
        brand_name = str(item.get("brandName") or osnap.parse_brand_from_product(product_name)).strip()

        rows.append(
            {
                "SKU": _first_nonempty(item.get("sku"), ""),
                "Available": _to_float(item.get("quantityAvailable")),
                "Product": product_name,
                "Cost": _to_float(item.get("unitCost")),
                "Location price": unit_price,
                "Price": unit_price,
                "Category": str(category),
                "Brand": brand_name,
                "Strain": _first_nonempty(item.get("strain"), ""),
                "Vendor": _first_nonempty(item.get("vendor"), item.get("producer"), ""),
                "Tags": _tag_names(item.get("tags")),
                "Strain Type": _first_nonempty(item.get("strainType"), ""),
                "Store": store_name,
                "Store Code": store_code,
            }
        )

    return _clean_flat_dataframe(pd.DataFrame(rows))


def _normalize_transactions_api_sales_rows(
    transactions_payload: Any,
    products_payload: Any,
    store_code: str,
) -> pd.DataFrame:
    rows: List[Dict[str, Any]] = []
    product_lookup = _product_lookup_by_id(products_payload)

    for tx in transactions_payload or []:
        if not isinstance(tx, dict):
            continue

        tx_id = tx.get("transactionId")
        tx_time = _first_nonempty(tx.get("transactionDateLocalTime"), tx.get("transactionDate"), tx.get("lastModifiedDateUTC"))
        budtender = _first_nonempty(tx.get("completedByUser"), tx.get("terminalName"), "")
        customer_type = _first_nonempty(tx.get("customerTypeName"), tx.get("customerType"), tx.get("customerTypeId"), "")
        tx_is_return = bool(tx.get("isReturn"))

        for item in tx.get("items") or []:
            if not isinstance(item, dict):
                continue

            try:
                product_key = int(item.get("productId") or 0)
            except Exception:
                product_key = 0
            product_info = product_lookup.get(product_key, {})
            product_name = str(
                _first_nonempty(
                    product_info.get("productName"),
                    product_info.get("internalName"),
                    f"Unknown Product {item.get('productId')}",
                )
            )
            category = str(_first_nonempty(product_info.get("category"), product_info.get("masterCategory"), "Unknown"))
            quantity = _to_float(item.get("quantity"))
            unit_weight = _to_float(item.get("unitWeight"))
            gross_sales = _to_float(item.get("totalPrice"))
            discount_amount = _to_float(item.get("totalDiscount"))
            net_sales = gross_sales - discount_amount
            unit_cost = _to_float(_first_nonempty(item.get("unitCost"), product_info.get("unitCost")))
            inventory_cost = unit_cost * quantity
            sale_price = _to_float(_first_nonempty(item.get("unitPrice"), product_info.get("price"), product_info.get("recPrice"), product_info.get("medPrice")))

            is_return = bool(item.get("isReturned")) or tx_is_return
            sign = -1.0 if is_return else 1.0

            quantity = abs(quantity) * sign
            gross_sales = abs(gross_sales) * sign
            discount_amount = abs(discount_amount) * sign
            net_sales = abs(net_sales) * sign
            inventory_cost = abs(inventory_cost) * sign
            total_weight = abs(quantity) * unit_weight * (1.0 if sign >= 0 else -1.0)
            order_profit = net_sales - inventory_cost

            rows.append(
                {
                    "Order ID": tx_id,
                    "Order Time": tx_time,
                    "Budtender Name": budtender,
                    "Customer Type": customer_type,
                    "Vendor Name": _first_nonempty(item.get("vendor"), product_info.get("vendorName"), product_info.get("producerName"), ""),
                    "Product Name": product_name,
                    "Category": category,
                    "Major Category": category,
                    "Package ID": _first_nonempty(item.get("packageId"), ""),
                    "Batch ID": _first_nonempty(item.get("batchName"), ""),
                    "External Package ID": _first_nonempty(item.get("sourcePackageId"), item.get("packageId"), ""),
                    "Total Inventory Sold": quantity,
                    "Unit Weight Sold": unit_weight,
                    "Total Weight Sold": total_weight,
                    "Gross Sales": gross_sales,
                    "Inventory Cost": inventory_cost,
                    "Discounted Amount": discount_amount,
                    "Loyalty as Discount": 0.0,
                    "Net Sales": net_sales,
                    "Return Date": _first_nonempty(item.get("returnDate"), tx_time if is_return else None),
                    "Producer": _first_nonempty(product_info.get("producerName"), item.get("vendor"), ""),
                    "Order Profit": order_profit,
                    "Unit Price": sale_price,
                    "Price": sale_price,
                    "Location price": sale_price,
                    "Store": _store_name_from_abbr(store_code),
                    "Store Code": store_code,
                    "SKU": _first_nonempty(product_info.get("sku"), ""),
                }
            )

    out = _clean_flat_dataframe(pd.DataFrame(rows))
    for date_col in ["Order Time", "Return Date"]:
        if date_col in out.columns:
            out[date_col] = pd.to_datetime(out[date_col], errors="coerce")
    return out


def _read_sales_source_file(path: Path) -> pd.DataFrame:
    if str(path.suffix).lower() == ".csv":
        df = pd.read_csv(path)
        df = _clean_flat_dataframe(df)
        for date_col in ["Order Time", "Return Date"]:
            if date_col in df.columns:
                df[date_col] = pd.to_datetime(df[date_col], errors="coerce")
        return df
    return osnap.read_export(path)


def _sales_export_candidates(directory: Path, abbr: str) -> List[Path]:
    candidates: List[Path] = []
    for pattern in (f"{abbr}*.xlsx", f"{abbr}*.csv"):
        candidates.extend(directory.glob(pattern))
    return sorted(candidates, key=lambda p: p.stat().st_mtime, reverse=True)


def _iter_sales_api_chunks(start_day: date, end_day: date, max_days: int = SALES_API_MAX_WINDOW_DAYS) -> List[Tuple[date, date]]:
    chunks: List[Tuple[date, date]] = []
    if end_day < start_day:
        return chunks

    window_days = max(1, int(max_days))
    chunk_start = start_day
    while chunk_start <= end_day:
        chunk_end = min(chunk_start + timedelta(days=window_days - 1), end_day)
        chunks.append((chunk_start, chunk_end))
        chunk_start = chunk_end + timedelta(days=1)
    return chunks


def _fetch_sales_exports_via_api(
    paths: RunPaths,
    selected_store_codes: Sequence[str],
    start_day: date,
    end_day: date,
    env_file: str,
    logger: Optional[Callable[[str], None]],
    api_workers: int = DEFAULT_PACKET_API_WORKERS,
) -> Tuple[Dict[str, Path], List[str]]:
    archived: Dict[str, Path] = {}
    missing: List[str] = []
    store_keys, integrator_key = _api_store_keys(env_file, selected_store_codes)
    chunks = _iter_sales_api_chunks(start_day, end_day)
    worker_count = resolve_worker_count(api_workers, len(selected_store_codes))
    worker_label = "serial mode" if worker_count == 1 else f"{worker_count} store worker threads"
    log_lock = Lock()

    def log_safe(message: str) -> None:
        with log_lock:
            _log(message, logger)

    log_safe(f"[SALES] Dutchie API sales fetch using {worker_label}.")

    def fetch_store(abbr: str) -> Tuple[str, Optional[Path], Optional[str]]:
        session = None
        try:
            session = dutchie_create_session(store_keys[abbr], integrator_key)
            products_payload = dutchie_request_json(session, "/reporting/products")
            transactions_payload: List[Dict[str, Any]] = []
            for idx, (chunk_start, chunk_end) in enumerate(chunks, start=1):
                from_utc, to_utc = dutchie_local_date_range_to_utc_strings(
                    chunk_start.isoformat(),
                    chunk_end.isoformat(),
                    REPORT_TZ,
                )
                sales_params = {
                    "FromDateUTC": from_utc,
                    "ToDateUTC": to_utc,
                    "IncludeDetail": True,
                    "IncludeTaxes": True,
                    "IncludeOrderIds": True,
                    "IncludeFeesAndDonations": True,
                }
                log_safe(
                    f"[SALES] API chunk {abbr} {idx}/{len(chunks)}: {chunk_start.isoformat()} -> {chunk_end.isoformat()}",
                )
                payload = dutchie_request_json(session, "/reporting/transactions", params=sales_params)
                if isinstance(payload, list) and payload:
                    transactions_payload.extend(payload)
            df = _normalize_transactions_api_sales_rows(transactions_payload, products_payload, abbr)
            dst_name = safe_filename(
                f"{abbr} - Sales API Export - {osnap.store_label(_store_name_from_abbr(abbr))} - "
                f"{start_day.isoformat()}_to_{end_day.isoformat()}.csv"
            )
            dst = paths.raw_sales_dir / dst_name
            df.to_csv(dst, index=False)
            log_safe(f"[API] Sales {abbr}: {len(df)} row(s) -> {dst}")
            return abbr, dst, None
        except Exception as exc:
            return abbr, None, str(exc)
        finally:
            if session is not None:
                try:
                    session.close()
                except Exception:
                    pass

    if worker_count == 1:
        results = [fetch_store(abbr) for abbr in selected_store_codes]
    else:
        with ThreadPoolExecutor(max_workers=worker_count) as executor:
            future_map = {executor.submit(fetch_store, abbr): abbr for abbr in selected_store_codes}
            results = []
            for future in as_completed(future_map):
                abbr = future_map[future]
                try:
                    results.append(future.result())
                except Exception as exc:
                    results.append((abbr, None, str(exc)))

    for abbr, path, error in results:
        if path is not None:
            archived[abbr] = path
        else:
            missing.append(abbr)
            _log(f"[WARN] API sales fetch failed for {abbr}: {error or 'unknown error'}", logger)

    return archived, missing


def _fetch_catalog_exports_via_api(
    paths: RunPaths,
    selected_store_codes: Sequence[str],
    env_file: str,
    logger: Optional[Callable[[str], None]],
    api_workers: int = DEFAULT_PACKET_API_WORKERS,
) -> Tuple[List[Path], List[str]]:
    copied: List[Path] = []
    missing: List[str] = []
    store_keys, integrator_key = _api_store_keys(env_file, selected_store_codes)
    worker_count = resolve_worker_count(api_workers, len(selected_store_codes))
    worker_label = "serial mode" if worker_count == 1 else f"{worker_count} store worker threads"
    log_lock = Lock()

    def log_safe(message: str) -> None:
        with log_lock:
            _log(message, logger)

    log_safe(f"[CATALOG] Dutchie API catalog fetch using {worker_label}.")

    def fetch_store(abbr: str) -> Tuple[str, Optional[Path], Optional[str]]:
        session = None
        try:
            session = dutchie_create_session(store_keys[abbr], integrator_key)
            inventory_payload = dutchie_request_json(session, "/reporting/inventory")
            df = _normalize_inventory_api_catalog_rows(inventory_payload, abbr)
            dst_name = safe_filename(f"catalog_{abbr}_{datetime.now().strftime('%Y%m%d_%H%M%S')}.csv")
            dst = paths.raw_catalog_dir / dst_name
            df.to_csv(dst, index=False)
            log_safe(f"[API] Catalog {abbr}: {len(df)} row(s) -> {dst}")
            return abbr, dst, None
        except Exception as exc:
            return abbr, None, str(exc)
        finally:
            if session is not None:
                try:
                    session.close()
                except Exception:
                    pass

    if worker_count == 1:
        results = [fetch_store(abbr) for abbr in selected_store_codes]
    else:
        with ThreadPoolExecutor(max_workers=worker_count) as executor:
            future_map = {executor.submit(fetch_store, abbr): abbr for abbr in selected_store_codes}
            results = []
            for future in as_completed(future_map):
                abbr = future_map[future]
                try:
                    results.append(future.result())
                except Exception as exc:
                    results.append((abbr, None, str(exc)))

    for abbr, path, error in results:
        if path is not None:
            copied.append(path)
        else:
            missing.append(abbr)
            _log(f"[WARN] API catalog fetch failed for {abbr}: {error or 'unknown error'}", logger)

    return copied, missing


def build_brand_aliases_from_catalog(
    brand: str,
    catalog_df: Optional[pd.DataFrame],
    logger: Optional[Callable[[str], None]] = None,
) -> List[str]:
    target = canon(brand)
    aliases_raw: set[str] = {str(brand or "").strip()}
    aliases_canon: set[str] = {target} if target else set()

    if catalog_df is not None and not catalog_df.empty:
        work = catalog_df.copy()
        work.columns = [str(c).strip() for c in work.columns]

        brand_col = "Brand" if "Brand" in work.columns else None
        product_col = "Product" if "Product" in work.columns else ("Product Name" if "Product Name" in work.columns else None)

        if brand_col:
            for b in work[brand_col].dropna().astype(str).str.strip().unique().tolist():
                cb = canon(b)
                if not cb:
                    continue
                if cb == target:
                    aliases_raw.add(b)
                    aliases_canon.add(cb)

        if product_col:
            parsed = work[product_col].dropna().astype(str).apply(osnap.parse_brand_from_product)
            for p in parsed.unique().tolist():
                cp = canon(p)
                if not cp:
                    continue
                if cp == target or cp in aliases_canon:
                    aliases_raw.add(p)
                    aliases_canon.add(cp)

    out = []
    seen: set[str] = set()
    for a in sorted(aliases_raw, key=lambda x: (len(canon(x)), str(x).lower()), reverse=True):
        ca = canon(a)
        if not ca or len(ca) < 2 or ca in seen:
            continue
        out.append(str(a).strip())
        seen.add(ca)

    if out:
        _log(f"[BRAND] Sales brand aliases: {', '.join(out[:8])}", logger)
    return out or [brand]


# ---------------------------------------------------------------------------
# Paths + run folders
# ---------------------------------------------------------------------------
def build_run_paths(output_root: Path, brand: str, start_day: date, end_day: date) -> RunPaths:
    brand_dir = output_root / safe_filename(brand)
    run_dir = brand_dir / f"{start_day.isoformat()}_to_{end_day.isoformat()}"
    raw_sales_dir = run_dir / "raw_sales"
    raw_catalog_dir = run_dir / "raw_catalog"
    pdf_dir = run_dir / "pdf"
    cache_dir = run_dir / "cache"

    for p in [raw_sales_dir, raw_catalog_dir, pdf_dir, cache_dir]:
        p.mkdir(parents=True, exist_ok=True)

    return RunPaths(
        run_dir=run_dir,
        raw_sales_dir=raw_sales_dir,
        raw_catalog_dir=raw_catalog_dir,
        pdf_dir=pdf_dir,
        cache_dir=cache_dir,
    )


def _find_latest_brand_run_with_sales(brand_dir: Path) -> Optional[Path]:
    if not brand_dir.exists():
        return None
    runs = [p for p in brand_dir.iterdir() if p.is_dir() and (p / "raw_sales").exists()]
    if not runs:
        return None
    return sorted(runs, key=lambda p: p.stat().st_mtime, reverse=True)[0]

def _find_best_brand_run_with_sales(
    brand_dir: Path,
    needed_start: Optional[date] = None,
    needed_end: Optional[date] = None,
) -> Optional[Path]:
    if not brand_dir.exists():
        return None

    runs = [p for p in brand_dir.iterdir() if p.is_dir() and (p / "raw_sales").exists()]
    if not runs:
        return None

    if needed_start is None or needed_end is None:
        return sorted(runs, key=lambda p: p.stat().st_mtime, reverse=True)[0]

    covering: List[Path] = []
    for run in runs:
        parsed = osnap.parse_range_from_folder_name(run)
        if not parsed:
            continue
        s, e = parsed
        if s <= needed_start and e >= needed_end:
            covering.append(run)

    if covering:
        return sorted(covering, key=lambda p: p.stat().st_mtime, reverse=True)[0]
    return sorted(runs, key=lambda p: p.stat().st_mtime, reverse=True)[0]


def _find_best_any_run_with_sales(
    output_root: Path,
    needed_start: Optional[date] = None,
    needed_end: Optional[date] = None,
    exclude_brand_dir: Optional[Path] = None,
) -> Optional[Path]:
    if not output_root.exists():
        return None

    runs: List[Path] = []
    for brand_dir in output_root.iterdir():
        if not brand_dir.is_dir():
            continue
        if exclude_brand_dir is not None and brand_dir.resolve() == exclude_brand_dir.resolve():
            continue
        for run in brand_dir.iterdir():
            if run.is_dir() and (run / "raw_sales").exists():
                runs.append(run)

    if not runs:
        return None

    if needed_start is None or needed_end is None:
        return sorted(runs, key=lambda p: p.stat().st_mtime, reverse=True)[0]

    covering: List[Path] = []
    for run in runs:
        parsed = osnap.parse_range_from_folder_name(run)
        if not parsed:
            continue
        s, e = parsed
        if s <= needed_start and e >= needed_end:
            covering.append(run)

    if covering:
        return sorted(covering, key=lambda p: p.stat().st_mtime, reverse=True)[0]
    return sorted(runs, key=lambda p: p.stat().st_mtime, reverse=True)[0]


def _reuse_or_seed_sales_exports(
    paths: RunPaths,
    brand: str,
    selected_store_codes: Sequence[str],
    needed_start: Optional[date],
    needed_end: Optional[date],
    logger: Optional[Callable[[str], None]],
) -> Dict[str, Path]:
    found: Dict[str, Path] = {}

    for abbr in selected_store_codes:
        existing = _sales_export_candidates(paths.raw_sales_dir, abbr)
        if existing:
            found[abbr] = existing[0]

    if found:
        _log(f"[SALES] Reusing {len(found)} cached exports from {paths.raw_sales_dir}", logger)

    def _copy_missing_from_raw_dir(src_raw: Path, source_label: str) -> int:
        copied = 0
        if not src_raw.exists():
            return copied
        try:
            if src_raw.resolve() == paths.raw_sales_dir.resolve():
                return copied
        except OSError:
            pass

        pending = [abbr for abbr in selected_store_codes if abbr not in found]
        for abbr in pending:
            matches = _sales_export_candidates(src_raw, abbr)
            for p in matches:
                dst = paths.raw_sales_dir / p.name
                shutil.copy2(p, dst)
                found[abbr] = dst
                copied += 1
                break
        if copied:
            _log(f"[SALES] Seeded {copied} exports from {source_label}", logger)
        return copied

    missing = [abbr for abbr in selected_store_codes if abbr not in found]
    if not missing:
        return found

    brand_dir = paths.run_dir.parent
    latest = _find_best_brand_run_with_sales(brand_dir, needed_start=needed_start, needed_end=needed_end)
    if latest:
        _copy_missing_from_raw_dir(latest / "raw_sales", f"previous run {latest}")

    missing = [abbr for abbr in selected_store_codes if abbr not in found]
    if not missing:
        return found

    output_root = paths.run_dir.parent.parent
    any_latest = _find_best_any_run_with_sales(
        output_root=output_root,
        needed_start=needed_start,
        needed_end=needed_end,
        exclude_brand_dir=brand_dir,
    )
    if any_latest:
        _copy_missing_from_raw_dir(any_latest / "raw_sales", f"cross-brand run {any_latest}")

    missing = [abbr for abbr in selected_store_codes if abbr not in found]
    if not missing:
        return found

    seeded_from_files = 0
    for abbr in [code for code in selected_store_codes if code not in found]:
        src = FILES_DIR / f"sales{abbr}.xlsx"
        if src.exists():
            dst_name = f"{abbr} - Sales Export - {safe_filename(_store_name_from_abbr(abbr))}.xlsx"
            dst = paths.raw_sales_dir / dst_name
            shutil.copy2(src, dst)
            found[abbr] = dst
            seeded_from_files += 1

    if seeded_from_files:
        _log(f"[SALES] Seeded {seeded_from_files} exports from {FILES_DIR}", logger)
    else:
        _log("[SALES] No prior exports available to reuse.", logger)
    return found


def prepare_sales_exports(
    paths: RunPaths,
    brand: str,
    selected_store_codes: Sequence[str],
    acquisition_start: date,
    acquisition_end: date,
    allow_export: bool,
    force_refresh: bool,
    use_api: bool,
    api_env_file: str,
    api_workers: int,
    logger: Optional[Callable[[str], None]],
) -> Tuple[Dict[str, Path], List[str], bool]:
    sales_paths: Dict[str, Path] = {}
    did_export = False

    if not force_refresh:
        sales_paths = _reuse_or_seed_sales_exports(
            paths=paths,
            brand=brand,
            selected_store_codes=selected_store_codes,
            needed_start=acquisition_start,
            needed_end=acquisition_end,
            logger=logger,
        )
        missing = [abbr for abbr in selected_store_codes if abbr not in sales_paths]
        if not missing:
            return sales_paths, [], did_export
        if not allow_export:
            return sales_paths, missing, did_export
        _log(
            f"[SALES] Missing cached exports for {', '.join(missing)}. Refreshing sales export.",
            logger,
        )
    elif not allow_export:
        _log("[SALES] Force refresh requested, but this action is build-only. Reusing saved sales files instead.", logger)
        sales_paths = _reuse_or_seed_sales_exports(
            paths=paths,
            brand=brand,
            selected_store_codes=selected_store_codes,
            needed_start=acquisition_start,
            needed_end=acquisition_end,
            logger=logger,
        )
        missing = [abbr for abbr in selected_store_codes if abbr not in sales_paths]
        return sales_paths, missing, did_export

    did_export = True
    if use_api:
        _log(
            f"[SALES] Fetching API sales window {acquisition_start.isoformat()} -> {acquisition_end.isoformat()}",
            logger,
        )
        archived, _missing_from_export = _fetch_sales_exports_via_api(
            paths=paths,
            selected_store_codes=selected_store_codes,
            start_day=acquisition_start,
            end_day=acquisition_end,
            env_file=api_env_file,
            logger=logger,
            api_workers=api_workers,
        )
    else:
        _log(
            f"[SALES] Exporting acquisition window {acquisition_start.isoformat()} -> {acquisition_end.isoformat()}",
            logger,
        )
        run_sales_export(acquisition_start, acquisition_end, logger)
        archived, _missing_from_export = archive_sales_exports(
            paths,
            acquisition_start,
            acquisition_end,
            selected_store_codes,
            logger,
        )

    if force_refresh:
        sales_paths = archived
    else:
        sales_paths.update(archived)
    missing = [abbr for abbr in selected_store_codes if abbr not in sales_paths]
    return sales_paths, missing, did_export


# ---------------------------------------------------------------------------
# Exports
# ---------------------------------------------------------------------------
def run_sales_export(start_day: date, end_day: date, logger: Optional[Callable[[str], None]]) -> None:
    _log(f"[EXPORT] Running sales export for {start_day} -> {end_day}", logger)
    FILES_DIR.mkdir(parents=True, exist_ok=True)

    start_dt = datetime(start_day.year, start_day.month, start_day.day)
    end_dt = datetime(end_day.year, end_day.month, end_day.day)
    run_sales_report(start_dt, end_dt)
    _log("[EXPORT] Sales export completed.", logger)


def archive_sales_exports(
    paths: RunPaths,
    start_day: date,
    end_day: date,
    selected_store_codes: Sequence[str],
    logger: Optional[Callable[[str], None]],
) -> Tuple[Dict[str, Path], List[str]]:
    archived: Dict[str, Path] = {}
    missing: List[str] = []

    for store_name, abbr in store_abbr_map.items():
        if abbr not in selected_store_codes:
            continue

        src = FILES_DIR / f"sales{abbr}.xlsx"
        if not src.exists():
            missing.append(abbr)
            _log(f"[WARN] Missing sales export for {abbr}: {src}", logger)
            continue

        dst_name = safe_filename(
            f"{abbr} - Sales Export - {osnap.store_label(store_name)} - {start_day.isoformat()}_to_{end_day.isoformat()}.xlsx"
        )
        dst = paths.raw_sales_dir / dst_name
        shutil.copy2(src, dst)
        archived[abbr] = dst
        _log(f"[ARCHIVE] Sales {abbr}: {dst}", logger)

    return archived, missing


def run_catalog_export(logger: Optional[Callable[[str], None]]) -> Dict[str, Tuple[int, float]]:
    _log("[CATALOG] Running getCatalog.py export...", logger)
    FILES_DIR.mkdir(parents=True, exist_ok=True)
    before = _file_snapshot(FILES_DIR, (".csv",))

    cmd = [sys.executable, str(THIS_DIR / "getCatalog.py")]
    proc = subprocess.run(cmd, cwd=str(THIS_DIR), capture_output=True, text=True)

    if proc.stdout.strip():
        lines = proc.stdout.strip().splitlines()
        preview = "\n".join(lines[-12:])
        _log(f"[CATALOG] getCatalog.py output (tail):\n{preview}", logger)
    if proc.stderr.strip():
        lines = proc.stderr.strip().splitlines()
        preview = "\n".join(lines[-12:])
        _log(f"[CATALOG] getCatalog.py stderr (tail):\n{preview}", logger)

    if proc.returncode != 0:
        _log(f"[WARN] getCatalog.py exited with code {proc.returncode}; continuing with available catalog files.", logger)
    else:
        _log("[CATALOG] Catalog export completed.", logger)

    return before


def _catalog_candidate_paths(
    before_snapshot: Dict[str, Tuple[int, float]],
    logger: Optional[Callable[[str], None]],
) -> List[Path]:
    changed = _changed_files_after(FILES_DIR, before_snapshot, (".csv",))
    if changed:
        return changed

    # Fallback to likely catalog files when script output naming varies by environment.
    candidates = sorted(FILES_DIR.glob("*.csv"), key=lambda p: p.stat().st_mtime, reverse=True)
    if not candidates:
        _log("[WARN] No CSV catalog files found in files/.", logger)
        return []

    preferred = [p for p in candidates if p.name.lower().startswith("catalog")]
    if preferred:
        return preferred
    return candidates[:18]


def archive_catalog_exports(
    paths: RunPaths,
    selected_store_codes: Sequence[str],
    before_snapshot: Dict[str, Tuple[int, float]],
    logger: Optional[Callable[[str], None]],
) -> Tuple[List[Path], List[str]]:
    missing_codes = list(selected_store_codes)
    copied: List[Path] = []

    candidates = _catalog_candidate_paths(before_snapshot, logger)
    if not candidates:
        return copied, missing_codes

    per_store_latest: Dict[str, Path] = {}
    unknown: List[Path] = []

    for p in candidates:
        abbr = _abbr_from_filename(p)
        if abbr and abbr in selected_store_codes:
            prev = per_store_latest.get(abbr)
            if prev is None or p.stat().st_mtime > prev.stat().st_mtime:
                per_store_latest[abbr] = p
        else:
            unknown.append(p)

    for abbr, src in per_store_latest.items():
        dst_name = safe_filename(f"catalog_{abbr}_{datetime.now().strftime('%Y%m%d_%H%M%S')}.csv")
        dst = paths.raw_catalog_dir / dst_name
        shutil.copy2(src, dst)
        copied.append(dst)
        if abbr in missing_codes:
            missing_codes.remove(abbr)
        _log(f"[ARCHIVE] Catalog {abbr}: {dst}", logger)

    # If no store code could be parsed, keep a few recent files and parse store from columns later.
    if not copied:
        for src in unknown[:10]:
            dst = paths.raw_catalog_dir / safe_filename(src.name)
            shutil.copy2(src, dst)
            copied.append(dst)
            _log(f"[ARCHIVE] Catalog file: {dst}", logger)

    return copied, missing_codes


def prepare_catalog_exports(
    paths: RunPaths,
    selected_store_codes: Sequence[str],
    run_export: bool,
    force_refresh: bool,
    use_api: bool,
    api_env_file: str,
    api_workers: int,
    logger: Optional[Callable[[str], None]],
) -> Tuple[List[Path], List[str], bool]:
    existing = sorted(paths.raw_catalog_dir.glob("*.csv"), key=lambda p: p.stat().st_mtime, reverse=True)
    if existing and not force_refresh:
        _log(f"[CATALOG] Reusing {len(existing)} cached catalog files from {paths.raw_catalog_dir}", logger)
        return existing, [], False

    if force_refresh and existing:
        _log("[CATALOG] Force refresh enabled. Existing catalog cache will be replaced as new files are archived.", logger)

    before_catalog_snapshot: Dict[str, Tuple[int, float]] = {}
    did_export = False
    if run_export:
        if use_api:
            _log("[CATALOG] Using API inventory/catalog fetch", logger)
            copied, missing = _fetch_catalog_exports_via_api(
                paths=paths,
                selected_store_codes=selected_store_codes,
                env_file=api_env_file,
                logger=logger,
                api_workers=api_workers,
            )
            return copied, missing, True

        before_catalog_snapshot = run_catalog_export(logger)
        did_export = True
    elif existing:
        _log(f"[CATALOG] Using existing catalog files in {paths.raw_catalog_dir}", logger)
        return existing, [], did_export
    else:
        _log("[CATALOG] No cached catalog files in this run. Using any available files from files/.", logger)

    copied, missing = archive_catalog_exports(
        paths,
        selected_store_codes,
        before_snapshot=before_catalog_snapshot,
        logger=logger,
    )
    return copied, missing, did_export


# ---------------------------------------------------------------------------
# Product normalization / merge keys
# ---------------------------------------------------------------------------
def normalize_text(value: Any) -> str:
    s = str(value or "").upper()
    s = s.replace("&", " ")
    s = re.sub(r"[^A-Z0-9\.]+", " ", s)
    s = re.sub(r"\s+", " ", s).strip()
    return s


def extract_size_token(text: str) -> str:
    s = normalize_text(text)

    # Common weights / units
    patterns = [
        r"\b(\d+(?:\.\d+)?)\s*(MG)\b",
        r"\b(\d+(?:\.\d+)?)\s*(G|GR|GRAM|GRAMS)\b",
        r"\b(\d+(?:\.\d+)?)\s*(ML)\b",
        r"\b(\d+(?:\.\d+)?)\s*(OZ)\b",
    ]
    for pat in patterns:
        m = re.search(pat, s)
        if not m:
            continue
        qty = m.group(1)
        unit = m.group(2)
        unit_norm = {
            "GR": "G",
            "GRAM": "G",
            "GRAMS": "G",
        }.get(unit, unit)
        return f"{qty}{unit_norm}"

    # Fractional ounce cues
    frac_map = {
        "1 8": "3.5G",
        "1 4": "7G",
        "1 2": "14G",
    }
    frac = re.search(r"\b(1)\s*/\s*(8|4|2)\b", s)
    if frac:
        return frac_map.get(f"{frac.group(1)} {frac.group(2)}", "")

    return ""


def extract_variant_type(text: str) -> str:
    s = normalize_text(text)
    for key in VARIANT_KEYWORDS:
        if key in s:
            return key
    return ""


def normalize_category_value(value: Any) -> str:
    s = normalize_text(value)
    return s or "UNKNOWN"


def _remove_token_words(text: str, token: str) -> str:
    if not token:
        return text
    out = re.sub(rf"\b{re.escape(token)}\b", " ", text)
    out = re.sub(r"\s+", " ", out).strip()
    return out


def _merge_num_token(value: Any) -> str:
    try:
        v = float(value)
    except Exception:
        return "NA"
    if not np.isfinite(v) or v <= 0:
        return "NA"
    return f"{v:.2f}"


def _bucket_money(value: Any, bucket: float) -> float:
    try:
        v = float(value)
    except Exception:
        return float("nan")
    if not np.isfinite(v):
        return float("nan")
    try:
        b = float(bucket)
    except Exception:
        b = 0.0
    if b <= 0:
        return v
    return round(v / b) * b


def _bucket_token(value: Any, bucket: float) -> str:
    try:
        v = float(value)
    except Exception:
        return "NA"
    if not np.isfinite(v) or v <= 0:
        return "NA"
    vb = _bucket_money(v, bucket)
    if not np.isfinite(vb) or vb <= 0:
        return "NA"
    return f"{vb:.2f}"


def _compose_supply_keys(
    category_norm: Any,
    size_norm: Any,
    supply_family: Any,
    variant_type: Any,
    price_basis: Any,
    cost_basis: Any,
) -> Tuple[str, str, str, str, str]:
    cat = normalize_category_value(category_norm)
    size = str(size_norm or "").strip().upper() or "NA"
    fam = normalize_text(supply_family)
    if not fam or fam == "UNKNOWN":
        fam = cat
    parts = [cat, size, fam]
    var_tok = normalize_text(variant_type)
    if SUPPLY_BASE_USE_VARIANT and var_tok:
        parts.append(var_tok)
    base = "|".join(parts)

    price_tok = _bucket_token(price_basis, SUPPLY_PRICE_BUCKET)
    cost_tok = _bucket_token(cost_basis, SUPPLY_COST_BUCKET)
    merge = f"{base}|P{price_tok}|C{cost_tok}"
    return base, merge, price_tok, cost_tok, fam


def _derive_supply_keys_from_row(
    product_name: Any,
    category_value: Any,
    size_value: Any,
    variant_value: Any,
    core_name_value: Any,
    norm_product_value: Any,
    price_value: Any,
    cost_value: Any,
) -> Tuple[str, str, str]:
    product_norm = normalize_text(norm_product_value)
    if not product_norm:
        product_norm = normalize_text(product_name)
    category_norm = normalize_category_value(category_value)

    size_norm = str(size_value or "").strip().upper()
    if not size_norm or size_norm == "NA":
        size_norm = extract_size_token(product_norm) or "NA"

    variant_norm = normalize_text(variant_value)
    core_norm = normalize_text(core_name_value)
    if not core_norm:
        core_norm = product_norm

    fam = _derive_supply_family(core_norm, product_norm)
    if not fam or fam == "UNKNOWN":
        fam = category_norm

    base, merge, _pt, _ct, fam_norm = _compose_supply_keys(
        category_norm=category_norm,
        size_norm=size_norm,
        supply_family=fam,
        variant_type=variant_norm,
        price_basis=price_value,
        cost_basis=cost_value,
    )
    return fam_norm, base, merge


def _derive_supply_family(core_name: Any, product_norm: Any = "") -> str:
    core = normalize_text(core_name)
    prod = normalize_text(product_norm)
    base = core or prod
    if not base:
        return "UNKNOWN"

    toks = [t for t in base.split() if t]
    if not toks:
        return "UNKNOWN"

    if "BATTERY" in toks:
        if "VARIABLE" in toks and "VOLTAGE" in toks:
            return "VARIABLE VOLTAGE BATTERY"
        return "BATTERY"
    if "STARTER" in toks and "KIT" in toks:
        return "STARTER KIT"

    strip_tokens = {"H", "I", "S", "HYBRID", "INDICA", "SATIVA", "STRAIN"}
    toks = [t for t in toks if t not in strip_tokens]
    if not toks:
        return "UNKNOWN"

    form_tokens = {
        "AIO", "POD", "CART", "CARTRIDGE", "DISPOSABLE", "BADDER", "BATTER", "SAUCE",
        "FLOWER", "ROLL", "ROLLS", "PREROLL", "PRE", "GUMMY", "GUMMIES", "EDIBLE",
        "VAPE", "HASH", "ROSIN", "RESIN", "PAX",
    }
    tech_tokens = {"LR", "LRO", "LIVE", "ROSIN", "RESIN", "DISTILLATE", "INFUSED", "DNA", "LRE"}

    keep = [t for t in toks[:6] if t in form_tokens or t in tech_tokens]
    if keep:
        if len(keep) >= 2 and keep[0] == "PRE" and keep[1] in {"ROLL", "ROLLS", "PREROLL"}:
            return "PRE ROLL"
        if len(keep) >= 2 and keep[0] in tech_tokens and keep[1] in form_tokens:
            return f"{keep[0]} {keep[1]}"
        if len(keep) >= 2 and keep[0] in {"STARTER"} and keep[1] == "KIT":
            return "STARTER KIT"
        return keep[0] if keep[0] in form_tokens else " ".join(keep[:2])

    if len(toks) >= 2:
        return f"{toks[0]} {toks[1]}"
    return toks[0]


def _supply_base_from_merge_key(supply_merge_key: Any) -> str:
    s = str(supply_merge_key or "").strip()
    if not s:
        return ""
    m = re.match(r"^(?P<base>.*)\|P[^|]+\|C[^|]+$", s)
    if m:
        return str(m.group("base"))
    return s


def derive_merge_fields(
    product_name: Any,
    category_value: Any,
    brand_name: str,
    merge_price_basis: Optional[float] = None,
    merge_cost_basis: Optional[float] = None,
) -> Dict[str, str]:
    product_norm = normalize_text(product_name)
    category_norm = normalize_category_value(category_value)
    size_norm = extract_size_token(product_norm)
    variant_type = extract_variant_type(product_norm)

    core = product_norm

    # Remove brand tokens and common descriptors/noise
    brand_norm = normalize_text(brand_name)
    for tok in brand_norm.split():
        if len(tok) > 1:
            core = _remove_token_words(core, tok)

    if size_norm:
        # Remove size with optional space variants.
        core = re.sub(rf"\b{re.escape(size_norm)}\b", " ", core)
        m = re.match(r"^(\d+(?:\.\d+)?)([A-Z]+)$", size_norm)
        if m:
            core = re.sub(rf"\b{re.escape(m.group(1))}\s*{re.escape(m.group(2))}\b", " ", core)

    for vkey in VARIANT_KEYWORDS:
        core = _remove_token_words(core, vkey)

    for tok in NOISE_TOKENS:
        core = _remove_token_words(core, tok)

    # Trim leftover single-char noise blocks.
    core = re.sub(r"\b[A-Z]\b", " ", core)
    core = re.sub(r"\s+", " ", core).strip()
    if not core:
        core = product_norm or "UNKNOWN"

    supply_family = _derive_supply_family(core, product_norm)
    if not supply_family or supply_family == "UNKNOWN":
        supply_family = category_norm

    supply_base_key, supply_merge_key, price_tok, cost_tok, supply_family = _compose_supply_keys(
        category_norm=category_norm,
        size_norm=size_norm or "NA",
        supply_family=supply_family,
        variant_type=variant_type,
        price_basis=merge_price_basis,
        cost_basis=merge_cost_basis,
    )

    merge_key = f"{category_norm}|{size_norm or 'NA'}|{core}|P{price_tok}|C{cost_tok}"
    product_group_key = f"{category_norm}|{size_norm or 'NA'}|P{price_tok}|C{cost_tok}"

    core_disp = core.title()
    disp_bits = [core_disp]
    if size_norm:
        disp_bits.append(size_norm)
    if variant_type:
        disp_bits.append(variant_type.title())
    display_name = " • ".join(disp_bits)
    group_display_bits = [category_norm.title()]
    if price_tok != "NA":
        group_display_bits.append(f"${price_tok}")
    if cost_tok != "NA":
        group_display_bits.append(f"C${cost_tok}")
    product_group_display = " • ".join(group_display_bits)

    return {
        "norm_product_name": product_norm,
        "size_normalized": size_norm,
        "variant_type": variant_type,
        "category_normalized": category_norm,
        "core_name_normalized": core,
        "merge_price_basis": price_tok,
        "merge_cost_basis": cost_tok,
        "merge_key": merge_key,
        "product_group_key": product_group_key,
        "product_group_display": product_group_display,
        "supply_family_name": supply_family,
        "supply_base_key": supply_base_key,
        "supply_merge_key": supply_merge_key,
        "display_product": display_name,
    }


def _ordering_key_text(value: Any) -> str:
    text = str(value or "").strip().lower()
    text = re.sub(r"\s+", " ", text)
    return text


def _first_nonempty_text(*values: Any, default: str = "") -> str:
    for value in values:
        if value is None:
            continue
        try:
            if pd.isna(value):
                continue
        except Exception:
            pass
        text = str(value).strip()
        if text and text.lower() not in {"nan", "none", "null"}:
            return text
    return default


def _ordering_display_product_without_brand(product: Any) -> str:
    raw_product = str(product or "").strip()
    if not raw_product:
        return ""
    parts = [part.strip() for part in raw_product.split("|") if part.strip()]
    if len(parts) <= 1:
        return raw_product
    return " | ".join(parts[1:])


def _ordering_normalize_strain_bucket(value: Any) -> str:
    text = str(value or "").strip().lower()
    if not text:
        return ""
    if re.fullmatch(r"i|ind|indica", text):
        return "I"
    if re.fullmatch(r"h|hyb|hybrid", text):
        return "H"
    if re.fullmatch(r"s|sat|sativa", text):
        return "S"
    return ""


def _ordering_combine_group_products(values: Any) -> str:
    unique_values = list(
        OrderedDict.fromkeys(
            str(value).strip()
            for value in values
            if str(value).strip()
        )
    )
    if not unique_values:
        return ""
    if len(unique_values) == 1:
        return unique_values[0]

    tokenized = [[part.strip() for part in value.split("|") if part.strip()] for value in unique_values]
    min_len = min((len(tokens) for tokens in tokenized), default=0)
    prefix_length = 0
    for index in range(min_len):
        candidate_key = _ordering_key_text(tokenized[0][index])
        if all(_ordering_key_text(tokens[index]) == candidate_key for tokens in tokenized[1:]):
            prefix_length += 1
            continue
        break

    if prefix_length > 0:
        prefix = tokenized[0][:prefix_length]
        suffixes = list(
            OrderedDict.fromkeys(
                " | ".join(tokens[prefix_length:]).strip()
                for tokens in tokenized
                if " | ".join(tokens[prefix_length:]).strip()
            )
        )
        if suffixes:
            return " | ".join(prefix + [" / ".join(suffixes)])

    return " / ".join(unique_values)


def _ordering_base_row_key(row: pd.Series, include_store: bool = False) -> str:
    store_code = str(row.get("_store_abbr", "") or "").strip().upper() if include_store else ""
    prefix = f"{store_code}|" if store_code else ""
    sku_text = _first_nonempty_text(row.get("SKU"), row.get("Sku"), row.get("sku"))
    if sku_text:
        return f"{prefix}sku:{sku_text}"

    raw_product = _first_nonempty_text(
        row.get("Product"),
        row.get("Product Name"),
        row.get("_product_raw"),
        row.get("display_product"),
        default="Unknown Product",
    )
    brand_key = str(row.get("brand_key") or "unknown").strip() or "unknown"
    product_key = _ordering_key_text(raw_product)
    if product_key:
        return f"{prefix}fallback:{brand_key}|{product_key}"

    brand_product_text = str(row.get("brand_product_key") or "").strip()
    if brand_product_text:
        return f"{prefix}product:{brand_product_text}"
    return f"{prefix}fallback:{brand_key}|unknown"


def _ordering_family_group_metadata(row: pd.Series, include_store: bool = False) -> Dict[str, Any]:
    raw_product = _first_nonempty_text(
        row.get("Product"),
        row.get("Product Name"),
        row.get("_product_raw"),
        row.get("display_product"),
        default="Unknown Product",
    )
    row_key = _ordering_base_row_key(row, include_store=include_store)
    parts = [part.strip() for part in raw_product.split("|") if part.strip()]
    product_parts = parts[1:] if len(parts) > 1 else ([raw_product] if raw_product else [])

    strain_index = None
    strain_bucket = ""
    for index, token in enumerate(product_parts):
        normalized = _ordering_normalize_strain_bucket(token)
        if normalized:
            strain_index = index
            strain_bucket = normalized
            break

    if strain_index is not None and strain_index > 0:
        base_parts = product_parts[:strain_index]
        family_display = " | ".join(base_parts + [strain_bucket]).strip()
        family_key = _ordering_key_text(family_display)
        category_key = _ordering_key_text(row.get("category_normalized") or row.get("Category") or "UNKNOWN")
        brand_text = str(row.get("brand_key") or "unknownbrand").strip() or "unknownbrand"
        store_code = str(row.get("_store_abbr", "") or "").strip().upper() if include_store else ""
        prefix = f"{store_code}|" if store_code else ""
        return {
            "ordering_product_key": f"{prefix}family:{brand_text}|{category_key}|{family_key}",
            "ordering_product_display": _ordering_display_product_without_brand(raw_product),
            "ordering_product_is_family": True,
        }

    return {
        "ordering_product_key": row_key,
        "ordering_product_display": _ordering_display_product_without_brand(raw_product),
        "ordering_product_is_family": False,
    }


def _apply_weekly_ordering_product_identity(df: pd.DataFrame, include_store: bool = False) -> pd.DataFrame:
    if df is None or df.empty:
        return pd.DataFrame(columns=df.columns if df is not None else [])
    out = df.copy()
    meta = pd.DataFrame(
        [_ordering_family_group_metadata(row, include_store=include_store) for _, row in out.iterrows()],
        index=out.index,
    )
    for col in meta.columns:
        out[col] = meta[col]
    out["ordering_product_key"] = out["ordering_product_key"].fillna("").astype(str)
    out["ordering_product_display"] = out["ordering_product_display"].fillna("").astype(str)
    return out


def _merge_token_to_float(token: Any, fallback: float) -> float:
    try:
        v = float(token)
    except Exception:
        return float(fallback or 0.0)
    if not np.isfinite(v) or v <= 0:
        return float(fallback or 0.0)
    return float(v)


def _mode_pair_map(
    df: pd.DataFrame,
    key_cols: Sequence[str],
) -> Dict[Any, Tuple[str, str]]:
    if df is None or df.empty or not key_cols:
        return {}

    tmp = df.copy()
    for col in key_cols:
        if col not in tmp.columns:
            tmp[col] = ""
        tmp[col] = tmp[col].fillna("").astype(str).str.strip()

    if "Price_Used" not in tmp.columns:
        tmp["Price_Used"] = 0.0
    if "Cost" not in tmp.columns:
        tmp["Cost"] = 0.0
    tmp["price_num"] = osnap.to_number(tmp["Price_Used"]).fillna(0.0).astype(float)
    tmp["cost_num"] = osnap.to_number(tmp["Cost"]).fillna(0.0).astype(float)
    tmp = tmp[(tmp["price_num"] > 0) | (tmp["cost_num"] > 0)].copy()
    if tmp.empty:
        return {}

    tmp["price_tok"] = tmp["price_num"].map(lambda v: _bucket_token(v, SUPPLY_PRICE_BUCKET))
    tmp["cost_tok"] = tmp["cost_num"].map(lambda v: _bucket_token(v, SUPPLY_COST_BUCKET))

    if "Available" in tmp.columns:
        tmp["_w"] = osnap.to_number(tmp["Available"]).fillna(0.0).astype(float).clip(lower=0.0) + 1.0
    else:
        tmp["_w"] = 1.0

    agg = tmp.groupby(list(key_cols) + ["price_tok", "cost_tok"], as_index=False).agg(weight=("_w", "sum"))
    sort_cols = list(key_cols) + ["weight", "price_tok", "cost_tok"]
    asc = [True] * len(key_cols) + [False, False, False]
    best = agg.sort_values(sort_cols, ascending=asc).drop_duplicates(subset=list(key_cols), keep="first")

    out: Dict[Any, Tuple[str, str]] = {}
    for _, r in best.iterrows():
        key = tuple(str(r[c]) for c in key_cols)
        if len(key) == 1:
            key = key[0]
        out[key] = (str(r["price_tok"]), str(r["cost_tok"]))
    return out


def build_catalog_merge_maps(catalog_brand_df: pd.DataFrame) -> Dict[str, Dict[Any, Tuple[str, str]]]:
    if catalog_brand_df is None or catalog_brand_df.empty:
        return {}

    tmp = catalog_brand_df.copy()
    if "_store_abbr" not in tmp.columns:
        tmp["_store_abbr"] = ""
    tmp["_store_abbr"] = tmp["_store_abbr"].fillna("").astype(str).str.upper().str.strip()
    if "norm_product_name" not in tmp.columns:
        if "_product_raw" in tmp.columns:
            tmp["norm_product_name"] = tmp["_product_raw"].map(normalize_text)
        else:
            tmp["norm_product_name"] = ""
    else:
        tmp["norm_product_name"] = tmp["norm_product_name"].fillna("").astype(str).str.strip()
    if "category_normalized" not in tmp.columns:
        tmp["category_normalized"] = "UNKNOWN"
    tmp["category_normalized"] = tmp["category_normalized"].map(normalize_category_value)
    if "size_normalized" not in tmp.columns:
        tmp["size_normalized"] = ""
    tmp["size_normalized"] = tmp["size_normalized"].fillna("").astype(str).str.upper().str.strip()
    tmp["size_normalized"] = tmp["size_normalized"].replace({"": "NA"})
    if "Cost" not in tmp.columns:
        tmp["Cost"] = 0.0
    tmp["cost_tok_key"] = osnap.to_number(tmp["Cost"]).fillna(0.0).astype(float).map(
        lambda v: _bucket_token(v, SUPPLY_COST_BUCKET)
    )

    exact_store = _mode_pair_map(
        tmp[tmp["_store_abbr"] != ""],
        ["_store_abbr", "norm_product_name"],
    )
    exact_global = _mode_pair_map(tmp, ["norm_product_name"])
    family_store = _mode_pair_map(
        tmp[tmp["_store_abbr"] != ""],
        ["_store_abbr", "category_normalized", "size_normalized", "cost_tok_key"],
    )
    family_global = _mode_pair_map(tmp, ["category_normalized", "size_normalized", "cost_tok_key"])
    size_store = _mode_pair_map(
        tmp[tmp["_store_abbr"] != ""],
        ["_store_abbr", "category_normalized", "size_normalized"],
    )
    size_global = _mode_pair_map(tmp, ["category_normalized", "size_normalized"])

    return {
        "exact_store": exact_store,
        "exact_global": exact_global,
        "family_store": family_store,
        "family_global": family_global,
        "size_store": size_store,
        "size_global": size_global,
    }


def resolve_catalog_merge_basis(
    catalog_maps: Optional[Dict[str, Dict[Any, Tuple[str, str]]]],
    store_abbr: Any,
    product_norm: Any,
    category_norm: Any,
    size_norm: Any,
    observed_price: float,
    observed_cost: float,
) -> Tuple[float, float, bool]:
    if not catalog_maps:
        return float(observed_price or 0.0), float(observed_cost or 0.0), False

    store = str(store_abbr or "").upper().strip()
    prod = normalize_text(product_norm)
    cat = normalize_category_value(category_norm)
    size = str(size_norm or "").upper().strip() or "NA"
    cost_tok = _bucket_token(observed_cost, SUPPLY_COST_BUCKET)

    candidates: List[Tuple[str, Any]] = []
    if store and prod:
        candidates.append(("exact_store", (store, prod)))
    if prod:
        candidates.append(("exact_global", prod))
    if store:
        candidates.append(("family_store", (store, cat, size, cost_tok)))
    candidates.append(("family_global", (cat, size, cost_tok)))
    if store:
        candidates.append(("size_store", (store, cat, size)))
    candidates.append(("size_global", (cat, size)))

    for map_name, key in candidates:
        pair = (catalog_maps.get(map_name) or {}).get(key)
        if not pair:
            continue
        price_tok, cost_tok_sel = pair
        return (
            _merge_token_to_float(price_tok, float(observed_price or 0.0)),
            _merge_token_to_float(cost_tok_sel, float(observed_cost or 0.0)),
            True,
        )

    return float(observed_price or 0.0), float(observed_cost or 0.0), False


# ---------------------------------------------------------------------------
# Sales preprocessing + metrics
# ---------------------------------------------------------------------------
def _prepare_sales_df_for_brand(
    df: pd.DataFrame,
    store_code: str,
    brand: str,
    brand_aliases: Optional[Sequence[str]],
    include_kickbacks: bool,
    logger: Optional[Callable[[str], None]],
    catalog_merge_maps: Optional[Dict[str, Dict[Any, Tuple[str, str]]]] = None,
) -> pd.DataFrame:
    if df is None or df.empty:
        return pd.DataFrame()

    work = df.copy()

    # Force sales-based margins by default (no kickback effect in margin math).
    if include_kickbacks:
        try:
            work = osnap.enrich_with_deal_kickbacks_by_brand(work, store_code)
        except Exception as exc:
            _log(f"[WARN] Kickback enrichment failed for {store_code}: {exc}", logger)

    date_col = osnap.find_col(work, osnap.COLUMN_CANDIDATES["date"])
    product_col = osnap.find_col(work, osnap.COLUMN_CANDIDATES["product"])
    net_col = osnap.find_col(work, osnap.COLUMN_CANDIDATES["net_sales"])
    if not date_col or not product_col or not net_col:
        _log(f"[WARN] Missing critical columns for {store_code}; skipping.", logger)
        return pd.DataFrame()

    tx_col = osnap.find_col(work, osnap.COLUMN_CANDIDATES["transaction_id"])
    category_col = osnap.find_col(work, osnap.COLUMN_CANDIDATES["category"])
    qty_col = osnap.find_col(work, osnap.COLUMN_CANDIDATES["quantity"])
    gross_col = osnap.find_col(work, osnap.COLUMN_CANDIDATES["gross_sales"])
    loc_price_col = osnap.find_col(work, ["Location price", "Location Price"])
    price_col = osnap.find_col(work, ["Price", "Unit Price", "Retail Price", "List Price"])
    disc_main_col = osnap.find_col(work, osnap.COLUMN_CANDIDATES["discount_main"])
    disc_loyal_col = osnap.find_col(work, osnap.COLUMN_CANDIDATES["discount_loyalty"])
    cogs_col = osnap.find_col(work, osnap.COLUMN_CANDIDATES["cogs"])
    profit_col = osnap.find_col(work, osnap.COLUMN_CANDIDATES["profit"])
    return_col = osnap.find_col(work, osnap.COLUMN_CANDIDATES["return_date"])
    weight_col = osnap.find_col(work, osnap.COLUMN_CANDIDATES["total_weight_sold"])

    work[date_col] = pd.to_datetime(work[date_col], errors="coerce")
    work = work[work[date_col].notna()].copy()
    if work.empty:
        return pd.DataFrame()

    work["_date"] = work[date_col].dt.date
    work["_product_raw"] = work[product_col].fillna("Unknown").astype(str)

    target = canon(brand)
    alias_values = [str(a).strip() for a in (brand_aliases or []) if str(a).strip()]
    alias_values.append(str(brand).strip())
    alias_canons = {canon(a) for a in alias_values if canon(a)}
    if target:
        alias_canons.add(target)

    parsed_brand = work["_product_raw"].apply(osnap.parse_brand_from_product)
    parsed_canon = parsed_brand.apply(canon)
    mask_primary = parsed_canon.isin(alias_canons)

    prod_txt = work["_product_raw"].fillna("").astype(str).str.strip()
    prod_l = prod_txt.str.lower()
    mask_prefix_alias = pd.Series(False, index=work.index, dtype=bool)
    for a in alias_values:
        al = str(a).strip().lower()
        if not al:
            continue
        mask_prefix_alias = (
            mask_prefix_alias
            | prod_l.eq(al)
            | prod_l.str.startswith(al + "|")
            | prod_l.str.startswith(al + " |")
            | prod_l.str.startswith(al + " -")
            | prod_l.str.startswith(al + "-")
            | prod_l.str.startswith(al + " ")
        )

    work = work[mask_primary | mask_prefix_alias].copy()
    if work.empty:
        return pd.DataFrame()

    work["_store_abbr"] = store_code
    work["_store_name"] = _store_name_from_abbr(store_code)

    work["_net"] = osnap.to_number(work[net_col]).fillna(0.0).astype(float)
    work["_gross"] = osnap.to_number(work[gross_col]).fillna(0.0).astype(float) if gross_col else 0.0
    work["_qty"] = osnap.to_number(work[qty_col]).fillna(0.0).astype(float) if qty_col else 1.0

    work["_disc_main"] = osnap.to_number(work[disc_main_col]).fillna(0.0).astype(float) if disc_main_col else 0.0
    work["_disc_loyal"] = osnap.to_number(work[disc_loyal_col]).fillna(0.0).astype(float) if disc_loyal_col else 0.0
    work["_disc_total"] = (work["_disc_main"] + work["_disc_loyal"]).astype(float)

    if tx_col:
        tx_txt = work[tx_col].astype(str).str.strip()
        tx_missing = work[tx_col].isna() | tx_txt.eq("") | tx_txt.str.lower().isin({"nan", "none"})
        work["_tx_key"] = tx_txt
        work.loc[tx_missing, "_tx_key"] = "__row_" + work.loc[tx_missing].index.astype(str)
    else:
        work["_tx_key"] = "__row_" + work.index.astype(str)

    work["_kickback_amt"] = (
        osnap.to_number(work["_deal_kickback_amt"]).fillna(0.0).astype(float)
        if "_deal_kickback_amt" in work.columns
        else 0.0
    )

    if "_cogs_raw" in work.columns:
        work["_cogs_real"] = osnap.to_number(work["_cogs_raw"]).fillna(0.0).astype(float)
    else:
        work["_cogs_real"] = osnap.to_number(work[cogs_col]).fillna(0.0).astype(float) if cogs_col else 0.0

    # Margin basis stays sales-only (no kickback lift).
    work["_cogs_kb"] = work["_cogs_real"]

    if profit_col:
        work["_profit_real"] = osnap.to_number(work[profit_col]).fillna(0.0).astype(float)
    elif "_profit_adj" in work.columns and "_deal_kickback_amt" in work.columns:
        work["_profit_real"] = (osnap.to_number(work["_profit_adj"]).fillna(0.0) - work["_kickback_amt"]).astype(float)
    else:
        work["_profit_real"] = (work["_net"] - work["_cogs_real"]).astype(float)

    work["_profit_kb"] = work["_profit_real"]
    work["_kickback_amt"] = 0.0

    work["_weight"] = osnap.to_number(work[weight_col]).fillna(0.0).astype(float) if weight_col else 0.0
    work["_is_return"] = work[return_col].notna() if return_col else False

    loc_price = osnap.to_number(work[loc_price_col]).fillna(0.0).astype(float) if loc_price_col else pd.Series(0.0, index=work.index)
    base_price = osnap.to_number(work[price_col]).fillna(0.0).astype(float) if price_col else pd.Series(0.0, index=work.index)
    units = work["_qty"].replace({0: np.nan})
    unit_sale_price = (work["_net"] / units).replace([np.inf, -np.inf], np.nan).fillna(0.0)
    unit_cost = (work["_cogs_real"] / units).replace([np.inf, -np.inf], np.nan).fillna(0.0)

    merge_price_basis = pd.Series(
        np.where(loc_price > 0, loc_price, np.where(base_price > 0, base_price, unit_sale_price)),
        index=work.index,
    ).astype(float)

    category_raw = work[category_col].fillna("Unknown").astype(str) if category_col else "Unknown"
    merge_rows: List[Dict[str, str]] = []
    catalog_match_flags: List[bool] = []
    matched_catalog_basis = 0
    for pn, cat, mp, mc in zip(work["_product_raw"], category_raw, merge_price_basis, unit_cost):
        pn_norm = normalize_text(pn)
        cat_norm = normalize_category_value(cat)
        sz_norm = extract_size_token(pn_norm) or "NA"
        resolved_price, resolved_cost, matched = resolve_catalog_merge_basis(
            catalog_maps=catalog_merge_maps,
            store_abbr=store_code,
            product_norm=pn_norm,
            category_norm=cat_norm,
            size_norm=sz_norm,
            observed_price=float(mp),
            observed_cost=float(mc),
        )
        if matched:
            matched_catalog_basis += 1
        catalog_match_flags.append(bool(matched))
        merge_rows.append(derive_merge_fields(pn, cat, brand, resolved_price, resolved_cost))

    if catalog_merge_maps:
        _log(
            f"[MERGE] {store_code}: catalog-resolved basis {matched_catalog_basis}/{len(work)} rows.",
            logger,
        )
    merge_df = pd.DataFrame(merge_rows, index=work.index)
    for col in merge_df.columns:
        work[col] = merge_df[col]
    work["_catalog_basis_matched"] = pd.Series(catalog_match_flags, index=work.index).fillna(False).astype(bool)

    work["_deal_rule"] = work.get("_deal_rule", "")
    work["_deal_brand"] = work.get("_deal_brand", "")

    return work


def _empty_prepared_brand_sales_df() -> pd.DataFrame:
    return pd.DataFrame(columns=[
        "_date",
        "_product_raw",
        "_store_abbr",
        "_store_name",
        "_net",
        "_gross",
        "_qty",
        "_disc_main",
        "_disc_loyal",
        "_disc_total",
        "_tx_key",
        "_kickback_amt",
        "_cogs_real",
        "_cogs_kb",
        "_profit_real",
        "_profit_kb",
        "_weight",
        "_is_return",
        "norm_product_name",
        "size_normalized",
        "variant_type",
        "category_normalized",
        "core_name_normalized",
        "merge_price_basis",
        "merge_cost_basis",
        "merge_key",
        "product_group_key",
        "product_group_display",
        "supply_family_name",
        "supply_base_key",
        "supply_merge_key",
        "display_product",
        "_catalog_basis_matched",
        "_deal_rule",
        "_deal_brand",
    ])


def _prepare_sales_df_all_brands(
    df: pd.DataFrame,
    store_code: str,
    logger: Optional[Callable[[str], None]],
    catalog_merge_maps: Optional[Dict[str, Dict[Any, Tuple[str, str]]]] = None,
    brand_display_map: Optional[Dict[str, str]] = None,
) -> pd.DataFrame:
    if df is None or df.empty:
        return pd.DataFrame()

    work = df.copy()

    date_col = osnap.find_col(work, osnap.COLUMN_CANDIDATES["date"])
    product_col = osnap.find_col(work, osnap.COLUMN_CANDIDATES["product"])
    net_col = osnap.find_col(work, osnap.COLUMN_CANDIDATES["net_sales"])
    if not date_col or not product_col or not net_col:
        _log(f"[WARN] Missing critical columns for {store_code}; skipping.", logger)
        return pd.DataFrame()

    tx_col = osnap.find_col(work, osnap.COLUMN_CANDIDATES["transaction_id"])
    category_col = osnap.find_col(work, osnap.COLUMN_CANDIDATES["category"])
    qty_col = osnap.find_col(work, osnap.COLUMN_CANDIDATES["quantity"])
    gross_col = osnap.find_col(work, osnap.COLUMN_CANDIDATES["gross_sales"])
    loc_price_col = osnap.find_col(work, ["Location price", "Location Price"])
    price_col = osnap.find_col(work, ["Price", "Unit Price", "Retail Price", "List Price"])
    disc_main_col = osnap.find_col(work, osnap.COLUMN_CANDIDATES["discount_main"])
    disc_loyal_col = osnap.find_col(work, osnap.COLUMN_CANDIDATES["discount_loyalty"])
    cogs_col = osnap.find_col(work, osnap.COLUMN_CANDIDATES["cogs"])
    profit_col = osnap.find_col(work, osnap.COLUMN_CANDIDATES["profit"])
    return_col = osnap.find_col(work, osnap.COLUMN_CANDIDATES["return_date"])
    weight_col = osnap.find_col(work, osnap.COLUMN_CANDIDATES["total_weight_sold"])

    work[date_col] = pd.to_datetime(work[date_col], errors="coerce")
    work = work[work[date_col].notna()].copy()
    if work.empty:
        return pd.DataFrame()

    work["_date"] = work[date_col].dt.date
    work["_product_raw"] = work[product_col].fillna("Unknown").astype(str)
    parsed_brand = work["_product_raw"].apply(osnap.parse_brand_from_product).fillna("Unknown").astype(str).str.strip()
    work["brand_key"] = parsed_brand.apply(lambda x: canon(x) or "unknown")
    display_map = brand_display_map or {}
    work["brand_name"] = work["brand_key"].map(display_map).fillna(parsed_brand).replace({"": "Unknown"}).astype(str)

    work["_store_abbr"] = store_code
    work["_store_name"] = _store_name_from_abbr(store_code)

    work["_net"] = osnap.to_number(work[net_col]).fillna(0.0).astype(float)
    work["_gross"] = osnap.to_number(work[gross_col]).fillna(0.0).astype(float) if gross_col else 0.0
    work["_qty"] = osnap.to_number(work[qty_col]).fillna(0.0).astype(float) if qty_col else 1.0

    work["_disc_main"] = osnap.to_number(work[disc_main_col]).fillna(0.0).astype(float) if disc_main_col else 0.0
    work["_disc_loyal"] = osnap.to_number(work[disc_loyal_col]).fillna(0.0).astype(float) if disc_loyal_col else 0.0
    work["_disc_total"] = (work["_disc_main"] + work["_disc_loyal"]).astype(float)

    if tx_col:
        tx_txt = work[tx_col].astype(str).str.strip()
        tx_missing = work[tx_col].isna() | tx_txt.eq("") | tx_txt.str.lower().isin({"nan", "none"})
        work["_tx_key"] = tx_txt
        work.loc[tx_missing, "_tx_key"] = "__row_" + work.loc[tx_missing].index.astype(str)
    else:
        work["_tx_key"] = "__row_" + work.index.astype(str)

    if "_cogs_raw" in work.columns:
        work["_cogs_real"] = osnap.to_number(work["_cogs_raw"]).fillna(0.0).astype(float)
    else:
        work["_cogs_real"] = osnap.to_number(work[cogs_col]).fillna(0.0).astype(float) if cogs_col else 0.0

    work["_cogs_kb"] = work["_cogs_real"]
    if profit_col:
        work["_profit_real"] = osnap.to_number(work[profit_col]).fillna(0.0).astype(float)
    else:
        work["_profit_real"] = (work["_net"] - work["_cogs_real"]).astype(float)

    work["_profit_kb"] = work["_profit_real"]
    work["_kickback_amt"] = 0.0
    work["_weight"] = osnap.to_number(work[weight_col]).fillna(0.0).astype(float) if weight_col else 0.0
    work["_is_return"] = work[return_col].notna() if return_col else False

    loc_price = osnap.to_number(work[loc_price_col]).fillna(0.0).astype(float) if loc_price_col else pd.Series(0.0, index=work.index)
    base_price = osnap.to_number(work[price_col]).fillna(0.0).astype(float) if price_col else pd.Series(0.0, index=work.index)
    units = work["_qty"].replace({0: np.nan})
    unit_sale_price = (work["_net"] / units).replace([np.inf, -np.inf], np.nan).fillna(0.0)
    unit_cost = (work["_cogs_real"] / units).replace([np.inf, -np.inf], np.nan).fillna(0.0)
    merge_price_basis = pd.Series(
        np.where(loc_price > 0, loc_price, np.where(base_price > 0, base_price, unit_sale_price)),
        index=work.index,
    ).astype(float)

    category_raw = work[category_col].fillna("Unknown").astype(str) if category_col else "Unknown"
    merge_rows: List[Dict[str, str]] = []
    matched_catalog_basis = 0
    catalog_match_flags: List[bool] = []
    for pn, cat, brand_name, mp, mc in zip(work["_product_raw"], category_raw, work["brand_name"], merge_price_basis, unit_cost):
        pn_norm = normalize_text(pn)
        cat_norm = normalize_category_value(cat)
        sz_norm = extract_size_token(pn_norm) or "NA"
        resolved_price, resolved_cost, matched = resolve_catalog_merge_basis(
            catalog_maps=catalog_merge_maps,
            store_abbr=store_code,
            product_norm=pn_norm,
            category_norm=cat_norm,
            size_norm=sz_norm,
            observed_price=float(mp),
            observed_cost=float(mc),
        )
        if matched:
            matched_catalog_basis += 1
        catalog_match_flags.append(bool(matched))
        merge_rows.append(derive_merge_fields(pn, cat, brand_name, resolved_price, resolved_cost))

    if catalog_merge_maps:
        _log(
            f"[MERGE] {store_code}: catalog-resolved basis {matched_catalog_basis}/{len(work)} rows.",
            logger,
        )

    merge_df = pd.DataFrame(merge_rows, index=work.index)
    for col in merge_df.columns:
        work[col] = merge_df[col]
    work["_catalog_basis_matched"] = pd.Series(catalog_match_flags, index=work.index).fillna(False).astype(bool)
    work["brand_category_key"] = work["brand_key"].fillna("unknown").astype(str) + "|" + work["category_normalized"].fillna("UNKNOWN").astype(str)
    work["brand_product_key"] = work["brand_key"].fillna("unknown").astype(str) + "|" + work["merge_key"].fillna("").astype(str)
    work["brand_product_display"] = work["brand_name"].fillna("Unknown").astype(str) + " | " + work["display_product"].fillna("").astype(str)
    return work


def _date_filter(df: pd.DataFrame, start_day: date, end_day: date) -> pd.DataFrame:
    if df is None or df.empty:
        return pd.DataFrame(columns=df.columns if df is not None else [])
    return df[(df["_date"] >= start_day) & (df["_date"] <= end_day)].copy()


def summarize_metrics(df: pd.DataFrame) -> Dict[str, float]:
    if df is None or df.empty:
        keys = [
            "net_revenue", "gross_sales", "tickets", "items", "basket", "items_per_ticket", "net_price_per_item",
            "discount", "discount_main", "loyalty_discount", "discount_rate",
            "cogs_real", "cogs", "profit_real", "profit", "margin_real", "margin",
            "returns_net", "returns_tickets", "weight_sold",
            "kickback_total", "kickback_rows", "row_count",
        ]
        return {k: 0.0 for k in keys}

    net = float(df["_net"].sum())
    gross = float(df["_gross"].sum())
    tickets = float(df["_tx_key"].nunique())
    items = float(df["_qty"].sum())

    discount_main = float(df["_disc_main"].sum())
    discount_loyal = float(df["_disc_loyal"].sum())
    discount = float(df["_disc_total"].sum())

    cogs_real = float(df["_cogs_real"].sum())
    cogs_kb = float(df["_cogs_kb"].sum())

    profit_real = float(df["_profit_real"].sum())
    profit_kb = float(df["_profit_kb"].sum())

    ret_df = df[df["_is_return"]]
    returns_net = float(ret_df["_net"].sum()) if not ret_df.empty else 0.0
    returns_tickets = float(ret_df["_tx_key"].nunique()) if not ret_df.empty else 0.0

    kickback_total = float(df["_kickback_amt"].sum()) if "_kickback_amt" in df.columns else 0.0
    kickback_rows = float((df["_kickback_amt"] > 0).sum()) if "_kickback_amt" in df.columns else 0.0

    if gross:
        discount_rate = discount / gross
    else:
        approx_g = net + discount
        discount_rate = (discount / approx_g) if approx_g else 0.0

    basket = (net / tickets) if tickets else 0.0
    items_per_ticket = (items / tickets) if tickets else 0.0
    net_price_per_item = (net / items) if items else 0.0

    margin_real = (profit_real / net) if net else 0.0
    margin_kb = (profit_kb / net) if net else 0.0

    return {
        "net_revenue": net,
        "gross_sales": gross,
        "tickets": tickets,
        "items": items,
        "basket": basket,
        "items_per_ticket": items_per_ticket,
        "net_price_per_item": net_price_per_item,
        "discount": discount,
        "discount_main": discount_main,
        "loyalty_discount": discount_loyal,
        "discount_rate": discount_rate,
        "cogs_real": cogs_real,
        "cogs": cogs_kb,
        "profit_real": profit_real,
        "profit": profit_kb,
        "margin_real": margin_real,
        "margin": margin_kb,
        "returns_net": returns_net,
        "returns_tickets": returns_tickets,
        "weight_sold": float(df["_weight"].sum()) if "_weight" in df.columns else 0.0,
        "kickback_total": kickback_total,
        "kickback_rows": kickback_rows,
        "row_count": float(len(df)),
    }


def summarize_group(df: pd.DataFrame, group_col: str) -> pd.DataFrame:
    if df is None or df.empty or group_col not in df.columns:
        return pd.DataFrame()

    g = df.groupby(group_col, as_index=False).agg(
        net_revenue=("_net", "sum"),
        gross_sales=("_gross", "sum"),
        tickets=("_tx_key", "nunique"),
        items=("_qty", "sum"),
        discount=("_disc_total", "sum"),
        cogs=("_cogs_kb", "sum"),
        cogs_real=("_cogs_real", "sum"),
        profit=("_profit_kb", "sum"),
        profit_real=("_profit_real", "sum"),
        weight_sold=("_weight", "sum"),
        kickback_total=("_kickback_amt", "sum"),
    )

    ret = df[df["_is_return"]]
    if not ret.empty:
        ret_net = ret.groupby(group_col, as_index=False)["_net"].sum().rename(columns={"_net": "returns_net"})
        ret_tix = ret.groupby(group_col, as_index=False)["_tx_key"].nunique().rename(columns={"_tx_key": "returns_tickets"})
        g = g.merge(ret_net, on=group_col, how="left")
        g = g.merge(ret_tix, on=group_col, how="left")

    if "returns_net" in g.columns:
        g["returns_net"] = pd.to_numeric(g["returns_net"], errors="coerce").fillna(0.0).astype(float)
    else:
        g["returns_net"] = 0.0

    if "returns_tickets" in g.columns:
        g["returns_tickets"] = pd.to_numeric(g["returns_tickets"], errors="coerce").fillna(0.0).astype(float)
    else:
        g["returns_tickets"] = 0.0

    g["basket"] = g["net_revenue"] / g["tickets"].replace({0: np.nan})
    g["items_per_ticket"] = g["items"] / g["tickets"].replace({0: np.nan})
    g["net_price_per_item"] = g["net_revenue"] / g["items"].replace({0: np.nan})
    g["margin"] = g["profit"] / g["net_revenue"].replace({0: np.nan})
    g["margin_real"] = g["profit_real"] / g["net_revenue"].replace({0: np.nan})

    approx_g = (g["net_revenue"] + g["discount"]).replace({0: np.nan})
    g["discount_rate"] = np.where(
        g["gross_sales"] > 0,
        g["discount"] / g["gross_sales"].replace({0: np.nan}),
        g["discount"] / approx_g,
    )

    g = g.fillna(0.0)
    return g.sort_values("net_revenue", ascending=False)


def summarize_daily(df: pd.DataFrame) -> pd.DataFrame:
    if df is None or df.empty:
        return pd.DataFrame(columns=["date", "net_revenue", "tickets", "discount_rate", "margin", "margin_real"])

    g = df.groupby("_date", as_index=False).agg(
        net_revenue=("_net", "sum"),
        gross_sales=("_gross", "sum"),
        tickets=("_tx_key", "nunique"),
        discount=("_disc_total", "sum"),
        profit=("_profit_kb", "sum"),
        profit_real=("_profit_real", "sum"),
    )
    g["margin"] = g["profit"] / g["net_revenue"].replace({0: np.nan})
    g["margin_real"] = g["profit_real"] / g["net_revenue"].replace({0: np.nan})

    approx_g = (g["net_revenue"] + g["discount"]).replace({0: np.nan})
    g["discount_rate"] = np.where(
        g["gross_sales"] > 0,
        g["discount"] / g["gross_sales"].replace({0: np.nan}),
        g["discount"] / approx_g,
    )

    g = g.rename(columns={"_date": "date"}).fillna(0.0)
    g = g.sort_values("_date") if "_date" in g.columns else g.sort_values("date")
    if "_date" in g.columns:
        g = g.rename(columns={"_date": "date"})
    return g


def summarize_weekly(df: pd.DataFrame) -> pd.DataFrame:
    if df is None or df.empty:
        return pd.DataFrame(columns=["week_start", "net_revenue", "tickets", "margin", "margin_real"])

    tmp = df.copy()
    tmp["week_start"] = tmp["_date"].apply(lambda d: d - timedelta(days=d.weekday()))
    g = tmp.groupby("week_start", as_index=False).agg(
        net_revenue=("_net", "sum"),
        tickets=("_tx_key", "nunique"),
        discount=("_disc_total", "sum"),
        gross_sales=("_gross", "sum"),
        profit=("_profit_kb", "sum"),
        profit_real=("_profit_real", "sum"),
    )
    g["margin"] = g["profit"] / g["net_revenue"].replace({0: np.nan})
    g["margin_real"] = g["profit_real"] / g["net_revenue"].replace({0: np.nan})
    approx_g = (g["net_revenue"] + g["discount"]).replace({0: np.nan})
    g["discount_rate"] = np.where(
        g["gross_sales"] > 0,
        g["discount"] / g["gross_sales"].replace({0: np.nan}),
        g["discount"] / approx_g,
    )
    return g.fillna(0.0).sort_values("week_start")


def summarize_product_groups(df: pd.DataFrame) -> pd.DataFrame:
    if df is None or df.empty:
        return pd.DataFrame(columns=[
            "product_group_key", "product_group_display", "category_normalized", "size_normalized", "variant_type",
            "net_revenue", "units", "profit", "profit_real", "margin", "margin_real", "avg_price_per_item",
            "avg_cost_per_item", "tickets", "discount", "discount_rate", "merged_count", "raw_names_top5", "product_list",
        ])

    df = _filter_product_group_rows(df)
    if df.empty:
        return pd.DataFrame(columns=[
            "product_group_key", "product_group_display", "category_normalized", "size_normalized", "variant_type",
            "net_revenue", "units", "profit", "profit_real", "margin", "margin_real", "avg_price_per_item",
            "avg_cost_per_item", "tickets", "discount", "discount_rate", "merged_count", "raw_names_top5", "product_list",
        ])

    df = _apply_weekly_ordering_product_identity(df, include_store=False)
    group_col = "ordering_product_key" if "ordering_product_key" in df.columns else (
        "product_group_key" if "product_group_key" in df.columns else "merge_key"
    )
    display_col = "ordering_product_display" if "ordering_product_display" in df.columns else (
        "product_group_display" if "product_group_display" in df.columns else "display_product"
    )

    grouped = df.groupby(group_col, as_index=False).agg(
        product_group_display=(display_col, _ordering_combine_group_products),
        category_normalized=("category_normalized", lambda s: s.mode().iloc[0] if not s.mode().empty else str(s.iloc[0])),
        size_normalized=("size_normalized", lambda s: s.mode().iloc[0] if not s.mode().empty else str(s.iloc[0])),
        variant_type=("variant_type", lambda s: s.mode().iloc[0] if not s.mode().empty else str(s.iloc[0])),
        net_revenue=("_net", "sum"),
        gross_sales=("_gross", "sum"),
        units=("_qty", "sum"),
        discount=("_disc_total", "sum"),
        cogs_real=("_cogs_real", "sum"),
        profit=("_profit_kb", "sum"),
        profit_real=("_profit_real", "sum"),
        tickets=("_tx_key", "nunique"),
    )

    raw_map: Dict[str, str] = {}
    display_map: Dict[str, str] = {}
    product_list_map: Dict[str, str] = {}
    merged_count_map: Dict[str, int] = {}
    for mk, part in df.groupby(group_col):
        names = sorted(
            {
                str(x).strip()
                for x in part["_product_raw"].dropna().astype(str).tolist()
                if str(x).strip()
            }
        )
        if display_col in part.columns:
            display_name = _ordering_combine_group_products(part[display_col].fillna("").astype(str).tolist())
        elif not names:
            display_name = ""
        elif len(names) == 1:
            display_name = _ordering_display_product_without_brand(names[0])
        else:
            display_name = _ordering_display_product_without_brand(names[0])

        top5 = names[:5]
        raw_map[mk] = " | ".join(top5)
        display_map[mk] = display_name
        product_list_map[mk] = "; ".join(names)
        merged_count_map[mk] = int(len(names))

    grouped["product_group_display"] = grouped[group_col].map(display_map).fillna(grouped["product_group_display"]).astype(str)
    grouped["product_list"] = grouped[group_col].map(product_list_map).fillna("").astype(str)
    grouped["raw_names_top5"] = grouped[group_col].map(raw_map).fillna("")
    grouped["merged_count"] = grouped[group_col].map(merged_count_map).fillna(1).astype(int)

    grouped["margin"] = grouped["profit"] / grouped["net_revenue"].replace({0: np.nan})
    grouped["margin_real"] = grouped["profit_real"] / grouped["net_revenue"].replace({0: np.nan})
    grouped["avg_price_per_item"] = grouped["net_revenue"] / grouped["units"].replace({0: np.nan})
    grouped["avg_cost_per_item"] = grouped["cogs_real"] / grouped["units"].replace({0: np.nan})
    grouped["discount_rate"] = grouped["discount"] / grouped["gross_sales"].replace({0: np.nan})

    if group_col != "product_group_key":
        grouped = grouped.rename(columns={group_col: "product_group_key"})

    num_cols = grouped.select_dtypes(include=[np.number]).columns.tolist()
    if num_cols:
        grouped[num_cols] = grouped[num_cols].fillna(0.0)
    for c in ["product_group_display", "category_normalized", "size_normalized", "variant_type", "raw_names_top5", "product_list"]:
        if c in grouped.columns:
            grouped[c] = grouped[c].fillna("").astype(str)
    return grouped.sort_values("net_revenue", ascending=False)


def relabel_product_group_movers(movers_df: pd.DataFrame, product_summary: pd.DataFrame) -> pd.DataFrame:
    if movers_df is None or movers_df.empty:
        return pd.DataFrame(columns=["product_group_display", "current_net", "prior_net", "delta"])

    out = movers_df.copy()
    key_col = "product_group_key" if "product_group_key" in out.columns else None

    if key_col:
        label_map: Dict[str, str] = {}
        if product_summary is not None and not product_summary.empty and {
            "product_group_key", "product_group_display",
        }.issubset(set(product_summary.columns)):
            label_map = {
                str(r["product_group_key"]): str(r["product_group_display"])
                for _, r in product_summary.iterrows()
            }
        out["product_group_display"] = out[key_col].astype(str).map(label_map).fillna(out[key_col].astype(str))
    elif "product_group_display" in out.columns:
        out["product_group_display"] = out["product_group_display"].fillna("").astype(str)
    else:
        first_col = out.columns[0]
        out["product_group_display"] = out[first_col].fillna("").astype(str)

    for c in ["current_net", "prior_net", "delta"]:
        if c not in out.columns:
            out[c] = 0.0
        out[c] = pd.to_numeric(out[c], errors="coerce").fillna(0.0).astype(float)

    return out[["product_group_display", "current_net", "prior_net", "delta"]].copy()


def _best_group_key_col(df: pd.DataFrame) -> str:
    for c in ["ordering_product_key", "product_group_key", "supply_merge_key", "merge_key"]:
        if c in df.columns:
            return c
    return ""


def build_product_group_supply_maps(
    catalog_brand_df: pd.DataFrame,
    last14_sales_df: pd.DataFrame,
    trend_days: int,
) -> Tuple[Dict[str, float], Dict[str, float], Dict[str, Dict[str, float]], Dict[str, Dict[str, float]]]:
    trend_days = max(int(trend_days or 14), 1)

    all_units_day_map: Dict[str, float] = {}
    all_dos_map: Dict[str, float] = {}
    store_units_day_map: Dict[str, Dict[str, float]] = {}
    store_dos_map: Dict[str, Dict[str, float]] = {}

    inv_all = pd.DataFrame(columns=["group_key", "units_available"])
    inv_store = pd.DataFrame(columns=["_store_abbr", "group_key", "units_available"])
    if catalog_brand_df is not None and not catalog_brand_df.empty:
        inv = _inventory_reporting_rows(_filter_product_group_rows(catalog_brand_df))
        if inv.empty:
            inv = pd.DataFrame(columns=catalog_brand_df.columns)
        inv = _apply_weekly_ordering_product_identity(inv, include_store=False)
        if "_store_abbr" not in inv.columns:
            inv["_store_abbr"] = ""
        inv["_store_abbr"] = inv["_store_abbr"].fillna("").astype(str).str.upper().str.strip()
        gcol = _best_group_key_col(inv)
        if gcol:
            inv["group_key"] = inv[gcol].fillna("").astype(str)
        else:
            inv["group_key"] = ""
        if "Available" not in inv.columns:
            inv["Available"] = 0.0
        inv = inv[(inv["group_key"] != "") & (inv["Available"] >= MIN_REPORTABLE_INVENTORY_UNITS)].copy()
        if not inv.empty:
            inv_all = inv.groupby("group_key", as_index=False).agg(units_available=("Available", "sum"))
            inv_store = inv.groupby(["_store_abbr", "group_key"], as_index=False).agg(units_available=("Available", "sum"))
            inv_store = inv_store[inv_store["_store_abbr"] != ""].copy()

    sales_all = pd.DataFrame(columns=["group_key", "units_14d"])
    sales_store = pd.DataFrame(columns=["_store_abbr", "group_key", "units_14d"])
    if last14_sales_df is not None and not last14_sales_df.empty:
        s14 = _filter_product_group_rows(last14_sales_df)
        if s14.empty:
            s14 = pd.DataFrame(columns=last14_sales_df.columns)
        s14 = _apply_weekly_ordering_product_identity(s14, include_store=False)
        if "_is_return" in s14.columns:
            s14 = s14[~s14["_is_return"]].copy()
        if "_store_abbr" not in s14.columns:
            s14["_store_abbr"] = ""
        s14["_store_abbr"] = s14["_store_abbr"].fillna("").astype(str).str.upper().str.strip()
        gcol = _best_group_key_col(s14)
        if gcol:
            s14["group_key"] = s14[gcol].fillna("").astype(str)
        else:
            s14["group_key"] = ""
        if "_qty" in s14.columns:
            s14["_qty"] = osnap.to_number(s14["_qty"]).fillna(0.0).astype(float)
        else:
            s14["_qty"] = 1.0
        s14 = s14[(s14["group_key"] != "") & (s14["_qty"] > 0)].copy()
        if not s14.empty:
            sales_all = s14.groupby("group_key", as_index=False).agg(units_14d=("_qty", "sum"))
            sales_store = s14.groupby(["_store_abbr", "group_key"], as_index=False).agg(units_14d=("_qty", "sum"))
            sales_store = sales_store[sales_store["_store_abbr"] != ""].copy()

    all_df = inv_all.merge(sales_all, on="group_key", how="outer")
    if not all_df.empty:
        all_df["units_available"] = pd.to_numeric(all_df["units_available"], errors="coerce").fillna(0.0).astype(float)
        all_df["units_14d"] = pd.to_numeric(all_df["units_14d"], errors="coerce").fillna(0.0).astype(float)
        all_df["trend_units_per_day_14d"] = all_df["units_14d"] / float(trend_days)
        all_df["days_of_supply"] = all_df.apply(
            lambda r: _safe_dos(float(r.get("units_available", 0.0)), float(r.get("trend_units_per_day_14d", 0.0))),
            axis=1,
        )
        all_units_day_map = {str(r["group_key"]): float(r["trend_units_per_day_14d"]) for _, r in all_df.iterrows()}
        all_dos_map = {str(r["group_key"]): float(r["days_of_supply"]) for _, r in all_df.iterrows()}

    store_df = inv_store.merge(sales_store, on=["_store_abbr", "group_key"], how="outer")
    if not store_df.empty:
        store_df["units_available"] = pd.to_numeric(store_df["units_available"], errors="coerce").fillna(0.0).astype(float)
        store_df["units_14d"] = pd.to_numeric(store_df["units_14d"], errors="coerce").fillna(0.0).astype(float)
        store_df["trend_units_per_day_14d"] = store_df["units_14d"] / float(trend_days)
        store_df["days_of_supply"] = store_df.apply(
            lambda r: _safe_dos(float(r.get("units_available", 0.0)), float(r.get("trend_units_per_day_14d", 0.0))),
            axis=1,
        )
        for _, r in store_df.iterrows():
            abbr = str(r.get("_store_abbr", "")).upper().strip()
            if not abbr:
                continue
            key = str(r.get("group_key", ""))
            if not key:
                continue
            store_units_day_map.setdefault(abbr, {})[key] = float(r.get("trend_units_per_day_14d", 0.0))
            store_dos_map.setdefault(abbr, {})[key] = float(r.get("days_of_supply", np.nan))

    return all_units_day_map, all_dos_map, store_units_day_map, store_dos_map


def add_supply_to_product_groups(
    product_df: pd.DataFrame,
    units_day_map: Dict[str, float],
    dos_map: Dict[str, float],
) -> pd.DataFrame:
    if product_df is None or product_df.empty:
        return pd.DataFrame(columns=(product_df.columns if product_df is not None else []))

    out = product_df.copy()
    key_col = _best_group_key_col(out)
    if not key_col:
        out["trend_units_per_day_14d"] = 0.0
        out["days_of_supply"] = np.nan
        return out

    keys = out[key_col].fillna("").astype(str)
    out["trend_units_per_day_14d"] = keys.map(units_day_map).fillna(0.0).astype(float)
    out["days_of_supply"] = pd.to_numeric(keys.map(dos_map), errors="coerce")
    return out


def _best_inventory_key_col(df: pd.DataFrame) -> str:
    for c in ["supply_merge_key", "merge_key", "product_group_key", "display_product"]:
        if c in df.columns:
            return c
    return ""


def _merge_inventory_trend_columns(
    inv_products_30: pd.DataFrame,
    inv_products_14: Optional[pd.DataFrame],
    inv_products_7: Optional[pd.DataFrame],
) -> pd.DataFrame:
    out = inv_products_30.copy() if inv_products_30 is not None else pd.DataFrame()
    if out.empty:
        return out

    key_col = _best_inventory_key_col(out)
    if not key_col:
        out["trend_units_per_day_30d"] = 0.0
        out["trend_units_per_day_14d"] = 0.0
        out["trend_units_per_day_7d"] = 0.0
        return out

    out["trend_units_per_day_30d"] = pd.to_numeric(out.get("trend_units_per_day_14d", 0.0), errors="coerce").fillna(0.0).astype(float)

    def _map_trend(src: Optional[pd.DataFrame]) -> Dict[str, float]:
        if src is None or src.empty or key_col not in src.columns:
            return {}
        tmp = src.copy()
        tmp[key_col] = tmp[key_col].fillna("").astype(str)
        vals = pd.to_numeric(tmp.get("trend_units_per_day_14d", 0.0), errors="coerce").fillna(0.0).astype(float)
        tmp["__v"] = vals
        return {str(r[key_col]): float(r["__v"]) for _, r in tmp.iterrows()}

    map14 = _map_trend(inv_products_14)
    map7 = _map_trend(inv_products_7)
    out_keys = out[key_col].fillna("").astype(str)
    out["trend_units_per_day_14d"] = out_keys.map(map14).fillna(0.0).astype(float)
    out["trend_units_per_day_7d"] = out_keys.map(map7).fillna(0.0).astype(float)
    return out


def _merge_inventory_store_trend_columns(
    inv_store_30: pd.DataFrame,
    inv_store_14: Optional[pd.DataFrame],
    inv_store_7: Optional[pd.DataFrame],
) -> pd.DataFrame:
    out = inv_store_30.copy() if inv_store_30 is not None else pd.DataFrame()
    if out.empty or "_store_abbr" not in out.columns:
        return out
    out["_store_abbr"] = out["_store_abbr"].fillna("").astype(str).str.upper()
    out["trend_units_per_day_30d"] = pd.to_numeric(out.get("trend_units_per_day_14d", 0.0), errors="coerce").fillna(0.0).astype(float)
    out["days_of_supply_30d"] = pd.to_numeric(out.get("days_of_supply", np.nan), errors="coerce")

    def _map_by_store(src: Optional[pd.DataFrame], col_name: str) -> Dict[str, float]:
        if src is None or src.empty or "_store_abbr" not in src.columns:
            return {}
        tmp = src.copy()
        tmp["_store_abbr"] = tmp["_store_abbr"].fillna("").astype(str).str.upper()
        tmp[col_name] = pd.to_numeric(tmp.get(col_name, 0.0), errors="coerce").fillna(0.0).astype(float)
        return {str(r["_store_abbr"]): float(r[col_name]) for _, r in tmp.iterrows()}

    map14 = _map_by_store(inv_store_14, "trend_units_per_day_14d")
    map7 = _map_by_store(inv_store_7, "trend_units_per_day_14d")
    dos14 = _map_by_store(inv_store_14, "days_of_supply")
    dos7 = _map_by_store(inv_store_7, "days_of_supply")

    out["trend_units_per_day_14d"] = out["_store_abbr"].map(map14).fillna(0.0).astype(float)
    out["trend_units_per_day_7d"] = out["_store_abbr"].map(map7).fillna(0.0).astype(float)
    out["days_of_supply_14d"] = pd.to_numeric(out["_store_abbr"].map(dos14), errors="coerce")
    out["days_of_supply_7d"] = pd.to_numeric(out["_store_abbr"].map(dos7), errors="coerce")
    return out


def _merge_inventory_overview_trends(
    inv_over_30: Dict[str, float],
    inv_over_14: Optional[Dict[str, float]],
    inv_over_7: Optional[Dict[str, float]],
) -> Dict[str, float]:
    out = dict(inv_over_30 or {})
    t30 = float(out.get("trend_units_per_day_14d", 0.0) or 0.0)
    t14 = float((inv_over_14 or {}).get("trend_units_per_day_14d", 0.0) or 0.0)
    t7 = float((inv_over_7 or {}).get("trend_units_per_day_14d", 0.0) or 0.0)
    out["trend_units_per_day_30d"] = t30
    out["trend_units_per_day_14d"] = t14
    out["trend_units_per_day_7d"] = t7
    out["days_of_supply_30d"] = float(out.get("days_of_supply", np.nan))
    out["days_of_supply_14d"] = float((inv_over_14 or {}).get("days_of_supply", np.nan))
    out["days_of_supply_7d"] = float((inv_over_7 or {}).get("days_of_supply", np.nan))
    return out


def summarize_kickback_rules(df: pd.DataFrame) -> pd.DataFrame:
    if df is None or df.empty or "_kickback_amt" not in df.columns:
        return pd.DataFrame(columns=["rule", "kickback_total", "rows"])

    tmp = df[df["_kickback_amt"] > 0].copy()
    if tmp.empty:
        return pd.DataFrame(columns=["rule", "kickback_total", "rows"])

    tmp["_deal_rule"] = tmp.get("_deal_rule", "").fillna("").astype(str)
    tmp["_deal_rule"] = tmp["_deal_rule"].replace({"": "(Unspecified Rule)"})

    out = tmp.groupby("_deal_rule", as_index=False).agg(
        kickback_total=("_kickback_amt", "sum"),
        rows=("_kickback_amt", "size"),
    ).rename(columns={"_deal_rule": "rule"})

    return out.sort_values("kickback_total", ascending=False)


def compute_movers(
    current_df: pd.DataFrame,
    prior_df: pd.DataFrame,
    group_col: str,
    top_n: int = 2,
) -> pd.DataFrame:
    if (current_df is None or current_df.empty) and (prior_df is None or prior_df.empty):
        return pd.DataFrame(columns=[group_col, "current_net", "prior_net", "delta"])
    if prior_df is None or prior_df.empty:
        return pd.DataFrame(columns=[group_col, "current_net", "prior_net", "delta"])

    cur = summarize_group(current_df, group_col)
    prv = summarize_group(prior_df, group_col)

    cur = cur[[group_col, "net_revenue"]].rename(columns={"net_revenue": "current_net"}) if not cur.empty else pd.DataFrame(columns=[group_col, "current_net"])
    prv = prv[[group_col, "net_revenue"]].rename(columns={"net_revenue": "prior_net"}) if not prv.empty else pd.DataFrame(columns=[group_col, "prior_net"])

    out = cur.merge(prv, on=group_col, how="outer")
    if group_col in out.columns:
        out[group_col] = out[group_col].fillna("Unknown").astype(str)
    for col in ["current_net", "prior_net"]:
        if col in out.columns:
            out[col] = pd.to_numeric(out[col], errors="coerce").fillna(0.0).astype(float)
    out["delta"] = out["current_net"] - out["prior_net"]
    out = out.sort_values("delta", ascending=False)

    top_up = out.head(top_n)
    top_down = out.sort_values("delta").head(top_n)
    merged = pd.concat([top_up, top_down], ignore_index=True).drop_duplicates(subset=[group_col])
    return merged


# ---------------------------------------------------------------------------
# Catalog preprocessing + metrics
# ---------------------------------------------------------------------------
def _load_catalog_exports(paths: RunPaths, selected_store_codes: Sequence[str], logger: Optional[Callable[[str], None]]) -> pd.DataFrame:
    files = sorted(paths.raw_catalog_dir.glob("*.csv"))
    if not files:
        return pd.DataFrame()

    frames: List[pd.DataFrame] = []
    for p in files:
        try:
            df = pd.read_csv(p)
        except Exception as exc:
            _log(f"[WARN] Could not read catalog file {p.name}: {exc}", logger)
            continue

        if df is None or df.empty:
            continue

        df.columns = [str(c).strip() for c in df.columns]
        if "Product" not in df.columns and "Product Name" not in df.columns:
            continue

        abbr = _abbr_from_filename(p)
        if "Store" in df.columns:
            mapped = df["Store"].dropna().astype(str).map(_abbr_from_store_name).dropna().unique().tolist()
            if len(mapped) == 1:
                abbr = mapped[0]

        if abbr:
            df["_store_abbr"] = abbr
        elif "Store" in df.columns:
            df["_store_abbr"] = df["Store"].astype(str).map(_abbr_from_store_name)
        else:
            df["_store_abbr"] = None

        if selected_store_codes:
            df = df[(df["_store_abbr"].isna()) | (df["_store_abbr"].isin(selected_store_codes))].copy()

        df["_source_file"] = p.name
        frames.append(df)

    if not frames:
        return pd.DataFrame()

    return pd.concat(frames, ignore_index=True)


def _load_sales_exports(paths: RunPaths, selected_store_codes: Sequence[str], logger: Optional[Callable[[str], None]]) -> Dict[str, pd.DataFrame]:
    files = sorted(
        list(paths.raw_sales_dir.glob("*.xlsx")) + list(paths.raw_sales_dir.glob("*.csv")),
        key=lambda p: p.stat().st_mtime,
    )
    if not files:
        return {}

    out: Dict[str, pd.DataFrame] = {}
    selected = {str(code).upper().strip() for code in selected_store_codes if str(code).strip()}
    for p in files:
        abbr = _abbr_from_filename(p)
        if not abbr:
            continue
        abbr = str(abbr).upper().strip()
        if selected and abbr not in selected:
            continue
        try:
            df = _read_sales_source_file(p)
        except Exception as exc:
            _log(f"[WARN] Could not read sales export {p.name}: {exc}", logger)
            continue
        if df is None or df.empty:
            continue
        out[abbr] = df
    return out


def _resolve_brand_identity(product_name: Any, catalog_brand: Any = "") -> Tuple[str, str]:
    parsed_brand = str(osnap.parse_brand_from_product(product_name) or "").strip()
    catalog_brand_txt = str(catalog_brand or "").strip()

    key_source = parsed_brand if parsed_brand and canon(parsed_brand) != "unknown" else catalog_brand_txt
    display_name = catalog_brand_txt or parsed_brand or "Unknown"
    brand_key = canon(key_source or display_name) or "unknown"
    return brand_key, display_name


def prepare_catalog_for_brand(
    catalog_df: pd.DataFrame,
    brand: str,
    selected_store_codes: Sequence[str],
    brand_aliases: Optional[Sequence[str]] = None,
) -> pd.DataFrame:
    if catalog_df is None or catalog_df.empty:
        return pd.DataFrame()

    df = catalog_df.copy()

    brand_col = "Brand" if "Brand" in df.columns else None
    product_col = "Product" if "Product" in df.columns else ("Product Name" if "Product Name" in df.columns else None)
    category_col = "Category" if "Category" in df.columns else None

    if not product_col:
        return pd.DataFrame()

    price_col = "Price" if "Price" in df.columns else None
    loc_price_col = "Location price" if "Location price" in df.columns else ("Location Price" if "Location Price" in df.columns else None)
    cost_col = "Cost" if "Cost" in df.columns else None
    avail_col = "Available" if "Available" in df.columns else None

    df["_product_raw"] = df[product_col].fillna("Unknown").astype(str)

    if brand_col:
        brand_series = df[brand_col].fillna("").astype(str)
    else:
        brand_series = df["_product_raw"].apply(osnap.parse_brand_from_product)

    target = canon(brand)
    alias_values = [str(a).strip() for a in (brand_aliases or []) if str(a).strip()]
    alias_values.append(str(brand).strip())
    alias_canons = {canon(a) for a in alias_values if canon(a)}
    if target:
        alias_canons.add(target)

    mask_brand = brand_series.apply(canon).isin(alias_canons)

    prod_txt = df["_product_raw"].fillna("").astype(str).str.strip()
    prod_l = prod_txt.str.lower()
    mask_prefix_alias = pd.Series(False, index=df.index, dtype=bool)
    for a in alias_values:
        al = str(a).strip().lower()
        if not al:
            continue
        mask_prefix_alias = (
            mask_prefix_alias
            | prod_l.eq(al)
            | prod_l.str.startswith(al + "|")
            | prod_l.str.startswith(al + " |")
            | prod_l.str.startswith(al + " -")
            | prod_l.str.startswith(al + "-")
            | prod_l.str.startswith(al + " ")
        )

    df = df[mask_brand | mask_prefix_alias].copy()
    if df.empty:
        return pd.DataFrame()

    if selected_store_codes:
        if "_store_abbr" in df.columns:
            df = df[(df["_store_abbr"].isna()) | (df["_store_abbr"].isin(selected_store_codes))].copy()

    price = osnap.to_number(df[price_col]).fillna(0.0).astype(float) if price_col else 0.0
    loc_price = osnap.to_number(df[loc_price_col]).fillna(0.0).astype(float) if loc_price_col else 0.0
    cost = osnap.to_number(df[cost_col]).fillna(0.0).astype(float) if cost_col else 0.0
    avail = osnap.to_number(df[avail_col]).fillna(0.0).astype(float) if avail_col else 0.0

    price_used = np.where(loc_price > 0, loc_price, price)
    effective = price_used * 0.63

    df["Price_Used"] = price_used
    df["Cost"] = cost
    df["Available"] = avail
    df["Effective_Price"] = effective
    df["Out_The_Door"] = effective * 1.33

    denom = pd.Series(effective).replace({0: np.nan})
    df["Margin_Current"] = ((effective - cost) / denom).fillna(0.0)

    df["Inventory_Value"] = df["Cost"] * df["Available"]
    df["Potential_Revenue"] = df["Price_Used"] * df["Available"]
    df["Potential_Profit"] = (df["Effective_Price"] - df["Cost"]) * df["Available"]

    cat_raw = df[category_col].fillna("Unknown").astype(str) if category_col else "Unknown"
    merge_rows = [
        derive_merge_fields(pn, cat, brand, float(pp), float(cc))
        for pn, cat, pp, cc in zip(df["_product_raw"], cat_raw, df["Price_Used"], df["Cost"])
    ]
    merge_df = pd.DataFrame(merge_rows, index=df.index)
    for col in merge_df.columns:
        df[col] = merge_df[col]

    return df


def prepare_catalog_for_all_brands(
    catalog_df: pd.DataFrame,
    selected_store_codes: Sequence[str],
) -> pd.DataFrame:
    if catalog_df is None or catalog_df.empty:
        return pd.DataFrame()

    df = catalog_df.copy()

    product_col = "Product" if "Product" in df.columns else ("Product Name" if "Product Name" in df.columns else None)
    if not product_col:
        return pd.DataFrame()

    brand_col = "Brand" if "Brand" in df.columns else None
    category_col = "Category" if "Category" in df.columns else None
    price_col = "Price" if "Price" in df.columns else None
    loc_price_col = "Location price" if "Location price" in df.columns else ("Location Price" if "Location Price" in df.columns else None)
    cost_col = "Cost" if "Cost" in df.columns else None
    avail_col = "Available" if "Available" in df.columns else None

    if selected_store_codes and "_store_abbr" in df.columns:
        selected = {str(code).upper().strip() for code in selected_store_codes if str(code).strip()}
        df["_store_abbr"] = df["_store_abbr"].fillna("").astype(str).str.upper().str.strip()
        df = df[(df["_store_abbr"] == "") | (df["_store_abbr"].isin(selected))].copy()

    df["_product_raw"] = df[product_col].fillna("Unknown").astype(str)
    catalog_brand_series = df[brand_col].fillna("").astype(str) if brand_col else pd.Series("", index=df.index)
    brand_pairs = [_resolve_brand_identity(pn, cb) for pn, cb in zip(df["_product_raw"], catalog_brand_series)]
    df["brand_key"] = [pair[0] for pair in brand_pairs]
    df["brand_name"] = [pair[1] for pair in brand_pairs]

    price = osnap.to_number(df[price_col]).fillna(0.0).astype(float) if price_col else 0.0
    loc_price = osnap.to_number(df[loc_price_col]).fillna(0.0).astype(float) if loc_price_col else 0.0
    cost = osnap.to_number(df[cost_col]).fillna(0.0).astype(float) if cost_col else 0.0
    avail = osnap.to_number(df[avail_col]).fillna(0.0).astype(float) if avail_col else 0.0

    price_used = np.where(loc_price > 0, loc_price, price)
    effective = price_used * 0.63

    df["Price_Used"] = price_used
    df["Cost"] = cost
    df["Available"] = avail
    df["Effective_Price"] = effective
    df["Out_The_Door"] = effective * 1.33

    denom = pd.Series(effective).replace({0: np.nan})
    df["Margin_Current"] = ((effective - cost) / denom).fillna(0.0)

    df["Inventory_Value"] = df["Cost"] * df["Available"]
    df["Potential_Revenue"] = df["Price_Used"] * df["Available"]
    df["Potential_Profit"] = (df["Effective_Price"] - df["Cost"]) * df["Available"]

    cat_raw = df[category_col].fillna("Unknown").astype(str) if category_col else "Unknown"
    merge_rows = [
        derive_merge_fields(pn, cat, brand_name, float(pp), float(cc))
        for pn, cat, brand_name, pp, cc in zip(df["_product_raw"], cat_raw, df["brand_name"], df["Price_Used"], df["Cost"])
    ]
    merge_df = pd.DataFrame(merge_rows, index=df.index)
    for col in merge_df.columns:
        df[col] = merge_df[col]

    df["brand_category_key"] = df["brand_key"].fillna("unknown").astype(str) + "|" + df["category_normalized"].fillna("UNKNOWN").astype(str)
    df["brand_product_key"] = df["brand_key"].fillna("unknown").astype(str) + "|" + df["merge_key"].fillna("").astype(str)
    df["brand_product_display"] = df["brand_name"].fillna("Unknown").astype(str) + " | " + df["display_product"].fillna("").astype(str)
    return df


def build_brand_display_map(catalog_all_df: pd.DataFrame) -> Dict[str, str]:
    if catalog_all_df is None or catalog_all_df.empty or "brand_key" not in catalog_all_df.columns:
        return {}

    out: Dict[str, str] = {}
    tmp = catalog_all_df.copy()
    tmp["brand_key"] = tmp["brand_key"].fillna("unknown").astype(str)
    if "brand_name" not in tmp.columns:
        tmp["brand_name"] = "Unknown"
    tmp["brand_name"] = tmp["brand_name"].fillna("Unknown").astype(str)
    for brand_key, part in tmp.groupby("brand_key"):
        names = [str(x).strip() for x in part["brand_name"].tolist() if str(x).strip() and str(x).strip().lower() != "unknown"]
        out[str(brand_key)] = Counter(names).most_common(1)[0][0] if names else "Unknown"
    return out


def summarize_inventory_overview(df: pd.DataFrame) -> Dict[str, float]:
    df = _inventory_reporting_rows(df)
    if df is None or df.empty:
        return {
            "units": 0.0,
            "inventory_value": 0.0,
            "potential_revenue": 0.0,
            "potential_profit": 0.0,
            "avg_margin": 0.0,
        }

    units = float(df["Available"].sum())
    inv_cost = float(df["Inventory_Value"].sum())
    rev = float(df["Potential_Revenue"].sum())
    profit = float(df["Potential_Profit"].sum())
    effective_total = float((df["Effective_Price"] * df["Available"]).sum())
    avg_margin = (profit / effective_total) if effective_total else 0.0

    return {
        "units": units,
        "inventory_value": inv_cost,
        "potential_revenue": rev,
        "potential_profit": profit,
        "avg_margin": avg_margin,
    }


def summarize_inventory_products(df: pd.DataFrame) -> pd.DataFrame:
    df = _inventory_reporting_rows(df)
    if df is None or df.empty:
        return pd.DataFrame(columns=[
            "merge_key", "display_product", "category_normalized", "units_available", "shelf_price", "cost",
            "effective_price", "out_the_door", "margin_current", "inventory_value", "potential_revenue",
            "potential_profit", "supply_base_key", "supply_merge_key", "merged_count", "raw_names_top5",
        ])

    tmp = _filter_product_group_rows(df)
    if tmp.empty:
        return pd.DataFrame(columns=[
            "merge_key", "display_product", "category_normalized", "units_available", "shelf_price", "cost",
            "effective_price", "out_the_door", "margin_current", "inventory_value", "potential_revenue",
            "potential_profit", "supply_base_key", "supply_merge_key", "merged_count", "raw_names_top5",
        ])
    tmp = _apply_weekly_ordering_product_identity(tmp, include_store=False)
    tmp["effective_total"] = tmp["Effective_Price"] * tmp["Available"]
    if "supply_merge_key" not in tmp.columns:
        tmp["supply_merge_key"] = ""
    if "supply_base_key" not in tmp.columns:
        tmp["supply_base_key"] = tmp["supply_merge_key"].map(_supply_base_from_merge_key)

    group_col = "ordering_product_key" if "ordering_product_key" in tmp.columns else "merge_key"
    display_col = "ordering_product_display" if "ordering_product_display" in tmp.columns else "display_product"

    grouped = tmp.groupby(group_col, as_index=False).agg(
        display_product=(display_col, _ordering_combine_group_products),
        category_normalized=("category_normalized", lambda s: s.mode().iloc[0] if not s.mode().empty else str(s.iloc[0])),
        supply_base_key=("supply_base_key", lambda s: s.mode().iloc[0] if not s.mode().empty else (str(s.iloc[0]) if len(s) else "")),
        supply_merge_key=("supply_merge_key", lambda s: s.mode().iloc[0] if not s.mode().empty else (str(s.iloc[0]) if len(s) else "")),
        units_available=("Available", "sum"),
        inventory_value=("Inventory_Value", "sum"),
        potential_revenue=("Potential_Revenue", "sum"),
        potential_profit=("Potential_Profit", "sum"),
        effective_total=("effective_total", "sum"),
        price_weighted_num=("Potential_Revenue", "sum"),
        cost_weighted_num=("Inventory_Value", "sum"),
        eff_weighted_num=("effective_total", "sum"),
    )

    grouped["shelf_price"] = grouped["price_weighted_num"] / grouped["units_available"].replace({0: np.nan})
    grouped["cost"] = grouped["cost_weighted_num"] / grouped["units_available"].replace({0: np.nan})
    grouped["effective_price"] = grouped["eff_weighted_num"] / grouped["units_available"].replace({0: np.nan})
    grouped["out_the_door"] = grouped["effective_price"] * 1.33
    grouped["margin_current"] = grouped["potential_profit"] / grouped["effective_total"].replace({0: np.nan})

    raw_map: Dict[str, str] = {}
    merged_count_map: Dict[str, int] = {}
    for mk, part in tmp.groupby(group_col):
        cnt = Counter(part["_product_raw"].astype(str).tolist())
        raw_map[mk] = " | ".join([name for name, _n in cnt.most_common(5)])
        merged_count_map[mk] = int(len(cnt))

    grouped["raw_names_top5"] = grouped[group_col].map(raw_map).fillna("")
    grouped["merged_count"] = grouped[group_col].map(merged_count_map).fillna(1).astype(int)
    if group_col != "merge_key":
        grouped = grouped.rename(columns={group_col: "merge_key"})

    keep_cols = [
        "merge_key", "display_product", "category_normalized", "units_available", "shelf_price", "cost",
        "effective_price", "out_the_door", "margin_current", "inventory_value", "potential_revenue",
        "potential_profit", "supply_base_key", "supply_merge_key", "merged_count", "raw_names_top5",
    ]

    out = grouped[keep_cols].replace([np.inf, -np.inf], np.nan).fillna(0.0)
    return out.sort_values("potential_profit", ascending=False)


def summarize_inventory_by_category(df: pd.DataFrame) -> pd.DataFrame:
    df = _inventory_reporting_rows(df)
    if df is None or df.empty:
        return pd.DataFrame(columns=["category_normalized", "inventory_value", "potential_profit", "units_available"])

    out = df.groupby("category_normalized", as_index=False).agg(
        inventory_value=("Inventory_Value", "sum"),
        potential_profit=("Potential_Profit", "sum"),
        units_available=("Available", "sum"),
    )
    return out.sort_values("inventory_value", ascending=False)


def summarize_inventory_by_store(df: pd.DataFrame) -> pd.DataFrame:
    df = _inventory_reporting_rows(df)
    if df is None or df.empty or "_store_abbr" not in df.columns:
        return pd.DataFrame(columns=[
            "_store_abbr", "units_available", "inventory_value", "potential_revenue", "potential_profit", "avg_margin",
        ])

    tmp = df.copy()
    tmp["_store_abbr"] = tmp["_store_abbr"].fillna("").astype(str).str.upper()
    tmp = tmp[tmp["_store_abbr"] != ""].copy()
    if tmp.empty:
        return pd.DataFrame(columns=[
            "_store_abbr", "units_available", "inventory_value", "potential_revenue", "potential_profit", "avg_margin",
        ])

    tmp["effective_total"] = tmp["Effective_Price"] * tmp["Available"]

    out = tmp.groupby("_store_abbr", as_index=False).agg(
        units_available=("Available", "sum"),
        inventory_value=("Inventory_Value", "sum"),
        potential_revenue=("Potential_Revenue", "sum"),
        potential_profit=("Potential_Profit", "sum"),
        effective_total=("effective_total", "sum"),
    )
    out["avg_margin"] = out["potential_profit"] / out["effective_total"].replace({0: np.nan})
    out = out.replace([np.inf, -np.inf], np.nan).fillna(0.0)
    return out.sort_values("inventory_value", ascending=False)


def add_inventory_supply_metrics(
    inv_overview: Dict[str, float],
    inv_store: pd.DataFrame,
    inv_products: pd.DataFrame,
    catalog_brand_df: pd.DataFrame,
    last14_sales_df: pd.DataFrame,
    trend_start: date,
    trend_end: date,
    as_of_day: date,
) -> Tuple[Dict[str, float], pd.DataFrame, pd.DataFrame]:
    trend_days = max(window_days(trend_start, trend_end), 1)
    out_overview = dict(inv_overview or {})
    out_store = inv_store.copy() if inv_store is not None else pd.DataFrame()
    out_products = inv_products.copy() if inv_products is not None else pd.DataFrame()

    def _safe_dos(units: float, per_day: float) -> float:
        if per_day > 0 and units > 0:
            return float(units / per_day)
        return float("nan")

    def _est_oos(v: Any) -> str:
        try:
            fv = float(v)
        except Exception:
            return "n/a"
        if not np.isfinite(fv) or fv <= 0:
            return "n/a"
        return (as_of_day + timedelta(days=int(math.ceil(fv)))).isoformat()

    sales14 = last14_sales_df.copy() if last14_sales_df is not None else pd.DataFrame()
    if not sales14.empty:
        if "_is_return" in sales14.columns:
            sales14 = sales14[~sales14["_is_return"]].copy()
        sales14 = _apply_weekly_ordering_product_identity(sales14, include_store=False)
        if "_qty" not in sales14.columns:
            sales14["_qty"] = 1.0
        sales14["_qty"] = osnap.to_number(sales14["_qty"]).fillna(0.0).astype(float)
        if ("supply_merge_key" not in sales14.columns) or ("supply_base_key" not in sales14.columns):
            price_series = osnap.to_number(sales14.get("merge_price_basis", 0.0)).fillna(0.0).astype(float)
            if "merge_cost_basis" in sales14.columns:
                cost_series = osnap.to_number(sales14["merge_cost_basis"]).fillna(0.0).astype(float)
            else:
                qty_nonzero = sales14["_qty"].replace({0: np.nan})
                cost_series = (
                    osnap.to_number(sales14.get("_cogs_real", 0.0)).fillna(0.0).astype(float) / qty_nonzero
                ).replace([np.inf, -np.inf], np.nan).fillna(0.0).astype(float)

            out_family: List[str] = []
            out_base: List[str] = []
            out_merge: List[str] = []
            for pn, cat, sz, var, core, norm, mp, mc in zip(
                sales14.get("_product_raw", pd.Series("", index=sales14.index)).fillna("").astype(str),
                sales14.get("category_normalized", pd.Series("UNKNOWN", index=sales14.index)).fillna("UNKNOWN").astype(str),
                sales14.get("size_normalized", pd.Series("", index=sales14.index)).fillna("").astype(str),
                sales14.get("variant_type", pd.Series("", index=sales14.index)).fillna("").astype(str),
                sales14.get("core_name_normalized", pd.Series("", index=sales14.index)).fillna("").astype(str),
                sales14.get("norm_product_name", pd.Series("", index=sales14.index)).fillna("").astype(str),
                price_series,
                cost_series,
            ):
                fam, base, merge = _derive_supply_keys_from_row(
                    product_name=pn,
                    category_value=cat,
                    size_value=sz,
                    variant_value=var,
                    core_name_value=core,
                    norm_product_value=norm,
                    price_value=mp,
                    cost_value=mc,
                )
                out_family.append(fam)
                out_base.append(base)
                out_merge.append(merge)
            sales14["supply_family_name"] = out_family
            sales14["supply_base_key"] = out_base
            sales14["supply_merge_key"] = out_merge

        sales14["supply_merge_key"] = sales14.get("supply_merge_key", "").fillna("").astype(str)
        sales14["supply_base_key"] = sales14.get("supply_base_key", "").fillna("").astype(str)
        if "supply_base_key" in sales14.columns:
            missing_base = sales14["supply_base_key"].eq("")
            if missing_base.any():
                sales14.loc[missing_base, "supply_base_key"] = sales14.loc[missing_base, "supply_merge_key"].map(_supply_base_from_merge_key)
        if "_store_abbr" in sales14.columns:
            sales14["_store_abbr"] = sales14["_store_abbr"].fillna("").astype(str).str.upper()

    catalog_rows = _inventory_reporting_rows(catalog_brand_df.copy() if catalog_brand_df is not None else pd.DataFrame())
    if not catalog_rows.empty:
        catalog_rows = _apply_weekly_ordering_product_identity(catalog_rows, include_store=False)
        if ("supply_merge_key" not in catalog_rows.columns) or ("supply_base_key" not in catalog_rows.columns):
            price_series = osnap.to_number(catalog_rows.get("Price_Used", 0.0)).fillna(0.0).astype(float)
            cost_series = osnap.to_number(catalog_rows.get("Cost", 0.0)).fillna(0.0).astype(float)

            out_family: List[str] = []
            out_base: List[str] = []
            out_merge: List[str] = []
            for pn, cat, sz, var, core, norm, mp, mc in zip(
                catalog_rows.get("_product_raw", pd.Series("", index=catalog_rows.index)).fillna("").astype(str),
                catalog_rows.get("category_normalized", pd.Series("UNKNOWN", index=catalog_rows.index)).fillna("UNKNOWN").astype(str),
                catalog_rows.get("size_normalized", pd.Series("", index=catalog_rows.index)).fillna("").astype(str),
                catalog_rows.get("variant_type", pd.Series("", index=catalog_rows.index)).fillna("").astype(str),
                catalog_rows.get("core_name_normalized", pd.Series("", index=catalog_rows.index)).fillna("").astype(str),
                catalog_rows.get("norm_product_name", pd.Series("", index=catalog_rows.index)).fillna("").astype(str),
                price_series,
                cost_series,
            ):
                fam, base, merge = _derive_supply_keys_from_row(
                    product_name=pn,
                    category_value=cat,
                    size_value=sz,
                    variant_value=var,
                    core_name_value=core,
                    norm_product_value=norm,
                    price_value=mp,
                    cost_value=mc,
                )
                out_family.append(fam)
                out_base.append(base)
                out_merge.append(merge)
            catalog_rows["supply_family_name"] = out_family
            catalog_rows["supply_base_key"] = out_base
            catalog_rows["supply_merge_key"] = out_merge

        catalog_rows["supply_merge_key"] = catalog_rows.get("supply_merge_key", "").fillna("").astype(str)
        catalog_rows["supply_base_key"] = catalog_rows.get("supply_base_key", "").fillna("").astype(str)
        missing_base = catalog_rows["supply_base_key"].eq("")
        if missing_base.any():
            catalog_rows.loc[missing_base, "supply_base_key"] = catalog_rows.loc[missing_base, "supply_merge_key"].map(_supply_base_from_merge_key)
        if "_store_abbr" in catalog_rows.columns:
            catalog_rows["_store_abbr"] = catalog_rows["_store_abbr"].fillna("").astype(str).str.upper()
        if "Available" not in catalog_rows.columns:
            catalog_rows["Available"] = 0.0
        catalog_rows = catalog_rows[catalog_rows["Available"] >= MIN_REPORTABLE_INVENTORY_UNITS].copy()

    if sales14.empty or catalog_rows.empty or "supply_merge_key" not in sales14.columns or "supply_merge_key" not in catalog_rows.columns:
        total_units_14d = float(sales14["_qty"].sum()) if not sales14.empty else 0.0
        units_per_day_14d = (total_units_14d / trend_days) if trend_days else 0.0
        out_overview["trend_units_14d"] = total_units_14d
        out_overview["trend_units_per_day_14d"] = units_per_day_14d
        out_overview["days_of_supply"] = _safe_dos(float(out_overview.get("units", 0.0)), units_per_day_14d)
        out_overview["est_oos_date"] = _est_oos(out_overview["days_of_supply"])

        if not out_store.empty and "_store_abbr" in out_store.columns:
            out_store["_store_abbr"] = out_store["_store_abbr"].fillna("").astype(str).str.upper()
            by_store_units = pd.DataFrame(columns=["_store_abbr", "trend_units_14d"])
            if not sales14.empty and "_store_abbr" in sales14.columns and "_qty" in sales14.columns:
                by_store_units = sales14.groupby("_store_abbr", as_index=False).agg(trend_units_14d=("_qty", "sum"))
            st_map = {str(r["_store_abbr"]).upper(): float(r["trend_units_14d"]) for _, r in by_store_units.iterrows()}
            out_store["trend_units_14d"] = out_store["_store_abbr"].map(st_map).fillna(0.0).astype(float)
            out_store["trend_units_per_day_14d"] = out_store["trend_units_14d"] / float(trend_days)
            out_store["days_of_supply"] = out_store.apply(
                lambda r: _safe_dos(float(r.get("units_available", 0.0)), float(r.get("trend_units_per_day_14d", 0.0))),
                axis=1,
            )
            out_store["est_oos_date"] = out_store["days_of_supply"].apply(_est_oos)
            out_store = out_store.sort_values("inventory_value", ascending=False)

        if not out_products.empty:
            out_products["trend_units_14d"] = 0.0
            out_products["trend_units_per_day_14d"] = 0.0
            out_products["days_of_supply"] = np.nan
            out_products["est_oos_date"] = "n/a"
        return out_overview, out_store, out_products

    # Sales trend maps: exact key and base key.
    sales_exact = sales14.groupby("supply_merge_key", as_index=False).agg(trend_units_14d=("_qty", "sum"))
    sales_base = sales14.groupby("supply_base_key", as_index=False).agg(trend_units_14d=("_qty", "sum"))
    exact_map = {str(r["supply_merge_key"]): float(r["trend_units_14d"]) for _, r in sales_exact.iterrows()}
    base_map = {str(r["supply_base_key"]): float(r["trend_units_14d"]) for _, r in sales_base.iterrows()}

    # Inventory families from in-stock catalog rows.
    inv_family = catalog_rows.groupby(["supply_merge_key", "supply_base_key"], as_index=False).agg(
        units_available=("Available", "sum")
    )
    inv_family["exact_units_14d"] = inv_family["supply_merge_key"].map(exact_map).fillna(0.0).astype(float)
    inv_family["trend_units_14d"] = inv_family["exact_units_14d"].astype(float)

    # Allocate base-key demand to variants with no exact trend (weighted by inventory units).
    for base_key, grp in inv_family.groupby("supply_base_key"):
        idx = grp.index
        base_total = float(base_map.get(str(base_key), 0.0))
        if base_total <= 0:
            continue
        exact_sum = float(inv_family.loc[idx, "exact_units_14d"].sum())
        residual = max(base_total - exact_sum, 0.0)
        if residual <= 0:
            continue
        unmatched = inv_family.loc[idx][inv_family.loc[idx, "exact_units_14d"] <= 0].index
        if len(unmatched) == 0:
            continue
        units_total = float(inv_family.loc[unmatched, "units_available"].sum())
        if units_total <= 0:
            continue
        shares = inv_family.loc[unmatched, "units_available"] / units_total
        inv_family.loc[unmatched, "trend_units_14d"] = shares * residual

    inv_family["trend_units_per_day_14d"] = inv_family["trend_units_14d"] / float(trend_days)

    total_units_14d = float(inv_family["trend_units_14d"].sum()) if not inv_family.empty else 0.0
    units_per_day_14d = float(inv_family["trend_units_per_day_14d"].sum()) if not inv_family.empty else 0.0
    out_overview["trend_units_14d"] = total_units_14d
    out_overview["trend_units_per_day_14d"] = units_per_day_14d
    out_overview["days_of_supply"] = _safe_dos(float(out_overview.get("units", 0.0)), units_per_day_14d)
    out_overview["est_oos_date"] = _est_oos(out_overview["days_of_supply"])

    if not out_store.empty and "_store_abbr" in out_store.columns:
        out_store["_store_abbr"] = out_store["_store_abbr"].fillna("").astype(str).str.upper()
        inv_store_family = catalog_rows.groupby(["_store_abbr", "supply_merge_key", "supply_base_key"], as_index=False).agg(
            units_available=("Available", "sum")
        )

        sales_store_exact = pd.DataFrame(columns=["_store_abbr", "supply_merge_key", "trend_units_14d"])
        sales_store_base = pd.DataFrame(columns=["_store_abbr", "supply_base_key", "trend_units_14d"])
        if "_store_abbr" in sales14.columns:
            sales_store_exact = sales14.groupby(["_store_abbr", "supply_merge_key"], as_index=False).agg(
                trend_units_14d=("_qty", "sum")
            )
            sales_store_base = sales14.groupby(["_store_abbr", "supply_base_key"], as_index=False).agg(
                trend_units_14d=("_qty", "sum")
            )

        exact_store_map = {
            (str(r["_store_abbr"]).upper(), str(r["supply_merge_key"])): float(r["trend_units_14d"])
            for _, r in sales_store_exact.iterrows()
        }
        base_store_map = {
            (str(r["_store_abbr"]).upper(), str(r["supply_base_key"])): float(r["trend_units_14d"])
            for _, r in sales_store_base.iterrows()
        }

        inv_store_family["exact_units_14d"] = inv_store_family.apply(
            lambda r: exact_store_map.get((str(r["_store_abbr"]).upper(), str(r["supply_merge_key"])), 0.0),
            axis=1,
        )
        inv_store_family["trend_units_14d"] = inv_store_family["exact_units_14d"].astype(float)

        for (store_abbr, base_key), grp in inv_store_family.groupby(["_store_abbr", "supply_base_key"]):
            idx = grp.index
            base_total = float(base_store_map.get((str(store_abbr).upper(), str(base_key)), 0.0))
            if base_total <= 0:
                continue
            exact_sum = float(inv_store_family.loc[idx, "exact_units_14d"].sum())
            residual = max(base_total - exact_sum, 0.0)
            if residual <= 0:
                continue
            unmatched = inv_store_family.loc[idx][inv_store_family.loc[idx, "exact_units_14d"] <= 0].index
            if len(unmatched) == 0:
                continue
            units_total = float(inv_store_family.loc[unmatched, "units_available"].sum())
            if units_total <= 0:
                continue
            shares = inv_store_family.loc[unmatched, "units_available"] / units_total
            inv_store_family.loc[unmatched, "trend_units_14d"] = shares * residual

        inv_store_family["trend_units_per_day_14d"] = inv_store_family["trend_units_14d"] / float(trend_days)
        sf_roll = inv_store_family.groupby("_store_abbr", as_index=False).agg(
            trend_units_14d=("trend_units_14d", "sum"),
            trend_units_per_day_14d=("trend_units_per_day_14d", "sum"),
        )
        st_map_14 = {str(r["_store_abbr"]).upper(): float(r["trend_units_14d"]) for _, r in sf_roll.iterrows()}
        st_map_day = {str(r["_store_abbr"]).upper(): float(r["trend_units_per_day_14d"]) for _, r in sf_roll.iterrows()}

        out_store["trend_units_14d"] = out_store["_store_abbr"].map(st_map_14).fillna(0.0).astype(float)
        out_store["trend_units_per_day_14d"] = out_store["_store_abbr"].map(st_map_day).fillna(0.0).astype(float)
        out_store["days_of_supply"] = out_store.apply(
            lambda r: _safe_dos(float(r.get("units_available", 0.0)), float(r.get("trend_units_per_day_14d", 0.0))),
            axis=1,
        )
        out_store["est_oos_date"] = out_store["days_of_supply"].apply(_est_oos)
        out_store = out_store.sort_values("inventory_value", ascending=False)

    if not out_products.empty and "merge_key" in out_products.columns:
        if "supply_merge_key" not in out_products.columns:
            out_products["supply_merge_key"] = ""
        if "supply_base_key" not in out_products.columns:
            out_products["supply_base_key"] = out_products["supply_merge_key"].map(_supply_base_from_merge_key)

        # Backfill keys from row-level catalog if missing.
        if "merge_key" in catalog_rows.columns:
            if "supply_merge_key" in catalog_rows.columns:
                mk_to_merge = catalog_rows.groupby("merge_key")["supply_merge_key"].agg(
                    lambda s: s.mode().iloc[0] if not s.mode().empty else str(s.iloc[0])
                )
                out_products["supply_merge_key"] = out_products["supply_merge_key"].replace("", np.nan)
                out_products["supply_merge_key"] = out_products["supply_merge_key"].fillna(out_products["merge_key"].map(mk_to_merge)).fillna("")
            if "supply_base_key" in catalog_rows.columns:
                mk_to_base = catalog_rows.groupby("merge_key")["supply_base_key"].agg(
                    lambda s: s.mode().iloc[0] if not s.mode().empty else str(s.iloc[0])
                )
                out_products["supply_base_key"] = out_products["supply_base_key"].replace("", np.nan)
                out_products["supply_base_key"] = out_products["supply_base_key"].fillna(out_products["merge_key"].map(mk_to_base)).fillna(
                    out_products["supply_merge_key"].map(_supply_base_from_merge_key)
                ).fillna("")

        fam_trend_map = {
            str(r["supply_merge_key"]): float(r["trend_units_14d"])
            for _, r in inv_family.iterrows()
        }
        fam_day_map = {
            str(r["supply_merge_key"]): float(r["trend_units_per_day_14d"])
            for _, r in inv_family.iterrows()
        }
        out_products["trend_units_14d"] = out_products["supply_merge_key"].map(fam_trend_map).fillna(0.0).astype(float)
        out_products["trend_units_per_day_14d"] = out_products["supply_merge_key"].map(fam_day_map).fillna(0.0).astype(float)
        if "ordering_product_key" in sales14.columns:
            product_trend = sales14.groupby("ordering_product_key", as_index=False).agg(trend_units_14d=("_qty", "sum"))
            product_trend_map = {
                str(r["ordering_product_key"]): float(r["trend_units_14d"])
                for _, r in product_trend.iterrows()
            }
            product_trend_series = out_products["merge_key"].astype(str).map(product_trend_map)
            has_product_trend = product_trend_series.notna()
            out_products.loc[has_product_trend, "trend_units_14d"] = product_trend_series.loc[has_product_trend].astype(float)
            out_products["trend_units_per_day_14d"] = out_products["trend_units_14d"] / float(trend_days)
        out_products["days_of_supply"] = out_products.apply(
            lambda r: _safe_dos(float(r.get("units_available", 0.0)), float(r.get("trend_units_per_day_14d", 0.0))),
            axis=1,
        )
        out_products["est_oos_date"] = out_products["days_of_supply"].apply(_est_oos)

    return out_overview, out_store, out_products


# ---------------------------------------------------------------------------
# All-store assortment health report
# ---------------------------------------------------------------------------
def _first_mode(series: pd.Series, default: str = "") -> str:
    if series is None or len(series) == 0:
        return default
    clean = series.fillna("").astype(str).str.strip()
    clean = clean[clean != ""]
    if clean.empty:
        return default
    mode = clean.mode()
    if not mode.empty:
        return str(mode.iloc[0])
    return str(clean.iloc[0])


def summarize_all_store_inventory_products(df: pd.DataFrame) -> pd.DataFrame:
    cols = [
        "brand_product_key", "brand_key", "brand_name", "brand_category_key", "category_normalized",
        "display_product", "brand_product_display", "units_available", "inventory_value", "potential_revenue",
        "potential_profit", "margin_current", "stores_with_inventory", "merged_count", "raw_names_top5",
    ]
    if df is None or df.empty:
        return pd.DataFrame(columns=cols)

    tmp = _inventory_reporting_rows(_filter_product_group_rows(df))
    if tmp.empty:
        return pd.DataFrame(columns=cols)

    if "brand_product_key" not in tmp.columns:
        tmp["brand_product_key"] = ""
    tmp["brand_product_key"] = tmp["brand_product_key"].fillna("").astype(str)
    tmp = tmp[tmp["brand_product_key"] != ""].copy()
    if tmp.empty:
        return pd.DataFrame(columns=cols)

    tmp["effective_total"] = tmp["Effective_Price"] * tmp["Available"]
    if "_store_abbr" not in tmp.columns:
        tmp["_store_abbr"] = ""
    tmp["_store_abbr"] = tmp["_store_abbr"].fillna("").astype(str).str.upper()

    grouped = tmp.groupby("brand_product_key", as_index=False).agg(
        brand_key=("brand_key", lambda s: _first_mode(s, "unknown")),
        brand_name=("brand_name", lambda s: _first_mode(s, "Unknown")),
        brand_category_key=("brand_category_key", lambda s: _first_mode(s, "")),
        category_normalized=("category_normalized", lambda s: _first_mode(s, "UNKNOWN")),
        display_product=("display_product", lambda s: _first_mode(s, "")),
        brand_product_display=("brand_product_display", lambda s: _first_mode(s, "")),
        units_available=("Available", "sum"),
        inventory_value=("Inventory_Value", "sum"),
        potential_revenue=("Potential_Revenue", "sum"),
        potential_profit=("Potential_Profit", "sum"),
        effective_total=("effective_total", "sum"),
        stores_with_inventory=("_store_abbr", lambda s: int(s.replace("", np.nan).dropna().nunique())),
    )

    raw_map: Dict[str, str] = {}
    merged_count_map: Dict[str, int] = {}
    for key, part in tmp.groupby("brand_product_key"):
        names = sorted({str(x).strip() for x in part["_product_raw"].dropna().astype(str).tolist() if str(x).strip()})
        raw_map[str(key)] = " | ".join(names[:5])
        merged_count_map[str(key)] = int(len(names))

    grouped["margin_current"] = grouped["potential_profit"] / grouped["effective_total"].replace({0: np.nan})
    grouped["raw_names_top5"] = grouped["brand_product_key"].map(raw_map).fillna("")
    grouped["merged_count"] = grouped["brand_product_key"].map(merged_count_map).fillna(1).astype(int)
    grouped = grouped.drop(columns=["effective_total"]).replace([np.inf, -np.inf], np.nan)
    num_cols = grouped.select_dtypes(include=[np.number]).columns.tolist()
    if num_cols:
        grouped[num_cols] = grouped[num_cols].fillna(0.0)
    text_cols = [
        "brand_product_key", "brand_key", "brand_name", "brand_category_key", "category_normalized",
        "display_product", "brand_product_display", "raw_names_top5",
    ]
    for col in text_cols:
        grouped[col] = grouped[col].fillna("").astype(str)
    return grouped.sort_values("inventory_value", ascending=False)


def summarize_all_store_sales_products(df: pd.DataFrame) -> pd.DataFrame:
    cols = [
        "brand_product_key", "brand_key", "brand_name", "brand_category_key", "category_normalized",
        "display_product", "brand_product_display", "units_sold_window", "net_revenue_window", "gross_sales_window",
        "tickets_window", "sales_days", "stores_with_sales", "last_sale_date",
    ]
    if df is None or df.empty:
        return pd.DataFrame(columns=cols)

    tmp = _filter_product_group_rows(df)
    if tmp.empty:
        return pd.DataFrame(columns=cols)
    if "_is_return" in tmp.columns:
        tmp = tmp[~tmp["_is_return"]].copy()
    if "brand_product_key" not in tmp.columns:
        tmp["brand_product_key"] = ""
    tmp["brand_product_key"] = tmp["brand_product_key"].fillna("").astype(str)
    tmp = tmp[tmp["brand_product_key"] != ""].copy()
    if tmp.empty:
        return pd.DataFrame(columns=cols)

    if "_store_abbr" not in tmp.columns:
        tmp["_store_abbr"] = ""
    tmp["_store_abbr"] = tmp["_store_abbr"].fillna("").astype(str).str.upper()
    grouped = tmp.groupby("brand_product_key", as_index=False).agg(
        brand_key=("brand_key", lambda s: _first_mode(s, "unknown")),
        brand_name=("brand_name", lambda s: _first_mode(s, "Unknown")),
        brand_category_key=("brand_category_key", lambda s: _first_mode(s, "")),
        category_normalized=("category_normalized", lambda s: _first_mode(s, "UNKNOWN")),
        display_product=("display_product", lambda s: _first_mode(s, "")),
        brand_product_display=("brand_product_display", lambda s: _first_mode(s, "")),
        units_sold_window=("_qty", "sum"),
        net_revenue_window=("_net", "sum"),
        gross_sales_window=("_gross", "sum"),
        tickets_window=("_tx_key", "nunique"),
        sales_days=("_date", "nunique"),
        stores_with_sales=("_store_abbr", lambda s: int(s.replace("", np.nan).dropna().nunique())),
        last_sale_date=("_date", "max"),
    )
    return grouped.sort_values("net_revenue_window", ascending=False)


def _coalesce_text_columns(df: pd.DataFrame, base_name: str, default: str = "") -> pd.Series:
    inv_col = f"{base_name}_inv"
    sales_col = f"{base_name}_sales"
    left = df[inv_col].fillna("").astype(str) if inv_col in df.columns else pd.Series("", index=df.index)
    right = df[sales_col].fillna("").astype(str) if sales_col in df.columns else pd.Series("", index=df.index)
    out = left.where(left.str.strip() != "", right)
    out = out.fillna("").astype(str)
    if default:
        out = out.replace({"": default})
    return out


def _percent_rank_score(values: pd.Series, *, higher_is_worse: bool) -> pd.Series:
    s = pd.to_numeric(values, errors="coerce")
    if s.notna().sum() <= 1:
        return pd.Series(50.0 if s.notna().sum() == 1 else 0.0, index=values.index, dtype=float)
    rank = s.rank(method="average", pct=True, ascending=True)
    if not higher_is_worse:
        rank = 1.0 - rank
    return rank.fillna(0.0) * 100.0


def _all_store_report_thresholds(report_days: int) -> Dict[str, float]:
    days = max(int(report_days or 1), 1)
    return {
        "review_days_of_supply": max(75.0, float(days) * 1.25),
        "cut_days_of_supply": max(120.0, float(days) * 2.0),
        "low_sell_through": 0.25,
        "cut_sell_through": 0.12,
    }


def _label_product_action(row: pd.Series, thresholds: Dict[str, float]) -> str:
    inv_units = float(row.get("units_available", 0.0) or 0.0)
    sold_units = float(row.get("units_sold_window", 0.0) or 0.0)
    dos = float(row.get("days_of_supply", np.nan))
    sell_through = float(row.get("sell_through_ratio", 0.0) or 0.0)
    inventory_value = float(row.get("inventory_value", 0.0) or 0.0)

    if inv_units <= 0:
        return "Healthy"
    if sold_units <= 0:
        return "Cut candidate - no sales"
    if np.isfinite(dos) and dos >= thresholds["cut_days_of_supply"]:
        return "Cut candidate - very high supply"
    if sell_through <= thresholds["cut_sell_through"] and inventory_value > 0:
        return "Cut candidate - very low sell-through"
    if np.isfinite(dos) and dos >= thresholds["review_days_of_supply"]:
        return "Review - slow mover"
    if sell_through <= thresholds["low_sell_through"]:
        return "Review - low sell-through"
    return "Healthy"


def _label_category_action(row: pd.Series, thresholds: Dict[str, float]) -> str:
    inv_units = float(row.get("units_available", 0.0) or 0.0)
    sold_units = float(row.get("units_sold_window", 0.0) or 0.0)
    dos = float(row.get("days_of_supply", np.nan))
    sell_through = float(row.get("sell_through_ratio", 0.0) or 0.0)
    stale_share = float(row.get("stale_sku_share", 0.0) or 0.0)

    if inv_units <= 0:
        return "Healthy"
    if sold_units <= 0:
        return "Cut candidate - dead category"
    if np.isfinite(dos) and dos >= thresholds["cut_days_of_supply"]:
        return "Cut candidate - very high supply"
    if stale_share >= 0.50:
        return "Cut candidate - too many stale SKUs"
    if np.isfinite(dos) and dos >= thresholds["review_days_of_supply"]:
        return "Review - slow brand category"
    if sell_through <= thresholds["low_sell_through"]:
        return "Review - low sell-through"
    return "Healthy"


def build_all_store_assortment_views(
    catalog_all_df: pd.DataFrame,
    sales_report_df: pd.DataFrame,
    report_days: int,
) -> Tuple[pd.DataFrame, pd.DataFrame, pd.DataFrame]:
    thresholds = _all_store_report_thresholds(report_days)

    inv_products = summarize_all_store_inventory_products(catalog_all_df)
    sales_products = summarize_all_store_sales_products(sales_report_df)
    product_all = inv_products.merge(sales_products, on="brand_product_key", how="outer", suffixes=("_inv", "_sales"))
    if product_all.empty:
        empty_cols = [
            "brand_product_key", "brand_key", "brand_name", "brand_category_key", "category_normalized",
            "display_product", "brand_product_display", "units_available", "inventory_value", "potential_revenue",
            "potential_profit", "margin_current", "stores_with_inventory", "merged_count", "raw_names_top5",
            "units_sold_window", "net_revenue_window", "gross_sales_window", "tickets_window", "sales_days",
            "stores_with_sales", "last_sale_date", "units_per_day", "inventory_turns", "sell_through_ratio",
            "days_of_supply", "assortment_priority_score", "action",
        ]
        return pd.DataFrame(columns=empty_cols), pd.DataFrame(columns=[]), pd.DataFrame(columns=[])

    for col in [
        "brand_key", "brand_name", "brand_category_key", "category_normalized",
        "display_product", "brand_product_display",
    ]:
        default = "unknown" if col == "brand_key" else ("Unknown" if col == "brand_name" else "")
        product_all[col] = _coalesce_text_columns(product_all, col, default)

    numeric_defaults = {
        "units_available": 0.0,
        "inventory_value": 0.0,
        "potential_revenue": 0.0,
        "potential_profit": 0.0,
        "margin_current": 0.0,
        "stores_with_inventory": 0.0,
        "merged_count": 0.0,
        "units_sold_window": 0.0,
        "net_revenue_window": 0.0,
        "gross_sales_window": 0.0,
        "tickets_window": 0.0,
        "sales_days": 0.0,
        "stores_with_sales": 0.0,
    }
    for col, default in numeric_defaults.items():
        inv_col = f"{col}_inv"
        sales_col = f"{col}_sales"
        if col in product_all.columns:
            product_all[col] = pd.to_numeric(product_all[col], errors="coerce").fillna(default).astype(float)
            continue
        if inv_col in product_all.columns or sales_col in product_all.columns:
            left = pd.to_numeric(product_all[inv_col], errors="coerce") if inv_col in product_all.columns else pd.Series(np.nan, index=product_all.index)
            right = pd.to_numeric(product_all[sales_col], errors="coerce") if sales_col in product_all.columns else pd.Series(np.nan, index=product_all.index)
            product_all[col] = left.fillna(right).fillna(default).astype(float)
        else:
            product_all[col] = float(default)

    if "raw_names_top5" not in product_all.columns:
        product_all["raw_names_top5"] = ""
    else:
        product_all["raw_names_top5"] = product_all["raw_names_top5"].fillna("").astype(str)

    if "last_sale_date" not in product_all.columns:
        product_all["last_sale_date"] = pd.NaT
    else:
        product_all["last_sale_date"] = pd.to_datetime(product_all["last_sale_date"], errors="coerce")

    product_all["units_per_day"] = product_all["units_sold_window"] / float(max(report_days, 1))
    product_all["inventory_turns"] = product_all["units_sold_window"] / product_all["units_available"].replace({0: np.nan})
    product_all["sell_through_ratio"] = product_all["units_sold_window"] / (
        product_all["units_sold_window"] + product_all["units_available"]
    ).replace({0: np.nan})
    product_all["days_of_supply"] = product_all.apply(
        lambda r: _safe_dos(float(r.get("units_available", 0.0)), float(r.get("units_per_day", 0.0))),
        axis=1,
    )

    dos_for_rank = product_all["days_of_supply"].copy()
    rank_cap = max(thresholds["cut_days_of_supply"] * 1.5, float(max(report_days, 1)) * 4.0)
    dos_for_rank = dos_for_rank.fillna(rank_cap)
    product_all["assortment_priority_score"] = (
        0.40 * _percent_rank_score(product_all["inventory_value"], higher_is_worse=True)
        + 0.20 * _percent_rank_score(product_all["units_available"], higher_is_worse=True)
        + 0.25 * _percent_rank_score(dos_for_rank, higher_is_worse=True)
        + 0.15 * _percent_rank_score(product_all["sell_through_ratio"], higher_is_worse=False)
    ).round(1)
    product_all["action"] = product_all.apply(lambda r: _label_product_action(r, thresholds), axis=1)
    product_all["last_sale_date"] = product_all["last_sale_date"].dt.date
    product_all = product_all.sort_values(
        ["assortment_priority_score", "inventory_value", "units_available"],
        ascending=[False, False, False],
    )

    product_all["has_inventory"] = (product_all["units_available"] > 0).astype(int)
    product_all["has_sales"] = (product_all["units_sold_window"] > 0).astype(int)
    product_all["stale_sku"] = ((product_all["units_available"] > 0) & (product_all["units_sold_window"] <= 0)).astype(int)
    product_all["slow_sku"] = (
        (product_all["units_available"] > 0)
        & (
            product_all["days_of_supply"].fillna(rank_cap) >= thresholds["review_days_of_supply"]
        )
    ).astype(int)

    category_all = product_all.groupby("brand_category_key", as_index=False).agg(
        brand_key=("brand_key", lambda s: _first_mode(s, "unknown")),
        brand_name=("brand_name", lambda s: _first_mode(s, "Unknown")),
        category_normalized=("category_normalized", lambda s: _first_mode(s, "UNKNOWN")),
        sku_count=("brand_product_key", "nunique"),
        inventory_sku_count=("has_inventory", "sum"),
        selling_sku_count=("has_sales", "sum"),
        stale_sku_count=("stale_sku", "sum"),
        slow_sku_count=("slow_sku", "sum"),
        units_available=("units_available", "sum"),
        inventory_value=("inventory_value", "sum"),
        potential_revenue=("potential_revenue", "sum"),
        potential_profit=("potential_profit", "sum"),
        units_sold_window=("units_sold_window", "sum"),
        net_revenue_window=("net_revenue_window", "sum"),
        gross_sales_window=("gross_sales_window", "sum"),
        tickets_window=("tickets_window", "sum"),
    )
    category_all["units_per_day"] = category_all["units_sold_window"] / float(max(report_days, 1))
    category_all["inventory_turns"] = category_all["units_sold_window"] / category_all["units_available"].replace({0: np.nan})
    category_all["sell_through_ratio"] = category_all["units_sold_window"] / (
        category_all["units_sold_window"] + category_all["units_available"]
    ).replace({0: np.nan})
    category_all["days_of_supply"] = category_all.apply(
        lambda r: _safe_dos(float(r.get("units_available", 0.0)), float(r.get("units_per_day", 0.0))),
        axis=1,
    )
    category_all["stale_sku_share"] = category_all["stale_sku_count"] / category_all["sku_count"].replace({0: np.nan})
    category_all["slow_sku_share"] = category_all["slow_sku_count"] / category_all["sku_count"].replace({0: np.nan})
    category_all["avg_inventory_value_per_sku"] = category_all["inventory_value"] / category_all["inventory_sku_count"].replace({0: np.nan})
    category_dos_rank = category_all["days_of_supply"].fillna(rank_cap)
    category_all["assortment_priority_score"] = (
        0.40 * _percent_rank_score(category_all["inventory_value"], higher_is_worse=True)
        + 0.20 * _percent_rank_score(category_all["units_available"], higher_is_worse=True)
        + 0.20 * _percent_rank_score(category_dos_rank, higher_is_worse=True)
        + 0.20 * _percent_rank_score(category_all["stale_sku_share"], higher_is_worse=True)
    ).round(1)
    category_all["action"] = category_all.apply(lambda r: _label_category_action(r, thresholds), axis=1)
    category_all = category_all.sort_values(
        ["assortment_priority_score", "inventory_value", "units_available"],
        ascending=[False, False, False],
    )

    store_inventory = pd.DataFrame(columns=["_store_abbr", "brand_category_key", "units_available", "inventory_value"])
    if catalog_all_df is not None and not catalog_all_df.empty:
        tmp_inv = catalog_all_df.copy()
        if "_store_abbr" not in tmp_inv.columns:
            tmp_inv["_store_abbr"] = ""
        if "brand_category_key" not in tmp_inv.columns:
            tmp_inv["brand_category_key"] = ""
        tmp_inv["_store_abbr"] = tmp_inv["_store_abbr"].fillna("").astype(str).str.upper()
        tmp_inv["brand_category_key"] = tmp_inv["brand_category_key"].fillna("").astype(str)
        tmp_inv = tmp_inv[(tmp_inv["_store_abbr"] != "") & (tmp_inv["brand_category_key"] != "")].copy()
        if not tmp_inv.empty:
            store_inventory = tmp_inv.groupby(["_store_abbr", "brand_category_key"], as_index=False).agg(
                units_available=("Available", "sum"),
                inventory_value=("Inventory_Value", "sum"),
            )

    store_sales = pd.DataFrame(columns=["_store_abbr", "brand_category_key", "units_sold_window", "net_revenue_window"])
    if sales_report_df is not None and not sales_report_df.empty:
        tmp_sales = sales_report_df.copy()
        if "_is_return" in tmp_sales.columns:
            tmp_sales = tmp_sales[~tmp_sales["_is_return"]].copy()
        if "_store_abbr" not in tmp_sales.columns:
            tmp_sales["_store_abbr"] = ""
        if "brand_category_key" not in tmp_sales.columns:
            tmp_sales["brand_category_key"] = ""
        tmp_sales["_store_abbr"] = tmp_sales["_store_abbr"].fillna("").astype(str).str.upper()
        tmp_sales["brand_category_key"] = tmp_sales["brand_category_key"].fillna("").astype(str)
        tmp_sales = tmp_sales[(tmp_sales["_store_abbr"] != "") & (tmp_sales["brand_category_key"] != "")].copy()
        if not tmp_sales.empty:
            store_sales = tmp_sales.groupby(["_store_abbr", "brand_category_key"], as_index=False).agg(
                units_sold_window=("_qty", "sum"),
                net_revenue_window=("_net", "sum"),
            )

    store_category_all = store_inventory.merge(store_sales, on=["_store_abbr", "brand_category_key"], how="outer")
    if not store_category_all.empty:
        store_category_all = store_category_all.merge(
            category_all[["brand_category_key", "brand_name", "category_normalized"]],
            on="brand_category_key",
            how="left",
        )
        for col in ["units_available", "inventory_value", "units_sold_window", "net_revenue_window"]:
            store_category_all[col] = pd.to_numeric(store_category_all.get(col, 0.0), errors="coerce").fillna(0.0).astype(float)
        store_category_all["units_per_day"] = store_category_all["units_sold_window"] / float(max(report_days, 1))
        store_category_all["sell_through_ratio"] = store_category_all["units_sold_window"] / (
            store_category_all["units_sold_window"] + store_category_all["units_available"]
        ).replace({0: np.nan})
        store_category_all["days_of_supply"] = store_category_all.apply(
            lambda r: _safe_dos(float(r.get("units_available", 0.0)), float(r.get("units_per_day", 0.0))),
            axis=1,
        )
        store_category_all["action"] = store_category_all.apply(lambda r: _label_category_action(r, thresholds), axis=1)
        store_category_all = store_category_all.sort_values(["inventory_value", "units_available"], ascending=[False, False])

    product_all = product_all.drop(columns=["has_inventory", "has_sales", "stale_sku", "slow_sku"])
    product_cols = [
        "brand_product_key", "brand_key", "brand_name", "brand_category_key", "category_normalized",
        "display_product", "brand_product_display", "units_available", "inventory_value", "potential_revenue",
        "potential_profit", "margin_current", "stores_with_inventory", "units_sold_window", "net_revenue_window",
        "gross_sales_window", "tickets_window", "sales_days", "stores_with_sales", "last_sale_date",
        "units_per_day", "inventory_turns", "sell_through_ratio", "days_of_supply", "assortment_priority_score",
        "action", "merged_count", "raw_names_top5",
    ]
    product_all = product_all[[c for c in product_cols if c in product_all.columns]].copy()

    category_cols = [
        "brand_category_key", "brand_key", "brand_name", "category_normalized", "sku_count",
        "inventory_sku_count", "selling_sku_count", "stale_sku_count", "slow_sku_count", "units_available",
        "inventory_value", "potential_revenue", "potential_profit", "units_sold_window", "net_revenue_window",
        "gross_sales_window", "tickets_window", "units_per_day", "inventory_turns", "sell_through_ratio",
        "days_of_supply", "stale_sku_share", "slow_sku_share", "avg_inventory_value_per_sku",
        "assortment_priority_score", "action",
    ]
    category_all = category_all[[c for c in category_cols if c in category_all.columns]].copy()

    if not store_category_all.empty:
        store_category_cols = [
            "_store_abbr", "brand_category_key", "brand_name", "category_normalized", "units_available",
            "inventory_value", "units_sold_window", "net_revenue_window", "units_per_day", "sell_through_ratio",
            "days_of_supply", "action",
        ]
        store_category_all = store_category_all[[c for c in store_category_cols if c in store_category_all.columns]].copy()
    return product_all, category_all, store_category_all


def summarize_all_store_brand_candidates(
    category_all: pd.DataFrame,
    product_all: pd.DataFrame,
) -> pd.DataFrame:
    cols = [
        "brand_key", "brand_name", "flagged_category_count", "flagged_product_count", "flagged_inventory_value",
        "flagged_units", "flagged_inventory_share", "total_inventory_value", "total_units_available",
        "total_sku_count", "no_sales_products", "max_days_of_supply", "avg_priority_score",
        "worst_category", "lead_action",
    ]
    if product_all is None or product_all.empty:
        return pd.DataFrame(columns=cols)

    prod_all = product_all.copy()
    if "brand_key" not in prod_all.columns:
        prod_all["brand_key"] = "unknown"
    if "brand_name" not in prod_all.columns:
        prod_all["brand_name"] = "Unknown"
    if "inventory_value" not in prod_all.columns:
        prod_all["inventory_value"] = 0.0
    if "units_available" not in prod_all.columns:
        prod_all["units_available"] = 0.0
    if "action" not in prod_all.columns:
        prod_all["action"] = ""
    prod_all["brand_key"] = prod_all["brand_key"].fillna("unknown").astype(str)
    prod_all["brand_name"] = prod_all["brand_name"].fillna("Unknown").astype(str)
    prod_all["inventory_value"] = pd.to_numeric(prod_all["inventory_value"], errors="coerce").fillna(0.0).astype(float)
    prod_all["units_available"] = pd.to_numeric(prod_all["units_available"], errors="coerce").fillna(0.0).astype(float)

    total_brand = prod_all.groupby("brand_key", as_index=False).agg(
        brand_name=("brand_name", lambda s: _first_mode(s, "Unknown")),
        total_inventory_value=("inventory_value", "sum"),
        total_units_available=("units_available", "sum"),
        total_sku_count=("brand_product_key", "nunique"),
    )

    flagged_products = prod_all[prod_all["action"].fillna("").astype(str) != "Healthy"].copy()
    if flagged_products.empty:
        return pd.DataFrame(columns=cols)

    if "units_sold_window" not in flagged_products.columns:
        flagged_products["units_sold_window"] = 0.0
    if "days_of_supply" not in flagged_products.columns:
        flagged_products["days_of_supply"] = np.nan
    if "assortment_priority_score" not in flagged_products.columns:
        flagged_products["assortment_priority_score"] = 0.0
    flagged_products["units_sold_window"] = pd.to_numeric(flagged_products["units_sold_window"], errors="coerce").fillna(0.0).astype(float)
    flagged_products["days_of_supply"] = pd.to_numeric(flagged_products["days_of_supply"], errors="coerce")
    flagged_products["assortment_priority_score"] = pd.to_numeric(
        flagged_products["assortment_priority_score"], errors="coerce"
    ).fillna(0.0).astype(float)

    product_summary = flagged_products.groupby("brand_key", as_index=False).agg(
        brand_name=("brand_name", lambda s: _first_mode(s, "Unknown")),
        flagged_product_count=("brand_product_key", "nunique"),
        flagged_inventory_value=("inventory_value", "sum"),
        flagged_units=("units_available", "sum"),
        no_sales_products=("units_sold_window", lambda s: int((pd.to_numeric(s, errors="coerce").fillna(0.0) <= 0).sum())),
        max_days_of_supply=("days_of_supply", "max"),
        avg_priority_score=("assortment_priority_score", "mean"),
    )

    category_summary = pd.DataFrame(columns=["brand_key", "flagged_category_count", "worst_category", "lead_action"])
    if category_all is not None and not category_all.empty:
        flagged_categories = category_all.copy()
        if "action" not in flagged_categories.columns:
            flagged_categories["action"] = ""
        flagged_categories = flagged_categories[flagged_categories["action"].fillna("").astype(str) != "Healthy"].copy()
        if not flagged_categories.empty:
            if "assortment_priority_score" not in flagged_categories.columns:
                flagged_categories["assortment_priority_score"] = 0.0
            if "inventory_value" not in flagged_categories.columns:
                flagged_categories["inventory_value"] = 0.0
            if "brand_key" not in flagged_categories.columns:
                flagged_categories["brand_key"] = "unknown"
            if "brand_name" not in flagged_categories.columns:
                flagged_categories["brand_name"] = "Unknown"
            flagged_categories["assortment_priority_score"] = pd.to_numeric(
                flagged_categories["assortment_priority_score"], errors="coerce"
            ).fillna(0.0).astype(float)
            flagged_categories["inventory_value"] = pd.to_numeric(
                flagged_categories["inventory_value"], errors="coerce"
            ).fillna(0.0).astype(float)
            flagged_categories["brand_key"] = flagged_categories["brand_key"].fillna("unknown").astype(str)
            flagged_categories["brand_name"] = flagged_categories["brand_name"].fillna("Unknown").astype(str)
            count_df = flagged_categories.groupby("brand_key", as_index=False).agg(
                flagged_category_count=("brand_category_key", "nunique")
            )
            flagged_categories = flagged_categories.sort_values(
                ["assortment_priority_score", "inventory_value"],
                ascending=[False, False],
            )
            top_rows = flagged_categories.groupby("brand_key", as_index=False).head(1).copy()
            top_rows = top_rows[["brand_key", "category_normalized", "action"]].rename(
                columns={"category_normalized": "worst_category", "action": "lead_action"}
            )
            category_summary = count_df.merge(top_rows, on="brand_key", how="left")

    brand_summary = total_brand.merge(product_summary, on=["brand_key", "brand_name"], how="left")
    if not category_summary.empty:
        brand_summary = brand_summary.merge(category_summary, on="brand_key", how="left")
    else:
        brand_summary["flagged_category_count"] = 0.0
        brand_summary["worst_category"] = ""
        brand_summary["lead_action"] = ""

    for col in [
        "flagged_category_count", "flagged_product_count", "flagged_inventory_value", "flagged_units",
        "no_sales_products", "avg_priority_score",
    ]:
        if col not in brand_summary.columns:
            brand_summary[col] = 0.0
        brand_summary[col] = pd.to_numeric(brand_summary[col], errors="coerce").fillna(0.0).astype(float)
    if "max_days_of_supply" not in brand_summary.columns:
        brand_summary["max_days_of_supply"] = np.nan
    brand_summary["max_days_of_supply"] = pd.to_numeric(brand_summary["max_days_of_supply"], errors="coerce")
    brand_summary["flagged_inventory_share"] = brand_summary["flagged_inventory_value"] / brand_summary["total_inventory_value"].replace({0: np.nan})
    brand_summary["flagged_inventory_share"] = pd.to_numeric(brand_summary["flagged_inventory_share"], errors="coerce").fillna(0.0).astype(float)
    if "worst_category" not in brand_summary.columns:
        brand_summary["worst_category"] = ""
    if "lead_action" not in brand_summary.columns:
        brand_summary["lead_action"] = ""
    brand_summary["worst_category"] = brand_summary["worst_category"].fillna("").astype(str)
    brand_summary["lead_action"] = brand_summary["lead_action"].fillna("").astype(str)
    brand_summary = brand_summary[
        (brand_summary["flagged_product_count"] > 0) | (brand_summary["flagged_category_count"] > 0)
    ].copy()
    brand_summary = brand_summary.sort_values(
        ["flagged_inventory_value", "flagged_product_count", "flagged_category_count"],
        ascending=[False, False, False],
    )
    return brand_summary[[c for c in cols if c in brand_summary.columns]].copy()


def _store_product_action_rank(action: Any) -> int:
    text = str(action or "").strip().lower()
    if text.startswith("cut"):
        return 3
    if text.startswith("review"):
        return 2
    if text.startswith("new"):
        return 1
    return 0


def _label_store_product_action(
    row: pd.Series,
    thresholds: Dict[str, float],
    new_launch_days: int,
) -> str:
    inv_units = float(row.get("units_available", 0.0) or 0.0)
    sold_units = float(row.get("units_sold_window", 0.0) or 0.0)
    inventory_value = float(row.get("inventory_value", 0.0) or 0.0)
    dos = float(row.get("days_of_supply", np.nan))
    sell_through = float(row.get("sell_through_ratio", 0.0) or 0.0)
    days_since_last_sale = float(row.get("days_since_last_sale", np.nan))
    is_new = bool(row.get("is_new_sku", False))

    if inv_units <= 0:
        return "Healthy"
    if is_new and sold_units > 0:
        return "New / Monitor"
    if sold_units <= 0 and is_new:
        if inventory_value >= 500.0 or inv_units >= 20:
            return "Review SKU - no sales yet"
        return "New / Monitor"
    if sold_units <= 0 and (inventory_value >= 200.0 or inv_units >= 8):
        return "Cut SKU - no sales"
    if not is_new and np.isfinite(days_since_last_sale) and days_since_last_sale >= 21 and (inventory_value >= 150.0 or inv_units >= 6):
        return "Cut SKU - stale"
    if not is_new and np.isfinite(dos) and dos >= thresholds["cut_days_of_supply"] and sell_through <= thresholds["cut_sell_through"]:
        return "Cut SKU - low turns"
    if sold_units <= 0:
        return "Review SKU - low proof"
    if not is_new and np.isfinite(days_since_last_sale) and days_since_last_sale >= 14 and sell_through <= 0.12:
        return "Review SKU - stale"
    if not is_new and np.isfinite(dos) and dos >= thresholds["review_days_of_supply"] and sell_through <= thresholds["low_sell_through"]:
        return "Review SKU - slow"
    return "Healthy"


def summarize_store_inventory_products(df: pd.DataFrame) -> pd.DataFrame:
    cols = [
        "_store_abbr", "brand_product_key", "brand_key", "brand_name", "brand_category_key", "category_normalized",
        "display_product", "brand_product_display", "units_available", "inventory_value", "potential_revenue",
        "potential_profit", "margin_current", "merged_count", "raw_names_top5", "store_brand_key",
        "store_brand_category_key",
    ]
    if df is None or df.empty:
        return pd.DataFrame(columns=cols)

    tmp = _inventory_reporting_rows(_filter_product_group_rows(df, exclude_accessories=True))
    if tmp.empty:
        return pd.DataFrame(columns=cols)

    if "brand_product_key" not in tmp.columns:
        tmp["brand_product_key"] = ""
    if "_store_abbr" not in tmp.columns:
        tmp["_store_abbr"] = ""
    tmp["brand_product_key"] = tmp["brand_product_key"].fillna("").astype(str)
    tmp["_store_abbr"] = tmp["_store_abbr"].fillna("").astype(str).str.upper()
    tmp = tmp[(tmp["brand_product_key"] != "") & (tmp["_store_abbr"] != "")].copy()
    if tmp.empty:
        return pd.DataFrame(columns=cols)

    tmp["effective_total"] = tmp["Effective_Price"] * tmp["Available"]
    grouped = tmp.groupby(["_store_abbr", "brand_product_key"], as_index=False).agg(
        brand_key=("brand_key", lambda s: _first_mode(s, "unknown")),
        brand_name=("brand_name", lambda s: _first_mode(s, "Unknown")),
        brand_category_key=("brand_category_key", lambda s: _first_mode(s, "")),
        category_normalized=("category_normalized", lambda s: _first_mode(s, "UNKNOWN")),
        display_product=("display_product", lambda s: _first_mode(s, "")),
        brand_product_display=("brand_product_display", lambda s: _first_mode(s, "")),
        units_available=("Available", "sum"),
        inventory_value=("Inventory_Value", "sum"),
        potential_revenue=("Potential_Revenue", "sum"),
        potential_profit=("Potential_Profit", "sum"),
        effective_total=("effective_total", "sum"),
    )

    raw_map: Dict[Tuple[str, str], str] = {}
    merged_count_map: Dict[Tuple[str, str], int] = {}
    for key, part in tmp.groupby(["_store_abbr", "brand_product_key"]):
        names = sorted({str(x).strip() for x in part["_product_raw"].dropna().astype(str).tolist() if str(x).strip()})
        raw_map[(str(key[0]), str(key[1]))] = " | ".join(names[:5])
        merged_count_map[(str(key[0]), str(key[1]))] = int(len(names))

    grouped["margin_current"] = grouped["potential_profit"] / grouped["effective_total"].replace({0: np.nan})
    grouped["raw_names_top5"] = grouped.apply(lambda r: raw_map.get((str(r["_store_abbr"]), str(r["brand_product_key"])), ""), axis=1)
    grouped["merged_count"] = grouped.apply(lambda r: merged_count_map.get((str(r["_store_abbr"]), str(r["brand_product_key"])), 1), axis=1)
    grouped["store_brand_key"] = grouped["_store_abbr"].fillna("").astype(str) + "|" + grouped["brand_key"].fillna("unknown").astype(str)
    grouped["store_brand_category_key"] = grouped["_store_abbr"].fillna("").astype(str) + "|" + grouped["brand_category_key"].fillna("").astype(str)
    grouped = grouped.drop(columns=["effective_total"]).replace([np.inf, -np.inf], np.nan)
    num_cols = grouped.select_dtypes(include=[np.number]).columns.tolist()
    if num_cols:
        grouped[num_cols] = grouped[num_cols].fillna(0.0)
    for col in [
        "_store_abbr", "brand_product_key", "brand_key", "brand_name", "brand_category_key", "category_normalized",
        "display_product", "brand_product_display", "raw_names_top5", "store_brand_key", "store_brand_category_key",
    ]:
        grouped[col] = grouped[col].fillna("").astype(str)
    return grouped[[c for c in cols if c in grouped.columns]].copy()


def summarize_store_sales_products(df: pd.DataFrame) -> pd.DataFrame:
    cols = [
        "_store_abbr", "brand_product_key", "brand_key", "brand_name", "brand_category_key", "category_normalized",
        "display_product", "brand_product_display", "units_sold_window", "net_revenue_window", "gross_sales_window",
        "tickets_window", "sales_days", "first_sale_date", "last_sale_date", "store_brand_key",
        "store_brand_category_key",
    ]
    if df is None or df.empty:
        return pd.DataFrame(columns=cols)

    tmp = _filter_product_group_rows(df, exclude_accessories=True)
    if tmp.empty:
        return pd.DataFrame(columns=cols)
    if "_is_return" in tmp.columns:
        tmp = tmp[~tmp["_is_return"]].copy()
    if "brand_product_key" not in tmp.columns:
        tmp["brand_product_key"] = ""
    if "_store_abbr" not in tmp.columns:
        tmp["_store_abbr"] = ""
    tmp["brand_product_key"] = tmp["brand_product_key"].fillna("").astype(str)
    tmp["_store_abbr"] = tmp["_store_abbr"].fillna("").astype(str).str.upper()
    tmp = tmp[(tmp["brand_product_key"] != "") & (tmp["_store_abbr"] != "")].copy()
    if tmp.empty:
        return pd.DataFrame(columns=cols)

    grouped = tmp.groupby(["_store_abbr", "brand_product_key"], as_index=False).agg(
        brand_key=("brand_key", lambda s: _first_mode(s, "unknown")),
        brand_name=("brand_name", lambda s: _first_mode(s, "Unknown")),
        brand_category_key=("brand_category_key", lambda s: _first_mode(s, "")),
        category_normalized=("category_normalized", lambda s: _first_mode(s, "UNKNOWN")),
        display_product=("display_product", lambda s: _first_mode(s, "")),
        brand_product_display=("brand_product_display", lambda s: _first_mode(s, "")),
        units_sold_window=("_qty", "sum"),
        net_revenue_window=("_net", "sum"),
        gross_sales_window=("_gross", "sum"),
        tickets_window=("_tx_key", "nunique"),
        sales_days=("_date", "nunique"),
        first_sale_date=("_date", "min"),
        last_sale_date=("_date", "max"),
    )
    grouped["store_brand_key"] = grouped["_store_abbr"].fillna("").astype(str) + "|" + grouped["brand_key"].fillna("unknown").astype(str)
    grouped["store_brand_category_key"] = grouped["_store_abbr"].fillna("").astype(str) + "|" + grouped["brand_category_key"].fillna("").astype(str)
    return grouped[[c for c in cols if c in grouped.columns]].copy()


def build_store_level_assortment_views(
    catalog_all_df: pd.DataFrame,
    sales_report_df: pd.DataFrame,
    report_days: int,
    end_day: date,
) -> Tuple[pd.DataFrame, pd.DataFrame, pd.DataFrame, pd.DataFrame, pd.DataFrame]:
    thresholds = _all_store_report_thresholds(report_days)
    new_launch_days = min(max(14, report_days // 2), 21)

    inv_products = summarize_store_inventory_products(catalog_all_df)
    sales_products = summarize_store_sales_products(sales_report_df)
    product_eval = inv_products.merge(
        sales_products,
        on=["_store_abbr", "brand_product_key"],
        how="outer",
        suffixes=("_inv", "_sales"),
    )
    if product_eval.empty:
        empty_store = pd.DataFrame(columns=["_store_abbr"])
        return pd.DataFrame(), pd.DataFrame(), pd.DataFrame(), pd.DataFrame(), empty_store

    for col in ["brand_key", "brand_name", "brand_category_key", "category_normalized", "display_product", "brand_product_display", "store_brand_key", "store_brand_category_key"]:
        default = "unknown" if col == "brand_key" else ("Unknown" if col == "brand_name" else "")
        product_eval[col] = _coalesce_text_columns(product_eval, col, default)

    numeric_defaults = {
        "units_available": 0.0,
        "inventory_value": 0.0,
        "potential_revenue": 0.0,
        "potential_profit": 0.0,
        "margin_current": 0.0,
        "merged_count": 0.0,
        "units_sold_window": 0.0,
        "net_revenue_window": 0.0,
        "gross_sales_window": 0.0,
        "tickets_window": 0.0,
        "sales_days": 0.0,
    }
    for col, default in numeric_defaults.items():
        inv_col = f"{col}_inv"
        sales_col = f"{col}_sales"
        if col in product_eval.columns:
            product_eval[col] = pd.to_numeric(product_eval[col], errors="coerce").fillna(default).astype(float)
        elif inv_col in product_eval.columns or sales_col in product_eval.columns:
            left = pd.to_numeric(product_eval[inv_col], errors="coerce") if inv_col in product_eval.columns else pd.Series(np.nan, index=product_eval.index)
            right = pd.to_numeric(product_eval[sales_col], errors="coerce") if sales_col in product_eval.columns else pd.Series(np.nan, index=product_eval.index)
            product_eval[col] = left.fillna(right).fillna(default).astype(float)
        else:
            product_eval[col] = float(default)

    product_eval["raw_names_top5"] = product_eval["raw_names_top5"].fillna("").astype(str) if "raw_names_top5" in product_eval.columns else ""
    product_eval["first_sale_date"] = pd.to_datetime(product_eval["first_sale_date_sales"], errors="coerce") if "first_sale_date_sales" in product_eval.columns else pd.NaT
    product_eval["last_sale_date"] = pd.to_datetime(product_eval["last_sale_date_sales"], errors="coerce") if "last_sale_date_sales" in product_eval.columns else pd.NaT

    product_eval["units_per_day"] = product_eval["units_sold_window"] / float(max(report_days, 1))
    product_eval["inventory_turns"] = product_eval["units_sold_window"] / product_eval["units_available"].replace({0: np.nan})
    product_eval["sell_through_ratio"] = product_eval["units_sold_window"] / (
        product_eval["units_sold_window"] + product_eval["units_available"]
    ).replace({0: np.nan})
    product_eval["days_of_supply"] = product_eval.apply(
        lambda r: _safe_dos(float(r.get("units_available", 0.0)), float(r.get("units_per_day", 0.0))),
        axis=1,
    )

    end_ts = pd.Timestamp(end_day)
    product_eval["launch_age_days"] = np.where(
        product_eval["first_sale_date"].notna(),
        (end_ts - product_eval["first_sale_date"]).dt.days + 1,
        np.nan,
    )
    product_eval["days_since_last_sale"] = np.where(
        product_eval["last_sale_date"].notna(),
        (end_ts - product_eval["last_sale_date"]).dt.days,
        np.nan,
    )
    product_eval["is_new_sku"] = product_eval["launch_age_days"].fillna(9999).astype(float) <= float(new_launch_days)
    product_eval["action"] = product_eval.apply(
        lambda r: _label_store_product_action(r, thresholds, new_launch_days),
        axis=1,
    )
    product_eval["action_rank"] = product_eval["action"].map(_store_product_action_rank).fillna(0).astype(int)

    dos_rank = pd.to_numeric(product_eval["days_of_supply"], errors="coerce").fillna(max(thresholds["cut_days_of_supply"] * 1.5, report_days * 4.0))
    stale_rank = pd.to_numeric(product_eval["days_since_last_sale"], errors="coerce").fillna(report_days * 2.0)
    product_eval["assortment_priority_score"] = (
        0.35 * _percent_rank_score(product_eval["inventory_value"], higher_is_worse=True)
        + 0.20 * _percent_rank_score(product_eval["units_available"], higher_is_worse=True)
        + 0.20 * _percent_rank_score(dos_rank, higher_is_worse=True)
        + 0.15 * _percent_rank_score(product_eval["sell_through_ratio"], higher_is_worse=False)
        + 0.10 * _percent_rank_score(stale_rank, higher_is_worse=True)
        + product_eval["action_rank"] * 4.0
    ).round(1)
    product_eval["cut_flag"] = (product_eval["action_rank"] >= 3).astype(int)
    product_eval["review_flag"] = (product_eval["action_rank"] == 2).astype(int)
    product_eval["monitor_flag"] = (product_eval["action_rank"] == 1).astype(int)
    product_eval["actionable_flag"] = (product_eval["action_rank"] >= 2).astype(int)
    product_eval["cut_inventory_value"] = np.where(product_eval["cut_flag"] > 0, product_eval["inventory_value"], 0.0)
    product_eval["actionable_inventory_value"] = np.where(product_eval["actionable_flag"] > 0, product_eval["inventory_value"], 0.0)
    product_eval["store_name"] = product_eval["_store_abbr"].fillna("").astype(str).map(_store_name_from_abbr)
    product_eval["first_sale_date"] = pd.to_datetime(product_eval["first_sale_date"], errors="coerce").dt.date
    product_eval["last_sale_date"] = pd.to_datetime(product_eval["last_sale_date"], errors="coerce").dt.date
    product_eval = product_eval.sort_values(
        ["_store_abbr", "action_rank", "assortment_priority_score", "inventory_value"],
        ascending=[True, False, False, False],
    )

    product_candidates = product_eval[product_eval["action"].fillna("").astype(str).str.startswith(("Cut", "Review"))].copy()
    product_candidates = product_candidates.sort_values(
        ["_store_abbr", "action_rank", "assortment_priority_score", "inventory_value"],
        ascending=[True, False, False, False],
    )

    category_source = product_eval.copy()
    category_source["store_brand_category_key"] = category_source["store_brand_category_key"].fillna("").astype(str)
    category_source["cut_inventory_value"] = np.where(category_source["action"].fillna("").astype(str).str.startswith("Cut"), category_source["inventory_value"], 0.0)
    category_source["actionable_inventory_value"] = np.where(category_source["action"].fillna("").astype(str).str.startswith(("Cut", "Review")), category_source["inventory_value"], 0.0)
    category_summary = category_source.groupby(["_store_abbr", "store_brand_category_key"], as_index=False).agg(
        brand_key=("brand_key", lambda s: _first_mode(s, "unknown")),
        brand_name=("brand_name", lambda s: _first_mode(s, "Unknown")),
        brand_category_key=("brand_category_key", lambda s: _first_mode(s, "")),
        category_normalized=("category_normalized", lambda s: _first_mode(s, "UNKNOWN")),
        sku_count=("brand_product_key", "nunique"),
        cut_sku_count=("cut_flag", "sum"),
        review_sku_count=("review_flag", "sum"),
        monitor_sku_count=("monitor_flag", "sum"),
        units_available=("units_available", "sum"),
        inventory_value=("inventory_value", "sum"),
        potential_revenue=("potential_revenue", "sum"),
        potential_profit=("potential_profit", "sum"),
        units_sold_window=("units_sold_window", "sum"),
        net_revenue_window=("net_revenue_window", "sum"),
        cut_inventory_value=("cut_inventory_value", "sum"),
        actionable_inventory_value=("actionable_inventory_value", "sum"),
    )
    category_summary["store_name"] = category_summary["_store_abbr"].fillna("").astype(str).map(_store_name_from_abbr)
    category_summary["units_per_day"] = category_summary["units_sold_window"] / float(max(report_days, 1))
    category_summary["sell_through_ratio"] = category_summary["units_sold_window"] / (
        category_summary["units_sold_window"] + category_summary["units_available"]
    ).replace({0: np.nan})
    category_summary["days_of_supply"] = category_summary.apply(
        lambda r: _safe_dos(float(r.get("units_available", 0.0)), float(r.get("units_per_day", 0.0))),
        axis=1,
    )
    category_summary["actionable_inventory_share"] = category_summary["actionable_inventory_value"] / category_summary["inventory_value"].replace({0: np.nan})
    category_summary["status"] = np.where(
        (category_summary["cut_sku_count"] >= 1) & (
            (category_summary["cut_inventory_value"] >= 200.0)
            | (category_summary["cut_sku_count"] >= 2)
            | (pd.to_numeric(category_summary["days_of_supply"], errors="coerce").fillna(0.0) >= thresholds["cut_days_of_supply"])
        ),
        "Trim category SKUs",
        np.where(
            (category_summary["review_sku_count"] >= 1) & (pd.to_numeric(category_summary["actionable_inventory_share"], errors="coerce").fillna(0.0) >= 0.20),
            "Review category mix",
            "Healthy",
        ),
    )
    category_candidates = category_summary[category_summary["status"] != "Healthy"].copy()
    category_candidates = category_candidates.sort_values(
        ["_store_abbr", "cut_inventory_value", "actionable_inventory_value", "inventory_value"],
        ascending=[True, False, False, False],
    )
    category_cols = [
        "_store_abbr", "store_name", "store_brand_category_key", "brand_key", "brand_name", "brand_category_key",
        "category_normalized", "sku_count", "cut_sku_count", "review_sku_count", "monitor_sku_count",
        "units_available", "inventory_value", "potential_revenue", "potential_profit", "units_sold_window",
        "net_revenue_window", "units_per_day", "sell_through_ratio", "days_of_supply", "cut_inventory_value",
        "actionable_inventory_value", "actionable_inventory_share", "status",
    ]
    category_candidates = category_candidates[[c for c in category_cols if c in category_candidates.columns]].copy()

    brand_source = product_eval.copy()
    top_product_map: Dict[str, Tuple[str, str]] = {}
    if not product_candidates.empty:
        tmp_top = product_candidates.sort_values(
            ["_store_abbr", "action_rank", "assortment_priority_score", "inventory_value"],
            ascending=[True, False, False, False],
        )
        for store_brand_key, part in tmp_top.groupby("store_brand_key"):
            row0 = part.iloc[0]
            top_product_map[str(store_brand_key)] = (
                str(row0.get("display_product", "")),
                str(row0.get("action", "")),
            )

    brand_summary = brand_source.groupby(["_store_abbr", "store_brand_key"], as_index=False).agg(
        brand_key=("brand_key", lambda s: _first_mode(s, "unknown")),
        brand_name=("brand_name", lambda s: _first_mode(s, "Unknown")),
        total_sku_count=("brand_product_key", "nunique"),
        cut_sku_count=("cut_flag", "sum"),
        review_sku_count=("review_flag", "sum"),
        monitor_sku_count=("monitor_flag", "sum"),
        total_inventory_value=("inventory_value", "sum"),
        total_units_available=("units_available", "sum"),
        cut_inventory_value=("cut_inventory_value", "sum"),
        actionable_inventory_value=("actionable_inventory_value", "sum"),
        units_sold_window=("units_sold_window", "sum"),
        net_revenue_window=("net_revenue_window", "sum"),
    )
    brand_summary["store_name"] = brand_summary["_store_abbr"].fillna("").astype(str).map(_store_name_from_abbr)
    brand_summary["units_per_day"] = brand_summary["units_sold_window"] / float(max(report_days, 1))
    brand_summary["sell_through_ratio"] = brand_summary["units_sold_window"] / (
        brand_summary["units_sold_window"] + brand_summary["total_units_available"]
    ).replace({0: np.nan})
    brand_summary["days_of_supply"] = brand_summary.apply(
        lambda r: _safe_dos(float(r.get("total_units_available", 0.0)), float(r.get("units_per_day", 0.0))),
        axis=1,
    )
    brand_summary["cut_inventory_share"] = brand_summary["cut_inventory_value"] / brand_summary["total_inventory_value"].replace({0: np.nan})
    brand_summary["actionable_inventory_share"] = brand_summary["actionable_inventory_value"] / brand_summary["total_inventory_value"].replace({0: np.nan})
    brand_summary["no_sales_cut_skus"] = 0
    if not product_candidates.empty:
        cut_zero = product_candidates[
            product_candidates["action"].fillna("").astype(str).str.startswith("Cut")
            & (pd.to_numeric(product_candidates["units_sold_window"], errors="coerce").fillna(0.0) <= 0)
        ].copy()
        if not cut_zero.empty:
            zero_map = cut_zero.groupby("store_brand_key", as_index=False).agg(no_sales_cut_skus=("brand_product_key", "nunique"))
            brand_summary = brand_summary.merge(zero_map, on="store_brand_key", how="left", suffixes=("", "_drop"))
            if "no_sales_cut_skus_drop" in brand_summary.columns:
                brand_summary["no_sales_cut_skus"] = pd.to_numeric(brand_summary["no_sales_cut_skus_drop"], errors="coerce").fillna(brand_summary["no_sales_cut_skus"]).astype(float)
                brand_summary = brand_summary.drop(columns=["no_sales_cut_skus_drop"])
    brand_summary["report_status"] = np.where(
        (pd.to_numeric(brand_summary["cut_sku_count"], errors="coerce").fillna(0.0) <= 0)
        & (pd.to_numeric(brand_summary["review_sku_count"], errors="coerce").fillna(0.0) <= 0),
        "Healthy",
        np.where(
            (pd.to_numeric(brand_summary["sell_through_ratio"], errors="coerce").fillna(0.0) >= 0.35)
            & (pd.to_numeric(brand_summary["days_of_supply"], errors="coerce").fillna(9999.0) <= min(75.0, thresholds["review_days_of_supply"]))
            & (pd.to_numeric(brand_summary["cut_sku_count"], errors="coerce").fillna(0.0) <= 2)
            & (pd.to_numeric(brand_summary["cut_inventory_share"], errors="coerce").fillna(0.0) <= 0.15),
            "Healthy brand, trim a few SKUs",
            np.where(
                (pd.to_numeric(brand_summary["cut_sku_count"], errors="coerce").fillna(0.0) >= 3)
                | (pd.to_numeric(brand_summary["cut_inventory_share"], errors="coerce").fillna(0.0) >= 0.30)
                | (pd.to_numeric(brand_summary["cut_inventory_value"], errors="coerce").fillna(0.0) >= 1500.0),
                "Trim brand SKU list",
                np.where(
                    pd.to_numeric(brand_summary["cut_sku_count"], errors="coerce").fillna(0.0) >= 1,
                    "Trim a few SKUs",
                    "Review brand SKU mix",
                ),
            ),
        ),
    )
    brand_summary["report_include"] = (
        (pd.to_numeric(brand_summary["cut_sku_count"], errors="coerce").fillna(0.0) >= 1)
        | (
            (pd.to_numeric(brand_summary["review_sku_count"], errors="coerce").fillna(0.0) >= 2)
            & (pd.to_numeric(brand_summary["actionable_inventory_share"], errors="coerce").fillna(0.0) >= 0.20)
        )
    )
    brand_summary["lead_sku"] = brand_summary["store_brand_key"].map(lambda k: top_product_map.get(str(k), ("", ""))[0]).fillna("")
    brand_summary["lead_sku_action"] = brand_summary["store_brand_key"].map(lambda k: top_product_map.get(str(k), ("", ""))[1]).fillna("")
    brand_summary = brand_summary.sort_values(
        ["_store_abbr", "cut_inventory_value", "actionable_inventory_value", "total_inventory_value"],
        ascending=[True, False, False, False],
    )
    brand_cols = [
        "_store_abbr", "store_name", "store_brand_key", "brand_key", "brand_name", "report_status", "report_include",
        "total_sku_count", "cut_sku_count", "review_sku_count", "monitor_sku_count", "no_sales_cut_skus",
        "total_inventory_value", "total_units_available", "cut_inventory_value", "actionable_inventory_value",
        "cut_inventory_share", "actionable_inventory_share", "units_sold_window", "net_revenue_window",
        "units_per_day", "sell_through_ratio", "days_of_supply", "lead_sku", "lead_sku_action",
    ]
    brand_summary = brand_summary[[c for c in brand_cols if c in brand_summary.columns]].copy()
    report_brands = brand_summary[brand_summary["report_include"]].copy()

    store_summary = report_brands.groupby("_store_abbr", as_index=False).agg(
        brands_to_trim=("store_brand_key", "nunique"),
        cut_sku_count=("cut_sku_count", "sum"),
        review_sku_count=("review_sku_count", "sum"),
        inventory_at_risk=("actionable_inventory_value", "sum"),
        cut_inventory_value=("cut_inventory_value", "sum"),
    ) if not report_brands.empty else pd.DataFrame(columns=["_store_abbr", "brands_to_trim", "cut_sku_count", "review_sku_count", "inventory_at_risk", "cut_inventory_value"])
    if not report_brands.empty:
        top_brand_rows = report_brands.sort_values(["_store_abbr", "cut_inventory_value", "actionable_inventory_value"], ascending=[True, False, False]).groupby("_store_abbr", as_index=False).head(1)
        top_brand_map = {str(r["_store_abbr"]): str(r["brand_name"]) for _, r in top_brand_rows.iterrows()}
        store_summary["top_brand"] = store_summary["_store_abbr"].map(top_brand_map).fillna("")
    else:
        store_summary["top_brand"] = ""
    store_summary["store_name"] = store_summary["_store_abbr"].fillna("").astype(str).map(_store_name_from_abbr)
    store_summary = store_summary[[
        c for c in [
            "_store_abbr", "store_name", "brands_to_trim", "cut_sku_count", "review_sku_count",
            "inventory_at_risk", "cut_inventory_value", "top_brand",
        ]
        if c in store_summary.columns
    ]].copy()

    product_cols = [
        "_store_abbr", "store_name", "store_brand_key", "store_brand_category_key", "brand_product_key", "brand_key",
        "brand_name", "brand_category_key", "category_normalized", "display_product", "brand_product_display",
        "units_available", "inventory_value", "potential_revenue", "potential_profit", "margin_current",
        "units_sold_window", "net_revenue_window", "gross_sales_window", "tickets_window", "sales_days",
        "first_sale_date", "last_sale_date", "days_since_last_sale", "is_new_sku", "units_per_day",
        "inventory_turns", "sell_through_ratio", "days_of_supply", "assortment_priority_score", "action_rank",
        "action", "cut_flag", "review_flag", "monitor_flag", "actionable_flag", "cut_inventory_value",
        "actionable_inventory_value", "merged_count", "raw_names_top5",
    ]
    product_all = product_eval[[c for c in product_cols if c in product_eval.columns]].copy()
    product_candidates = product_candidates[[c for c in product_cols if c in product_candidates.columns]].copy()
    return product_all, product_candidates, category_candidates, brand_summary, store_summary


def build_all_store_assortment_report_xlsx(
    out_xlsx: Path,
    summary_df: pd.DataFrame,
    store_summary: pd.DataFrame,
    store_brand_summary: pd.DataFrame,
    store_category_candidates: pd.DataFrame,
    store_product_candidates: pd.DataFrame,
    store_product_all: pd.DataFrame,
) -> None:
    out_xlsx.parent.mkdir(parents=True, exist_ok=True)
    with pd.ExcelWriter(out_xlsx, engine="openpyxl") as writer:
        summary_df.to_excel(writer, sheet_name="Summary", index=False)
        store_summary.to_excel(writer, sheet_name="Store_Summary", index=False)
        store_brand_summary.to_excel(writer, sheet_name="Store_Brand_Summary", index=False)
        store_category_candidates.to_excel(writer, sheet_name="Store_Category_Cuts", index=False)
        store_product_candidates.to_excel(writer, sheet_name="Store_Product_Cuts", index=False)
        store_product_all.to_excel(writer, sheet_name="Store_Product_All", index=False)


def generate_all_store_slow_mover_report(
    start_day: date,
    end_day: date,
    output_root: Path = DEFAULT_OUTPUT_ROOT,
    selected_store_codes: Optional[Sequence[str]] = None,
    force_refresh_data: bool = False,
    email_results: bool = False,
    logger: Optional[Callable[[str], None]] = None,
) -> AllStoreSlowMoverArtifacts:
    selected = order_store_codes(selected_store_codes or list(store_abbr_map.values()))
    if not selected:
        raise ValueError("At least one store is required.")

    output_root = Path(output_root).expanduser().resolve()
    paths = build_run_paths(output_root, ALL_STORE_SLOW_MOVER_REPORT_NAME, start_day, end_day)
    report_days = max(window_days(start_day, end_day), 1)
    thresholds = _all_store_report_thresholds(report_days)

    _log(
        f"[START] Building store-level SKU cut report for {start_day.isoformat()} -> {end_day.isoformat()}",
        logger,
    )

    sales_paths, missing_sales_stores, _did_export_sales = prepare_sales_exports(
        paths=paths,
        brand=ALL_STORE_SLOW_MOVER_REPORT_NAME,
        selected_store_codes=selected,
        acquisition_start=start_day,
        acquisition_end=end_day,
        allow_export=True,
        force_refresh=force_refresh_data,
        use_api=False,
        api_env_file=DEFAULT_API_ENV_FILE,
        api_workers=DEFAULT_PACKET_API_WORKERS,
        logger=logger,
    )
    catalog_paths, missing_catalog_stores, _did_export_catalog = prepare_catalog_exports(
        paths=paths,
        selected_store_codes=selected,
        run_export=force_refresh_data,
        force_refresh=force_refresh_data,
        use_api=False,
        api_env_file=DEFAULT_API_ENV_FILE,
        api_workers=DEFAULT_PACKET_API_WORKERS,
        logger=logger,
    )
    if not sales_paths:
        raise RuntimeError("No usable sales exports were available for the all-store slow mover report.")
    if not catalog_paths:
        raise RuntimeError("No usable catalog exports were available for the all-store slow mover report.")

    sales_raw = _load_sales_exports(paths, selected, logger)
    if not sales_raw:
        raise RuntimeError("Sales exports were archived, but none could be read back from the run folder.")

    catalog_raw = _load_catalog_exports(paths, selected, logger)
    catalog_all = prepare_catalog_for_all_brands(catalog_raw, selected)
    if catalog_all.empty:
        raise RuntimeError("Catalog files were available, but no inventory rows could be prepared for the report.")

    brand_display_map = build_brand_display_map(catalog_all)
    catalog_merge_maps = build_catalog_merge_maps(catalog_all)

    sales_frames: List[pd.DataFrame] = []
    for abbr in selected:
        raw_df = sales_raw.get(abbr)
        if raw_df is None or raw_df.empty:
            continue
        prepared = _prepare_sales_df_all_brands(
            raw_df,
            store_code=abbr,
            logger=logger,
            catalog_merge_maps=catalog_merge_maps,
            brand_display_map=brand_display_map,
        )
        if prepared.empty:
            _log(f"[WARN] No usable sales rows found for {abbr}.", logger)
            continue
        sales_frames.append(prepared)

    sales_all = pd.concat(sales_frames, ignore_index=True) if sales_frames else pd.DataFrame()
    report_sales = _date_filter(sales_all, start_day, end_day)
    if sales_all.empty:
        _log("[WARN] No sales rows were prepared; the report will show inventory-heavy cut candidates only.", logger)

    store_product_all, store_product_candidates, store_category_candidates, store_brand_all, store_summary = build_store_level_assortment_views(
        catalog_all,
        report_sales,
        report_days,
        end_day,
    )
    if store_product_all.empty and store_brand_all.empty:
        raise RuntimeError("The report did not produce any inventory or sales rows to write.")
    store_brand_summary = store_brand_all[
        store_brand_all["report_include"].fillna(False).astype(bool)
    ].copy() if not store_brand_all.empty and "report_include" in store_brand_all.columns else pd.DataFrame(columns=store_brand_all.columns)
    store_brand_summary = store_brand_summary.sort_values(
        ["_store_abbr", "cut_inventory_value", "actionable_inventory_value", "total_inventory_value"],
        ascending=[True, False, False, False],
    )

    cut_actions = pd.Series(dtype=bool)
    review_actions = pd.Series(dtype=bool)
    if not store_product_candidates.empty and "action" in store_product_candidates.columns:
        action_series = store_product_candidates["action"].fillna("").astype(str)
        cut_actions = action_series.str.startswith("Cut")
        review_actions = action_series.str.startswith("Review")
    cut_sku_count = int(cut_actions.sum()) if len(cut_actions) else 0
    review_sku_count = int(review_actions.sum()) if len(review_actions) else 0
    no_sales_cut_skus = 0
    if not store_product_candidates.empty:
        no_sales_cut_skus = int((
            cut_actions
            & (pd.to_numeric(store_product_candidates["units_sold_window"], errors="coerce").fillna(0.0) <= 0)
        ).sum()) if len(cut_actions) else 0
    healthy_trim_brands = int((
        store_brand_summary["report_status"].fillna("").astype(str) == "Healthy brand, trim a few SKUs"
    ).sum()) if not store_brand_summary.empty and "report_status" in store_brand_summary.columns else 0
    inventory_at_risk = float(pd.to_numeric(store_brand_summary["actionable_inventory_value"], errors="coerce").fillna(0.0).sum()) if not store_brand_summary.empty else 0.0
    cut_inventory_value = float(pd.to_numeric(store_brand_summary["cut_inventory_value"], errors="coerce").fillna(0.0).sum()) if not store_brand_summary.empty else 0.0

    summary_rows = [
        {"metric": "Generated At", "value": datetime.now(ZoneInfo(REPORT_TZ)).isoformat(timespec="seconds")},
        {"metric": "Report Start", "value": start_day.isoformat()},
        {"metric": "Report End", "value": end_day.isoformat()},
        {"metric": "Window Days", "value": report_days},
        {"metric": "Stores Included", "value": ", ".join(selected)},
        {"metric": "Sales Files Loaded", "value": len(sales_raw)},
        {"metric": "Catalog Files Loaded", "value": len(catalog_paths)},
        {"metric": "Missing Sales Stores", "value": ", ".join(missing_sales_stores) if missing_sales_stores else "None"},
        {"metric": "Missing Catalog Stores", "value": ", ".join(missing_catalog_stores) if missing_catalog_stores else "None"},
        {"metric": "Stores With Recommendations", "value": int(len(store_summary))},
        {"metric": "Store-Brand Rows", "value": int(len(store_brand_summary))},
        {"metric": "Category Rows", "value": int(len(store_category_candidates))},
        {"metric": "SKU Candidates", "value": int(len(store_product_candidates))},
        {"metric": "SKUs To Cut", "value": cut_sku_count},
        {"metric": "SKUs To Review", "value": review_sku_count},
        {"metric": "No-Sales Cut SKUs", "value": no_sales_cut_skus},
        {"metric": "Healthy Brands With Trims", "value": healthy_trim_brands},
        {"metric": "Inventory At Risk", "value": inventory_at_risk},
        {"metric": "Cut Inventory Value", "value": cut_inventory_value},
        {"metric": "Review DOS Threshold", "value": thresholds["review_days_of_supply"]},
        {"metric": "Cut DOS Threshold", "value": thresholds["cut_days_of_supply"]},
        {"metric": "Low Sell-Through Threshold", "value": thresholds["low_sell_through"]},
        {"metric": "Report Logic", "value": "Store-by-store SKU rationalization"},
    ]
    summary_df = pd.DataFrame(summary_rows)

    product_all_csv = paths.cache_dir / "all_store_store_product_all.csv"
    product_candidates_csv = paths.cache_dir / "all_store_store_product_candidates.csv"
    category_candidates_csv = paths.cache_dir / "all_store_store_category_candidates.csv"
    brand_all_csv = paths.cache_dir / "all_store_store_brand_all.csv"
    brand_summary_csv = paths.cache_dir / "all_store_store_brand_summary.csv"
    store_summary_csv = paths.cache_dir / "all_store_store_summary.csv"
    store_product_all.to_csv(product_all_csv, index=False)
    store_product_candidates.to_csv(product_candidates_csv, index=False)
    store_category_candidates.to_csv(category_candidates_csv, index=False)
    store_brand_all.to_csv(brand_all_csv, index=False)
    store_brand_summary.to_csv(brand_summary_csv, index=False)
    store_summary.to_csv(store_summary_csv, index=False)

    out_xlsx = paths.pdf_dir / safe_filename(
        f"Store SKU Cuts - {start_day.isoformat()}_to_{end_day.isoformat()}.xlsx"
    )
    build_all_store_assortment_report_xlsx(
        out_xlsx=out_xlsx,
        summary_df=summary_df,
        store_summary=store_summary,
        store_brand_summary=store_brand_summary,
        store_category_candidates=store_category_candidates,
        store_product_candidates=store_product_candidates,
        store_product_all=store_product_all,
    )
    _log(f"[XLSX] Created: {out_xlsx}", logger)
    out_pdf = paths.pdf_dir / safe_filename(
        f"Store SKU Cuts - {start_day.isoformat()}_to_{end_day.isoformat()}.pdf"
    )
    build_all_store_slow_mover_pdf(
        out_pdf=out_pdf,
        start_day=start_day,
        end_day=end_day,
        selected_store_codes=selected,
        report_days=report_days,
        store_summary=store_summary,
        store_brand_summary=store_brand_summary,
        store_category_candidates=store_category_candidates,
        store_product_candidates=store_product_candidates,
        thresholds=thresholds,
    )
    _log(f"[PDF] Created (Store SKU Cuts): {out_pdf}", logger)

    if email_results:
        send_all_store_slow_mover_email(
            attachments=[out_pdf, out_xlsx],
            start_day=start_day,
            end_day=end_day,
            store_summary=store_summary,
            store_brand_summary=store_brand_summary,
            store_product_candidates=store_product_candidates,
            to_email=DEFAULT_REPORT_EMAIL,
            logger=logger,
        )

    return AllStoreSlowMoverArtifacts(
        pdf_path=out_pdf,
        xlsx_path=out_xlsx,
        cache_dir=paths.cache_dir,
        category_candidates=int(len(store_category_candidates)),
        product_candidates=int(len(store_product_candidates)),
    )


# ---------------------------------------------------------------------------
# Owner top-brands rollup report
# ---------------------------------------------------------------------------
def _owner_brand_display_map(catalog_all_df: pd.DataFrame, sales_all_df: pd.DataFrame) -> Dict[str, str]:
    out = build_brand_display_map(catalog_all_df)
    if sales_all_df is not None and not sales_all_df.empty and "brand_key" in sales_all_df.columns:
        tmp = sales_all_df.copy()
        if "brand_name" not in tmp.columns:
            tmp["brand_name"] = ""
        tmp["brand_key"] = tmp["brand_key"].fillna("unknown").astype(str)
        tmp["brand_name"] = tmp["brand_name"].fillna("").astype(str)
        for brand_key, part in tmp.groupby("brand_key"):
            names = [
                str(name).strip()
                for name in part["brand_name"].tolist()
                if str(name).strip() and str(name).strip().lower() != "unknown"
            ]
            if names:
                out.setdefault(str(brand_key), Counter(names).most_common(1)[0][0])
    return out


def _owner_top_label_map(
    df: pd.DataFrame,
    brand_col: str,
    label_col: str,
    value_col: str = "_net",
    default: str = "n/a",
) -> Dict[str, str]:
    if df is None or df.empty or brand_col not in df.columns or label_col not in df.columns or value_col not in df.columns:
        return {}
    tmp = df.copy()
    tmp[brand_col] = tmp[brand_col].fillna("").astype(str)
    tmp[label_col] = tmp[label_col].fillna("").astype(str)
    tmp[value_col] = pd.to_numeric(tmp[value_col], errors="coerce").fillna(0.0)
    grouped = tmp.groupby([brand_col, label_col], as_index=False).agg(value=(value_col, "sum"))
    grouped = grouped.sort_values([brand_col, "value"], ascending=[True, False])
    out: Dict[str, str] = {}
    for brand_key, part in grouped.groupby(brand_col):
        label = str(part.iloc[0][label_col]).strip()
        out[str(brand_key)] = label or default
    return out


def _owner_inventory_by_brand(catalog_all_df: pd.DataFrame) -> pd.DataFrame:
    cols = [
        "brand_key", "brand_name", "units_available", "inventory_value", "potential_revenue",
        "potential_profit", "avg_inventory_margin",
    ]
    if catalog_all_df is None or catalog_all_df.empty or "brand_key" not in catalog_all_df.columns:
        return pd.DataFrame(columns=cols)

    tmp = catalog_all_df.copy()
    if tmp.empty:
        return pd.DataFrame(columns=cols)
    if "brand_name" not in tmp.columns:
        tmp["brand_name"] = "Unknown"
    for col in ["Available", "Inventory_Value", "Potential_Revenue", "Potential_Profit", "Effective_Price"]:
        if col not in tmp.columns:
            tmp[col] = 0.0
        tmp[col] = pd.to_numeric(tmp[col], errors="coerce").fillna(0.0).astype(float)
    tmp = tmp[tmp["Available"] > 0].copy()
    if tmp.empty:
        return pd.DataFrame(columns=cols)
    tmp["effective_total"] = tmp["Effective_Price"] * tmp["Available"]
    grouped = tmp.groupby("brand_key", as_index=False).agg(
        brand_name=("brand_name", lambda s: _first_mode(s, "Unknown")),
        units_available=("Available", "sum"),
        inventory_value=("Inventory_Value", "sum"),
        potential_revenue=("Potential_Revenue", "sum"),
        potential_profit=("Potential_Profit", "sum"),
        effective_total=("effective_total", "sum"),
    )
    grouped["avg_inventory_margin"] = grouped["potential_profit"] / grouped["effective_total"].replace({0: np.nan})
    grouped = grouped.replace([np.inf, -np.inf], np.nan).fillna(0.0)
    return grouped[[c for c in cols if c in grouped.columns]].copy()


def build_owner_brand_rollup_scorecard(
    sales_all_df: pd.DataFrame,
    catalog_all_df: pd.DataFrame,
    start_day: date,
    end_day: date,
    top_n: int = 20,
    target_margin: Optional[float] = None,
    targets_payload: Optional[Dict[str, Any]] = None,
    credit_rows: Optional[Sequence[Dict[str, Any]]] = None,
    include_prior_data: bool = True,
) -> pd.DataFrame:
    cols = [
        "rank", "brand_key", "brand_name", "net_revenue", "sales_share_pct", "sales_vs_prior_pct",
        "prior_net_revenue", "units", "units_vs_prior_pct", "prior_units", "margin_real",
        "target_margin", "margin_gap_pp", "discount_rate", "inventory_value", "units_available",
        "days_supply", "sell_through_pct", "credit_gap", "credit_gap_pct_sales", "status",
        "recommended_action", "top_store", "top_category", "top_product", "credit_rows",
    ]
    if sales_all_df is None or sales_all_df.empty or "brand_key" not in sales_all_df.columns:
        return pd.DataFrame(columns=cols)

    targets_payload = targets_payload or load_targets(THIS_DIR / "brand_meeting_targets.json")
    report_days = max(window_days(start_day, end_day), 1)
    report_df = _date_filter(sales_all_df, start_day, end_day)
    if include_prior_data:
        prior_start, prior_end = compute_prior_report_window(start_day, end_day)
        prior_df = _date_filter(sales_all_df, prior_start, prior_end)
    else:
        prior_df = pd.DataFrame(columns=sales_all_df.columns)
    if report_df.empty:
        return pd.DataFrame(columns=cols)

    current = summarize_group(report_df, "brand_key")
    if current.empty:
        return pd.DataFrame(columns=cols)

    display_map = _owner_brand_display_map(catalog_all_df, sales_all_df)
    current["brand_key"] = current["brand_key"].fillna("unknown").astype(str)
    current["brand_name"] = current["brand_key"].map(display_map).fillna("Unknown")

    prior = summarize_group(prior_df, "brand_key") if prior_df is not None and not prior_df.empty else pd.DataFrame()
    if not prior.empty:
        prior = prior[["brand_key", "net_revenue", "items"]].rename(
            columns={"net_revenue": "prior_net_revenue", "items": "prior_units"}
        )
        prior["brand_key"] = prior["brand_key"].fillna("unknown").astype(str)
        current = current.merge(prior, on="brand_key", how="left")
    else:
        current["prior_net_revenue"] = 0.0
        current["prior_units"] = 0.0

    inv = _owner_inventory_by_brand(catalog_all_df)
    if not inv.empty:
        current = current.merge(
            inv[["brand_key", "units_available", "inventory_value"]],
            on="brand_key",
            how="left",
        )
    else:
        current["units_available"] = 0.0
        current["inventory_value"] = 0.0

    for col in ["prior_net_revenue", "prior_units"]:
        numeric = pd.to_numeric(current.get(col, 0.0), errors="coerce").astype(float)
        current[col] = numeric.fillna(0.0) if include_prior_data else float("nan")
    for col in ["units_available", "inventory_value"]:
        current[col] = pd.to_numeric(current.get(col, 0.0), errors="coerce").fillna(0.0).astype(float)

    total_net_sales = float(current["net_revenue"].sum())
    current = current.sort_values("net_revenue", ascending=False).head(max(1, int(top_n))).copy()

    top_store_map = _owner_top_label_map(report_df, "brand_key", "_store_abbr")
    top_category_map = _owner_top_label_map(report_df, "brand_key", "category_normalized")
    top_product_col = "product_group_display" if "product_group_display" in report_df.columns else "_product_raw"
    top_product_map = _owner_top_label_map(report_df, "brand_key", top_product_col)

    rows: List[Dict[str, Any]] = []
    all_credit_rows = list(credit_rows or [])
    for rank, item in enumerate(current.itertuples(index=False), start=1):
        row = item._asdict()
        brand_key = str(row.get("brand_key") or "unknown")
        brand_name = str(row.get("brand_name") or display_map.get(brand_key) or "Unknown")
        net = float(row.get("net_revenue", 0.0) or 0.0)
        units = float(row.get("items", 0.0) or 0.0)
        prior_net = float(row.get("prior_net_revenue", 0.0) or 0.0)
        prior_units = float(row.get("prior_units", 0.0) or 0.0)
        units_available = float(row.get("units_available", 0.0) or 0.0)
        margin_real = float(row.get("margin_real", 0.0) or 0.0)
        discount_rate = float(row.get("discount_rate", 0.0) or 0.0)
        brand_targets = get_brand_targets(targets_payload, brand_name)
        effective_target = float(target_margin if target_margin is not None else brand_targets.get("target_margin", DEAL_TARGET_MARGIN))
        brand_targets["target_margin"] = effective_target

        brand_sales_df = report_df[report_df["brand_key"].fillna("").astype(str) == brand_key].copy()
        credit_summary, _credit_reconciliation = summarize_credit_reconciliation(
            all_credit_rows,
            brand_sales_df,
            brand=brand_name,
            start_day=start_day,
            end_day=end_day,
            target_margin=effective_target,
            system_expected_credit=0.0,
        )
        credit_gap = float(credit_summary.get("credit_gap", 0.0) or 0.0)

        units_per_day = safe_div(units, report_days, 0.0)
        out = {
            "rank": rank,
            "brand_key": brand_key,
            "brand_name": brand_name,
            "net_revenue": net,
            "sales_share_pct": safe_div(net, total_net_sales, 0.0),
            "sales_vs_prior_pct": pct_change(net, prior_net),
            "prior_net_revenue": prior_net,
            "units": units,
            "units_vs_prior_pct": pct_change(units, prior_units),
            "prior_units": prior_units,
            "margin_real": margin_real,
            "target_margin": effective_target,
            "margin_gap_pp": pp_change(margin_real, effective_target),
            "discount_rate": discount_rate,
            "inventory_value": float(row.get("inventory_value", 0.0) or 0.0),
            "units_available": units_available,
            "days_supply": safe_div(units_available, units_per_day, float("nan")),
            "sell_through_pct": safe_div(units, units + units_available, 0.0),
            "credit_gap": credit_gap,
            "credit_gap_pct_sales": safe_div(credit_gap, net, 0.0),
            "top_store": top_store_map.get(brand_key, "n/a"),
            "top_category": top_category_map.get(brand_key, "n/a"),
            "top_product": top_product_map.get(brand_key, "n/a"),
            "credit_rows": int(credit_summary.get("credit_rows", 0) or 0),
        }
        status, action = owner_brand_status_action(out, brand_targets)
        out["status"] = status
        out["recommended_action"] = action
        rows.append(out)

    return pd.DataFrame(rows, columns=cols)


def _owner_summary_from_scorecard(
    scorecard: pd.DataFrame,
    report_df: pd.DataFrame,
    catalog_all_df: pd.DataFrame,
    start_day: date,
    end_day: date,
    selected_store_codes: Sequence[str],
    missing_sales_stores: Sequence[str],
    missing_catalog_stores: Sequence[str],
) -> Dict[str, Any]:
    report_metrics = summarize_metrics(report_df)
    inv_brand = _owner_inventory_by_brand(catalog_all_df)
    total_units = float(report_metrics.get("items", 0.0) or 0.0)
    report_days = max(window_days(start_day, end_day), 1)
    inventory_value = float(pd.to_numeric(inv_brand.get("inventory_value", pd.Series(dtype=float)), errors="coerce").fillna(0.0).sum()) if not inv_brand.empty else 0.0
    inv_units = float(pd.to_numeric(inv_brand.get("units_available", pd.Series(dtype=float)), errors="coerce").fillna(0.0).sum()) if not inv_brand.empty else 0.0
    days_supply = safe_div(inv_units, safe_div(total_units, report_days, 0.0), float("nan"))

    reviewed = scorecard.copy() if scorecard is not None else pd.DataFrame()
    credit_gap = float(pd.to_numeric(reviewed.get("credit_gap", pd.Series(dtype=float)), errors="coerce").fillna(0.0).sum()) if not reviewed.empty else 0.0
    return {
        "generated_at": datetime.now(ZoneInfo(REPORT_TZ)).isoformat(timespec="seconds"),
        "start_date": start_day.isoformat(),
        "end_date": end_day.isoformat(),
        "stores": ", ".join(selected_store_codes),
        "total_net_sales": float(report_metrics.get("net_revenue", 0.0) or 0.0),
        "total_units_sold": total_units,
        "average_real_margin": float(report_metrics.get("margin_real", 0.0) or 0.0),
        "average_discount_rate": float(report_metrics.get("discount_rate", 0.0) or 0.0),
        "inventory_value": inventory_value,
        "days_supply": days_supply,
        "total_credit_gap": credit_gap,
        "brands_reviewed": int(len(reviewed)),
        "missing_sales_stores": ", ".join(missing_sales_stores) if missing_sales_stores else "None",
        "missing_catalog_stores": ", ".join(missing_catalog_stores) if missing_catalog_stores else "None",
    }


def _owner_short_text(value: Any, max_chars: int = 32) -> str:
    text = re.sub(r"\s+", " ", str(value or "")).strip()
    if len(text) <= max_chars:
        return text
    cut = text[: max_chars - 3].rsplit(" ", 1)[0].strip()
    return (cut or text[: max_chars - 3]).rstrip(" |-/") + "..."


def _owner_pdf_styles() -> Dict[str, ParagraphStyle]:
    base = getattr(osnap, "BASE_FONT", "Helvetica")
    bold = getattr(osnap, "BOLD_FONT", "Helvetica-Bold")
    return {
        "title": ParagraphStyle("OwnerTitle", fontName=bold, fontSize=20, leading=23, textColor=colors.HexColor("#111827"), spaceAfter=3),
        "subtitle": ParagraphStyle("OwnerSubtitle", fontName=base, fontSize=8.4, leading=10.2, textColor=colors.HexColor("#4B5563")),
        "section": ParagraphStyle("OwnerSection", fontName=bold, fontSize=12.5, leading=15, textColor=colors.HexColor("#111827"), spaceBefore=6, spaceAfter=4),
        "small": ParagraphStyle("OwnerSmall", fontName=base, fontSize=7.0, leading=8.4, textColor=colors.HexColor("#4B5563"), splitLongWords=0),
        "tiny": ParagraphStyle("OwnerTiny", fontName=base, fontSize=5.7, leading=6.7, textColor=colors.HexColor("#111827"), splitLongWords=0),
        "tiny_center": ParagraphStyle("OwnerTinyCenter", fontName=base, fontSize=5.7, leading=6.7, textColor=colors.HexColor("#111827"), alignment=TA_CENTER, splitLongWords=0),
        "tiny_right": ParagraphStyle("OwnerTinyRight", fontName=base, fontSize=5.7, leading=6.7, textColor=colors.HexColor("#111827"), alignment=TA_RIGHT, splitLongWords=0),
        "head": ParagraphStyle("OwnerHead", fontName=bold, fontSize=5.8, leading=6.8, textColor=colors.white, alignment=TA_CENTER, splitLongWords=0),
    }


def _owner_p(text: Any, style: ParagraphStyle) -> Paragraph:
    return Paragraph(xml_escape(str(text or "")), style)


def _owner_scorecard_table(scorecard: pd.DataFrame) -> Table:
    styles = _owner_pdf_styles()
    headers = [
        "#", "Brand", "Sales", "Share", "Sales +/-", "Units", "Unit +/-", "Margin", "Target",
        "Gap", "Disc", "Inv $", "DOS", "Sell", "Credit", "Cred %", "Status", "Action",
    ]
    rows: List[List[Any]] = [headers]
    if scorecard is not None and not scorecard.empty:
        for _, r in scorecard.iterrows():
            rows.append([
                int(r.get("rank", 0) or 0),
                _owner_short_text(r.get("brand_name", ""), 24),
                money0(r.get("net_revenue", 0.0)),
                pct1(r.get("sales_share_pct", 0.0)),
                _owner_pct_change_label(r.get("sales_vs_prior_pct"), r.get("net_revenue", 0.0), r.get("prior_net_revenue", 0.0)),
                int0(r.get("units", 0.0)),
                _owner_pct_change_label(r.get("units_vs_prior_pct"), r.get("units", 0.0), r.get("prior_units", 0.0)),
                pct1(r.get("margin_real", 0.0)),
                pct1(r.get("target_margin", 0.0)),
                _owner_margin_gap_label(r.get("margin_gap_pp", 0.0)),
                pct1(r.get("discount_rate", 0.0)),
                money0(r.get("inventory_value", 0.0)),
                days1(r.get("days_supply", np.nan)),
                pct1(r.get("sell_through_pct", 0.0)),
                money0(r.get("credit_gap", 0.0)),
                pct1(r.get("credit_gap_pct_sales", 0.0)),
                str(r.get("status", "")),
                str(r.get("recommended_action", "")),
            ])

    right_cols = {2, 3, 4, 5, 6, 7, 8, 9, 10, 11, 12, 13, 14, 15}
    center_cols = {0, 16}
    wrapped: List[List[Any]] = []
    for ridx, row in enumerate(rows):
        out_row: List[Any] = []
        for cidx, value in enumerate(row):
            if ridx == 0:
                out_row.append(_owner_p(value, styles["head"]))
            elif cidx in right_cols:
                out_row.append(_owner_p(value, styles["tiny_right"]))
            elif cidx in center_cols:
                out_row.append(_owner_p(value, styles["tiny_center"]))
            else:
                out_row.append(_owner_p(value, styles["tiny"]))
        wrapped.append(out_row)

    col_widths = [
        0.26 * inch, 1.00 * inch, 0.58 * inch, 0.42 * inch, 0.52 * inch, 0.42 * inch,
        0.52 * inch, 0.43 * inch, 0.43 * inch, 0.43 * inch, 0.42 * inch, 0.56 * inch,
        0.42 * inch, 0.42 * inch, 0.55 * inch, 0.44 * inch, 0.58 * inch, 0.83 * inch,
    ]
    table = Table(wrapped, colWidths=col_widths, repeatRows=1, hAlign="LEFT", splitByRow=1)
    style_cmds: List[Tuple[Any, ...]] = [
        ("BACKGROUND", (0, 0), (-1, 0), colors.HexColor("#111827")),
        ("TEXTCOLOR", (0, 0), (-1, 0), colors.white),
        ("GRID", (0, 0), (-1, -1), 0.25, colors.HexColor("#D7DEE0")),
        ("VALIGN", (0, 0), (-1, -1), "MIDDLE"),
        ("LEFTPADDING", (0, 0), (-1, -1), 2),
        ("RIGHTPADDING", (0, 0), (-1, -1), 2),
        ("TOPPADDING", (0, 0), (-1, -1), 2),
        ("BOTTOMPADDING", (0, 0), (-1, -1), 2),
        ("ROWBACKGROUNDS", (0, 1), (-1, -1), [colors.white, colors.HexColor("#F8FAFC")]),
    ]
    status_color = {
        "Grow": "#DFF3E8",
        "Good": "#EEF7EC",
        "Watch": "#FFF4D6",
        "Fix": "#FCE4E1",
        "Exit / Reduce": "#F6D5D1",
    }
    for ridx, row in enumerate(rows[1:], start=1):
        color = status_color.get(str(row[16]), "")
        if color:
            style_cmds.append(("BACKGROUND", (16, ridx), (17, ridx), colors.HexColor(color)))
    table.setStyle(TableStyle(style_cmds))
    return table


def _owner_panel_table(title: str, rows: Sequence[str], width: float = 3.15 * inch) -> Table:
    styles = _owner_pdf_styles()
    data = [[_owner_p(title, styles["head"])]] + [[_owner_p(row, styles["small"])] for row in rows]
    table = Table(data, colWidths=[width], hAlign="LEFT")
    table.setStyle(TableStyle([
        ("BACKGROUND", (0, 0), (-1, 0), colors.HexColor("#2F6B5D")),
        ("BACKGROUND", (0, 1), (-1, -1), colors.HexColor("#F6F8F7")),
        ("BOX", (0, 0), (-1, -1), 0.45, colors.HexColor("#D7DEE0")),
        ("LEFTPADDING", (0, 0), (-1, -1), 5),
        ("RIGHTPADDING", (0, 0), (-1, -1), 5),
        ("TOPPADDING", (0, 0), (-1, -1), 3),
        ("BOTTOMPADDING", (0, 0), (-1, -1), 3),
        ("VALIGN", (0, 0), (-1, -1), "TOP"),
    ]))
    return table


def _owner_panel_grid(panels: Sequence[Table], cols: int = 3) -> Table:
    rows: List[List[Any]] = []
    for idx in range(0, len(panels), cols):
        row = list(panels[idx:idx + cols])
        while len(row) < cols:
            row.append("")
        rows.append(row)
    table = Table(rows, colWidths=[3.25 * inch] * cols, hAlign="LEFT")
    table.setStyle(TableStyle([
        ("VALIGN", (0, 0), (-1, -1), "TOP"),
        ("LEFTPADDING", (0, 0), (-1, -1), 3),
        ("RIGHTPADDING", (0, 0), (-1, -1), 3),
        ("TOPPADDING", (0, 0), (-1, -1), 3),
        ("BOTTOMPADDING", (0, 0), (-1, -1), 3),
    ]))
    return table


def _owner_rank_lines(df: pd.DataFrame, value_col: str, formatter, top_n: int = 5, ascending: bool = False) -> List[str]:
    if df is None or df.empty or value_col not in df.columns:
        return ["No data"]
    tmp = df.copy()
    tmp[value_col] = pd.to_numeric(tmp[value_col], errors="coerce")
    tmp = tmp[tmp[value_col].notna()].sort_values(value_col, ascending=ascending).head(top_n)
    if tmp.empty:
        return ["No data"]
    lines: List[str] = []
    for idx, (_, r) in enumerate(tmp.iterrows(), start=1):
        lines.append(f"{idx}. {_owner_short_text(r.get('brand_name', ''), 24)} - {formatter(r.get(value_col, 0.0))}")
    return lines


def _owner_brand_card(row: pd.Series, width: float = 4.95 * inch) -> Table:
    lines = [
        f"Sales {money0(row.get('net_revenue', 0.0))} | Share {pct1(row.get('sales_share_pct', 0.0))}",
        f"Sales vs prior {_owner_pct_change_label(row.get('sales_vs_prior_pct'), row.get('net_revenue', 0.0), row.get('prior_net_revenue', 0.0))} | Units vs prior {_owner_pct_change_label(row.get('units_vs_prior_pct'), row.get('units', 0.0), row.get('prior_units', 0.0))}",
        f"Margin {pct1(row.get('margin_real', 0.0))} | Discount {pct1(row.get('discount_rate', 0.0))}",
        f"DOS {days1(row.get('days_supply', np.nan))} | Credit gap {money0(row.get('credit_gap', 0.0))}",
        f"Top store {row.get('top_store', 'n/a')} | Category {_owner_short_text(row.get('top_category', 'n/a'), 20)}",
        f"Top item {_owner_short_text(row.get('top_product', 'n/a'), 38)}",
        f"Main issue {row.get('status', '')}: {row.get('recommended_action', '')}",
    ]
    return _owner_panel_table(f"{int(row.get('rank', 0) or 0)}. {_owner_short_text(row.get('brand_name', ''), 32)}", lines, width=width)


def build_owner_brand_rollup_pdf(
    out_pdf: Path,
    start_day: date,
    end_day: date,
    selected_store_codes: Sequence[str],
    scorecard: pd.DataFrame,
    summary: Dict[str, Any],
    include_brand_cards: bool = True,
    compact: bool = True,
) -> None:
    osnap.setup_fonts()
    styles = _owner_pdf_styles()
    generated_at = datetime.now(ZoneInfo(REPORT_TZ)).strftime("%B %d, %Y at %I:%M %p %Z")
    out_pdf.parent.mkdir(parents=True, exist_ok=True)
    doc = SimpleDocTemplate(
        str(out_pdf),
        pagesize=landscape(letter),
        leftMargin=0.28 * inch,
        rightMargin=0.28 * inch,
        topMargin=0.30 * inch,
        bottomMargin=0.30 * inch,
        pageCompression=1,
        title=f"Owner Top Brands Review - {start_day.isoformat()} to {end_day.isoformat()}",
        author="Buzz Automation",
    )

    story: List[Any] = []
    story.append(_owner_p("Owner Top Brands Review", styles["title"]))
    story.append(_owner_p(
        f"{start_day.isoformat()} to {end_day.isoformat()} | Stores: {', '.join(selected_store_codes)} | Generated: {generated_at}",
        styles["subtitle"],
    ))
    story.append(Spacer(1, 0.08 * inch))

    kpi_cards = [
        _metric_card("Total Net Sales", money0(summary.get("total_net_sales", 0.0)), "all brands"),
        _metric_card("Units Sold", int0(summary.get("total_units_sold", 0.0)), "all brands"),
        _metric_card("Avg Real Margin", pct1(summary.get("average_real_margin", 0.0)), "sales margin"),
        _metric_card("Avg Discount", pct1(summary.get("average_discount_rate", 0.0)), "sales discount"),
        _metric_card("Inventory Value", money0(summary.get("inventory_value", 0.0)), "current on hand"),
        _metric_card("Days Supply", days1(summary.get("days_supply", np.nan)), "all inventory"),
        _metric_card("Credit Gap", money0(summary.get("total_credit_gap", 0.0)), "reviewed brands"),
        _metric_card("Brands Reviewed", int0(summary.get("brands_reviewed", 0)), "top brands"),
    ]
    story.append(_metric_grid(kpi_cards, cols=4))
    story.append(Spacer(1, 0.08 * inch))

    panels = [
        _owner_panel_table("Top 5 Brands By Sales", _owner_rank_lines(scorecard, "net_revenue", money0)),
        _owner_panel_table("Fastest Growers", _owner_rank_lines(scorecard, "sales_vs_prior_pct", pct1)),
        _owner_panel_table("Sales Decliners", _owner_rank_lines(scorecard, "sales_vs_prior_pct", pct1, ascending=True)),
        _owner_panel_table("Biggest Margin Risks", _owner_rank_lines(scorecard, "margin_gap_pp", _owner_margin_gap_label, ascending=True)),
        _owner_panel_table("Biggest Inventory Risks", _owner_rank_lines(scorecard, "days_supply", days1)),
        _owner_panel_table("Biggest Credit Gaps", _owner_rank_lines(scorecard, "credit_gap", money0)),
    ]
    story.append(_owner_panel_grid(panels, cols=3))
    if summary.get("missing_sales_stores") != "None" or summary.get("missing_catalog_stores") != "None":
        story.append(Spacer(1, 0.05 * inch))
        story.append(_owner_p(
            f"Missing sales stores: {summary.get('missing_sales_stores')} | Missing catalog stores: {summary.get('missing_catalog_stores')}",
            styles["small"],
        ))

    story.append(PageBreak())
    story.append(_owner_p("Top Brand Scorecard", styles["title"]))
    story.append(_owner_p("Ranked by net sales. Share is calculated against total brand sales in the selected stores/window.", styles["subtitle"]))
    story.append(Spacer(1, 0.06 * inch))
    story.append(_owner_scorecard_table(scorecard))

    if include_brand_cards and scorecard is not None and not scorecard.empty:
        cards_per_page = 4 if compact else 2
        card_cols = 2 if compact else 1
        card_width = 4.95 * inch if compact else 10.10 * inch
        table_col_widths = [5.10 * inch, 5.10 * inch] if compact else [10.35 * inch]
        cards = [_owner_brand_card(row, width=card_width) for _, row in scorecard.iterrows()]
        for idx in range(0, len(cards), cards_per_page):
            story.append(PageBreak())
            story.append(_owner_p("Compact Brand Cards", styles["title"]))
            story.append(_owner_p("One-card summary for fast owner review. No product appendix is included in this owner report.", styles["subtitle"]))
            chunk = cards[idx:idx + cards_per_page]
            card_rows: List[List[Any]] = []
            for cidx in range(0, len(chunk), card_cols):
                row = chunk[cidx:cidx + card_cols]
                while len(row) < card_cols:
                    row.append("")
                card_rows.append(row)
            card_table = Table(card_rows, colWidths=table_col_widths, hAlign="LEFT")
            card_table.setStyle(TableStyle([
                ("VALIGN", (0, 0), (-1, -1), "TOP"),
                ("LEFTPADDING", (0, 0), (-1, -1), 4),
                ("RIGHTPADDING", (0, 0), (-1, -1), 4),
                ("TOPPADDING", (0, 0), (-1, -1), 4),
                ("BOTTOMPADDING", (0, 0), (-1, -1), 4),
            ]))
            story.append(card_table)

    footer = _footer("Owner Top Brands Review", end_day)
    doc.build(story, onFirstPage=footer, onLaterPages=footer)


def send_owner_brand_rollup_email(
    pdf_path: Path,
    start_day: date,
    end_day: date,
    summary: Dict[str, Any],
    to_email: str = DEFAULT_REPORT_EMAIL,
    logger: Optional[Callable[[str], None]] = None,
) -> None:
    service = _build_gmail_service()
    subject = f"Owner Top Brands Review - {start_day.isoformat()} to {end_day.isoformat()}"
    msg = EmailMessage()
    msg["To"] = to_email
    msg["Subject"] = subject
    msg.set_content(
        "\n".join([
            "Owner Top Brands Review is attached.",
            "",
            f"Window: {start_day.isoformat()} to {end_day.isoformat()}",
            f"Total net sales: {money0(summary.get('total_net_sales', 0.0))}",
            f"Brands reviewed: {int0(summary.get('brands_reviewed', 0))}",
            f"Credit gap: {money0(summary.get('total_credit_gap', 0.0))}",
        ])
    )
    _attach_file_to_email(msg, pdf_path)
    raw = base64.urlsafe_b64encode(msg.as_bytes()).decode()
    service.users().messages().send(userId="me", body={"raw": raw}).execute()
    _log(f"[EMAIL] Sent owner top brands review to {to_email}", logger)


def generate_owner_brand_rollup_packet(
    *,
    start_date: date,
    end_date: date,
    stores: Sequence[str],
    top_n: int = 20,
    use_api: bool = False,
    run_export: bool = False,
    no_export: bool = False,
    no_catalog_export: bool = False,
    force_refresh: bool = False,
    include_prior_data: bool = True,
    include_creditflow: bool = False,
    target_margin: Optional[float] = None,
    output_root: Optional[Path] = None,
    email: bool = True,
    compact: bool = True,
    include_brand_cards: bool = True,
    api_env_file: str = DEFAULT_API_ENV_FILE,
    api_workers: int = DEFAULT_PACKET_API_WORKERS,
    credit_ledger_path: str = "brand_credit_ledger.json",
    creditflow_base_url: str = "https://creditflow.replit.app/api/v1",
    logger: Optional[Callable[[str], None]] = None,
) -> OwnerBrandRollupArtifacts:
    selected = order_store_codes(stores or list(store_abbr_map.values()))
    if not selected:
        raise ValueError("At least one store is required.")
    output_root = Path(output_root or DEFAULT_OUTPUT_ROOT).expanduser().resolve()
    paths = build_run_paths(output_root, OWNER_BRAND_ROLLUP_REPORT_NAME, start_date, end_date)
    prior_start, _prior_end = compute_prior_report_window(start_date, end_date)
    acquisition_start = prior_start if include_prior_data else start_date
    allow_export = bool((use_api or run_export) and not no_export)

    _log(
        f"[START] Building owner top brands review for {start_date.isoformat()} -> {end_date.isoformat()}",
        logger,
    )
    if use_api:
        effective_workers = resolve_worker_count(api_workers, len(selected))
        _log(f"[API] Store workers requested={api_workers}, effective={effective_workers}", logger)

    sales_paths, missing_sales_stores, _did_export_sales = prepare_sales_exports(
        paths=paths,
        brand=OWNER_BRAND_ROLLUP_REPORT_NAME,
        selected_store_codes=selected,
        acquisition_start=acquisition_start,
        acquisition_end=end_date,
        allow_export=allow_export,
        force_refresh=force_refresh,
        use_api=use_api,
        api_env_file=api_env_file,
        api_workers=api_workers,
        logger=logger,
    )
    catalog_paths, missing_catalog_stores, _did_export_catalog = prepare_catalog_exports(
        paths,
        selected,
        run_export=bool(not no_catalog_export and allow_export),
        force_refresh=force_refresh,
        use_api=use_api,
        api_env_file=api_env_file,
        api_workers=api_workers,
        logger=logger,
    )
    if not sales_paths:
        raise RuntimeError("No usable sales exports were available for the owner top brands review.")
    if not catalog_paths:
        raise RuntimeError("No usable catalog exports were available for the owner top brands review.")

    sales_raw = _load_sales_exports(paths, selected, logger)
    catalog_raw = _load_catalog_exports(paths, selected, logger)
    catalog_all = prepare_catalog_for_all_brands(catalog_raw, selected)
    if catalog_all.empty:
        raise RuntimeError("Catalog files were available, but no inventory rows could be prepared for the owner report.")

    brand_display_map = build_brand_display_map(catalog_all)
    catalog_merge_maps = build_catalog_merge_maps(catalog_all)
    sales_frames: List[pd.DataFrame] = []
    for abbr in selected:
        raw_df = sales_raw.get(abbr)
        if raw_df is None or raw_df.empty:
            continue
        prepared = _prepare_sales_df_all_brands(
            raw_df,
            store_code=abbr,
            logger=logger,
            catalog_merge_maps=catalog_merge_maps,
            brand_display_map=brand_display_map,
        )
        if prepared.empty:
            _log(f"[WARN] No usable sales rows found for {abbr}.", logger)
            continue
        sales_frames.append(prepared)

    sales_all = pd.concat(sales_frames, ignore_index=True) if sales_frames else pd.DataFrame()
    if sales_all.empty:
        raise RuntimeError("Sales files were loaded, but no all-brand sales rows could be prepared.")

    credit_rows: List[Dict[str, Any]] = []
    ledger_path = Path(credit_ledger_path or "brand_credit_ledger.json")
    if not ledger_path.is_absolute():
        ledger_path = THIS_DIR / ledger_path
    try:
        credit_rows.extend(load_credit_ledger(ledger_path))
    except Exception as exc:
        _log(f"[WARN] Could not load credit ledger for owner rollup: {exc}", logger)

    targets_payload = load_targets(THIS_DIR / "brand_meeting_targets.json")
    initial_scorecard = build_owner_brand_rollup_scorecard(
        sales_all,
        catalog_all,
        start_date,
        end_date,
        top_n=top_n,
        target_margin=target_margin,
        targets_payload=targets_payload,
        credit_rows=credit_rows,
        include_prior_data=include_prior_data,
    )

    if include_creditflow and not initial_scorecard.empty:
        creditflow_rows_all: List[Dict[str, Any]] = []
        aliases_by_brand = {}
        for brand_name in initial_scorecard["brand_name"].fillna("").astype(str).tolist():
            try:
                rows, meta = fetch_creditflow_credits_for_brand(
                    brand=brand_name,
                    start_day=start_date,
                    end_day=end_date,
                    env_file=THIS_DIR / api_env_file,
                    base_url=creditflow_base_url,
                    aliases=aliases_by_brand.get(brand_name, []),
                )
                if meta.get("warning"):
                    _log(f"[CREDITFLOW] {brand_name}: {meta.get('warning')}", logger)
                else:
                    _log(f"[CREDITFLOW] {brand_name}: matched {len(rows)} of {meta.get('raw_credits', 0)} credits.", logger)
                creditflow_rows_all.extend(rows)
            except Exception as exc:
                _log(f"[WARN] CreditFlow pull failed for {brand_name}: {exc}", logger)
        if creditflow_rows_all:
            credit_rows.extend(creditflow_rows_all)
            write_creditflow_cache(paths.cache_dir / "owner_rollup_creditflow_credits_cache.json", creditflow_rows_all, {"brands": len(initial_scorecard)})

    scorecard = build_owner_brand_rollup_scorecard(
        sales_all,
        catalog_all,
        start_date,
        end_date,
        top_n=top_n,
        target_margin=target_margin,
        targets_payload=targets_payload,
        credit_rows=credit_rows,
        include_prior_data=include_prior_data,
    )
    if scorecard.empty:
        raise RuntimeError("The owner top brands review did not produce any scorecard rows.")

    report_df = _date_filter(sales_all, start_date, end_date)
    summary = _owner_summary_from_scorecard(
        scorecard=scorecard,
        report_df=report_df,
        catalog_all_df=catalog_all,
        start_day=start_date,
        end_day=end_date,
        selected_store_codes=selected,
        missing_sales_stores=missing_sales_stores,
        missing_catalog_stores=missing_catalog_stores,
    )

    scorecard_csv = paths.cache_dir / "owner_top_brands_scorecard.csv"
    summary_csv = paths.cache_dir / "owner_top_brands_summary.csv"
    scorecard.to_csv(scorecard_csv, index=False)
    pd.DataFrame([summary]).to_csv(summary_csv, index=False)
    _log(f"[QA] Wrote owner scorecard: {scorecard_csv}", logger)

    out_pdf = paths.pdf_dir / safe_filename(
        f"Owner Top Brands Review - {start_date.isoformat()}_to_{end_date.isoformat()}.pdf"
    )
    build_owner_brand_rollup_pdf(
        out_pdf=out_pdf,
        start_day=start_date,
        end_day=end_date,
        selected_store_codes=selected,
        scorecard=scorecard,
        summary=summary,
        include_brand_cards=include_brand_cards,
        compact=compact,
    )
    _log(f"[PDF] Created (Owner Top Brands Review): {out_pdf}", logger)

    if email:
        send_owner_brand_rollup_email(out_pdf, start_date, end_date, summary, OWNER_BRAND_ROLLUP_EMAIL, logger)

    return OwnerBrandRollupArtifacts(
        pdf_path=out_pdf,
        scorecard_csv_path=scorecard_csv,
        summary_csv_path=summary_csv,
        cache_dir=paths.cache_dir,
        brand_count=int(len(scorecard)),
        missing_sales_stores=list(missing_sales_stores),
        missing_catalog_stores=list(missing_catalog_stores),
    )


# ---------------------------------------------------------------------------
# Charts
# ---------------------------------------------------------------------------
def _mpl_setup() -> None:
    plt.rcParams.update({
        "font.family": "DejaVu Sans",
        "font.size": 8.6,
        "axes.titlesize": 11.0,
        "axes.labelsize": 8.0,
        "axes.edgecolor": "#D1D5DB",
        "axes.linewidth": 0.7,
        "axes.titleweight": "bold",
        "axes.facecolor": "#F9FAFB",
        "figure.facecolor": "#FFFFFF",
        "grid.color": "#E5E7EB",
        "grid.linewidth": 0.7,
        "grid.alpha": 0.9,
        "xtick.color": "#374151",
        "ytick.color": "#374151",
    })


def _save_chart_image(buf: BytesIO, dpi: int = 170, quality: int = 85) -> None:
    try:
        plt.savefig(
            buf,
            format="jpeg",
            dpi=dpi,
            bbox_inches="tight",
            pad_inches=0.14,
            pil_kwargs={
                "quality": quality,
                "optimize": True,
                "progressive": True,
                "subsampling": 0,
            },
        )
    except TypeError:
        plt.savefig(buf, format="jpeg", dpi=dpi, bbox_inches="tight", pad_inches=0.14)
    except Exception:
        buf.seek(0)
        buf.truncate(0)
        plt.savefig(buf, format="png", dpi=dpi, bbox_inches="tight", pad_inches=0.14)


def chart_daily_net(daily_df: pd.DataFrame, title: str) -> BytesIO:
    _mpl_setup()
    buf = BytesIO()
    if daily_df is None or daily_df.empty:
        return buf

    d = daily_df.copy()
    if "date" not in d.columns:
        return buf

    d = d.sort_values("date")
    labels = [f"{x.month}/{x.day}" for x in d["date"].tolist()]
    vals = d["net_revenue"].astype(float).tolist()
    x = np.arange(len(vals))

    fig, ax = plt.subplots(figsize=(7.3, 3.1))
    ax.bar(x, vals, color=osnap.HEX_GREEN, edgecolor="#047857", linewidth=0.4, alpha=0.95, zorder=2)
    if vals:
        ax.plot(x, vals, color="#111827", linewidth=1.15, alpha=0.82, zorder=3)

    ax.set_title(title, pad=18)
    if len(labels) > 20:
        show = [i for i in range(len(labels)) if (i % 2 == 0) or i == len(labels) - 1]
        ax.set_xticks(show)
        ax.set_xticklabels([labels[i] for i in show], rotation=35, ha="right", fontsize=7.1)
    else:
        ax.set_xticks(x)
        ax.set_xticklabels(labels, rotation=35, ha="right", fontsize=7.3)

    ax.grid(True, axis="y", zorder=0)
    ax.spines["top"].set_visible(False)
    ax.spines["right"].set_visible(False)
    fig.subplots_adjust(left=0.06, right=0.992, bottom=0.28, top=0.80)

    _save_chart_image(buf)
    plt.close(fig)
    buf.seek(0)
    return buf


def chart_daily_brand_sales(daily_df: pd.DataFrame, title: str = "Daily Brand Sales") -> BytesIO:
    _mpl_setup()
    buf = BytesIO()
    if daily_df is None or daily_df.empty or "date" not in daily_df.columns:
        return buf

    d = daily_df.copy()
    d["date"] = pd.to_datetime(d["date"], errors="coerce")
    d = d.dropna(subset=["date"]).sort_values("date")
    if d.empty or "net_revenue" not in d.columns:
        return buf
    d["net_revenue"] = pd.to_numeric(d["net_revenue"], errors="coerce").fillna(0.0)
    date_values = d["date"].dt.date.tolist()
    labels = [f"{x.month}/{x.day}" for x in date_values]
    vals = d["net_revenue"].astype(float).tolist()
    x = np.arange(len(vals))

    fig, ax = plt.subplots(figsize=(7.35, 2.15))
    bars = ax.bar(x, vals, color=osnap.HEX_GREEN, edgecolor="#047857", linewidth=0.35, alpha=0.93, zorder=2)
    if vals:
        ax.plot(x, vals, color="#111827", linewidth=1.0, alpha=0.75, zorder=3)
    ax.set_title(title, pad=10)
    tick_step = 1 if len(labels) <= 16 else 2 if len(labels) <= 34 else 4
    show = [i for i in range(len(labels)) if i % tick_step == 0 or i == len(labels) - 1]
    ax.set_xticks(show)
    ax.set_xticklabels([labels[i] for i in show], rotation=35, ha="right", fontsize=6.8)
    ax.yaxis.set_major_formatter(plt.FuncFormatter(lambda v, _pos: f"${v/1000:.0f}k" if abs(v) >= 1000 else f"${v:.0f}"))
    ax.grid(True, axis="y", zorder=0)
    ax.spines["top"].set_visible(False)
    ax.spines["right"].set_visible(False)

    positive = [(idx, val) for idx, val in enumerate(vals) if val > 0]
    if positive:
        best_idx, best_val = max(positive, key=lambda t: t[1])
        low_idx, low_val = min(positive, key=lambda t: t[1])
        for idx, val, label, color in [
            (best_idx, best_val, f"Best {labels[best_idx]}", "#111827"),
            (low_idx, low_val, f"Low {labels[low_idx]}", "#6B7280"),
        ]:
            ax.annotate(
                label,
                xy=(idx, val),
                xytext=(0, 8),
                textcoords="offset points",
                ha="center",
                fontsize=6.7,
                color=color,
                arrowprops={"arrowstyle": "-", "lw": 0.5, "color": color},
            )
    for idx, val in enumerate(vals):
        if val <= 0 or len(vals) > 18:
            continue
        ax.text(bars[idx].get_x() + bars[idx].get_width() / 2, val, f"${val/1000:.1f}k", ha="center", va="bottom", fontsize=6.2, color="#111827")
    fig.subplots_adjust(left=0.075, right=0.99, bottom=0.30, top=0.82)
    _save_chart_image(buf, dpi=170, quality=86)
    plt.close(fig)
    buf.seek(0)
    return buf


def chart_daily_net_profit(daily_df: pd.DataFrame, title: str, compact: bool = False) -> BytesIO:
    _mpl_setup()
    buf = BytesIO()
    if daily_df is None or daily_df.empty or "date" not in daily_df.columns:
        return buf

    d = daily_df.copy()
    d["date"] = pd.to_datetime(d["date"], errors="coerce")
    d = d.dropna(subset=["date"]).sort_values("date")
    if d.empty or "net_revenue" not in d.columns:
        return buf
    d["net_revenue"] = pd.to_numeric(d["net_revenue"], errors="coerce").fillna(0.0)
    profit_col = "profit_real" if "profit_real" in d.columns else "profit" if "profit" in d.columns else ""
    d["__profit"] = pd.to_numeric(d[profit_col], errors="coerce").fillna(0.0) if profit_col else 0.0

    date_values = d["date"].dt.date.tolist()
    labels = [f"{x.month}/{x.day}" for x in date_values]
    net_vals = d["net_revenue"].astype(float).tolist()
    profit_vals = d["__profit"].astype(float).tolist()
    x = np.arange(len(net_vals))

    fig_h = 1.85 if compact else 2.35
    fig, ax = plt.subplots(figsize=(7.25, fig_h))
    ax.bar(x, net_vals, color=osnap.HEX_GREEN, edgecolor="#047857", linewidth=0.35, alpha=0.92, label="Net Sales", zorder=2)
    ax.plot(x, profit_vals, color="#111827", linewidth=1.15, marker="o" if not compact else None, markersize=2.2, label="Real Profit", zorder=3)
    ax.axhline(0, color="#9CA3AF", linewidth=0.6, zorder=1)
    ax.set_title(title, pad=9)
    tick_step = 1 if len(labels) <= 14 else 2 if len(labels) <= 34 else 4
    show = [i for i in range(len(labels)) if i % tick_step == 0 or i == len(labels) - 1]
    ax.set_xticks(show)
    ax.set_xticklabels([labels[i] for i in show], rotation=35, ha="right", fontsize=6.4 if compact else 6.9)
    ax.yaxis.set_major_formatter(plt.FuncFormatter(lambda v, _pos: f"${v/1000:.0f}k" if abs(v) >= 1000 else f"${v:.0f}"))
    ax.grid(True, axis="y", zorder=0)
    ax.legend(loc="upper left", fontsize=6.6, frameon=False, ncol=2)
    ax.spines["top"].set_visible(False)
    ax.spines["right"].set_visible(False)

    positive = [(idx, val) for idx, val in enumerate(net_vals) if val > 0]
    if positive and not compact:
        best_idx, best_val = max(positive, key=lambda t: t[1])
        ax.annotate(
            f"Best {labels[best_idx]}",
            xy=(best_idx, best_val),
            xytext=(0, 8),
            textcoords="offset points",
            ha="center",
            fontsize=6.6,
            color="#111827",
            arrowprops={"arrowstyle": "-", "lw": 0.5, "color": "#111827"},
        )
    fig.subplots_adjust(left=0.075, right=0.99, bottom=0.30 if compact else 0.27, top=0.80)
    _save_chart_image(buf, dpi=170, quality=86)
    plt.close(fig)
    buf.seek(0)
    return buf


def chart_location_net_profit(store_sales_packets: Dict[str, Dict[str, Any]], title: str = "Net Sales + Real Profit by Location") -> BytesIO:
    _mpl_setup()
    buf = BytesIO()
    rows: List[Dict[str, Any]] = []
    for abbr in order_store_codes(list(store_sales_packets.keys())):
        metrics = (((store_sales_packets.get(abbr, {}) or {}).get("window_metrics") or {}).get("report") or {})
        rows.append({
            "store": abbr,
            "net": float(metrics.get("net_revenue", 0.0) or 0.0),
            "profit": float(metrics.get("profit_real", metrics.get("profit", 0.0)) or 0.0),
        })
    if not rows:
        return buf
    df = pd.DataFrame(rows).sort_values("net", ascending=True)
    y = np.arange(len(df))
    fig, ax = plt.subplots(figsize=(7.25, 2.55))
    ax.barh(y, df["net"], color=osnap.HEX_GREEN, edgecolor="#047857", linewidth=0.35, height=0.55, label="Net Sales", zorder=2)
    ax.scatter(df["profit"], y, color="#111827", s=28, label="Real Profit", zorder=3)
    ax.set_yticks(y)
    ax.set_yticklabels(df["store"].tolist(), fontsize=8)
    ax.xaxis.set_major_formatter(plt.FuncFormatter(lambda v, _pos: f"${v/1000:.0f}k" if abs(v) >= 1000 else f"${v:.0f}"))
    ax.set_title(title, pad=10)
    ax.grid(True, axis="x", zorder=0)
    ax.legend(loc="lower right", fontsize=7, frameon=False)
    for idx, row in enumerate(df.itertuples(index=False)):
        ax.text(float(row.net), idx, f" {money0(row.net)}", va="center", fontsize=7.0, color="#111827")
        ax.text(float(row.profit), idx + 0.18, f"{money0(row.profit)}", ha="center", va="bottom", fontsize=6.5, color="#111827")
    ax.spines["top"].set_visible(False)
    ax.spines["right"].set_visible(False)
    fig.subplots_adjust(left=0.08, right=0.98, bottom=0.12, top=0.82)
    _save_chart_image(buf, dpi=170, quality=86)
    plt.close(fig)
    buf.seek(0)
    return buf


def chart_daily_margin(daily_df: pd.DataFrame, title: str) -> BytesIO:
    _mpl_setup()
    buf = BytesIO()
    if daily_df is None or daily_df.empty:
        return buf

    d = daily_df.sort_values("date").copy()
    if "date" not in d.columns:
        return buf

    x = np.arange(len(d))
    labels = [f"{x.month}/{x.day}" for x in d["date"].tolist()]
    margin_vals = (d["margin_real"].astype(float) * 100.0).tolist()

    fig, ax = plt.subplots(figsize=(7.3, 2.9))
    ax.plot(x, margin_vals, color=osnap.HEX_GREEN, linewidth=1.5, marker="o", markersize=2.6, label="Margin", zorder=3)

    ax.set_title(title, pad=16)
    if len(labels) > 20:
        show = [i for i in range(len(labels)) if (i % 2 == 0) or i == len(labels) - 1]
        ax.set_xticks(show)
        ax.set_xticklabels([labels[i] for i in show], rotation=35, ha="right", fontsize=7.0)
    else:
        ax.set_xticks(x)
        ax.set_xticklabels(labels, rotation=35, ha="right", fontsize=7.2)

    ax.set_ylabel("Margin %")
    ax.grid(True, axis="y", zorder=0)
    ax.spines["top"].set_visible(False)
    ax.spines["right"].set_visible(False)
    ax.legend(loc="lower left", bbox_to_anchor=(0.0, 1.03), ncol=1, frameon=False, fontsize=7.0, borderaxespad=0.0)
    fig.subplots_adjust(left=0.07, right=0.992, bottom=0.27, top=0.78)

    _save_chart_image(buf)
    plt.close(fig)
    buf.seek(0)
    return buf


def chart_rank_barh(df: pd.DataFrame, label_col: str, value_col: str, title: str, value_kind: str = "money") -> BytesIO:
    _mpl_setup()
    buf = BytesIO()
    if df is None or df.empty or label_col not in df.columns or value_col not in df.columns:
        return buf

    d = df.head(12).copy()
    labels = d[label_col].astype(str).tolist()[::-1]
    vals = d[value_col].astype(float).tolist()[::-1]

    fig, ax = plt.subplots(figsize=(7.3, 3.25))
    y = np.arange(len(labels))
    bars = ax.barh(y, vals, color=osnap.HEX_GREEN, edgecolor="#047857", linewidth=0.45, alpha=0.96, zorder=2)

    ax.set_title(title, pad=16)
    ax.set_yticks(y)
    ax.set_yticklabels(labels, fontsize=8.0)
    ax.grid(True, axis="x", zorder=0)
    ax.spines["top"].set_visible(False)
    ax.spines["right"].set_visible(False)
    ax.spines["left"].set_visible(False)

    vmax = max(vals) if vals else 0.0
    pad = vmax * 0.012 if vmax else 1.0
    for b, v in zip(bars, vals):
        if value_kind == "int":
            txt = int0(v)
        elif value_kind == "pct":
            txt = pct1(v)
        else:
            txt = money0(v)
        ax.text(
            v + pad,
            b.get_y() + (b.get_height() / 2.0),
            txt,
            va="center",
            ha="left",
            fontsize=7.2,
            fontweight="bold",
            color="#111827",
            clip_on=False,
        )
    if vmax > 0:
        ax.set_xlim(0, vmax * 1.18)

    left_margin = min(max(0.14, 0.095 + max((len(x) for x in labels), default=10) * 0.006), 0.24)
    fig.subplots_adjust(left=left_margin, right=0.97, bottom=0.16, top=0.80)

    _save_chart_image(buf)
    plt.close(fig)
    buf.seek(0)
    return buf


def chart_inventory_value_by_category(inv_cat_df: pd.DataFrame) -> BytesIO:
    return chart_rank_barh(inv_cat_df, "category_normalized", "inventory_value", "Inventory Value by Category", value_kind="money")


def chart_margin_distribution(inv_products_df: pd.DataFrame) -> BytesIO:
    _mpl_setup()
    buf = BytesIO()
    if inv_products_df is None or inv_products_df.empty or "margin_current" not in inv_products_df.columns:
        return buf

    vals = (inv_products_df["margin_current"].astype(float) * 100.0).tolist()
    if not vals:
        return buf

    fig, ax = plt.subplots(figsize=(7.3, 2.9))
    ax.hist(vals, bins=14, color=osnap.HEX_GREEN, edgecolor="#065F46", alpha=0.95)
    ax.set_title("Inventory Margin Distribution", pad=14)
    ax.set_xlabel("Margin %")
    ax.set_ylabel("SKU Groups")
    ax.grid(True, axis="y", zorder=0)
    ax.spines["top"].set_visible(False)
    ax.spines["right"].set_visible(False)
    fig.subplots_adjust(left=0.08, right=0.992, bottom=0.2, top=0.82)

    _save_chart_image(buf)
    plt.close(fig)
    buf.seek(0)
    return buf


def chart_top_inventory_profit(inv_products_df: pd.DataFrame) -> BytesIO:
    return chart_rank_barh(inv_products_df, "display_product", "potential_profit", "Top Inventory Profit SKUs", value_kind="money")


def chart_inventory_value_by_product_group(inv_products_df: pd.DataFrame, title: str) -> BytesIO:
    _mpl_setup()
    buf = BytesIO()
    if inv_products_df is None or inv_products_df.empty:
        return buf
    if "inventory_value" not in inv_products_df.columns:
        return buf

    d = inv_products_df.copy()
    label_col = "product_group_display" if "product_group_display" in d.columns else ("display_product" if "display_product" in d.columns else None)
    if label_col is None:
        return buf

    trend_col = "trend_units_per_day_30d" if "trend_units_per_day_30d" in d.columns else (
        "trend_units_per_day_14d" if "trend_units_per_day_14d" in d.columns else None
    )
    if trend_col is None:
        d["__trend"] = 0.0
        trend_col = "__trend"

    d["inventory_value"] = pd.to_numeric(d["inventory_value"], errors="coerce").fillna(0.0).astype(float)
    d[trend_col] = pd.to_numeric(d[trend_col], errors="coerce").fillna(0.0).astype(float)
    d = d.sort_values("inventory_value", ascending=False).head(12)
    if d.empty:
        return buf

    labels = d[label_col].astype(str).tolist()[::-1]
    vals = d["inventory_value"].astype(float).tolist()[::-1]
    trends = d[trend_col].astype(float).tolist()[::-1]

    fig, ax = plt.subplots(figsize=(7.3, 3.15))
    y = np.arange(len(labels))
    bars = ax.barh(y, vals, color=osnap.HEX_GREEN, edgecolor="#047857", linewidth=0.45, alpha=0.96, zorder=2)

    ax.set_title(title, pad=15)
    ax.set_yticks(y)
    ax.set_yticklabels(labels, fontsize=7.4)
    ax.grid(True, axis="x", zorder=0)
    ax.spines["top"].set_visible(False)
    ax.spines["right"].set_visible(False)
    ax.spines["left"].set_visible(False)

    vmax = max(vals) if vals else 0.0
    pad = vmax * 0.012 if vmax else 1.0
    for b, v, t in zip(bars, vals, trends):
        txt = f"{money0(v)} • {t:,.1f}/day"
        ax.text(
            v + pad,
            b.get_y() + (b.get_height() / 2.0),
            txt,
            va="center",
            ha="left",
            fontsize=7.0,
            color="#111827",
            clip_on=False,
        )

    if vmax > 0:
        ax.set_xlim(0, vmax * 1.22)

    left_margin = min(max(0.16, 0.105 + max((len(x) for x in labels), default=10) * 0.0065), 0.30)
    fig.subplots_adjust(left=left_margin, right=0.97, bottom=0.12, top=0.83)
    _save_chart_image(buf)
    plt.close(fig)
    buf.seek(0)
    return buf


def _display_part_is_size_token(value: Any) -> bool:
    part = normalize_text(value)
    if not part:
        return False
    return bool(re.fullmatch(r"\d+(?:\.\d+)?\s*(?:MG|G|ML|OZ)", part))


def _rollup_inventory_display_label(value: Any) -> str:
    label = str(value or "").strip()
    if not label:
        return ""

    if "•" in label:
        parts = [part.strip() for part in label.split("•") if str(part).strip()]
        kept = [part for part in parts if not _display_part_is_size_token(part)]
        if kept:
            return " • ".join(kept)
    return label


def rollup_inventory_units_on_hand(inv_products_df: pd.DataFrame) -> pd.DataFrame:
    cols = [
        "display_product",
        "category_normalized",
        "units_available",
        "inventory_value",
        "trend_units_per_day_30d",
        "trend_units_per_day_14d",
        "trend_units_per_day_7d",
        "days_of_supply",
    ]
    if inv_products_df is None or inv_products_df.empty:
        return pd.DataFrame(columns=cols)

    label_col = "display_product" if "display_product" in inv_products_df.columns else (
        "product_group_display" if "product_group_display" in inv_products_df.columns else None
    )
    if label_col is None:
        return pd.DataFrame(columns=cols)

    tmp = inv_products_df.copy()
    tmp[label_col] = tmp[label_col].fillna("").astype(str).str.strip()
    tmp = tmp[tmp[label_col] != ""].copy()
    if tmp.empty:
        return pd.DataFrame(columns=cols)

    tmp["display_product"] = tmp[label_col].map(_rollup_inventory_display_label)
    tmp["category_normalized"] = tmp.get("category_normalized", "UNKNOWN")
    tmp["category_normalized"] = tmp["category_normalized"].fillna("UNKNOWN").astype(str)
    tmp["__rollup_key"] = (
        tmp["category_normalized"].fillna("UNKNOWN").astype(str)
        + "|"
        + tmp["display_product"].map(normalize_text)
    )

    for col in ["units_available", "inventory_value", "trend_units_per_day_30d", "trend_units_per_day_14d", "trend_units_per_day_7d"]:
        tmp[col] = pd.to_numeric(tmp.get(col, 0.0), errors="coerce").fillna(0.0).astype(float)

    grouped = tmp.groupby("__rollup_key", as_index=False).agg(
        display_product=("display_product", lambda s: _first_mode(s, "")),
        category_normalized=("category_normalized", lambda s: _first_mode(s, "UNKNOWN")),
        units_available=("units_available", "sum"),
        inventory_value=("inventory_value", "sum"),
        trend_units_per_day_30d=("trend_units_per_day_30d", "sum"),
        trend_units_per_day_14d=("trend_units_per_day_14d", "sum"),
        trend_units_per_day_7d=("trend_units_per_day_7d", "sum"),
    )
    grouped["days_of_supply"] = grouped.apply(
        lambda r: _safe_dos(
            float(r.get("units_available", 0.0)),
            float(r.get("trend_units_per_day_30d", r.get("trend_units_per_day_14d", 0.0))),
        ),
        axis=1,
    )
    keep_cols = [c for c in cols if c in grouped.columns]
    return grouped.sort_values(["units_available", "inventory_value", "display_product"], ascending=[False, False, True])[keep_cols].reset_index(drop=True)


def chart_inventory_units_by_product_group(inv_products_df: pd.DataFrame, title: str) -> BytesIO:
    buf = BytesIO()
    if inv_products_df is None or inv_products_df.empty or "units_available" not in inv_products_df.columns:
        return buf

    d = rollup_inventory_units_on_hand(inv_products_df)
    if d.empty or "display_product" not in d.columns:
        return buf

    d["units_available"] = pd.to_numeric(d["units_available"], errors="coerce").fillna(0.0).astype(float)
    d = d.sort_values(["units_available", "display_product"], ascending=[False, True]).head(12)
    if d.empty:
        return buf

    plot_df = d[["display_product", "units_available"]].copy()
    return chart_rank_barh(plot_df, "display_product", "units_available", title, value_kind="int")


def chart_deal_scenario_margin(
    scenario_df: pd.DataFrame,
    title: str = "Deal Scenario Margin Comparison",
    target_margin: float = DEAL_TARGET_MARGIN,
) -> BytesIO:
    _mpl_setup()
    buf = BytesIO()
    if scenario_df is None or scenario_df.empty:
        return buf
    if "scenario" not in scenario_df.columns or "margin" not in scenario_df.columns:
        return buf

    d = scenario_df.copy()
    d["margin"] = pd.to_numeric(d["margin"], errors="coerce").fillna(0.0).astype(float)
    d["avg_profit_per_unit"] = pd.to_numeric(d.get("avg_profit_per_unit", 0.0), errors="coerce").fillna(0.0).astype(float)
    d["worth_it"] = d.get("worth_it", "").fillna("").astype(str).str.upper()
    if d.empty:
        return buf

    labels = d["scenario"].astype(str).tolist()
    margins = (d["margin"] * 100.0).tolist()
    profit_u = d["avg_profit_per_unit"].tolist()
    colors_bar = ["#A3A3A3" if w == "BASE" else (osnap.HEX_GREEN if w == "YES" else "#B91C1C") for w in d["worth_it"].tolist()]
    x = np.arange(len(labels))

    fig, ax = plt.subplots(figsize=(7.3, 3.2))
    bars = ax.bar(x, margins, color=colors_bar, edgecolor="#111827", linewidth=0.45, alpha=0.95, zorder=2)
    ax.axhline(float(target_margin) * 100.0, color="#1D4ED8", linewidth=1.1, linestyle="--", zorder=3, label=f"Target {target_margin * 100:.0f}%")

    ax.set_title(title, pad=16)
    ax.set_ylabel("Projected Margin %")
    ax.set_xticks(x)
    ax.set_xticklabels(labels, rotation=19, ha="right", fontsize=7.2)
    ax.grid(True, axis="y", zorder=0)
    ax.spines["top"].set_visible(False)
    ax.spines["right"].set_visible(False)

    for b, m, pu in zip(bars, margins, profit_u):
        ax.text(
            b.get_x() + (b.get_width() / 2.0),
            b.get_height() + 0.6,
            f"{m:.1f}%\n{money2(pu)}/u",
            ha="center",
            va="bottom",
            fontsize=6.9,
            color="#111827",
            fontweight="bold",
            clip_on=False,
        )

    ymin = min(min(margins) if margins else 0.0, float(target_margin) * 100.0)
    ymax = max(max(margins) if margins else 0.0, float(target_margin) * 100.0)
    pad_low = 7.0 if ymin < 0 else 3.0
    pad_high = 14.0
    ax.set_ylim(ymin - pad_low, ymax + pad_high)
    ax.legend(loc="upper left", frameon=False, fontsize=7.0)
    fig.subplots_adjust(left=0.09, right=0.992, bottom=0.32, top=0.80)

    _save_chart_image(buf)
    plt.close(fig)
    buf.seek(0)
    return buf


# ---------------------------------------------------------------------------
# PDF
# ---------------------------------------------------------------------------
def _footer(left_text: str, report_day: date):
    def _draw(canvas, doc):
        canvas.saveState()
        canvas.setFont(osnap.BASE_FONT, 8)
        canvas.setFillColor(osnap.THEME["muted"])
        canvas.drawString(doc.leftMargin, 0.30 * inch, f"{left_text} • {report_day.isoformat()} ({report_day.strftime('%A')})")
        page_width = float(getattr(doc, "pagesize", letter)[0])
        canvas.drawRightString(page_width - doc.rightMargin, 0.30 * inch, f"Page {canvas.getPageNumber()}")
        canvas.restoreState()

    return _draw


def _fit_col_widths(col_widths: Optional[Sequence[float]], max_width: float) -> Optional[List[float]]:
    if not col_widths:
        return None
    widths: List[float] = []
    for w in col_widths:
        try:
            widths.append(float(w))
        except Exception:
            widths.append(0.0)
    total = float(sum(widths))
    if total <= 0 or total <= max_width:
        return widths

    min_col = 0.44 * inch
    scaled = [max(min_col, w * (max_width / total)) for w in widths]
    over = float(sum(scaled) - max_width)
    if over > 0:
        flex = [i for i, v in enumerate(scaled) if v > (min_col + 1e-6)]
        while over > 1e-6 and flex:
            step = over / float(len(flex))
            new_flex: List[int] = []
            for i in flex:
                next_v = scaled[i] - step
                if next_v <= min_col:
                    over -= max(0.0, scaled[i] - min_col)
                    scaled[i] = min_col
                else:
                    scaled[i] = next_v
                    over -= step
                    if scaled[i] > (min_col + 1e-6):
                        new_flex.append(i)
            flex = new_flex
            if not flex:
                break

    total_scaled = float(sum(scaled))
    if total_scaled > max_width and total_scaled > 0:
        ratio = max_width / total_scaled
        scaled = [v * ratio for v in scaled]
    return scaled


def _wrap_table_value(value: Any, style: ParagraphStyle) -> Any:
    if isinstance(value, Paragraph):
        return value
    if value is None:
        return ""
    txt = str(value)
    if not txt:
        return ""
    # Keep compact one-token values (currency/integers/percents) as raw strings.
    if ("\n" not in txt) and (" " not in txt) and (len(txt) <= 20):
        return txt
    txt = xml_escape(txt).replace("\n", "<br/>")
    return Paragraph(txt, style)


def _build_table_fit(
    headers: List[Any],
    rows: List[List[Any]],
    col_widths: Optional[List[float]] = None,
) -> Any:
    max_width = float(letter[0] - osnap.PAGE_MARGINS["left"] - osnap.PAGE_MARGINS["right"])
    fitted_widths = _fit_col_widths(col_widths, max_width)

    cell_style = ParagraphStyle(
        name="PacketTableCellWrap",
        fontName=osnap.BASE_FONT,
        fontSize=8.2,
        leading=9.4,
        wordWrap="CJK",
    )
    wrapped_rows: List[List[Any]] = []
    for row in rows:
        wrapped_rows.append([_wrap_table_value(v, cell_style) for v in row])

    return osnap.build_table(headers, wrapped_rows, fitted_widths)


def _add_section(story: List[Any], title: str, styles: Dict[str, Any], min_height: float = 1.4 * inch) -> None:
    story.append(CondPageBreak(min_height))
    story.append(Paragraph(title, styles["Section"]))


def _credit_reconciliation_rows(reconciliation: pd.DataFrame, top_n: int = 12) -> List[List[Any]]:
    if reconciliation is None or reconciliation.empty:
        return []
    rows: List[List[Any]] = []
    for _, r in reconciliation.head(top_n).iterrows():
        rows.append([
            str(r.get("Type", "")),
            str(r.get("Scope", "")),
            money0(float(r.get("Expected", 0.0))),
            money0(float(r.get("Received", 0.0))),
            money0(float(r.get("Gap", 0.0))),
            str(r.get("Status", "")),
            osnap.pp1(float(r.get("Margin Lift Expected", 0.0))),
            osnap.pp1(float(r.get("Margin Lift Received", 0.0))),
            str(r.get("Notes", "")),
        ])
    return rows


def _credit_source_summary(reconciliation: Optional[pd.DataFrame]) -> pd.DataFrame:
    if reconciliation is None or reconciliation.empty:
        return pd.DataFrame(columns=["Source", "Rows", "Expected", "Received", "Gap", "Applied Expected", "Applied Received", "Open Rows"])
    rec = reconciliation.copy()
    for col in ["Expected", "Received", "Gap", "Applied Expected", "Applied Received"]:
        if col not in rec.columns:
            rec[col] = 0.0
        rec[col] = pd.to_numeric(rec[col], errors="coerce").fillna(0.0)
    if "Source" not in rec.columns:
        rec["Source"] = ""
    if "Status" not in rec.columns:
        rec["Status"] = ""
    rec["Source"] = rec["Source"].fillna("").astype(str).replace({"": "unknown"})
    rec["Status"] = rec["Status"].fillna("").astype(str).str.lower()
    out = rec.groupby("Source", as_index=False).agg(
        Rows=("Source", "size"),
        Expected=("Expected", "sum"),
        Received=("Received", "sum"),
        Gap=("Gap", "sum"),
        Applied_Expected=("Applied Expected", "sum"),
        Applied_Received=("Applied Received", "sum"),
    )
    open_rows = rec[rec["Status"].isin({"expected", "partial", "overdue"})].groupby("Source").size()
    out["Open Rows"] = out["Source"].map(open_rows).fillna(0).astype(int)
    return out.rename(columns={"Applied_Expected": "Applied Expected", "Applied_Received": "Applied Received"})


def _action_item_rows(action_items: Sequence[Dict[str, Any]], top_n: int = 8) -> List[List[Any]]:
    rows: List[List[Any]] = []
    for item in list(action_items or [])[:top_n]:
        affected = " / ".join([str(item.get(k, "")).strip() for k in ["store", "category_name", "product"] if str(item.get(k, "")).strip()])
        try:
            dollar_amount = float(item.get("dollar_amount", 0.0) or 0.0)
        except Exception:
            dollar_amount = 0.0
        rows.append([
            str(item.get("priority", "")),
            str(item.get("category", "")),
            str(item.get("problem", "")),
            str(item.get("evidence", "")),
            str(item.get("brand_action", "")),
            affected or "Brand",
            money0(dollar_amount) if dollar_amount else "",
        ])
    return rows


def _credit_metric_grid(
    styles: Dict[str, Any],
    credit_summary: Dict[str, Any],
    health_score: int,
    health_status: str,
) -> Any:
    cells = [
        osnap.kpi_cell(styles, "Brand Health", f"{health_score}/100", health_status),
        osnap.kpi_cell(styles, "Real Margin", pct1(credit_summary.get("real_margin", 0.0)), "No credit adjustment"),
        osnap.kpi_cell(styles, "Expected Credit Margin", pct1(credit_summary.get("expected_credit_margin", 0.0)), money0(credit_summary.get("expected_credit_amount", 0.0))),
        osnap.kpi_cell(styles, "Received Credit Margin", pct1(credit_summary.get("received_credit_margin", 0.0)), money0(credit_summary.get("received_credit_amount", 0.0))),
        osnap.kpi_cell(styles, "Credit Gap", money0(credit_summary.get("credit_gap", 0.0)), "Expected less received"),
        osnap.kpi_cell(styles, "Credit Needed", money0(credit_summary.get("credit_needed_to_hit_target", 0.0)), f"Target {pct1(credit_summary.get('target_margin', 0.35))}"),
        osnap.kpi_cell(
            styles,
            "Deals Reference" if credit_summary.get("system_expected_reference_only") else "Deals Expected",
            money0(credit_summary.get("system_expected_credit", 0.0)),
            "not double-counted" if credit_summary.get("system_expected_reference_only") else "deals.py",
        ),
        osnap.kpi_cell(styles, "CreditFlow Rec.", money0(credit_summary.get("creditflow_received_credit", 0.0)), f"open {int(credit_summary.get('creditflow_open_rows', 0) or 0)}"),
    ]
    return osnap.build_kpi_grid(styles, cells, cols=4)


def _store_credit_scorecard(
    store_df: pd.DataFrame,
    inv_store: pd.DataFrame,
    credit_summary: Dict[str, Any],
    target_margin: float,
    top_n: int = 12,
    credit_reconciliation: Optional[pd.DataFrame] = None,
) -> pd.DataFrame:
    if store_df is None or store_df.empty:
        return pd.DataFrame()
    out = store_df.copy()
    if "_store_abbr" not in out.columns:
        out["_store_abbr"] = ""
    out["_store_abbr"] = out["_store_abbr"].fillna("").astype(str).str.upper().str.strip()
    for col in ["net_revenue", "items", "tickets", "profit_real", "margin_real", "discount_rate"]:
        if col not in out.columns:
            out[col] = 0.0
        out[col] = pd.to_numeric(out[col], errors="coerce").fillna(0.0)
    if inv_store is not None and not inv_store.empty and "_store_abbr" in inv_store.columns:
        inv = inv_store.copy()
        inv["_store_abbr"] = inv["_store_abbr"].fillna("").astype(str).str.upper()
        keep = [c for c in ["_store_abbr", "inventory_value", "units_available", "days_of_supply"] if c in inv.columns]
        out = out.merge(inv[keep], on="_store_abbr", how="left")
    for col in ["inventory_value", "units_available", "days_of_supply"]:
        if col not in out.columns:
            out[col] = 0.0
        out[col] = pd.to_numeric(out[col], errors="coerce").fillna(0.0)
    total_net = float(out["net_revenue"].sum())
    out["revenue_share"] = out["net_revenue"] / total_net if total_net else 0.0
    out["received_credit_alloc"] = 0.0
    out["expected_credit_alloc"] = 0.0

    brand_expected_unassigned = float(credit_summary.get("expected_credit_amount", 0.0) or 0.0)
    brand_received_unassigned = float(credit_summary.get("received_credit_amount", 0.0) or 0.0)
    if credit_reconciliation is not None and not credit_reconciliation.empty:
        rec = credit_reconciliation.copy()
        if "Included In Margin" in rec.columns:
            rec = rec[rec["Included In Margin"].fillna(False).astype(bool)].copy()
        if "Source" in rec.columns:
            rec = rec[~rec["Source"].fillna("").astype(str).str.lower().eq("system")].copy()
        for col in ["Applied Expected", "Applied Received", "Expected", "Received"]:
            if col not in rec.columns:
                rec[col] = 0.0
            rec[col] = pd.to_numeric(rec[col], errors="coerce").fillna(0.0)
        expected_col = "Applied Expected" if "Applied Expected" in rec.columns else "Expected"
        received_col = "Applied Received" if "Applied Received" in rec.columns else "Received"
        if "Store" not in rec.columns:
            rec["Store"] = rec.get("Scope", "").astype(str).str.extract(r"^([A-Za-z]{2})", expand=False).fillna("")
        rec["Store"] = rec["Store"].fillna("").astype(str).str.upper().str.strip()
        store_rec = rec[rec["Store"].isin(out["_store_abbr"].fillna("").astype(str).str.upper())].copy()
        if not store_rec.empty:
            store_alloc = store_rec.groupby("Store", as_index=False).agg(
                expected_credit_alloc=(expected_col, "sum"),
                received_credit_alloc=(received_col, "sum"),
            )
            out = out.merge(store_alloc, left_on="_store_abbr", right_on="Store", how="left", suffixes=("", "_exact"))
            out["expected_credit_alloc"] = pd.to_numeric(out.get("expected_credit_alloc_exact"), errors="coerce").fillna(0.0)
            out["received_credit_alloc"] = pd.to_numeric(out.get("received_credit_alloc_exact"), errors="coerce").fillna(0.0)
            out = out.drop(columns=[c for c in ["Store", "expected_credit_alloc_exact", "received_credit_alloc_exact"] if c in out.columns])
        brand_level_rec = rec[~rec["Store"].isin(out["_store_abbr"].fillna("").astype(str).str.upper())].copy()
        brand_expected_unassigned = float(pd.to_numeric(brand_level_rec[expected_col], errors="coerce").fillna(0.0).sum())
        brand_received_unassigned = float(pd.to_numeric(brand_level_rec[received_col], errors="coerce").fillna(0.0).sum())

    if brand_expected_unassigned or brand_received_unassigned:
        out["expected_credit_alloc"] += out["revenue_share"] * brand_expected_unassigned
        out["received_credit_alloc"] += out["revenue_share"] * brand_received_unassigned
    out["received_credit_margin"] = (out["profit_real"] + out["received_credit_alloc"]) / out["net_revenue"].replace(0, np.nan)
    out["expected_credit_margin"] = (out["profit_real"] + out["expected_credit_alloc"]) / out["net_revenue"].replace(0, np.nan)
    out["credit_gap_alloc"] = (out["expected_credit_alloc"] - out["received_credit_alloc"]).clip(lower=0.0)
    out["status"] = np.where(
        out["received_credit_margin"].fillna(out["margin_real"]) + 0.05 < target_margin,
        "Needs Support",
        np.where(out["discount_rate"] > 0.45, "Watch", "Strong"),
    )
    return out.sort_values("net_revenue", ascending=False).head(top_n)


def _store_credit_rows(store_credit_df: pd.DataFrame) -> List[List[Any]]:
    if store_credit_df is None or store_credit_df.empty:
        return []
    rows: List[List[Any]] = []
    for _, r in store_credit_df.iterrows():
        rows.append([
            str(r.get("_store_abbr", "")),
            money0(float(r.get("net_revenue", 0.0))),
            int0(float(r.get("items", 0.0))),
            pct1(float(r.get("margin_real", 0.0))),
            pct1(float(r.get("received_credit_margin", 0.0) or 0.0)),
            money0(float(r.get("credit_gap_alloc", 0.0))),
            pct1(float(r.get("discount_rate", 0.0))),
            money0(float(r.get("inventory_value", 0.0))),
            days1(float(r.get("days_of_supply", np.nan))),
            str(r.get("status", "")),
        ])
    return rows


def _monthly_reference_rows(monthly_reference: Dict[str, Any], top_n: int = 8) -> List[List[Any]]:
    rows: List[List[Any]] = []
    if not monthly_reference or not monthly_reference.get("available"):
        return rows
    brand_rows = monthly_reference.get("brand_rows")
    if isinstance(brand_rows, pd.DataFrame) and not brand_rows.empty:
        for _, r in brand_rows.head(top_n).iterrows():
            rows.append([
                "Brand Summary",
                str(r.get("Store", r.get("store", r.get("_store_abbr", "All Stores")))),
                money0(float(r.get("net_revenue", r.get("Revenue", 0.0)) or 0.0)),
                pct1(float(r.get("margin_real", r.get("Real Margin", 0.0)) or 0.0)),
                pct1(float(r.get("discount_rate", r.get("Discount Rate", 0.0)) or 0.0)),
                "",
            ])
    inv_rows = monthly_reference.get("inventory_rows")
    if isinstance(inv_rows, pd.DataFrame) and not inv_rows.empty:
        for _, r in inv_rows.head(top_n).iterrows():
            rows.append([
                "Inventory Risk",
                str(r.get("Store", r.get("store", ""))),
                money0(float(r.get("Revenue", r.get("revenue", 0.0)) or 0.0)),
                "",
                "",
                str(r.get("Product", r.get("product", r.get("Recommended Action", "")))),
            ])
    return rows[:top_n]


def build_brand_packet_theme_v2() -> Dict[str, ParagraphStyle]:
    base = getattr(osnap, "BASE_FONT", "Helvetica")
    bold = getattr(osnap, "BOLD_FONT", "Helvetica-Bold")
    return {
        "deck_title": ParagraphStyle("DeckTitle", fontName=bold, fontSize=22, leading=25, textColor=colors.HexColor("#111827"), spaceAfter=4),
        "deck_subtitle": ParagraphStyle("DeckSubtitle", fontName=base, fontSize=8.5, leading=11, textColor=colors.HexColor("#4B5563")),
        "section_v2": ParagraphStyle("SectionV2", fontName=bold, fontSize=15, leading=18, textColor=colors.HexColor("#111827"), spaceAfter=6),
        "small_v2": ParagraphStyle("SmallV2", fontName=base, fontSize=7.5, leading=9.2, textColor=colors.HexColor("#4B5563")),
        "tiny_v2": ParagraphStyle("TinyV2", fontName=base, fontSize=6.8, leading=8.0, textColor=colors.HexColor("#6B7280")),
        "card_label": ParagraphStyle("CardLabel", fontName=bold, fontSize=6.8, leading=8.0, textColor=colors.HexColor("#4B5563")),
        "card_value": ParagraphStyle("CardValue", fontName=bold, fontSize=12.5, leading=14.5, textColor=colors.HexColor("#111827")),
        "card_note": ParagraphStyle("CardNote", fontName=base, fontSize=6.6, leading=8.0, textColor=colors.HexColor("#6B7280")),
        "table_cell": ParagraphStyle("PremiumTableCell", fontName=base, fontSize=7.1, leading=8.3, textColor=colors.HexColor("#111827"), wordWrap="CJK"),
        "table_cell_center": ParagraphStyle("PremiumTableCellCenter", fontName=base, fontSize=7.1, leading=8.3, textColor=colors.HexColor("#111827"), alignment=TA_CENTER, wordWrap="CJK"),
        "table_cell_right": ParagraphStyle("PremiumTableCellRight", fontName=base, fontSize=7.1, leading=8.3, textColor=colors.HexColor("#111827"), alignment=TA_RIGHT, wordWrap="CJK"),
        "table_head": ParagraphStyle("PremiumTableHead", fontName=bold, fontSize=7.2, leading=8.2, textColor=colors.white, alignment=TA_CENTER),
        "ask": ParagraphStyle("AskBox", fontName=bold, fontSize=11, leading=14, textColor=colors.HexColor("#111827")),
    }


def _p(text: Any, style: ParagraphStyle) -> Paragraph:
    return Paragraph(xml_escape(str(text or "")), style)


def _short_status(value: Any) -> str:
    text = str(value or "").strip()
    if text.lower() == "needs support":
        return "Needs Support"
    return text or "Watch"


def short_product_label(product_name: Any, max_chars: int = 62) -> str:
    text = str(product_name or "").strip()
    if not text:
        return ""
    text = re.sub(r"\s+", " ", text)
    if "(+" in text and " more)" in text:
        text = re.sub(r"\s*\(\+\d+\s+more\)", "", text).strip()
    parts = [part.strip() for part in text.split("|") if part.strip()]
    if len(parts) > 1 and len(parts[0]) <= 26 and not re.search(r"\d", parts[0]) and len(parts) >= 3:
        parts = parts[1:]
    type_map = {
        "h": "Hybrid",
        "hyb": "Hybrid",
        "hybrid": "Hybrid",
        "i": "Indica",
        "ind": "Indica",
        "indica": "Indica",
        "s": "Sativa",
        "sat": "Sativa",
        "sativa": "Sativa",
    }
    if len(parts) >= 3 and parts[1].strip().lower() in type_map:
        product_type = parts[0].replace("0.5G", "0.5g").replace("1G", "1g").replace("3.5G", "3.5g")
        strain_type = type_map[parts[1].strip().lower()]
        strain_blob = " | ".join(parts[2:])
        strains = [s.strip() for s in re.split(r"\s*/\s*", strain_blob) if s.strip()]
        if len(strains) > 1:
            text = f"{product_type} | {strain_type} Mix (+{len(strains) - 1})"
        elif strains:
            text = f"{product_type} | {strain_type} | {strains[0]}"
        else:
            text = f"{product_type} | {strain_type}"
    elif len(parts) >= 2:
        text = " | ".join(parts[:3])
    text = text.replace("(14PK)", "14pk").replace("(5pk)", "5pk").replace("  ", " ").strip()
    if len(text) <= max_chars:
        return text
    cut = text[: max_chars - 1].rsplit(" ", 1)[0].rstrip(" |-/")
    return f"{cut}..."


def _format_days_supply_v2(value: Any, units_per_day: Any = None, units_on_hand: Any = None) -> str:
    try:
        upd = float(units_per_day) if units_per_day is not None else None
    except Exception:
        upd = None
    try:
        units = float(units_on_hand) if units_on_hand is not None else 0.0
    except Exception:
        units = 0.0
    try:
        days = float(value)
    except Exception:
        days = float("nan")
    if (upd is not None and upd <= 0 and units > 0) or not np.isfinite(days):
        return "No sales" if units > 0 else "n/a"
    if days > 180:
        return ">180d"
    return f"{days:.0f}d" if days >= 10 else f"{days:.1f}d"


def limit_rows_for_pdf(df: pd.DataFrame, mode: str, section_type: str) -> pd.DataFrame:
    if df is None or df.empty:
        return pd.DataFrame(columns=df.columns if df is not None else [])
    mode = str(mode or "standard").lower()
    limits = {
        "quick": {"top_products": 8, "slow_movers": 8, "fast_movers": 8, "store_products": 5, "appendix": 0},
        "standard": {"top_products": 15, "slow_movers": 15, "fast_movers": 12, "store_products": 8, "appendix": 25},
        "deep": {"top_products": 25, "slow_movers": 25, "fast_movers": 20, "store_products": 15, "appendix": 50},
    }
    n = limits.get(mode, limits["standard"]).get(section_type, 10)
    if n <= 0:
        return df.iloc[0:0].copy()
    return df.head(n).copy()


def build_premium_table(
    rows: List[List[Any]],
    col_widths: List[float],
    alignments: Optional[List[str]] = None,
    repeat_header: bool = True,
    row_limit: Optional[int] = None,
    money_cols: Optional[Sequence[int]] = None,
    pct_cols: Optional[Sequence[int]] = None,
    status_cols: Optional[Sequence[int]] = None,
    compact: bool = True,
) -> Table:
    theme = build_brand_packet_theme_v2()
    if row_limit is not None and len(rows) > row_limit + 1:
        rows = rows[: row_limit + 1]
    alignments = alignments or []
    money_cols = set(money_cols or [])
    pct_cols = set(pct_cols or [])
    status_cols = set(status_cols or [])
    body_style = theme["table_cell"]
    center_style = theme["table_cell_center"]
    right_style = theme["table_cell_right"]
    wrapped: List[List[Any]] = []
    for ridx, row in enumerate(rows):
        out_row: List[Any] = []
        for cidx, value in enumerate(row):
            if ridx == 0:
                out_row.append(_p(value, theme["table_head"]))
                continue
            style = body_style
            if cidx in money_cols or cidx in pct_cols or (cidx < len(alignments) and alignments[cidx].lower() == "right"):
                style = right_style
            elif cidx in status_cols or (cidx < len(alignments) and alignments[cidx].lower() == "center"):
                style = center_style
            out_row.append(_p(value, style))
        wrapped.append(out_row)
    split_range = (2, -3) if len(wrapped) > 8 else None
    table = Table(
        wrapped,
        colWidths=col_widths,
        repeatRows=1 if repeat_header else 0,
        hAlign="LEFT",
        splitByRow=1,
        rowSplitRange=split_range,
    )
    pad = 3 if compact else 5
    table.setStyle(TableStyle([
        ("BACKGROUND", (0, 0), (-1, 0), colors.HexColor("#111111")),
        ("TEXTCOLOR", (0, 0), (-1, 0), colors.white),
        ("LINEBELOW", (0, 0), (-1, 0), 0.8, colors.HexColor("#D6B800")),
        ("GRID", (0, 0), (-1, -1), 0.25, colors.HexColor("#D9DEE3")),
        ("VALIGN", (0, 0), (-1, -1), "MIDDLE"),
        ("LEFTPADDING", (0, 0), (-1, -1), pad),
        ("RIGHTPADDING", (0, 0), (-1, -1), pad),
        ("TOPPADDING", (0, 0), (-1, -1), 2),
        ("BOTTOMPADDING", (0, 0), (-1, -1), 2),
        ("ROWBACKGROUNDS", (0, 1), (-1, -1), [colors.white, colors.HexColor("#F7F8FA")]),
    ]))
    return table


def append_section_safely(story: List[Any], section_title: str, flowables: List[Any], min_height: float = 1.5 * inch) -> None:
    theme = build_brand_packet_theme_v2()
    story.append(CondPageBreak(min_height))
    story.append(Paragraph(section_title, theme["section_v2"]))
    story.extend(flowables)


def _metric_card(label: str, value: Any, note: str = "", accent: str = "#2F6B5D") -> Table:
    theme = build_brand_packet_theme_v2()
    data = [[_p(label.upper(), theme["card_label"])], [_p(value, theme["card_value"])], [_p(note, theme["card_note"])]]
    table = Table(data, colWidths=[1.18 * inch], rowHeights=[0.18 * inch, 0.28 * inch, 0.22 * inch])
    table.setStyle(TableStyle([
        ("BACKGROUND", (0, 0), (-1, -1), colors.HexColor("#F5F7F6")),
        ("BOX", (0, 0), (-1, -1), 0.45, colors.HexColor("#D7DEE0")),
        ("LINEABOVE", (0, 0), (-1, 0), 1.3, colors.HexColor(accent)),
        ("VALIGN", (0, 0), (-1, -1), "TOP"),
        ("LEFTPADDING", (0, 0), (-1, -1), 5),
        ("RIGHTPADDING", (0, 0), (-1, -1), 5),
        ("TOPPADDING", (0, 0), (-1, -1), 3),
        ("BOTTOMPADDING", (0, 0), (-1, -1), 2),
    ]))
    return table


def _metric_grid(cards: List[Table], cols: int = 6) -> Table:
    rows: List[List[Any]] = []
    for i in range(0, len(cards), cols):
        row = cards[i:i + cols]
        while len(row) < cols:
            row.append("")
        rows.append(row)
    table = Table(rows, colWidths=[1.23 * inch] * cols, hAlign="LEFT")
    table.setStyle(TableStyle([
        ("VALIGN", (0, 0), (-1, -1), "TOP"),
        ("LEFTPADDING", (0, 0), (-1, -1), 2),
        ("RIGHTPADDING", (0, 0), (-1, -1), 2),
        ("TOPPADDING", (0, 0), (-1, -1), 2),
        ("BOTTOMPADDING", (0, 0), (-1, -1), 2),
    ]))
    return table


def _panel(title: str, lines: List[str], width: float = 2.35 * inch, accent: str = "#2F6B5D") -> Table:
    theme = build_brand_packet_theme_v2()
    data = [[_p(title, theme["card_label"])]] + [[_p(line, theme["small_v2"])] for line in lines]
    table = Table(data, colWidths=[width])
    table.setStyle(TableStyle([
        ("BACKGROUND", (0, 0), (-1, -1), colors.HexColor("#F5F7F6")),
        ("BOX", (0, 0), (-1, -1), 0.45, colors.HexColor("#D7DEE0")),
        ("LINEABOVE", (0, 0), (-1, 0), 1.2, colors.HexColor(accent)),
        ("LEFTPADDING", (0, 0), (-1, -1), 6),
        ("RIGHTPADDING", (0, 0), (-1, -1), 6),
        ("TOPPADDING", (0, 0), (-1, -1), 4),
        ("BOTTOMPADDING", (0, 0), (-1, -1), 4),
    ]))
    return table


def _deck_header(brand: str, start_day: date, end_day: date, stores: Sequence[str], packet_mode: str, generated_at: str) -> Table:
    theme = build_brand_packet_theme_v2()
    left = [_p(brand, theme["deck_title"]), _p(f"{start_day.isoformat()} to {end_day.isoformat()}  |  Stores: {', '.join(stores) or 'All'}", theme["deck_subtitle"])]
    right = [_p("BRAND MEETING REVIEW", theme["card_label"]), _p(f"Mode: {packet_mode.title()}  |  Generated: {generated_at}", theme["tiny_v2"])]
    table = Table([[left, right]], colWidths=[4.7 * inch, 2.8 * inch])
    table.setStyle(TableStyle([
        ("BACKGROUND", (0, 0), (-1, -1), colors.white),
        ("LINEBELOW", (0, 0), (-1, -1), 1.1, colors.HexColor("#2F6B5D")),
        ("VALIGN", (0, 0), (-1, -1), "TOP"),
        ("LEFTPADDING", (0, 0), (-1, -1), 0),
        ("RIGHTPADDING", (0, 0), (-1, -1), 0),
        ("BOTTOMPADDING", (0, 0), (-1, -1), 6),
    ]))
    return table


def _calc_sell_through(units_sold: Any, ending_quantity: Any) -> float:
    try:
        sold = float(units_sold)
    except Exception:
        sold = 0.0
    try:
        qty = float(ending_quantity)
    except Exception:
        qty = 0.0
    if not np.isfinite(sold):
        sold = 0.0
    if not np.isfinite(qty):
        qty = 0.0
    denom = max(sold + qty, 1.0)
    return sold / denom


def _series_or_default(df: pd.DataFrame, column: str, default: Any = "") -> pd.Series:
    if column in df.columns:
        return df[column]
    return pd.Series(default, index=df.index)


def _enriched_product_inventory(product_60: pd.DataFrame, inv_products: pd.DataFrame) -> pd.DataFrame:
    inv = inv_products.copy() if inv_products is not None else pd.DataFrame()
    prod = product_60.copy() if product_60 is not None else pd.DataFrame()
    if inv.empty and prod.empty:
        return pd.DataFrame()
    if inv.empty:
        inv = pd.DataFrame(columns=["merge_key", "display_product", "category_normalized", "units_available", "inventory_value", "trend_units_per_day_30d", "days_of_supply"])
    if prod.empty:
        prod = pd.DataFrame(columns=[
            "product_group_key", "product_group_display", "net_revenue", "units", "profit_real", "margin_real",
            "discount_rate", "category_normalized", "size_normalized", "variant_type",
        ])
    inv_key = "merge_key" if "merge_key" in inv.columns else "display_product"
    prod_key = "product_group_key" if "product_group_key" in prod.columns else "product_group_display"
    inv[inv_key] = _series_or_default(inv, inv_key, "").fillna("").astype(str)
    prod[prod_key] = _series_or_default(prod, prod_key, "").fillna("").astype(str)
    keep_prod = [
        c for c in [
            prod_key, "product_group_display", "net_revenue", "units", "profit_real", "margin_real",
            "discount_rate", "category_normalized", "size_normalized", "variant_type",
        ] if c in prod.columns
    ]
    out = inv.merge(prod[keep_prod], left_on=inv_key, right_on=prod_key, how="outer", suffixes=("_inv", "_sales"))
    fallback_series = pd.Series("", index=out.index)
    left_key = out[inv_key] if inv_key in out.columns else fallback_series
    right_key = out[prod_key] if prod_key in out.columns else fallback_series
    out["product_key"] = left_key.fillna(right_key).astype(str)
    display_series = out["display_product"] if "display_product" in out.columns else fallback_series
    product_group_series = out["product_group_display"] if "product_group_display" in out.columns else fallback_series
    out["product"] = display_series.replace("", np.nan).fillna(product_group_series).astype(str)
    category_series = out["category_normalized_sales"] if "category_normalized_sales" in out.columns else (
        out["category_normalized"] if "category_normalized" in out.columns else fallback_series
    )
    out["category"] = category_series.fillna("").astype(str)
    for col in ["net_revenue", "units", "profit_real", "margin_real", "discount_rate", "units_available", "inventory_value", "trend_units_per_day_30d", "days_of_supply"]:
        out[col] = pd.to_numeric(_series_or_default(out, col, 0.0), errors="coerce").replace([np.inf, -np.inf], np.nan).fillna(0.0)
    out["sell_through"] = out.apply(lambda r: _calc_sell_through(r.get("units", 0.0), r.get("units_available", 0.0)), axis=1)
    return out


def compute_inventory_risk_v2(product_60: pd.DataFrame, inv_products: pd.DataFrame) -> pd.DataFrame:
    out = _enriched_product_inventory(product_60, inv_products)
    if out.empty:
        return out
    risks: List[str] = []
    actions: List[str] = []
    scores: List[float] = []
    for _, r in out.iterrows():
        revenue = float(r.get("net_revenue", 0.0) or 0.0)
        inv_value = float(r.get("inventory_value", 0.0) or 0.0)
        sell = float(r.get("sell_through", 0.0) or 0.0)
        days = float(r.get("days_of_supply", 0.0) or 0.0)
        units = float(r.get("units_available", 0.0) or 0.0)
        high = (days > 90 and units > 0) or (sell < 0.15 and inv_value >= 1000) or (revenue <= 0 and inv_value >= 1000)
        med = (days > 60 and units > 0) or (sell < 0.25 and inv_value > 0)
        if high:
            risk = "High"
        elif med:
            risk = "Medium"
        else:
            risk = "Low"
        if revenue <= 0 and inv_value >= 1000:
            action = "Feature in promo"
        elif sell < 0.10 and inv_value >= 1000:
            action = "Fund markdown"
        elif days > 90:
            action = "Buy back"
        elif days > 60:
            action = "Transfer"
        elif float(r.get("trend_units_per_day_30d", 0.0) or 0.0) > 2 and days < 21:
            action = "Restock"
        else:
            action = "Watch depth"
        score = (100 if risk == "High" else 40 if risk == "Medium" else 0) + inv_value / 1000.0 + max(days - 30, 0) / 10.0
        risks.append(risk)
        actions.append(action)
        scores.append(score)
    out["risk"] = risks
    out["action"] = actions
    out["risk_score"] = scores
    return out.sort_values(["risk_score", "inventory_value"], ascending=False)


def compute_slow_movers_v2(product_60: pd.DataFrame, inv_products: pd.DataFrame) -> pd.DataFrame:
    risk = compute_inventory_risk_v2(product_60, inv_products)
    if risk.empty:
        return risk
    return risk[risk["risk"].isin(["High", "Medium"])].sort_values(["risk_score", "inventory_value"], ascending=False)


def compute_fast_movers_v2(product_60: pd.DataFrame, inv_products: pd.DataFrame) -> pd.DataFrame:
    risk = compute_inventory_risk_v2(product_60, inv_products)
    if risk.empty:
        return risk
    out = risk[(risk["units"] > 0) | (risk["trend_units_per_day_30d"] > 0)].copy()
    out["fast_score"] = out["units"] + (out["trend_units_per_day_30d"] * 10)
    out["action"] = np.where((out["units_available"] <= 0) | (out["days_of_supply"].between(0.01, 21)), "Restock", "Watch depth")
    return out.sort_values(["fast_score", "net_revenue"], ascending=False)


def _dashboard_float(value: Any, default: float = 0.0) -> float:
    try:
        out = float(value)
    except Exception:
        return default
    if not np.isfinite(out):
        return default
    return out


def _dashboard_optional_float(value: Any) -> float:
    try:
        out = float(value)
    except Exception:
        return float("nan")
    return out if np.isfinite(out) else float("nan")


def dashboard_days_supply(units_on_hand: Any, units_per_day: Any) -> float:
    units = _dashboard_float(units_on_hand, 0.0)
    velocity = _dashboard_float(units_per_day, 0.0)
    if units <= 0:
        return 0.0
    if velocity <= 0:
        return float("nan")
    return units / velocity


def dashboard_sell_through(units_sold: Any, units_on_hand: Any) -> float:
    sold = _dashboard_float(units_sold, 0.0)
    on_hand = _dashboard_float(units_on_hand, 0.0)
    denom = sold + on_hand
    if denom <= 0:
        return float("nan")
    return sold / denom


def dashboard_credit_gap_pct_sales(credit_gap: Any, net_sales: Any) -> float:
    return safe_div(credit_gap, net_sales, 0.0)


def _shorten_product_name(product_name: Any, max_chars: int = 44) -> str:
    limit = max(4, int(max_chars or 44))
    text = short_product_label(product_name, max_chars=limit)
    if len(text) <= limit:
        return text
    return text[: limit - 3].rstrip(" |-/") + "..."


def _format_delta(value: Optional[float], current: Any = 0.0, prior: Any = 0.0) -> str:
    try:
        val = float(value) if value is not None else float("nan")
    except Exception:
        val = float("nan")
    cur = _dashboard_float(current, 0.0)
    try:
        base = float(prior)
    except Exception:
        base = float("nan")
    if not np.isfinite(val):
        if cur > 0 and np.isfinite(base) and base == 0:
            return "New"
        return "n/a"
    return pct1(val)


def _dashboard_product_key_counts(report_df: pd.DataFrame) -> Tuple[Dict[str, int], Dict[str, date]]:
    if report_df is None or report_df.empty:
        return {}, {}
    tmp = _filter_product_group_rows(report_df.copy())
    if tmp.empty:
        return {}, {}
    tmp = _apply_weekly_ordering_product_identity(tmp, include_store=False)
    key_col = "ordering_product_key" if "ordering_product_key" in tmp.columns else (
        "product_group_key" if "product_group_key" in tmp.columns else "merge_key"
    )
    if key_col not in tmp.columns:
        return {}, {}
    tmp[key_col] = tmp[key_col].fillna("").astype(str)
    tmp = tmp[tmp[key_col] != ""].copy()
    if tmp.empty:
        return {}, {}
    if "_store_abbr" not in tmp.columns:
        tmp["_store_abbr"] = ""
    store_counts = tmp.groupby(key_col)["_store_abbr"].nunique().to_dict()
    last_sales: Dict[str, date] = {}
    if "_date" in tmp.columns:
        dt = pd.to_datetime(tmp["_date"], errors="coerce")
        tmp = tmp.assign(__date=dt)
        max_dates = tmp.dropna(subset=["__date"]).groupby(key_col)["__date"].max()
        last_sales = {str(k): v.date() for k, v in max_dates.items()}
    return {str(k): int(v) for k, v in store_counts.items()}, last_sales


def _classify_product_action(
    row: Dict[str, Any],
    target_margin: float = DEAL_TARGET_MARGIN,
    selected_store_count: int = 0,
) -> Tuple[str, str, str]:
    units_per_day = _dashboard_float(row.get("units_per_day"), 0.0)
    sales_delta = row.get("sales_vs_prior_pct")
    sales_delta_val = _dashboard_optional_float(sales_delta)
    margin = _dashboard_float(row.get("margin_pct"), 0.0)
    discount = _dashboard_float(row.get("discount_pct"), 0.0)
    on_hand = _dashboard_float(row.get("inventory_units"), 0.0)
    inv_value = _dashboard_float(row.get("inventory_value"), 0.0)
    days = _dashboard_optional_float(row.get("days_supply"))
    sell = _dashboard_float(row.get("sell_through_pct"), 0.0)
    stores = int(_dashboard_float(row.get("stores_selling"), 0.0))
    target = _dashboard_float(target_margin, DEAL_TARGET_MARGIN)

    effective_days = days if np.isfinite(days) else (999.0 if on_hand > 0 else 0.0)
    declining = np.isfinite(sales_delta_val) and sales_delta_val < -0.20
    healthy_margin = margin >= (target - 0.03)
    margin_weak = margin < (target - 0.07)
    low_coverage = selected_store_count > 0 and stores < max(2, math.ceil(selected_store_count * 0.65))

    if on_hand > 0 and (effective_days >= 150 or (units_per_day <= 0.05 and sell <= 0.12 and inv_value >= 500)) and (declining or margin_weak or sell <= 0.12):
        return "Cut / Buyback", "Critical", "Ask for buyback or stop buying until inventory clears."
    if on_hand > 0 and (effective_days >= 75 or sell <= 0.20) and units_per_day < 0.50:
        return "Discount / Move", "High", "Fund markdown, transfer inventory, or add promo support."
    if units_per_day >= 1.25 and healthy_margin and effective_days <= 45 and sell >= 0.35:
        return "Reorder / Expand", "Low", "Reorder and expand placement where store coverage is thin."
    if units_per_day >= 0.60 and margin >= (target - 0.08) and low_coverage:
        return "Grow with Promo", "Low", "Use promo support to expand into more stores."
    if declining or margin_weak or discount >= 0.45:
        return "Watch", "Medium", "Review pricing, discounting, and next buy before reordering."
    if units_per_day > 0 or on_hand > 0:
        return "Keep", "Low", "Maintain current depth and monitor next window."
    return "Watch", "Medium", "No current movement. Confirm menu status and buying plan."


def _classify_store_action(row: Dict[str, Any], target_margin: float = DEAL_TARGET_MARGIN) -> str:
    margin = _dashboard_float(row.get("margin_pct"), 0.0)
    discount = _dashboard_float(row.get("discount_pct"), 0.0)
    days = _dashboard_optional_float(row.get("days_supply"))
    sell = _dashboard_float(row.get("sell_through_pct"), 0.0)
    sales_delta = _dashboard_optional_float(row.get("sales_vs_prior_pct"))
    units_delta = _dashboard_optional_float(row.get("units_vs_prior_pct"))
    target = _dashboard_float(target_margin, DEAL_TARGET_MARGIN)
    effective_days = days if np.isfinite(days) else (999.0 if _dashboard_float(row.get("inventory_units"), 0.0) > 0 else 0.0)
    if margin < target - 0.06:
        return "Fix Margin"
    if effective_days > 90 or (sell > 0 and sell < 0.18):
        return "Move Inventory"
    if np.isfinite(sales_delta) and sales_delta < -0.15:
        return "Needs Promo"
    if discount > 0.45 and margin < target:
        return "Needs Promo"
    if np.isfinite(sales_delta) and sales_delta > 0.15 and (not np.isfinite(units_delta) or units_delta >= -0.05):
        return "Grow"
    if effective_days > 60:
        return "Reduce Buying"
    return "Good"


def _dashboard_action_priority(action: Any) -> int:
    return {
        "Cut / Buyback": 1,
        "Discount / Move": 2,
        "Reorder / Expand": 3,
        "Grow with Promo": 4,
        "Watch": 5,
        "Keep": 6,
    }.get(str(action), 9)


def build_dashboard_product_decision_board(
    product_60: pd.DataFrame,
    inv_products: pd.DataFrame,
    prior_product: Optional[pd.DataFrame],
    report_days: int,
    selected_store_count: int,
    store_count_map: Optional[Dict[str, int]] = None,
    last_sale_map: Optional[Dict[str, date]] = None,
    max_products: int = 20,
    target_margin: float = DEAL_TARGET_MARGIN,
    include_prior_data: bool = True,
) -> pd.DataFrame:
    cols = [
        "action", "risk", "product_key", "product_name", "product_short", "category", "size_type",
        "net_sales", "gross_profit", "units_sold", "units_per_day", "sales_vs_prior_pct",
        "margin_pct", "discount_pct", "inventory_units", "inventory_value", "days_supply",
        "sell_through_pct", "stores_selling", "last_sale_date", "recommendation", "decision_priority",
    ]
    base = _enriched_product_inventory(product_60, inv_products)
    if base.empty:
        return pd.DataFrame(columns=cols)
    store_count_map = store_count_map or {}
    last_sale_map = last_sale_map or {}
    days_n = max(1, int(report_days or 1))

    for col in ["net_revenue", "units", "margin_real", "discount_rate", "units_available", "inventory_value"]:
        base[col] = pd.to_numeric(base.get(col, 0.0), errors="coerce").replace([np.inf, -np.inf], np.nan).fillna(0.0)
    base["product_key"] = base.get("product_key", "").fillna("").astype(str)

    prior = prior_product.copy() if prior_product is not None else pd.DataFrame()
    if include_prior_data and prior is not None and not prior.empty:
        prior_key = "product_group_key" if "product_group_key" in prior.columns else (
            "merge_key" if "merge_key" in prior.columns else "product_group_display"
        )
        prior[prior_key] = prior[prior_key].fillna("").astype(str)
        prior_keep = prior[[c for c in [prior_key, "net_revenue", "units", "profit_real"] if c in prior.columns]].copy()
        prior_keep = prior_keep.rename(columns={
            prior_key: "product_key",
            "net_revenue": "prior_net_sales",
            "units": "prior_units",
            "profit_real": "prior_gross_profit",
        })
        base = base.merge(prior_keep, on="product_key", how="left")
    for col in ["prior_net_sales", "prior_units", "prior_gross_profit"]:
        if col not in base.columns:
            base[col] = np.nan if not include_prior_data else 0.0
        base[col] = pd.to_numeric(base[col], errors="coerce")
        if include_prior_data:
            base[col] = base[col].fillna(0.0)

    rows: List[Dict[str, Any]] = []
    for _, r in base.iterrows():
        product_key = str(r.get("product_key") or "")
        units = _dashboard_float(r.get("units"), 0.0)
        units_per_day = safe_div(units, days_n, 0.0)
        on_hand = _dashboard_float(r.get("units_available"), 0.0)
        inv_value = _dashboard_float(r.get("inventory_value"), 0.0)
        days_supply = dashboard_days_supply(on_hand, units_per_day)
        sell = dashboard_sell_through(units, on_hand)
        net = _dashboard_float(r.get("net_revenue"), 0.0)
        profit = _dashboard_float(r.get("profit_real"), 0.0)
        out = {
            "product_key": product_key,
            "product_name": str(r.get("product") or r.get("product_group_display") or r.get("display_product") or "Unknown"),
            "category": str(r.get("category") or r.get("category_normalized") or "UNKNOWN"),
            "size_type": " / ".join([str(r.get(c, "")).strip() for c in ["size_normalized", "variant_type"] if str(r.get(c, "")).strip()]),
            "net_sales": net,
            "gross_profit": profit,
            "units_sold": units,
            "units_per_day": units_per_day,
            "sales_vs_prior_pct": pct_change(net, r.get("prior_net_sales")) if include_prior_data else float("nan"),
            "margin_pct": _dashboard_float(r.get("margin_real"), 0.0),
            "discount_pct": _dashboard_float(r.get("discount_rate"), 0.0),
            "inventory_units": on_hand,
            "inventory_value": inv_value,
            "days_supply": days_supply,
            "sell_through_pct": sell,
            "stores_selling": int(store_count_map.get(product_key, 0)),
            "last_sale_date": last_sale_map.get(product_key, ""),
        }
        action, risk, recommendation = _classify_product_action(out, target_margin=target_margin, selected_store_count=selected_store_count)
        out["action"] = action
        out["risk"] = risk
        out["recommendation"] = recommendation
        out["decision_priority"] = _dashboard_action_priority(action)
        out["product_short"] = _shorten_product_name(out["product_name"], 44)
        rows.append(out)

    df = pd.DataFrame(rows, columns=cols)
    if df.empty:
        return df
    df["__days_sort"] = pd.to_numeric(df["days_supply"], errors="coerce").replace([np.inf, -np.inf], np.nan).fillna(999.0)
    df = df.sort_values(
        ["decision_priority", "inventory_value", "units_per_day", "net_sales", "__days_sort"],
        ascending=[True, False, False, False, False],
    ).drop(columns=["__days_sort"])
    return df.head(max(1, int(max_products or 20))).reset_index(drop=True)


def build_dashboard_fast_movers(decision_board: pd.DataFrame, max_products: int = 20) -> pd.DataFrame:
    if decision_board is None or decision_board.empty:
        return pd.DataFrame(columns=decision_board.columns if decision_board is not None else [])
    df = decision_board.copy()
    for col in ["units_per_day", "net_sales", "gross_profit", "sell_through_pct"]:
        df[col] = pd.to_numeric(df.get(col, 0.0), errors="coerce").replace([np.inf, -np.inf], np.nan).fillna(0.0)
    df["fast_score"] = (df["units_per_day"] * 100.0) + (df["net_sales"] / 100.0) + (df["gross_profit"] / 120.0) + (df["sell_through_pct"] * 25.0)
    return df[df["units_sold"].fillna(0.0).astype(float) > 0].sort_values(["fast_score", "net_sales"], ascending=False).head(max(1, int(max_products or 20))).reset_index(drop=True)


def build_dashboard_slow_movers(decision_board: pd.DataFrame, max_products: int = 20) -> pd.DataFrame:
    if decision_board is None or decision_board.empty:
        return pd.DataFrame(columns=decision_board.columns if decision_board is not None else [])
    df = decision_board.copy()
    risk_order = {"Critical": 1, "High": 2, "Medium": 3, "Low": 4}
    df["risk_priority"] = df["risk"].map(risk_order).fillna(9)
    df["days_sort"] = pd.to_numeric(df.get("days_supply", 0.0), errors="coerce").replace([np.inf, -np.inf], np.nan).fillna(999.0)
    mask = df["risk"].isin(["Critical", "High", "Medium"]) | df["action"].isin(["Cut / Buyback", "Discount / Move", "Watch"])
    return df[mask].sort_values(["risk_priority", "inventory_value", "days_sort"], ascending=[True, False, False]).head(max(1, int(max_products or 20))).reset_index(drop=True)


def build_dashboard_store_matrix(
    store_60: pd.DataFrame,
    store_sales_packets: Dict[str, Dict[str, Any]],
    selected_store_codes: Sequence[str],
    inv_store: pd.DataFrame,
    total_net_sales: float,
    target_margin: float = DEAL_TARGET_MARGIN,
) -> pd.DataFrame:
    cols = [
        "store", "net_sales", "sales_share_pct", "sales_vs_prior_pct", "units", "units_vs_prior_pct",
        "margin_pct", "discount_pct", "inventory_value", "inventory_units", "days_supply",
        "sell_through_pct", "fastest_product", "slowest_product", "store_action",
    ]
    store_df = store_60.copy() if store_60 is not None else pd.DataFrame()
    if not store_df.empty and "_store_abbr" in store_df.columns:
        store_df["_store_abbr"] = store_df["_store_abbr"].fillna("").astype(str).str.upper()
        store_df = store_df.set_index("_store_abbr", drop=False)
    inv_df = inv_store.copy() if inv_store is not None else pd.DataFrame()
    if not inv_df.empty and "_store_abbr" in inv_df.columns:
        inv_df["_store_abbr"] = inv_df["_store_abbr"].fillna("").astype(str).str.upper()
        inv_df = inv_df.set_index("_store_abbr", drop=False)
    rows: List[Dict[str, Any]] = []
    for abbr in order_store_codes(selected_store_codes):
        sales_row = store_df.loc[abbr] if abbr in getattr(store_df, "index", []) else {}
        inv_row = inv_df.loc[abbr] if abbr in getattr(inv_df, "index", []) else {}
        pkt = store_sales_packets.get(abbr, {}) if store_sales_packets else {}
        metrics = ((pkt.get("window_metrics", {}) or {}).get("report") or {})
        prior = ((pkt.get("window_metrics", {}) or {}).get("prior_report") or {})
        inv = pkt.get("inventory", {}) or {}
        net = _dashboard_float(metrics.get("net_revenue", getattr(sales_row, "net_revenue", 0.0)), 0.0)
        units = _dashboard_float(metrics.get("items", getattr(sales_row, "items", 0.0)), 0.0)
        inv_units = _dashboard_float(inv.get("units_available", getattr(inv_row, "units_available", 0.0)), 0.0)
        inv_value = _dashboard_float(inv.get("inventory_value", getattr(inv_row, "inventory_value", 0.0)), 0.0)
        days_supply = _dashboard_optional_float(inv.get("days_of_supply", getattr(inv_row, "days_of_supply", np.nan)))
        sell = dashboard_sell_through(units, inv_units)
        product = pkt.get("product_60", pd.DataFrame())
        fastest = ""
        if product is not None and not product.empty:
            fast_df = product.copy()
            fast_df["units"] = pd.to_numeric(fast_df.get("units", 0.0), errors="coerce").fillna(0.0)
            fastest = str(fast_df.sort_values(["units", "net_revenue"], ascending=False).iloc[0].get("product_group_display", ""))
        risk_df = compute_slow_movers_v2(product, pkt.get("inventory_products", pd.DataFrame()))
        slowest = str(risk_df.iloc[0].get("product", "")) if risk_df is not None and not risk_df.empty else ""
        out = {
            "store": abbr,
            "net_sales": net,
            "sales_share_pct": safe_div(net, total_net_sales, 0.0),
            "sales_vs_prior_pct": pct_change(net, prior.get("net_revenue", 0.0)),
            "units": units,
            "units_vs_prior_pct": pct_change(units, prior.get("items", 0.0)),
            "margin_pct": _dashboard_float(metrics.get("margin_real"), 0.0),
            "discount_pct": _dashboard_float(metrics.get("discount_rate"), 0.0),
            "inventory_value": inv_value,
            "inventory_units": inv_units,
            "days_supply": days_supply,
            "sell_through_pct": sell,
            "fastest_product": _shorten_product_name(fastest, 34),
            "slowest_product": _shorten_product_name(slowest, 34),
        }
        out["store_action"] = _classify_store_action(out, target_margin=target_margin)
        rows.append(out)
    return pd.DataFrame(rows, columns=cols)


def build_dashboard_category_mix(
    category_60: pd.DataFrame,
    inv_category: pd.DataFrame,
    product_60: pd.DataFrame,
    total_net_sales: float,
    report_days: int,
    target_margin: float = DEAL_TARGET_MARGIN,
) -> pd.DataFrame:
    cols = [
        "category", "net_sales", "sales_share_pct", "units", "margin_pct", "discount_pct",
        "inventory_value", "inventory_units", "days_supply", "sell_through_pct", "top_product", "category_action",
    ]
    cat = category_60.copy() if category_60 is not None else pd.DataFrame()
    inv = inv_category.copy() if inv_category is not None else pd.DataFrame()
    if cat.empty and inv.empty:
        return pd.DataFrame(columns=cols)
    if "category_normalized" not in cat.columns:
        cat["category_normalized"] = ""
    if "category_normalized" not in inv.columns:
        inv["category_normalized"] = ""
    cat["category_normalized"] = cat["category_normalized"].fillna("").astype(str)
    inv["category_normalized"] = inv["category_normalized"].fillna("").astype(str)
    merged = cat.merge(inv, on="category_normalized", how="outer", suffixes=("", "_inv"))
    prod = product_60.copy() if product_60 is not None else pd.DataFrame()
    top_map: Dict[str, str] = {}
    if not prod.empty and "category_normalized" in prod.columns:
        prod["category_normalized"] = prod["category_normalized"].fillna("").astype(str)
        prod["net_revenue"] = pd.to_numeric(prod.get("net_revenue", 0.0), errors="coerce").fillna(0.0)
        for cat_name, part in prod.sort_values("net_revenue", ascending=False).groupby("category_normalized"):
            top_map[str(cat_name)] = str(part.iloc[0].get("product_group_display", ""))
    rows: List[Dict[str, Any]] = []
    for _, r in merged.iterrows():
        category = str(r.get("category_normalized") or "UNKNOWN")
        net = _dashboard_float(r.get("net_revenue"), 0.0)
        units = _dashboard_float(r.get("items"), _dashboard_float(r.get("units"), 0.0))
        inv_units = _dashboard_float(r.get("units_available"), 0.0)
        units_per_day = safe_div(units, max(1, int(report_days or 1)), 0.0)
        days_supply = dashboard_days_supply(inv_units, units_per_day)
        sell = dashboard_sell_through(units, inv_units)
        margin = _dashboard_float(r.get("margin_real"), 0.0)
        discount = _dashboard_float(r.get("discount_rate"), 0.0)
        if margin < target_margin - 0.06:
            action = "Fix Margin"
        elif inv_units > 0 and (_dashboard_optional_float(days_supply) > 90 or _dashboard_float(sell, 0.0) < 0.18):
            action = "Reduce"
        elif net > 0 and margin >= target_margin:
            action = "Grow"
        else:
            action = "Watch"
        rows.append({
            "category": category,
            "net_sales": net,
            "sales_share_pct": safe_div(net, total_net_sales, 0.0),
            "units": units,
            "margin_pct": margin,
            "discount_pct": discount,
            "inventory_value": _dashboard_float(r.get("inventory_value"), 0.0),
            "inventory_units": inv_units,
            "days_supply": days_supply,
            "sell_through_pct": sell,
            "top_product": _shorten_product_name(top_map.get(category, ""), 36),
            "category_action": action,
        })
    return pd.DataFrame(rows, columns=cols).sort_values(["net_sales", "inventory_value"], ascending=False).reset_index(drop=True)


def build_dashboard_credit_margin_summary(
    report_metrics: Dict[str, Any],
    prior_metrics: Dict[str, Any],
    inv_overview: Dict[str, Any],
    credit_summary: Dict[str, Any],
    target_margin: float,
    selected_store_codes: Sequence[str],
) -> pd.DataFrame:
    net = _dashboard_float(report_metrics.get("net_revenue"), 0.0)
    units = _dashboard_float(report_metrics.get("items"), 0.0)
    profit = _dashboard_float(report_metrics.get("profit_real", report_metrics.get("profit")), 0.0)
    margin = _dashboard_float(report_metrics.get("margin_real"), 0.0)
    discount = _dashboard_float(report_metrics.get("discount_rate"), 0.0)
    inv_units = _dashboard_float(inv_overview.get("units"), 0.0)
    sell = dashboard_sell_through(units, inv_units)
    credit_gap = _dashboard_float(credit_summary.get("credit_gap"), 0.0)
    return pd.DataFrame([{
        "net_sales": net,
        "units_sold": units,
        "gross_profit": profit,
        "real_margin_pct": margin,
        "target_margin_pct": target_margin,
        "margin_gap_pp": pp_change(margin, target_margin),
        "discount_pct": discount,
        "inventory_value": _dashboard_float(inv_overview.get("inventory_value"), 0.0),
        "inventory_units": inv_units,
        "days_supply": _dashboard_optional_float(inv_overview.get("days_of_supply")),
        "sell_through_pct": sell,
        "credit_gap": credit_gap,
        "credit_gap_pct_sales": dashboard_credit_gap_pct_sales(credit_gap, net),
        "stores_active": len([s for s in selected_store_codes if str(s).strip()]),
        "sales_vs_prior_pct": pct_change(net, prior_metrics.get("net_revenue", 0.0)),
        "units_vs_prior_pct": pct_change(units, prior_metrics.get("items", 0.0)),
        "profit_vs_prior_pct": pct_change(profit, prior_metrics.get("profit_real", prior_metrics.get("profit", 0.0))),
        "margin_change_pp": pp_change(margin, prior_metrics.get("margin_real", 0.0)),
        "manual_expected_credit": _dashboard_float(credit_summary.get("manual_expected_credit", credit_summary.get("ledger_expected_credit")), 0.0),
        "manual_received_credit": _dashboard_float(credit_summary.get("manual_received_credit", credit_summary.get("ledger_received_credit")), 0.0),
        "creditflow_expected_credit": _dashboard_float(credit_summary.get("creditflow_expected_credit"), 0.0),
        "creditflow_received_credit": _dashboard_float(credit_summary.get("creditflow_received_credit"), 0.0),
    }])


def _classify_brand_status(snapshot_row: Dict[str, Any], target_margin: float = DEAL_TARGET_MARGIN) -> str:
    margin = _dashboard_float(snapshot_row.get("real_margin_pct"), 0.0)
    margin_gap = margin - _dashboard_float(target_margin, DEAL_TARGET_MARGIN)
    discount = _dashboard_float(snapshot_row.get("discount_pct"), 0.0)
    days = _dashboard_optional_float(snapshot_row.get("days_supply"))
    credit_gap = _dashboard_float(snapshot_row.get("credit_gap"), 0.0)
    credit_gap_pct = _dashboard_float(snapshot_row.get("credit_gap_pct_sales"), 0.0)
    sales_delta = _dashboard_optional_float(snapshot_row.get("sales_vs_prior_pct"))
    sell = _dashboard_float(snapshot_row.get("sell_through_pct"), 0.0)
    if credit_gap >= 1000 or credit_gap_pct >= 0.08 or margin_gap < -0.08:
        return "Fix"
    if np.isfinite(days) and days > 120 and sell < 0.15:
        return "Exit / Buyback"
    if np.isfinite(days) and days > 75:
        return "Reduce"
    if margin_gap < -0.04 or discount > 0.45 or (np.isfinite(sales_delta) and sales_delta < -0.15):
        return "Watch"
    if np.isfinite(sales_delta) and sales_delta > 0.12 and margin >= target_margin:
        return "Grow"
    return "Healthy"


def _dashboard_brand_ask(status: str, snapshot_row: Dict[str, Any], meeting_ask: str) -> str:
    if str(meeting_ask or "").strip():
        return str(meeting_ask).strip()
    credit_gap = _dashboard_float(snapshot_row.get("credit_gap"), 0.0)
    margin_gap = _dashboard_float(snapshot_row.get("margin_gap_pp"), 0.0)
    if credit_gap > 0:
        return f"Ask brand for {money0(credit_gap)} in credit support and a promo plan for slow inventory."
    if margin_gap < -0.04:
        return f"Margin is below target by {abs(margin_gap) * 100:.1f}pp. Fix cost, price, or discount support before the next reorder."
    if status == "Grow":
        return "Grow the fastest products in under-covered stores and protect margin on the next buy."
    if status in {"Reduce", "Exit / Buyback"}:
        return "Reduce future buys and discuss buyback, markdown, or transfer support for stuck inventory."
    return "Confirm next promo calendar, reorder timing, and support for any slow products."


def build_dashboard_packet_data(
    *,
    product_60: pd.DataFrame,
    inv_products: pd.DataFrame,
    prior_product: pd.DataFrame,
    report_df: pd.DataFrame,
    report_days: int,
    selected_store_codes: Sequence[str],
    store_60: pd.DataFrame,
    store_sales_packets: Dict[str, Dict[str, Any]],
    inv_store: pd.DataFrame,
    category_60: pd.DataFrame,
    inv_category: pd.DataFrame,
    window_metrics: Dict[str, Dict[str, float]],
    inv_overview: Dict[str, float],
    credit_summary: Dict[str, Any],
    target_margin: float,
    max_products: int = 20,
    include_prior_data: bool = True,
    meeting_ask: str = "",
) -> Dict[str, Any]:
    store_count_map, last_sale_map = _dashboard_product_key_counts(report_df)
    snapshot = build_dashboard_credit_margin_summary(
        window_metrics.get("report", {}),
        window_metrics.get("prior_report", {}),
        inv_overview,
        credit_summary,
        target_margin,
        selected_store_codes,
    )
    if not snapshot.empty and not include_prior_data:
        for col in ["sales_vs_prior_pct", "units_vs_prior_pct", "profit_vs_prior_pct", "margin_change_pp"]:
            snapshot.loc[:, col] = np.nan
    product_decisions = build_dashboard_product_decision_board(
        product_60,
        inv_products,
        prior_product,
        report_days=report_days,
        selected_store_count=len(selected_store_codes),
        store_count_map=store_count_map,
        last_sale_map=last_sale_map,
        max_products=max_products,
        target_margin=target_margin,
        include_prior_data=include_prior_data,
    )
    fast_movers = build_dashboard_fast_movers(product_decisions, max_products=max_products)
    slow_movers = build_dashboard_slow_movers(product_decisions, max_products=max_products)
    store_matrix = build_dashboard_store_matrix(
        store_60,
        store_sales_packets,
        selected_store_codes,
        inv_store,
        total_net_sales=_dashboard_float(window_metrics.get("report", {}).get("net_revenue"), 0.0),
        target_margin=target_margin,
    )
    if not include_prior_data and not store_matrix.empty:
        store_matrix.loc[:, ["sales_vs_prior_pct", "units_vs_prior_pct"]] = np.nan
    category_mix = build_dashboard_category_mix(
        category_60,
        inv_category,
        product_60,
        total_net_sales=_dashboard_float(window_metrics.get("report", {}).get("net_revenue"), 0.0),
        report_days=report_days,
        target_margin=target_margin,
    )
    status = _classify_brand_status(snapshot.iloc[0].to_dict(), target_margin) if not snapshot.empty else "Watch"
    ask = _dashboard_brand_ask(status, snapshot.iloc[0].to_dict() if not snapshot.empty else {}, meeting_ask)
    snapshot = snapshot.copy()
    if snapshot.empty:
        snapshot = pd.DataFrame([{}])
    snapshot.loc[:, "brand_status"] = status
    snapshot.loc[:, "meeting_ask"] = ask
    return {
        "snapshot": snapshot,
        "product_decision_board": product_decisions,
        "fast_movers": fast_movers,
        "slow_movers": slow_movers,
        "store_matrix": store_matrix,
        "category_mix": category_mix,
        "credit_margin_summary": snapshot.copy(),
        "brand_status": status,
        "meeting_ask": ask,
    }


def write_dashboard_cache(cache_dir: Path, dashboard_data: Dict[str, Any], logger: Optional[Callable[[str], None]] = None) -> None:
    cache_dir.mkdir(parents=True, exist_ok=True)
    outputs = {
        "dashboard_brand_snapshot.csv": dashboard_data.get("snapshot", pd.DataFrame()),
        "dashboard_product_decision_board.csv": dashboard_data.get("product_decision_board", pd.DataFrame()),
        "dashboard_fast_movers.csv": dashboard_data.get("fast_movers", pd.DataFrame()),
        "dashboard_slow_movers.csv": dashboard_data.get("slow_movers", pd.DataFrame()),
        "dashboard_store_matrix.csv": dashboard_data.get("store_matrix", pd.DataFrame()),
        "dashboard_category_mix.csv": dashboard_data.get("category_mix", pd.DataFrame()),
        "dashboard_credit_margin_summary.csv": dashboard_data.get("credit_margin_summary", pd.DataFrame()),
    }
    for name, df in outputs.items():
        out_df = df if isinstance(df, pd.DataFrame) else pd.DataFrame()
        out_df.to_csv(cache_dir / name, index=False)
    _log(f"[QA] Wrote dashboard cache CSVs to {cache_dir}", logger)


def _premium_action_rows(action_items: Sequence[Dict[str, Any]], top_n: int) -> List[List[Any]]:
    rows = [["Priority", "Area", "Problem", "Evidence", "Brand Action", "Impact"]]
    action_map = {
        "Fund additional credit": "Fund credit",
        "Pay outstanding credit": "Pay credit",
        "Lower invoice cost": "Lower cost",
        "Reduce required discount": "Reduce discount",
        "Fund markdown": "Fund markdown",
        "Send rep for store training": "Train staff",
        "Increase kickback percentage": "Increase support",
        "Confirm invoice credit": "Confirm credit",
    }
    area_map = {
        "Margin Support": "Margin",
        "Credit Follow-Up": "Credit",
        "Discount Strategy": "Discount",
        "Slow Inventory": "Slow Inv",
        "Fast-Mover Replenishment": "Replenish",
        "Store-Level Support": "Store",
        "Store Support": "Store",
        "Product Mix": "Mix",
        "Training / Education": "Training",
    }
    def _money_impact(item: Dict[str, Any]) -> str:
        try:
            amount = float(item.get("dollar_amount", 0.0) or 0.0)
        except Exception:
            amount = 0.0
        return money0(amount) if amount else str(item.get("store", "") or "Brand")

    def _compact_evidence(value: Any) -> str:
        text = re.sub(r"\s+", " ", str(value or "")).strip()
        for marker in [" has $", " at $", " generated $"]:
            if marker in text:
                before, after = text.split(marker, 1)
                text = f"{short_product_label(before, 44)}{marker}{after}"
                break
        text = re.sub(r"(\d+(?:\.\d+)?) days of supply", r"\1d supply", text)
        text = text.replace("real margin is", "margin")
        if len(text) <= 76:
            return text
        return text[:75].rsplit(" ", 1)[0].rstrip(" ,.;") + "..."

    for item in list(action_items or [])[:top_n]:
        priority = str(item.get("priority", "Low")).upper().replace("MEDIUM", "MED")
        area_raw = str(item.get("category", "")).replace("Slow-Moving Inventory", "Slow Inventory")
        area = area_map.get(area_raw, area_raw[:12])
        action = action_map.get(str(item.get("brand_action", "")), str(item.get("brand_action", "")))
        impact = _money_impact(item)
        rows.append([
            priority,
            area,
            str(item.get("problem", ""))[:70],
            _compact_evidence(item.get("evidence", "")),
            action,
            impact,
        ])
    if len(rows) == 1:
        rows.append(["LOW", "Watch", "No major rule-based issue triggered.", "Review margin, inventory, and promo plan in meeting.", "Watch depth", "Brand"])
    return rows


def _store_needed_from_brand(row: pd.Series) -> str:
    status = str(row.get("status", "Watch"))
    margin = float(row.get("received_credit_margin", row.get("margin_real", 0.0)) or 0.0)
    discount = float(row.get("discount_rate", 0.0) or 0.0)
    days = float(row.get("days_of_supply", 0.0) or 0.0)
    if status == "Needs Support" or margin < 0.30:
        return "Fund credit"
    if discount > 0.45:
        return "Reduce discount"
    if days > 75:
        return "Move inventory"
    return "Support winners"


def _store_scorecard_rows(store_credit_scorecard: pd.DataFrame) -> List[List[Any]]:
    rows = [["Store", "Revenue", "Units", "Real", "Rec.", "Credit Gap", "Discount", "Inventory", "Days", "Status", "Need"]]
    if store_credit_scorecard is None or store_credit_scorecard.empty:
        return rows
    for _, r in store_credit_scorecard.iterrows():
        status = _short_status(r.get("status", ""))
        table_status = "Support" if status == "Needs Support" else status
        rows.append([
            str(r.get("_store_abbr", "")),
            money0(r.get("net_revenue", 0.0)),
            int0(r.get("items", 0.0)),
            pct1(r.get("margin_real", 0.0)),
            pct1(r.get("received_credit_margin", r.get("margin_real", 0.0))),
            money0(r.get("credit_gap_alloc", 0.0)),
            pct1(r.get("discount_rate", 0.0)),
            money0(r.get("inventory_value", 0.0)),
            _format_days_supply_v2(r.get("days_of_supply", np.nan), units_on_hand=r.get("units_available", 0.0)),
            table_status,
            _store_needed_from_brand(r),
        ])
    return rows


def _product_perf_rows(product_df: pd.DataFrame, mode: str) -> List[List[Any]]:
    rows = [["Product Group", "Category", "Revenue", "Units", "Real Margin", "Discount", "Days"]]
    if product_df is None or product_df.empty:
        return rows
    tmp = limit_rows_for_pdf(product_df.sort_values("net_revenue", ascending=False), mode, "top_products")
    for _, r in tmp.iterrows():
        rows.append([
            short_product_label(r.get("product_group_display", r.get("display_product", "")), 56),
            str(r.get("category_normalized", r.get("category", "")))[:14],
            money0(r.get("net_revenue", 0.0)),
            int0(r.get("units", 0.0)),
            pct1(r.get("margin_real", 0.0)),
            pct1(r.get("discount_rate", 0.0)),
            _format_days_supply_v2(r.get("days_of_supply", np.nan), units_per_day=r.get("trend_units_per_day_30d", None), units_on_hand=r.get("units_available", 0.0)),
        ])
    return rows


def _inventory_rows(df: pd.DataFrame, mode: str, section_type: str) -> List[List[Any]]:
    rows = [["Product", "Revenue", "Units", "On Hand", "Sell-Thru", "Days", "Action"]]
    if df is None or df.empty:
        return rows
    tmp = limit_rows_for_pdf(df, mode, section_type)
    for _, r in tmp.iterrows():
        rows.append([
            short_product_label(r.get("product", r.get("display_product", "")), 42),
            money0(r.get("net_revenue", 0.0)),
            int0(r.get("units", 0.0)),
            int0(r.get("units_available", 0.0)),
            pct1(r.get("sell_through", 0.0)),
            _format_days_supply_v2(r.get("days_of_supply", np.nan), r.get("trend_units_per_day_30d", None), r.get("units_available", 0.0)),
            str(r.get("action", "Watch depth")),
        ])
    return rows


def _category_rows(category_60: pd.DataFrame, mode: str) -> List[List[Any]]:
    rows = [["Category", "Revenue", "Units", "Margin", "Discount"]]
    if category_60 is None or category_60.empty:
        return rows
    n = 6 if mode == "quick" else 10
    for _, r in category_60.head(n).iterrows():
        rows.append([
            str(r.get("category_normalized", "")),
            money0(r.get("net_revenue", 0.0)),
            int0(r.get("items", 0.0)),
            pct1(r.get("margin_real", 0.0)),
            pct1(r.get("discount_rate", 0.0)),
        ])
    return rows


def _location_overview_rows(store_sales_packets: Dict[str, Dict[str, Any]]) -> List[List[Any]]:
    rows = [["Store", "Net Sales", "Gross", "Real Profit", "Margin", "Tickets", "Basket", "Units", "Discount", "Inv Value", "Days"]]
    for abbr in order_store_codes(list(store_sales_packets.keys())):
        pkt = store_sales_packets.get(abbr, {}) or {}
        metrics = ((pkt.get("window_metrics") or {}).get("report") or {})
        inv = pkt.get("inventory", {}) or {}
        rows.append([
            abbr,
            money0(metrics.get("net_revenue", 0.0)),
            money0(metrics.get("gross_sales", 0.0)),
            money0(metrics.get("profit_real", metrics.get("profit", 0.0))),
            pct1(metrics.get("margin_real", metrics.get("margin", 0.0))),
            int0(metrics.get("tickets", 0.0)),
            money2(metrics.get("basket", 0.0)),
            int0(metrics.get("items", 0.0)),
            pct1(metrics.get("discount_rate", 0.0)),
            money0(inv.get("inventory_value", 0.0)),
            _format_days_supply_v2(inv.get("days_of_supply", np.nan), inv.get("trend_units_per_day_30d", inv.get("trend_units_per_day_14d", 0.0)), inv.get("units_available", 0.0)),
        ])
    return rows


def _store_metric_detail_rows(metrics: Dict[str, Any], inv: Dict[str, Any]) -> List[List[Any]]:
    return [
        ["Metric", "Value"],
        ["Net Sales", money0(metrics.get("net_revenue", 0.0))],
        ["Gross Sales", money0(metrics.get("gross_sales", 0.0))],
        ["Real Profit", money0(metrics.get("profit_real", metrics.get("profit", 0.0)))],
        ["COGS", money0(metrics.get("cogs_real", metrics.get("cogs", 0.0)))],
        ["Real Margin", pct1(metrics.get("margin_real", metrics.get("margin", 0.0)))],
        ["Tickets", int0(metrics.get("tickets", 0.0))],
        ["Avg Basket", money2(metrics.get("basket", 0.0))],
        ["Units Sold", int0(metrics.get("items", 0.0))],
        ["Discounts", money0(metrics.get("discount", 0.0))],
        ["Discount Rate", pct1(metrics.get("discount_rate", 0.0))],
        ["Returns", money0(metrics.get("returns_net", 0.0))],
        ["Inventory Value", money0(inv.get("inventory_value", 0.0))],
        ["Units On Hand", int0(inv.get("units_available", inv.get("units", 0.0)))],
        ["Days Supply", _format_days_supply_v2(inv.get("days_of_supply", np.nan), inv.get("trend_units_per_day_30d", inv.get("trend_units_per_day_14d", 0.0)), inv.get("units_available", inv.get("units", 0.0)))],
    ]


def _section_page(story: List[Any], title: str, flowables: List[Any], page_break: bool = True) -> None:
    theme = build_brand_packet_theme_v2()
    story.append(Paragraph(title, theme["section_v2"]))
    story.extend(flowables)
    if page_break:
        story.append(PageBreak())


def _chart_image(buf: BytesIO, width: float, height: float) -> Any:
    return Image(buf, width=width, height=height) if buf and buf.getbuffer().nbytes > 0 else Spacer(1, 0.01 * inch)


def build_cover_dashboard_v2(
    story: List[Any],
    brand: str,
    start_day: date,
    end_day: date,
    options: PacketOptions,
    selected_store_codes: Sequence[str],
    generated_at: str,
    report_metrics: Dict[str, float],
    credit_summary: Dict[str, Any],
    inv_overview: Dict[str, float],
    inventory_risk: pd.DataFrame,
    action_items: Sequence[Dict[str, Any]],
    health_score: int,
    health_status: str,
    health_reason: str,
    meeting_ask: str,
) -> None:
    theme = build_brand_packet_theme_v2()
    sell_through = 0.0
    if inventory_risk is not None and not inventory_risk.empty:
        sold = float(pd.to_numeric(inventory_risk.get("units", 0.0), errors="coerce").fillna(0.0).sum())
        qty = float(pd.to_numeric(inventory_risk.get("units_available", 0.0), errors="coerce").fillna(0.0).sum())
        sell_through = _calc_sell_through(sold, qty)
    story.append(_deck_header(brand, start_day, end_day, selected_store_codes, options.packet_mode, generated_at))
    story.append(Spacer(1, 0.08 * inch))
    cards = [
        _metric_card("Health", f"{health_score}/100", health_status, "#2F6B5D"),
        _metric_card("Net Revenue", money0(report_metrics.get("net_revenue", 0.0)), "brand sales"),
        _metric_card("Units Sold", int0(report_metrics.get("items", 0.0)), "report window"),
        _metric_card("Real Margin", pct1(report_metrics.get("margin_real", 0.0)), "before credits", "#D6B800"),
        _metric_card("Target", pct1(credit_summary.get("target_margin", 0.35)), "margin goal"),
        _metric_card("Credit Gap", money0(credit_summary.get("credit_gap", 0.0)), "owed support", "#B54034" if float(credit_summary.get("credit_gap", 0.0) or 0.0) else "#2F6B5D"),
        _metric_card("Expected Margin", pct1(credit_summary.get("expected_credit_margin", 0.0)), money0(credit_summary.get("expected_credit_amount", 0.0))),
        _metric_card("Received Margin", pct1(credit_summary.get("received_credit_margin", 0.0)), money0(credit_summary.get("received_credit_amount", 0.0))),
        _metric_card("Discount", pct1(report_metrics.get("discount_rate", 0.0)), money0(report_metrics.get("discount", 0.0))),
        _metric_card("Inventory", money0(inv_overview.get("inventory_value", 0.0)), f"{int0(inv_overview.get('units', 0.0))} units"),
        _metric_card("Sell-Through", pct1(sell_through), "sold / sold+on hand"),
        _metric_card("Days Supply", _format_days_supply_v2(inv_overview.get("days_of_supply", np.nan), inv_overview.get("trend_units_per_day_30d", inv_overview.get("trend_units_per_day_14d", 0.0)), inv_overview.get("units", 0.0)), "30d pace"),
    ]
    story.append(_metric_grid(cards, cols=6))
    story.append(Spacer(1, 0.10 * inch))
    ask_box = _panel("PRIMARY MEETING ASK", [meeting_ask or "Confirm margin support and next promo plan."], width=7.45 * inch, accent="#D6B800")
    story.append(ask_box)
    story.append(Spacer(1, 0.08 * inch))
    rows = _premium_action_rows(action_items, top_n=3)
    story.append(build_premium_table(
        rows,
        [0.55 * inch, 0.85 * inch, 1.35 * inch, 2.65 * inch, 1.1 * inch, 0.75 * inch],
        alignments=["center", "left", "left", "left", "left", "right"],
        status_cols=[0],
        money_cols=[5],
    ))
    story.append(Spacer(1, 0.05 * inch))
    story.append(Paragraph(f"Health note: {xml_escape(health_reason or 'No major risk flags.')}", theme["tiny_v2"]))
    at_risk_value = 0.0
    slow_count = 0
    if inventory_risk is not None and not inventory_risk.empty:
        risk_mask = inventory_risk.get("risk", pd.Series("", index=inventory_risk.index)).astype(str).isin(["High", "Medium"])
        at_risk_value = float(pd.to_numeric(inventory_risk.loc[risk_mask, "inventory_value"], errors="coerce").fillna(0.0).sum()) if "inventory_value" in inventory_risk.columns else 0.0
        slow_count = int(risk_mask.sum())
    focus_store = ""
    for item in action_items or []:
        store_val = str(item.get("store", "") or "").strip()
        if store_val:
            focus_store = store_val
            break
    if not focus_store:
        focus_store = "All stores"
    snapshot = Table([[
        _panel("MARGIN FOCUS", [
            f"Received margin: {pct1(credit_summary.get('received_credit_margin', report_metrics.get('margin_real', 0.0)))}",
            f"Support to target: {money0(credit_summary.get('credit_needed_to_hit_target', 0.0))}",
        ], width=2.42 * inch, accent="#D6B800"),
        _panel("INVENTORY FOCUS", [
            f"At-risk inventory: {money0(at_risk_value)}",
            f"Slow-moving rows: {int0(slow_count)}",
        ], width=2.42 * inch, accent="#B54034" if at_risk_value else "#2F6B5D"),
        _panel("STORE FOCUS", [
            f"Focus: {focus_store}",
            "Use scorecards to target support.",
        ], width=2.42 * inch, accent="#2F6B5D"),
    ]], colWidths=[2.48 * inch] * 3)
    snapshot.setStyle(TableStyle([("VALIGN", (0, 0), (-1, -1), "TOP"), ("LEFTPADDING", (0, 0), (-1, -1), 2), ("RIGHTPADDING", (0, 0), (-1, -1), 2)]))
    story.append(Spacer(1, 0.08 * inch))
    story.append(snapshot)
    story.append(PageBreak())


def build_margin_truth_page_v2(
    story: List[Any],
    credit_summary: Dict[str, Any],
    credit_reconciliation: pd.DataFrame,
    meeting_ask: str,
    mode: str,
    action_items: Optional[Sequence[Dict[str, Any]]] = None,
    daily_60: Optional[pd.DataFrame] = None,
) -> None:
    theme = build_brand_packet_theme_v2()
    panels = [
        _panel("REAL MARGIN", [
            f"Net revenue: {money0(credit_summary.get('net_revenue', 0.0))}",
            f"Real profit: {money0(credit_summary.get('real_profit', 0.0))}",
            f"Real margin: {pct1(credit_summary.get('real_margin', 0.0))}",
        ], width=2.42 * inch),
        _panel("EXPECTED SUPPORT", [
            (
                f"Deals reference: {money0(credit_summary.get('system_expected_credit', 0.0))}"
                if credit_summary.get("system_expected_reference_only")
                else f"Deals expected: {money0(credit_summary.get('system_expected_credit', 0.0))}"
            ),
            f"CreditFlow expected: {money0(credit_summary.get('creditflow_expected_credit', 0.0))}",
            f"Ledger expected: {money0(credit_summary.get('ledger_expected_credit', 0.0))}",
            f"Expected margin: {pct1(credit_summary.get('expected_credit_margin', 0.0))}",
        ], width=2.42 * inch, accent="#D6B800"),
        _panel("RECEIVED SUPPORT", [
            f"CreditFlow received: {money0(credit_summary.get('creditflow_received_credit', 0.0))}",
            f"Ledger received: {money0(credit_summary.get('ledger_received_credit', 0.0))}",
            f"Credit gap: {money0(credit_summary.get('credit_gap', 0.0))}",
            f"Open ERP rows: {int(credit_summary.get('creditflow_open_rows', 0) or 0)}",
            f"Received margin: {pct1(credit_summary.get('received_credit_margin', 0.0))}",
        ], width=2.42 * inch, accent="#2F6B5D"),
    ]
    panel_table = Table([panels], colWidths=[2.48 * inch] * 3)
    panel_table.setStyle(TableStyle([("VALIGN", (0, 0), (-1, -1), "TOP"), ("LEFTPADDING", (0, 0), (-1, -1), 2), ("RIGHTPADDING", (0, 0), (-1, -1), 2)]))
    flows: List[Any] = [panel_table, Spacer(1, 0.08 * inch)]
    rec_rows = [["Credit Type", "Source", "Scope", "Expected", "Received", "Gap", "Status", "Action"]]
    if credit_reconciliation is not None and not credit_reconciliation.empty:
        rec_limit = 5 if mode == "quick" else 10 if mode == "standard" else 18
        for _, r in credit_reconciliation.head(rec_limit).iterrows():
            gap = float(r.get("Gap", 0.0) or 0.0)
            action = "Pay credit" if gap > 0 else "Verify support"
            source = str(r.get("Source", ""))
            source_label = "CreditFlow" if source.lower() == "creditflow" else source.title() if source else ""
            rec_rows.append([
                str(r.get("Type", "")),
                source_label,
                str(r.get("Scope", "")),
                money0(r.get("Expected", 0.0)),
                money0(r.get("Received", 0.0)),
                money0(gap),
                str(r.get("Status", "")),
                action,
            ])
    else:
        rec_rows.append(["Manual ledger", "", "Brand", "$0", "$0", "$0", "None", "No manual credits entered"])
    flows.append(build_premium_table(rec_rows, [1.05 * inch, 0.75 * inch, 0.75 * inch, 0.78 * inch, 0.78 * inch, 0.68 * inch, 0.72 * inch, 0.9 * inch], money_cols=[3, 4, 5], status_cols=[6]))
    flows.append(Spacer(1, 0.08 * inch))
    flows.append(_panel("RECOMMENDED BRAND ASK", [meeting_ask], width=7.45 * inch, accent="#D6B800"))
    if daily_60 is not None and not daily_60.empty:
        flows.append(Spacer(1, 0.07 * inch))
        flows.append(_chart_image(chart_daily_brand_sales(daily_60, "Daily Brand Sales"), 7.35 * inch, 2.05 * inch))
    if str(mode or "").lower() == "quick":
        flows.append(Spacer(1, 0.08 * inch))
        flows.append(Paragraph("Detailed Brand Asks", theme["small_v2"]))
        flows.append(build_premium_table(
            _premium_action_rows(action_items or [], top_n=5),
            [0.55 * inch, 0.85 * inch, 1.35 * inch, 2.65 * inch, 1.1 * inch, 0.75 * inch],
            status_cols=[0],
            money_cols=[5],
        ))
    _section_page(story, "Margin Truth + Credit Reconciliation", flows)


def build_brand_action_page_v2(story: List[Any], action_items: Sequence[Dict[str, Any]], mode: str) -> None:
    top_n = 6 if mode == "quick" else 10 if mode == "standard" else 15
    rows = _premium_action_rows(action_items, top_n=top_n)
    _section_page(story, "What The Brand Can Do Better", [
        build_premium_table(rows, [0.55 * inch, 0.85 * inch, 1.35 * inch, 2.65 * inch, 1.1 * inch, 0.75 * inch], status_cols=[0], money_cols=[5]),
    ])


def build_store_scorecards_page_v2(story: List[Any], store_credit_scorecard: pd.DataFrame, mode: str) -> None:
    theme = build_brand_packet_theme_v2()
    cards: List[Any] = []
    if store_credit_scorecard is not None and not store_credit_scorecard.empty:
        for _, r in store_credit_scorecard.iterrows():
            label = f"{str(r.get('_store_abbr', ''))}  {_short_status(r.get('status', ''))}"
            lines = [
                f"Revenue {money0(r.get('net_revenue', 0.0))} | Units {int0(r.get('items', 0.0))}",
                f"Real {pct1(r.get('margin_real', 0.0))} | Rec {pct1(r.get('received_credit_margin', r.get('margin_real', 0.0)))}",
                f"Credits rec {money0(r.get('received_credit_alloc', 0.0))} | gap {money0(r.get('credit_gap_alloc', 0.0))}",
                f"Discount {pct1(r.get('discount_rate', 0.0))}",
                f"Inventory {money0(r.get('inventory_value', 0.0))} | Days {_format_days_supply_v2(r.get('days_of_supply', np.nan), units_on_hand=r.get('units_available', 0.0))}",
                f"Need: {_store_needed_from_brand(r)}",
            ]
            cards.append(_panel(label, lines, width=2.42 * inch, accent="#B54034" if str(r.get("status")) == "Needs Support" else "#D6B800" if str(r.get("status")) == "Watch" else "#2F6B5D"))
    card_rows: List[List[Any]] = []
    for i in range(0, len(cards), 3):
        row = cards[i:i + 3]
        while len(row) < 3:
            row.append("")
        card_rows.append(row)
    flowables: List[Any] = []
    if card_rows:
        card_table = Table(card_rows, colWidths=[2.48 * inch] * 3)
        card_table.setStyle(TableStyle([("VALIGN", (0, 0), (-1, -1), "TOP"), ("LEFTPADDING", (0, 0), (-1, -1), 2), ("RIGHTPADDING", (0, 0), (-1, -1), 2), ("BOTTOMPADDING", (0, 0), (-1, -1), 5)]))
        flowables.append(card_table)
        flowables.append(Spacer(1, 0.06 * inch))
    rows = _store_scorecard_rows(store_credit_scorecard)
    flowables.append(build_premium_table(rows, [0.42 * inch, 0.72 * inch, 0.42 * inch, 0.52 * inch, 0.52 * inch, 0.66 * inch, 0.58 * inch, 0.68 * inch, 0.48 * inch, 0.68 * inch, 0.85 * inch], money_cols=[1, 5, 7], pct_cols=[3, 4, 6], status_cols=[9]))
    if len(rows) <= 1:
        flowables.append(Paragraph("No store-level rows were available for this brand/window.", theme["small_v2"]))
    _section_page(story, "Store Scorecards", flowables)


def build_location_performance_overview_v2(story: List[Any], store_sales_packets: Dict[str, Dict[str, Any]]) -> None:
    if not store_sales_packets:
        return
    flows: List[Any] = [
        Paragraph("Revenue, profit, traffic, discounting, and inventory by location.", build_brand_packet_theme_v2()["small_v2"]),
        Spacer(1, 0.05 * inch),
        _chart_image(chart_location_net_profit(store_sales_packets), 7.25 * inch, 2.45 * inch),
        Spacer(1, 0.06 * inch),
        build_premium_table(
            _location_overview_rows(store_sales_packets),
            [0.42 * inch, 0.77 * inch, 0.72 * inch, 0.78 * inch, 0.58 * inch, 0.55 * inch, 0.58 * inch, 0.52 * inch, 0.58 * inch, 0.72 * inch, 0.55 * inch],
            money_cols=[1, 2, 3, 6, 9],
            pct_cols=[4, 8],
            alignments=["center", "right", "right", "right", "right", "right", "right", "right", "right", "right", "right"],
        ),
    ]
    _section_page(story, "Location Performance Overview", flows)


def build_product_category_page_v2(story: List[Any], category_60: pd.DataFrame, product_60: pd.DataFrame, mode: str) -> None:
    chart_flows: List[Any] = []
    cat_chart_df = category_60.copy() if category_60 is not None else pd.DataFrame()
    prod_chart_df = product_60.copy() if product_60 is not None else pd.DataFrame()
    if not prod_chart_df.empty:
        prod_chart_df = prod_chart_df.head(8).copy()
        prod_chart_df["short_product"] = prod_chart_df["product_group_display"].map(lambda x: short_product_label(x, 34))
    ch_cat = chart_rank_barh(cat_chart_df.head(6), "category_normalized", "net_revenue", "Revenue by Category", value_kind="money") if not cat_chart_df.empty else BytesIO()
    ch_prod = chart_rank_barh(prod_chart_df, "short_product", "net_revenue", "Revenue by Product Family", value_kind="money") if not prod_chart_df.empty else BytesIO()
    chart_table = Table([[_chart_image(ch_cat, 3.45 * inch, 2.05 * inch), _chart_image(ch_prod, 3.45 * inch, 2.05 * inch)]], colWidths=[3.65 * inch, 3.65 * inch])
    chart_table.setStyle(TableStyle([("VALIGN", (0, 0), (-1, -1), "TOP"), ("LEFTPADDING", (0, 0), (-1, -1), 0), ("RIGHTPADDING", (0, 0), (-1, -1), 0)]))
    product_rows = _product_perf_rows(product_60, mode)
    insights: List[str] = []
    if product_60 is not None and not product_60.empty:
        top = product_60.sort_values("net_revenue", ascending=False).iloc[0]
        insights.append(f"Top revenue driver: {short_product_label(top.get('product_group_display', ''), 52)} at {money0(top.get('net_revenue', 0.0))}.")
        low_margin = product_60[pd.to_numeric(product_60.get("margin_real", 0.0), errors="coerce").fillna(0.0) < 0.25].sort_values("net_revenue", ascending=False)
        if not low_margin.empty:
            r = low_margin.iloc[0]
            insights.append(f"Margin review: {short_product_label(r.get('product_group_display', ''), 52)} at {pct1(r.get('margin_real', 0.0))}.")
    if not insights:
        insights.append("No major product mix flags triggered.")
    _section_page(story, "Product + Category Performance", [
        chart_table,
        Spacer(1, 0.05 * inch),
        build_premium_table(product_rows, [2.55 * inch, 0.8 * inch, 0.78 * inch, 0.52 * inch, 0.7 * inch, 0.65 * inch, 0.55 * inch], money_cols=[2], pct_cols=[4, 5]),
        Spacer(1, 0.06 * inch),
        _panel("PRODUCT SIGNALS", insights[:3], width=7.45 * inch),
    ])


def build_inventory_sellthrough_page_v2(story: List[Any], inv_overview: Dict[str, float], inventory_risk: pd.DataFrame, fast_movers: pd.DataFrame, slow_movers: pd.DataFrame, mode: str) -> None:
    high_count = int((inventory_risk.get("risk", pd.Series(dtype=str)).astype(str) == "High").sum()) if inventory_risk is not None and not inventory_risk.empty else 0
    at_risk = 0.0
    if inventory_risk is not None and not inventory_risk.empty:
        at_risk = float(pd.to_numeric(inventory_risk.loc[inventory_risk["risk"].isin(["High", "Medium"]), "inventory_value"], errors="coerce").fillna(0.0).sum())
    cards = [
        _metric_card("Inventory Value", money0(inv_overview.get("inventory_value", 0.0)), "cost on hand"),
        _metric_card("Units On Hand", int0(inv_overview.get("units", 0.0)), "current inventory"),
        _metric_card("Days Supply", _format_days_supply_v2(inv_overview.get("days_of_supply", np.nan), inv_overview.get("trend_units_per_day_30d", inv_overview.get("trend_units_per_day_14d", 0.0)), inv_overview.get("units", 0.0)), "30d pace"),
        _metric_card("At-Risk Inv", money0(at_risk), f"{high_count} high-risk rows", "#B54034" if at_risk else "#2F6B5D"),
        _metric_card("Fast Movers", int0(len(fast_movers) if fast_movers is not None else 0), "restock/watch"),
        _metric_card("Slow Movers", int0(len(slow_movers) if slow_movers is not None else 0), "support needed"),
    ]
    fast_rows = _inventory_rows(fast_movers, mode, "fast_movers")
    slow_rows = _inventory_rows(slow_movers, mode, "slow_movers")
    _section_page(story, "Inventory + Sell-Through", [
        _metric_grid(cards, cols=6),
        Spacer(1, 0.08 * inch),
        Paragraph("Fast Movers", build_brand_packet_theme_v2()["small_v2"]),
        build_premium_table(fast_rows, [1.95 * inch, 0.7 * inch, 0.45 * inch, 0.55 * inch, 0.65 * inch, 0.5 * inch, 0.85 * inch], money_cols=[1], pct_cols=[4]),
        Spacer(1, 0.07 * inch),
        Paragraph("Slow Movers / Inventory Risk", build_brand_packet_theme_v2()["small_v2"]),
        build_premium_table(slow_rows, [1.95 * inch, 0.7 * inch, 0.45 * inch, 0.55 * inch, 0.65 * inch, 0.5 * inch, 0.85 * inch], money_cols=[1], pct_cols=[4]),
    ])


def build_store_detail_page_v2(story: List[Any], abbr: str, pkt: Dict[str, Any], mode: str, credit_summary: Dict[str, Any]) -> None:
    theme = build_brand_packet_theme_v2()
    wm = pkt.get("window_metrics", {}) or {}
    metrics = wm.get("report", {}) or {}
    inv = pkt.get("inventory", {}) or {}
    daily = pkt.get("daily", pd.DataFrame())
    category = pkt.get("category_60", pd.DataFrame())
    product = pkt.get("product_60", pd.DataFrame())
    inv_products = pkt.get("inventory_products", pd.DataFrame())
    risk = compute_inventory_risk_v2(product, inv_products)
    slow = limit_rows_for_pdf(risk[risk.get("risk", pd.Series(dtype=str)).isin(["High", "Medium"])] if not risk.empty else risk, mode, "store_products")
    top_products = limit_rows_for_pdf(product.sort_values("net_revenue", ascending=False) if product is not None and not product.empty else pd.DataFrame(), mode, "store_products")
    cards = [
        _metric_card("Revenue", money0(metrics.get("net_revenue", 0.0)), abbr),
        _metric_card("Units", int0(metrics.get("items", 0.0)), "sold"),
        _metric_card("Real Margin", pct1(metrics.get("margin_real", 0.0)), "before credits"),
        _metric_card("Discount", pct1(metrics.get("discount_rate", 0.0)), money0(metrics.get("discount", 0.0))),
        _metric_card("Inventory", money0(inv.get("inventory_value", 0.0)), f"{int0(inv.get('units_available', 0.0))} units"),
        _metric_card("Days Supply", _format_days_supply_v2(inv.get("days_of_supply", np.nan), inv.get("trend_units_per_day_30d", 0.0), inv.get("units_available", 0.0)), "store pace"),
    ]
    product_rows = _product_perf_rows(top_products, mode)
    slow_rows = _inventory_rows(slow, mode, "store_products")
    metric_table = build_premium_table(_store_metric_detail_rows(metrics, inv), [1.2 * inch, 1.25 * inch], money_cols=[1], pct_cols=[1])
    category_rows = _category_rows(category, mode)
    category_table = build_premium_table(category_rows, [1.35 * inch, 0.7 * inch, 0.42 * inch, 0.55 * inch, 0.55 * inch], money_cols=[1], pct_cols=[3, 4])
    trend_chart = _chart_image(chart_daily_net_profit(daily, f"{abbr} Daily Net Sales + Profit"), 7.25 * inch, 2.25 * inch)
    margin_chart = _chart_image(chart_daily_margin(daily, f"{abbr} Daily Real Margin"), 3.55 * inch, 1.75 * inch) if daily is not None and not daily.empty else Spacer(1, 0.01 * inch)
    top_flowables: List[Any] = [
        _metric_grid(cards, cols=6),
        Spacer(1, 0.06 * inch),
        trend_chart,
        Spacer(1, 0.05 * inch),
        Table([
            [
                metric_table,
                Table([[margin_chart], [category_table]], colWidths=[4.9 * inch]),
            ]
        ], colWidths=[2.45 * inch, 4.95 * inch]),
        Spacer(1, 0.06 * inch),
        _panel("STORE ASK", [f"{abbr}: {_store_needed_from_brand(pd.Series({'status': 'Watch', 'discount_rate': metrics.get('discount_rate', 0.0), 'days_of_supply': inv.get('days_of_supply', 0.0), 'margin_real': metrics.get('margin_real', 0.0)}))}."], width=7.45 * inch),
    ]
    _section_page(story, f"{abbr} Store Detail", top_flowables)

    detail_flowables: List[Any] = [
        Paragraph("Top Product Groups", theme["small_v2"]),
        build_premium_table(
            product_rows,
            [2.70 * inch, 0.85 * inch, 0.82 * inch, 0.55 * inch, 0.72 * inch, 0.68 * inch, 0.58 * inch],
            money_cols=[2],
            pct_cols=[4, 5],
        ),
        Spacer(1, 0.05 * inch),
        Paragraph("Slow Movers / Inventory Risk", theme["small_v2"]),
        build_premium_table(
            slow_rows,
            [2.70 * inch, 0.82 * inch, 0.52 * inch, 0.62 * inch, 0.70 * inch, 0.58 * inch, 0.95 * inch],
            money_cols=[1],
            pct_cols=[4],
        ),
        Spacer(1, 0.05 * inch),
    ]
    _section_page(story, f"{abbr} Products + Inventory Risk", detail_flowables)


def build_appendix_v2(story: List[Any], product_60: pd.DataFrame, category_60: pd.DataFrame, credit_reconciliation: pd.DataFrame, inventory_risk: pd.DataFrame, mode: str) -> None:
    if mode == "quick":
        return
    appendix_n = 25 if mode == "standard" else 50
    flows: List[Any] = []
    if credit_reconciliation is not None and not credit_reconciliation.empty:
        rows = [["Source", "Type", "Store", "Expected", "Received", "Gap", "Status", "Memo"]]
        for _, r in credit_reconciliation.head(appendix_n).iterrows():
            rows.append([
                r.get("Source", ""),
                r.get("Type", ""),
                r.get("Store", "") or r.get("Scope", ""),
                money0(r.get("Expected", 0.0)),
                money0(r.get("Received", 0.0)),
                money0(r.get("Gap", 0.0)),
                r.get("Status", ""),
                r.get("Invoice Reference", "") or r.get("Credit ID", ""),
            ])
        flows.extend([Paragraph("Credit Ledger Detail", build_brand_packet_theme_v2()["small_v2"]), build_premium_table(rows, [0.75 * inch, 1.0 * inch, 0.62 * inch, 0.78 * inch, 0.78 * inch, 0.7 * inch, 0.68 * inch, 1.08 * inch], money_cols=[3, 4, 5]), Spacer(1, 0.08 * inch)])
    rows = _product_perf_rows(product_60.head(appendix_n), "deep")
    flows.extend([Paragraph("Top Product Detail", build_brand_packet_theme_v2()["small_v2"]), build_premium_table(rows, [2.55 * inch, 0.8 * inch, 0.78 * inch, 0.52 * inch, 0.7 * inch, 0.65 * inch, 0.55 * inch], money_cols=[2], pct_cols=[4, 5])])
    _section_page(story, "Appendix / Data Detail", flows, page_break=False)


def build_brand_packet_premium_pdf(
    out_pdf: Path,
    brand: str,
    start_day: date,
    end_day: date,
    options: PacketOptions,
    windows: Dict[str, Tuple[date, date]],
    window_metrics: Dict[str, Dict[str, float]],
    prior_window_covered: bool,
    daily_60: pd.DataFrame,
    store_60: pd.DataFrame,
    category_60: pd.DataFrame,
    product_60: pd.DataFrame,
    movers_store: pd.DataFrame,
    movers_category: pd.DataFrame,
    movers_product: pd.DataFrame,
    inv_overview: Dict[str, float],
    inv_products: pd.DataFrame,
    inv_category: pd.DataFrame,
    inv_store: pd.DataFrame,
    store_sales_packets: Dict[str, Dict[str, Any]],
    missing_sales_stores: Sequence[str],
    missing_catalog_stores: Sequence[str],
    credit_summary: Optional[Dict[str, Any]] = None,
    credit_reconciliation: Optional[pd.DataFrame] = None,
    action_items: Optional[Sequence[Dict[str, Any]]] = None,
    monthly_reference: Optional[Dict[str, Any]] = None,
    health_score: int = 0,
    health_status: str = "",
    health_reason: str = "",
    meeting_ask: str = "",
    store_credit_scorecard: Optional[pd.DataFrame] = None,
) -> None:
    osnap.setup_fonts()
    theme = build_brand_packet_theme_v2()
    credit_summary = credit_summary or {}
    credit_reconciliation = credit_reconciliation if credit_reconciliation is not None else pd.DataFrame()
    action_items = list(action_items or [])
    store_credit_scorecard = store_credit_scorecard if store_credit_scorecard is not None else pd.DataFrame()
    mode = str(options.packet_mode or "standard").lower()
    report_metrics = window_metrics.get("report", {})
    generated_at = datetime.now(ZoneInfo(REPORT_TZ)).strftime("%b %d, %Y %I:%M %p")
    inventory_risk = compute_inventory_risk_v2(product_60, inv_products)
    fast_movers = compute_fast_movers_v2(product_60, inv_products)
    slow_movers = compute_slow_movers_v2(product_60, inv_products)

    out_pdf.parent.mkdir(parents=True, exist_ok=True)
    doc = SimpleDocTemplate(
        str(out_pdf),
        pagesize=letter,
        leftMargin=0.42 * inch,
        rightMargin=0.42 * inch,
        topMargin=0.38 * inch,
        bottomMargin=0.42 * inch,
        pageCompression=1,
        title=f"Brand Meeting Packet - {brand}",
        author="Buzz Automation",
    )
    story: List[Any] = []
    build_cover_dashboard_v2(story, brand, start_day, end_day, options, order_store_codes(list(store_sales_packets.keys()) or list(store_60.get("_store_abbr", []))), generated_at, report_metrics, credit_summary, inv_overview, inventory_risk, action_items, health_score, health_status, health_reason, meeting_ask)
    build_margin_truth_page_v2(story, credit_summary, credit_reconciliation, meeting_ask, mode, action_items=action_items, daily_60=daily_60)
    if mode != "quick":
        build_brand_action_page_v2(story, action_items, mode)
    build_store_scorecards_page_v2(story, store_credit_scorecard, mode)
    build_location_performance_overview_v2(story, store_sales_packets)
    build_product_category_page_v2(story, category_60, product_60, mode)
    build_inventory_sellthrough_page_v2(story, inv_overview, inventory_risk, fast_movers, slow_movers, mode)
    if mode in {"standard", "deep"} and options.include_store_sections:
        for abbr in order_store_codes(list(store_sales_packets.keys())):
            build_store_detail_page_v2(story, abbr, store_sales_packets.get(abbr, {}), mode, credit_summary)
    if mode in {"standard", "deep"} and options.include_product_appendix:
        build_appendix_v2(story, product_60, category_60, credit_reconciliation, inventory_risk, mode)

    footer = _footer(f"Brand Review - {brand} - {mode.title()}", end_day)
    doc.build(story, onFirstPage=footer, onLaterPages=footer)


def _dashboard_section(story: List[Any], title: str, flowables: List[Any], page_break: bool = True) -> None:
    theme = build_brand_packet_theme_v2()
    story.append(Paragraph(title, theme["section_v2"]))
    story.extend(flowables)
    if page_break:
        story.append(PageBreak())


def _dashboard_header(brand: str, start_day: date, end_day: date, stores: Sequence[str], generated_at: str) -> Table:
    theme = build_brand_packet_theme_v2()
    left = [
        _p(brand, theme["deck_title"]),
        _p(f"{start_day.isoformat()} to {end_day.isoformat()} | Stores: {', '.join(stores) or 'All'}", theme["deck_subtitle"]),
    ]
    right = [
        _p("DASHBOARD / EASY READ", theme["card_label"]),
        _p(f"Generated: {generated_at}", theme["tiny_v2"]),
    ]
    table = Table([[left, right]], colWidths=[6.7 * inch, 3.6 * inch])
    table.setStyle(TableStyle([
        ("BACKGROUND", (0, 0), (-1, -1), colors.white),
        ("LINEBELOW", (0, 0), (-1, -1), 1.1, colors.HexColor("#2F6B5D")),
        ("VALIGN", (0, 0), (-1, -1), "TOP"),
        ("LEFTPADDING", (0, 0), (-1, -1), 0),
        ("RIGHTPADDING", (0, 0), (-1, -1), 0),
        ("BOTTOMPADDING", (0, 0), (-1, -1), 6),
    ]))
    return table


def _dashboard_metric_grid(cards: List[Table], cols: int = 6) -> Table:
    rows: List[List[Any]] = []
    for i in range(0, len(cards), cols):
        row = cards[i:i + cols]
        while len(row) < cols:
            row.append("")
        rows.append(row)
    table = Table(rows, colWidths=[1.70 * inch] * cols, hAlign="LEFT")
    table.setStyle(TableStyle([
        ("VALIGN", (0, 0), (-1, -1), "TOP"),
        ("LEFTPADDING", (0, 0), (-1, -1), 2),
        ("RIGHTPADDING", (0, 0), (-1, -1), 2),
        ("TOPPADDING", (0, 0), (-1, -1), 2),
        ("BOTTOMPADDING", (0, 0), (-1, -1), 2),
    ]))
    return table


def _dashboard_card(label: str, value: Any, note: str = "", accent: str = "#2F6B5D") -> Table:
    theme = build_brand_packet_theme_v2()
    data = [[_p(label.upper(), theme["card_label"])], [_p(value, theme["card_value"])], [_p(note, theme["card_note"])]]
    table = Table(data, colWidths=[1.62 * inch], rowHeights=[0.18 * inch, 0.28 * inch, 0.22 * inch])
    table.setStyle(TableStyle([
        ("BACKGROUND", (0, 0), (-1, -1), colors.HexColor("#F5F7F6")),
        ("BOX", (0, 0), (-1, -1), 0.45, colors.HexColor("#D7DEE0")),
        ("LINEABOVE", (0, 0), (-1, 0), 1.3, colors.HexColor(accent)),
        ("VALIGN", (0, 0), (-1, -1), "TOP"),
        ("LEFTPADDING", (0, 0), (-1, -1), 5),
        ("RIGHTPADDING", (0, 0), (-1, -1), 5),
        ("TOPPADDING", (0, 0), (-1, -1), 3),
        ("BOTTOMPADDING", (0, 0), (-1, -1), 2),
    ]))
    return table


def _dashboard_rows_products(df: pd.DataFrame, max_rows: int, include_last_sale: bool = False) -> List[List[Any]]:
    headers = [
        "Action", "Product", "Cat", "Sales", "Units", "U/Day", "Sales +/-", "Margin",
        "Disc", "On Hand", "DOS", "Sell", "Stores", "Recommendation",
    ]
    if include_last_sale:
        headers.insert(-1, "Last Sale")
    rows: List[List[Any]] = [headers]
    if df is not None and not df.empty:
        for _, r in df.head(max_rows).iterrows():
            row = [
                str(r.get("action", "")),
                _shorten_product_name(r.get("product_name", r.get("product_short", "")), 42),
                _dashboard_category_short(r.get("category", "")),
                money0(r.get("net_sales", 0.0)),
                int0(r.get("units_sold", 0.0)),
                f"{_dashboard_float(r.get('units_per_day'), 0.0):.1f}",
                _format_delta(r.get("sales_vs_prior_pct"), r.get("net_sales", 0.0), 0.0),
                pct1(r.get("margin_pct", 0.0)),
                pct1(r.get("discount_pct", 0.0)),
                int0(r.get("inventory_units", 0.0)),
                _format_days_supply_v2(r.get("days_supply", np.nan), r.get("units_per_day", 0.0), r.get("inventory_units", 0.0)),
                pct1(r.get("sell_through_pct", 0.0)),
                int0(r.get("stores_selling", 0.0)),
                str(r.get("recommendation", "")),
            ]
            if include_last_sale:
                row.insert(-1, str(r.get("last_sale_date", "") or "n/a"))
            rows.append(row)
    if len(rows) == 1:
        filler = ["No product rows available."] + [""] * (len(headers) - 1)
        rows.append(filler)
    return rows


def _dashboard_snapshot_decision_rows(df: pd.DataFrame, max_rows: int = 5) -> List[List[Any]]:
    rows = [["Action", "Product", "Sales", "On Hand", "DOS", "Recommendation"]]
    if df is not None and not df.empty:
        for _, r in df.head(max_rows).iterrows():
            rows.append([
                str(r.get("action", "")),
                _shorten_product_name(r.get("product_name", ""), 44),
                money0(r.get("net_sales", 0.0)),
                int0(r.get("inventory_units", 0.0)),
                _format_days_supply_v2(r.get("days_supply", np.nan), r.get("units_per_day", 0.0), r.get("inventory_units", 0.0)),
                str(r.get("recommendation", "")),
            ])
    if len(rows) == 1:
        rows.append(["No product actions available.", "", "", "", "", ""])
    return rows


def _dashboard_category_short(value: Any) -> str:
    text = str(value or "").strip().upper()
    return {
        "OUNCES": "OZ",
        "HALVES": "HALF",
        "QUARTERS": "QTR",
        "EIGHTHS": "8TH",
        "PREROLLS": "PRL",
        "PRE-ROLLS": "PRL",
        "PREROLL": "PRL",
        "PRE-ROLL": "PRL",
        "CONCENTRATES": "CONC",
        "BEVERAGES": "DRINK",
        "TINCTURES": "TINCT",
    }.get(text, str(value or "")[:12])


def _dashboard_action_short(value: Any) -> str:
    text = str(value or "").strip()
    return {
        "Move Inventory": "Move Inv",
        "Reduce Buying": "Reduce Buy",
        "Needs Promo": "Promo",
    }.get(text, text)


def _dashboard_fast_rows(df: pd.DataFrame, sort_col: str, max_rows: int) -> List[List[Any]]:
    rows = [["Product", "Cat", "Sales", "Units", "U/Day", "Margin", "On Hand", "DOS", "Action"]]
    if df is not None and not df.empty:
        work = df.copy()
        work[sort_col] = pd.to_numeric(work.get(sort_col, 0.0), errors="coerce").fillna(0.0)
        for _, r in work.sort_values(sort_col, ascending=False).head(max_rows).iterrows():
            rows.append([
                _shorten_product_name(r.get("product_name", ""), 32),
                _dashboard_category_short(r.get("category", "")),
                money0(r.get("net_sales", 0.0)),
                int0(r.get("units_sold", 0.0)),
                f"{_dashboard_float(r.get('units_per_day'), 0.0):.1f}",
                pct1(r.get("margin_pct", 0.0)),
                int0(r.get("inventory_units", 0.0)),
                _format_days_supply_v2(r.get("days_supply", np.nan), r.get("units_per_day", 0.0), r.get("inventory_units", 0.0)),
                str(r.get("action", "")),
            ])
    if len(rows) == 1:
        rows.append(["No fast movers available.", "", "", "", "", "", "", "", ""])
    return rows


def _dashboard_store_rows(store_matrix: pd.DataFrame) -> List[List[Any]]:
    rows = [[
        "Store", "Sales", "Share", "Sales +/-", "Units", "Units +/-", "Margin", "Disc",
        "Inv $", "DOS", "Sell", "Fastest", "Slowest", "Action",
    ]]
    if store_matrix is not None and not store_matrix.empty:
        for _, r in store_matrix.iterrows():
            rows.append([
                str(r.get("store", "")),
                money0(r.get("net_sales", 0.0)),
                pct1(r.get("sales_share_pct", 0.0)),
                _format_delta(r.get("sales_vs_prior_pct"), r.get("net_sales", 0.0), 0.0),
                int0(r.get("units", 0.0)),
                _format_delta(r.get("units_vs_prior_pct"), r.get("units", 0.0), 0.0),
                pct1(r.get("margin_pct", 0.0)),
                pct1(r.get("discount_pct", 0.0)),
                money0(r.get("inventory_value", 0.0)),
                _format_days_supply_v2(r.get("days_supply", np.nan), units_on_hand=r.get("inventory_units", 0.0)),
                pct1(r.get("sell_through_pct", 0.0)),
                str(r.get("fastest_product", "")),
                str(r.get("slowest_product", "")),
                _dashboard_action_short(r.get("store_action", "")),
            ])
    if len(rows) == 1:
        rows.append(["No store rows available."] + [""] * 13)
    return rows


def _dashboard_category_rows(category_mix: pd.DataFrame) -> List[List[Any]]:
    rows = [["Category", "Sales", "Share", "Units", "Margin", "Disc", "Inv $", "DOS", "Sell", "Top Product", "Action"]]
    if category_mix is not None and not category_mix.empty:
        for _, r in category_mix.head(12).iterrows():
            rows.append([
                str(r.get("category", "")),
                money0(r.get("net_sales", 0.0)),
                pct1(r.get("sales_share_pct", 0.0)),
                int0(r.get("units", 0.0)),
                pct1(r.get("margin_pct", 0.0)),
                pct1(r.get("discount_pct", 0.0)),
                money0(r.get("inventory_value", 0.0)),
                _format_days_supply_v2(r.get("days_supply", np.nan), units_on_hand=r.get("inventory_units", 0.0)),
                pct1(r.get("sell_through_pct", 0.0)),
                str(r.get("top_product", "")),
                str(r.get("category_action", "")),
            ])
    if len(rows) == 1:
        rows.append(["No category rows available."] + [""] * 10)
    return rows


def _dashboard_credit_rows(summary: pd.DataFrame, credit_reconciliation: pd.DataFrame, top_n: int = 10) -> List[List[Any]]:
    rows = [["Source", "Type", "Scope", "Expected", "Received", "Gap", "Status", "Action"]]
    if credit_reconciliation is not None and not credit_reconciliation.empty:
        for _, r in credit_reconciliation.head(top_n).iterrows():
            gap = _dashboard_float(r.get("Gap"), 0.0)
            rows.append([
                str(r.get("Source", "")),
                str(r.get("Type", "")),
                str(r.get("Scope", r.get("Store", ""))),
                money0(r.get("Expected", 0.0)),
                money0(r.get("Received", 0.0)),
                money0(gap),
                str(r.get("Status", "")),
                "Collect support" if gap > 0 else "Verify",
            ])
    if len(rows) == 1:
        snap = summary.iloc[0].to_dict() if summary is not None and not summary.empty else {}
        rows.append([
            "Summary", "Credits", "Brand", money0(snap.get("manual_expected_credit", 0.0)),
            money0(snap.get("manual_received_credit", 0.0)), money0(snap.get("credit_gap", 0.0)), "Open" if snap.get("credit_gap", 0.0) else "None", "Review",
        ])
    return rows


def build_brand_packet_dashboard_pdf(
    out_pdf: Path,
    brand: str,
    start_day: date,
    end_day: date,
    selected_store_codes: Sequence[str],
    options: PacketOptions,
    dashboard_data: Dict[str, Any],
    credit_reconciliation: Optional[pd.DataFrame] = None,
    daily_60: Optional[pd.DataFrame] = None,
    store_sales_packets: Optional[Dict[str, Dict[str, Any]]] = None,
    missing_sales_stores: Sequence[str] = (),
    missing_catalog_stores: Sequence[str] = (),
) -> None:
    osnap.setup_fonts()
    theme = build_brand_packet_theme_v2()
    out_pdf.parent.mkdir(parents=True, exist_ok=True)
    doc = SimpleDocTemplate(
        str(out_pdf),
        pagesize=landscape(letter),
        leftMargin=0.32 * inch,
        rightMargin=0.32 * inch,
        topMargin=0.30 * inch,
        bottomMargin=0.38 * inch,
        pageCompression=1,
        title=f"Brand Dashboard - {brand}",
        author="Buzz Automation",
    )
    story: List[Any] = []
    generated_at = datetime.now(ZoneInfo(REPORT_TZ)).strftime("%b %d, %Y %I:%M %p")
    story.append(_dashboard_header(brand, start_day, end_day, selected_store_codes, generated_at))
    story.append(Spacer(1, 0.08 * inch))

    snapshot_df = dashboard_data.get("snapshot", pd.DataFrame())
    snap = snapshot_df.iloc[0].to_dict() if snapshot_df is not None and not snapshot_df.empty else {}
    decisions = dashboard_data.get("product_decision_board", pd.DataFrame())
    status = str(dashboard_data.get("brand_status") or snap.get("brand_status") or "Watch")
    status_color = "#2F6B5D" if status in {"Grow", "Healthy"} else "#D6B800" if status in {"Watch", "Reduce"} else "#B54034"
    cards = [
        _dashboard_card("Brand Status", status, "single-brand read", status_color),
        _dashboard_card("Net Sales", money0(snap.get("net_sales", 0.0)), f"vs prior {_format_delta(snap.get('sales_vs_prior_pct'), snap.get('net_sales', 0.0), 0.0)}"),
        _dashboard_card("Units Sold", int0(snap.get("units_sold", 0.0)), f"vs prior {_format_delta(snap.get('units_vs_prior_pct'), snap.get('units_sold', 0.0), 0.0)}"),
        _dashboard_card("Gross Profit", money0(snap.get("gross_profit", 0.0)), f"vs prior {_format_delta(snap.get('profit_vs_prior_pct'), snap.get('gross_profit', 0.0), 0.0)}"),
        _dashboard_card("Real Margin", pct1(snap.get("real_margin_pct", 0.0)), f"gap {_owner_margin_gap_label(snap.get('margin_gap_pp', 0.0))}"),
        _dashboard_card("Target Margin", pct1(snap.get("target_margin_pct", DEAL_TARGET_MARGIN)), "goal"),
        _dashboard_card("Discount", pct1(snap.get("discount_pct", 0.0)), "gross discount rate", "#D6B800" if _dashboard_float(snap.get("discount_pct"), 0.0) > 0.35 else "#2F6B5D"),
        _dashboard_card("Inventory Value", money0(snap.get("inventory_value", 0.0)), f"{int0(snap.get('inventory_units', 0.0))} units"),
        _dashboard_card("Days Supply", _format_days_supply_v2(snap.get("days_supply", np.nan), units_on_hand=snap.get("inventory_units", 0.0)), "current pace"),
        _dashboard_card("Sell-Through", pct1(snap.get("sell_through_pct", 0.0)), "sold / sold+on hand"),
        _dashboard_card("Credit Gap", money0(snap.get("credit_gap", 0.0)), f"{pct1(snap.get('credit_gap_pct_sales', 0.0))} of sales", "#B54034" if _dashboard_float(snap.get("credit_gap"), 0.0) > 0 else "#2F6B5D"),
        _dashboard_card("Stores Active", int0(snap.get("stores_active", 0.0)), "selected stores"),
    ]
    story.append(_dashboard_metric_grid(cards, cols=6))
    story.append(Spacer(1, 0.08 * inch))
    warning_lines = []
    if missing_sales_stores:
        warning_lines.append(f"Missing sales: {', '.join(missing_sales_stores)}")
    if missing_catalog_stores:
        warning_lines.append(f"Missing catalog: {', '.join(missing_catalog_stores)}")
    ask_lines = [str(dashboard_data.get("meeting_ask") or snap.get("meeting_ask") or "Confirm next steps with brand.")]
    if warning_lines:
        ask_lines.extend(warning_lines)
    story.append(_panel("MEETING ASK", ask_lines, width=10.2 * inch, accent=status_color))
    story.append(Spacer(1, 0.08 * inch))
    story.append(Paragraph("Top Decision Flags", theme["small_v2"]))
    story.append(build_premium_table(
        _dashboard_snapshot_decision_rows(decisions, max_rows=5),
        [0.95 * inch, 2.05 * inch, 0.78 * inch, 0.72 * inch, 0.64 * inch, 4.15 * inch],
        money_cols=[2],
        status_cols=[0],
    ))
    story.append(PageBreak())

    _dashboard_section(story, "Product Decision Board", [
        build_premium_table(
            _dashboard_rows_products(decisions, max_rows=max(1, int(options.max_products or 20))),
            [0.78 * inch, 1.48 * inch, 0.52 * inch, 0.62 * inch, 0.42 * inch, 0.45 * inch, 0.58 * inch, 0.55 * inch, 0.48 * inch, 0.52 * inch, 0.50 * inch, 0.48 * inch, 0.43 * inch, 2.00 * inch],
            money_cols=[3],
            pct_cols=[6, 7, 8, 11],
            status_cols=[0],
        ),
    ])

    fast = dashboard_data.get("fast_movers", pd.DataFrame())
    fast_tables = [
        build_premium_table(_dashboard_fast_rows(fast, "units_per_day", 5), [1.05 * inch, 0.42 * inch, 0.48 * inch, 0.34 * inch, 0.36 * inch, 0.42 * inch, 0.38 * inch, 0.42 * inch, 0.58 * inch], money_cols=[2], pct_cols=[5]),
        build_premium_table(_dashboard_fast_rows(fast, "net_sales", 5), [1.05 * inch, 0.42 * inch, 0.48 * inch, 0.34 * inch, 0.36 * inch, 0.42 * inch, 0.38 * inch, 0.42 * inch, 0.58 * inch], money_cols=[2], pct_cols=[5]),
        build_premium_table(_dashboard_fast_rows(fast, "gross_profit", 5), [1.05 * inch, 0.42 * inch, 0.48 * inch, 0.34 * inch, 0.36 * inch, 0.42 * inch, 0.38 * inch, 0.42 * inch, 0.58 * inch], money_cols=[2], pct_cols=[5]),
        build_premium_table(_dashboard_fast_rows(fast, "sell_through_pct", 5), [1.05 * inch, 0.42 * inch, 0.48 * inch, 0.34 * inch, 0.36 * inch, 0.42 * inch, 0.38 * inch, 0.42 * inch, 0.58 * inch], money_cols=[2], pct_cols=[5]),
    ]
    fast_grid = Table([
        [Paragraph("Top Units / Day", theme["small_v2"]), Paragraph("Top Sales", theme["small_v2"])],
        [fast_tables[0], fast_tables[1]],
        [Paragraph("Top Profit", theme["small_v2"]), Paragraph("Top Sell-Through", theme["small_v2"])],
        [fast_tables[2], fast_tables[3]],
    ], colWidths=[5.05 * inch, 5.05 * inch])
    fast_grid.setStyle(TableStyle([("VALIGN", (0, 0), (-1, -1), "TOP"), ("LEFTPADDING", (0, 0), (-1, -1), 2), ("RIGHTPADDING", (0, 0), (-1, -1), 2), ("BOTTOMPADDING", (0, 0), (-1, -1), 4)]))
    _dashboard_section(story, "Fast Movers", [fast_grid])

    slow = dashboard_data.get("slow_movers", pd.DataFrame())
    _dashboard_section(story, "Slow Movers + Inventory Risk", [
        build_premium_table(
            _dashboard_rows_products(slow, max_rows=max(1, int(options.max_products or 20)), include_last_sale=True),
            [0.75 * inch, 1.40 * inch, 0.50 * inch, 0.58 * inch, 0.40 * inch, 0.42 * inch, 0.55 * inch, 0.52 * inch, 0.46 * inch, 0.48 * inch, 0.48 * inch, 0.46 * inch, 0.46 * inch, 0.62 * inch, 1.52 * inch],
            money_cols=[3],
            pct_cols=[6, 7, 8, 11],
            status_cols=[0],
        ),
    ])

    store_matrix = dashboard_data.get("store_matrix", pd.DataFrame())
    store_flows: List[Any] = []
    if store_sales_packets:
        store_flows.extend([_chart_image(chart_location_net_profit(store_sales_packets), 10.0 * inch, 2.25 * inch), Spacer(1, 0.06 * inch)])
    store_flows.append(build_premium_table(
        _dashboard_store_rows(store_matrix),
        [0.42 * inch, 0.58 * inch, 0.48 * inch, 0.55 * inch, 0.40 * inch, 0.55 * inch, 0.50 * inch, 0.48 * inch, 0.58 * inch, 0.48 * inch, 0.45 * inch, 1.25 * inch, 1.25 * inch, 0.82 * inch],
        money_cols=[1, 8],
        pct_cols=[2, 3, 5, 6, 7, 10],
        status_cols=[0, 13],
    ))
    _dashboard_section(story, "Store Performance Matrix", store_flows)

    category_mix = dashboard_data.get("category_mix", pd.DataFrame())
    chart_cat = BytesIO()
    if category_mix is not None and not category_mix.empty:
        chart_df = category_mix.rename(columns={"category": "category_normalized", "net_sales": "net_revenue"})
        chart_cat = chart_rank_barh(chart_df.head(8), "category_normalized", "net_revenue", "Sales by Category", value_kind="money")
    _dashboard_section(story, "Category / Product Group Mix", [
        Table([[_chart_image(chart_cat, 4.55 * inch, 2.45 * inch), build_premium_table(
            _dashboard_category_rows(category_mix),
            [0.62 * inch, 0.52 * inch, 0.38 * inch, 0.34 * inch, 0.38 * inch, 0.34 * inch, 0.45 * inch, 0.38 * inch, 0.34 * inch, 0.92 * inch, 0.55 * inch],
            money_cols=[1, 6],
            pct_cols=[2, 4, 5, 8],
            status_cols=[10],
        )]], colWidths=[4.75 * inch, 5.45 * inch]),
    ])

    credit_summary_df = dashboard_data.get("credit_margin_summary", snapshot_df)
    credit_snap = credit_summary_df.iloc[0].to_dict() if credit_summary_df is not None and not credit_summary_df.empty else snap
    margin_cards = [
        _dashboard_card("Real Margin", pct1(credit_snap.get("real_margin_pct", 0.0)), f"profit {money0(credit_snap.get('gross_profit', 0.0))}"),
        _dashboard_card("Target", pct1(credit_snap.get("target_margin_pct", DEAL_TARGET_MARGIN)), f"gap {_owner_margin_gap_label(credit_snap.get('margin_gap_pp', 0.0))}"),
        _dashboard_card("Discount", pct1(credit_snap.get("discount_pct", 0.0)), "discount pressure"),
        _dashboard_card("Manual Expected", money0(credit_snap.get("manual_expected_credit", 0.0)), "ledger"),
        _dashboard_card("Manual Received", money0(credit_snap.get("manual_received_credit", 0.0)), "ledger"),
        _dashboard_card("CreditFlow Expected", money0(credit_snap.get("creditflow_expected_credit", 0.0)), "ERP"),
        _dashboard_card("CreditFlow Received", money0(credit_snap.get("creditflow_received_credit", 0.0)), "ERP"),
        _dashboard_card("Credit Gap", money0(credit_snap.get("credit_gap", 0.0)), f"{pct1(credit_snap.get('credit_gap_pct_sales', 0.0))} of sales"),
    ]
    _dashboard_section(story, "Margin, Discount, and Credit Support", [
        _dashboard_metric_grid(margin_cards, cols=4),
        Spacer(1, 0.08 * inch),
        _panel("ASK FROM BRAND", [str(dashboard_data.get("meeting_ask") or "Confirm support plan.")], width=10.2 * inch, accent="#D6B800"),
        Spacer(1, 0.08 * inch),
        build_premium_table(
            _dashboard_credit_rows(credit_summary_df, credit_reconciliation if credit_reconciliation is not None else pd.DataFrame()),
            [0.82 * inch, 1.0 * inch, 1.1 * inch, 0.82 * inch, 0.82 * inch, 0.72 * inch, 0.72 * inch, 1.05 * inch],
            money_cols=[3, 4, 5],
            status_cols=[6],
        ),
    ], page_break=bool(options.include_product_appendix))

    if options.include_product_appendix:
        story.append(Paragraph("Appendix / Dashboard Product Detail", theme["section_v2"]))
        story.append(build_premium_table(
            _dashboard_rows_products(decisions, max_rows=min(50, max(1, int(options.max_products or 20)))),
            [0.78 * inch, 1.48 * inch, 0.52 * inch, 0.62 * inch, 0.42 * inch, 0.45 * inch, 0.58 * inch, 0.55 * inch, 0.48 * inch, 0.52 * inch, 0.50 * inch, 0.48 * inch, 0.43 * inch, 2.00 * inch],
            money_cols=[3],
            pct_cols=[6, 7, 8, 11],
            status_cols=[0],
        ))

    footer = _footer(f"Brand Dashboard - {brand}", end_day)
    doc.build(story, onFirstPage=footer, onLaterPages=footer)


def _metrics_table_rows(metrics: Dict[str, float]) -> List[List[Any]]:
    return [
        ["Net Revenue", money0(metrics.get("net_revenue", 0.0))],
        ["Gross Sales", money0(metrics.get("gross_sales", 0.0))],
        ["Tickets", int0(metrics.get("tickets", 0.0))],
        ["Items / Units", int0(metrics.get("items", 0.0))],
        ["Basket", money2(metrics.get("basket", 0.0))],
        ["Discount", money0(metrics.get("discount", 0.0))],
        ["Discount Rate", pct1(metrics.get("discount_rate", 0.0))],
        ["COGS", money0(metrics.get("cogs_real", 0.0))],
        ["Profit", money0(metrics.get("profit_real", 0.0))],
        ["Margin", pct1(metrics.get("margin_real", 0.0))],
        ["Returns Net", money0(metrics.get("returns_net", 0.0))],
        ["Returns Tickets", int0(metrics.get("returns_tickets", 0.0))],
        ["Weight Sold", f"{metrics.get('weight_sold', 0.0):,.1f}"],
    ]


def _inventory_summary_rows(inv: Dict[str, float]) -> List[List[Any]]:
    return [
        ["Inventory Units", int0(inv.get("units", 0.0))],
        ["Inventory Value", inventory_value_with_units(inv.get("inventory_value", 0.0), inv.get("units", 0.0))],
        ["Potential Revenue", money0(inv.get("potential_revenue", 0.0))],
        ["Potential Profit", money0(inv.get("potential_profit", 0.0))],
        ["Average Margin", pct1(inv.get("avg_margin", 0.0))],
        ["Trend Units / Day (30d)", f"{float(inv.get('trend_units_per_day_30d', inv.get('trend_units_per_day_14d', 0.0))):,.1f}"],
        ["Trend Units / Day (14d)", f"{float(inv.get('trend_units_per_day_14d', 0.0)):,.1f}"],
        ["Trend Units / Day (7d)", f"{float(inv.get('trend_units_per_day_7d', 0.0)):,.1f}"],
        ["Days of Supply", days1(inv.get("days_of_supply", np.nan))],
        ["Est. OOS Date", str(inv.get("est_oos_date", "n/a"))],
    ]


def _inventory_store_rows(inv_store: pd.DataFrame, top_n: int) -> List[List[Any]]:
    if inv_store is None or inv_store.empty:
        return []
    tmp = inv_store.copy()
    if "_store_abbr" in tmp.columns:
        tmp["_store_abbr"] = tmp["_store_abbr"].fillna("").astype(str).str.upper()
        tmp["_rank"] = tmp["_store_abbr"].map(lambda s: STORE_ORDER_RANK.get(str(s).upper(), 999))
        tmp = tmp.sort_values(["_rank", "_store_abbr"], ascending=[True, True])
    else:
        tmp = tmp.copy()
    rows: List[List[Any]] = []
    for _, r in tmp.head(top_n).iterrows():
        rows.append([
            str(r.get("_store_abbr", "")),
            int0(float(r.get("units_available", 0.0))),
            f"{float(r.get('trend_units_per_day_30d', r.get('trend_units_per_day_14d', 0.0))):,.1f}",
            days1(r.get("days_of_supply", np.nan)),
            str(r.get("est_oos_date", "n/a")),
            money0(float(r.get("inventory_value", 0.0))),
            money0(float(r.get("potential_revenue", 0.0))),
            money0(float(r.get("potential_profit", 0.0))),
            pct1(float(r.get("avg_margin", 0.0))),
        ])
    return rows


def _catalog_group_price_cost_maps(catalog_df: pd.DataFrame) -> Tuple[Dict[str, float], Dict[str, float]]:
    if catalog_df is None or catalog_df.empty:
        return {}, {}

    tmp = _filter_product_group_rows(catalog_df)
    if tmp.empty:
        return {}, {}

    key_col = _best_group_key_col(tmp)
    if not key_col:
        return {}, {}

    tmp["__group_key"] = tmp[key_col].fillna("").astype(str)
    tmp = tmp[tmp["__group_key"] != ""].copy()
    if tmp.empty:
        return {}, {}

    if "Price_Used" in tmp.columns:
        tmp["__price"] = osnap.to_number(tmp["Price_Used"]).fillna(0.0).astype(float)
    else:
        tmp["__price"] = osnap.to_number(tmp.get("shelf_price", 0.0)).fillna(0.0).astype(float)
    if "Cost" in tmp.columns:
        tmp["__cost"] = osnap.to_number(tmp["Cost"]).fillna(0.0).astype(float)
    else:
        tmp["__cost"] = osnap.to_number(tmp.get("cost", 0.0)).fillna(0.0).astype(float)

    if "Available" in tmp.columns:
        tmp["__w"] = osnap.to_number(tmp["Available"]).fillna(0.0).astype(float)
    else:
        tmp["__w"] = 0.0
    tmp["__w"] = np.where(tmp["__w"] > 0, tmp["__w"], 1.0)

    tmp["__price_num"] = tmp["__price"] * tmp["__w"]
    tmp["__cost_num"] = tmp["__cost"] * tmp["__w"]
    grp = tmp.groupby("__group_key", as_index=False).agg(
        price_num=("__price_num", "sum"),
        cost_num=("__cost_num", "sum"),
        w=("__w", "sum"),
    )
    grp["catalog_price_per_item"] = grp["price_num"] / grp["w"].replace({0: np.nan})
    grp["catalog_cost_per_item"] = grp["cost_num"] / grp["w"].replace({0: np.nan})
    grp = grp.replace([np.inf, -np.inf], np.nan)

    price_map = {
        str(r["__group_key"]): float(r["catalog_price_per_item"])
        for _, r in grp.iterrows()
        if pd.notna(r.get("catalog_price_per_item"))
    }
    cost_map = {
        str(r["__group_key"]): float(r["catalog_cost_per_item"])
        for _, r in grp.iterrows()
        if pd.notna(r.get("catalog_cost_per_item"))
    }
    return price_map, cost_map


def attach_catalog_price_cost_to_product_groups(
    product_df: pd.DataFrame,
    catalog_df: pd.DataFrame,
) -> pd.DataFrame:
    if product_df is None or product_df.empty:
        return pd.DataFrame(columns=(product_df.columns if product_df is not None else []))

    out = product_df.copy()
    key_col = _best_group_key_col(out)
    if not key_col:
        out["catalog_price_per_item"] = pd.to_numeric(out.get("avg_price_per_item", np.nan), errors="coerce")
        out["catalog_cost_per_item"] = pd.to_numeric(out.get("avg_cost_per_item", np.nan), errors="coerce")
        return out

    price_map, cost_map = _catalog_group_price_cost_maps(catalog_df)
    keys = out[key_col].fillna("").astype(str)

    out["catalog_price_per_item"] = pd.to_numeric(keys.map(price_map), errors="coerce")
    out["catalog_cost_per_item"] = pd.to_numeric(keys.map(cost_map), errors="coerce")

    if "avg_price_per_item" in out.columns:
        out["catalog_price_per_item"] = out["catalog_price_per_item"].fillna(
            pd.to_numeric(out["avg_price_per_item"], errors="coerce")
        )
    if "avg_cost_per_item" in out.columns:
        out["catalog_cost_per_item"] = out["catalog_cost_per_item"].fillna(
            pd.to_numeric(out["avg_cost_per_item"], errors="coerce")
        )

    return out


def _weighted_avg(values: pd.Series, weights: pd.Series) -> float:
    v = pd.to_numeric(values, errors="coerce")
    w = pd.to_numeric(weights, errors="coerce")
    mask = v.notna() & w.notna() & (w > 0)
    if not mask.any():
        return 0.0
    vv = v[mask].astype(float).to_numpy()
    ww = w[mask].astype(float).to_numpy()
    total_w = float(np.sum(ww))
    if total_w <= 0:
        return 0.0
    return float(np.sum(vv * ww) / total_w)


def build_deal_negotiation_data(
    inv_products_df: pd.DataFrame,
    target_margin: float = DEAL_TARGET_MARGIN,
) -> Tuple[pd.DataFrame, pd.DataFrame]:
    scenario_cols = [
        "scenario",
        "discount_pct",
        "kickback_pct",
        "avg_discounted_price",
        "avg_effective_price",
        "avg_out_the_door",
        "avg_cost",
        "avg_kickback_per_unit",
        "avg_profit_per_unit",
        "margin",
        "delta_profit_per_unit",
        "delta_margin_pp",
        "worth_it",
    ]
    break_even_cols = [
        "discount_pct",
        "required_kickback_pct_break_even",
        "required_kickback_pct_target_margin",
        "within_30pct_target",
    ]
    if inv_products_df is None or inv_products_df.empty:
        return pd.DataFrame(columns=scenario_cols), pd.DataFrame(columns=break_even_cols)

    work = inv_products_df.copy()
    price_col = next((c for c in ["shelf_price", "Price_Used", "price", "Price"] if c in work.columns), "")
    cost_col = next((c for c in ["cost", "Cost"] if c in work.columns), "")
    units_col = next((c for c in ["units_available", "Available", "units"] if c in work.columns), "")
    if not price_col or not cost_col:
        return pd.DataFrame(columns=scenario_cols), pd.DataFrame(columns=break_even_cols)

    work["_price"] = pd.to_numeric(work[price_col], errors="coerce").fillna(0.0).astype(float)
    work["_cost"] = pd.to_numeric(work[cost_col], errors="coerce").fillna(0.0).astype(float)
    if units_col:
        work["_units"] = pd.to_numeric(work[units_col], errors="coerce").fillna(0.0).astype(float)
    else:
        work["_units"] = 1.0

    work = work[(work["_price"] > 0) & (work["_cost"] >= 0) & (work["_units"] > 0)].copy()
    if work.empty:
        return pd.DataFrame(columns=scenario_cols), pd.DataFrame(columns=break_even_cols)

    units = work["_units"].astype(float)
    total_units = float(units.sum())
    if total_units <= 0:
        return pd.DataFrame(columns=scenario_cols), pd.DataFrame(columns=break_even_cols)

    rows: List[Dict[str, Any]] = []
    base_profit_per_unit = 0.0
    base_margin = 0.0
    for idx, sc in enumerate(DEAL_SCENARIOS):
        discount_pct = float(sc.get("discount_pct", 0.0))
        kickback_pct = float(sc.get("kickback_pct", 0.0))

        discounted_price = work["_price"] * (1.0 - discount_pct)
        effective_price = discounted_price * 0.63
        out_the_door = effective_price * 1.33
        kickback_amt = discounted_price * kickback_pct
        profit_unit = effective_price + kickback_amt - work["_cost"]

        total_effective = float((effective_price * units).sum())
        total_profit = float((profit_unit * units).sum())
        avg_profit_per_unit = total_profit / total_units
        margin = (total_profit / total_effective) if total_effective > 0 else 0.0

        if idx == 0:
            base_profit_per_unit = avg_profit_per_unit
            base_margin = margin

        worth_it = "BASE" if idx == 0 else ("YES" if (avg_profit_per_unit > 0 and margin >= target_margin) else "NO")
        rows.append({
            "scenario": str(sc.get("label", f"Scenario {idx + 1}")),
            "discount_pct": discount_pct,
            "kickback_pct": kickback_pct,
            "avg_discounted_price": _weighted_avg(discounted_price, units),
            "avg_effective_price": _weighted_avg(effective_price, units),
            "avg_out_the_door": _weighted_avg(out_the_door, units),
            "avg_cost": _weighted_avg(work["_cost"], units),
            "avg_kickback_per_unit": _weighted_avg(kickback_amt, units),
            "avg_profit_per_unit": avg_profit_per_unit,
            "margin": margin,
            "delta_profit_per_unit": avg_profit_per_unit - base_profit_per_unit,
            "delta_margin_pp": margin - base_margin,
            "worth_it": worth_it,
        })

    scenario_df = pd.DataFrame(rows, columns=scenario_cols)

    break_even_rows: List[Dict[str, Any]] = []
    for discount_pct in sorted({float(s.get("discount_pct", 0.0)) for s in DEAL_SCENARIOS if float(s.get("discount_pct", 0.0)) > 0}):
        discounted_price = work["_price"] * (1.0 - discount_pct)
        effective_price = discounted_price * 0.63
        denom = discounted_price.replace({0: np.nan})

        req_break_even = ((work["_cost"] - effective_price) / denom).replace([np.inf, -np.inf], np.nan).clip(lower=0.0)
        req_target = ((work["_cost"] - (1.0 - float(target_margin)) * effective_price) / denom).replace([np.inf, -np.inf], np.nan).clip(lower=0.0)

        avg_req_break_even = _weighted_avg(req_break_even.fillna(0.0), units)
        avg_req_target = _weighted_avg(req_target.fillna(0.0), units)
        break_even_rows.append({
            "discount_pct": discount_pct,
            "required_kickback_pct_break_even": avg_req_break_even,
            "required_kickback_pct_target_margin": avg_req_target,
            "within_30pct_target": "YES" if avg_req_target <= 0.30 else "NO",
        })

    break_even_df = pd.DataFrame(break_even_rows, columns=break_even_cols)
    return scenario_df, break_even_df


def _window_comp_rows(
    window_metrics: Dict[str, Dict[str, float]],
    windows: Optional[Dict[str, Tuple[date, date]]] = None,
) -> List[List[Any]]:
    order = [
        ("report", "Report Window"),
        ("last14", "Last 14 Days"),
        ("last7", "Last 7 Days"),
        ("mtd", "Month-to-Date"),
        ("prev_mtd", "Previous MTD"),
    ]
    rows: List[List[Any]] = []
    for key, label in order:
        label_out = label
        if key == "report" and windows and key in windows:
            ws, we = windows[key]
            label_out = f"{label} ({ws.isoformat()} to {we.isoformat()})"
        m = window_metrics.get(key, {})
        rows.append([
            label_out,
            money0(m.get("net_revenue", 0.0)),
            int0(m.get("tickets", 0.0)),
            money2(m.get("basket", 0.0)),
            pct1(m.get("margin_real", 0.0)),
            pct1(m.get("discount_rate", 0.0)),
        ])
    return rows


def resolve_baseline_window(
    window_metrics: Dict[str, Dict[str, float]],
    prior_window_covered: bool,
) -> Tuple[Dict[str, float], str, str, bool]:
    if prior_window_covered:
        return window_metrics.get("prior_report", {}), "Prior Window", "prior_report", True

    prev_mtd = window_metrics.get("prev_mtd", {})
    if float(prev_mtd.get("row_count", 0.0)) > 0:
        return prev_mtd, "Previous MTD", "prev_mtd", True

    return {}, "Prior Window", "prior_report", False


def window_label_with_dates(
    label: str,
    key: str,
    windows: Dict[str, Tuple[date, date]],
) -> str:
    rng = windows.get(key)
    if not rng:
        return label
    s, e = rng
    return f"{label} ({s.isoformat()} → {e.isoformat()})"


def _delta_currency(cur: float, base: float, label: str, enabled: bool) -> str:
    if not enabled:
        return f"<font color='#374151'>vs {label}: n/a</font>"
    return osnap.delta_html_currency(cur, base, label)


def _delta_int(cur: float, base: float, label: str, enabled: bool) -> str:
    if not enabled:
        return f"<font color='#374151'>vs {label}: n/a</font>"
    return osnap.delta_html_int(cur, base, label)


def _delta_pp(cur: float, base: float, label: str, enabled: bool) -> str:
    if not enabled:
        return f"<font color='#374151'>vs {label}: n/a</font>"
    return osnap.delta_html_pp(cur, base, label)


def build_exec_kpi_grid(
    styles: Dict[str, Any],
    report_metrics: Dict[str, float],
    baseline_metrics: Dict[str, float],
    baseline_label: str,
    compare_enabled: bool,
    inv_overview: Dict[str, float],
) -> Any:
    note = "<font color='#374151'>Current snapshot</font>"
    cells = [
        osnap.kpi_cell(
            styles,
            "Net Revenue",
            money0(report_metrics.get("net_revenue", 0.0)),
            _delta_currency(
                float(report_metrics.get("net_revenue", 0.0)),
                float(baseline_metrics.get("net_revenue", 0.0)),
                baseline_label,
                compare_enabled,
            ),
        ),
        osnap.kpi_cell(
            styles,
            "Profit",
            money0(report_metrics.get("profit_real", 0.0)),
            _delta_currency(
                float(report_metrics.get("profit_real", 0.0)),
                float(baseline_metrics.get("profit_real", 0.0)),
                baseline_label,
                compare_enabled,
            ),
        ),
        osnap.kpi_cell(
            styles,
            "Margin",
            pct1(report_metrics.get("margin_real", 0.0)),
            _delta_pp(
                float(report_metrics.get("margin_real", 0.0)),
                float(baseline_metrics.get("margin_real", 0.0)),
                baseline_label,
                compare_enabled,
            ),
        ),
        osnap.kpi_cell(
            styles,
            "Tickets",
            int0(report_metrics.get("tickets", 0.0)),
            _delta_int(
                float(report_metrics.get("tickets", 0.0)),
                float(baseline_metrics.get("tickets", 0.0)),
                baseline_label,
                compare_enabled,
            ),
        ),
        osnap.kpi_cell(
            styles,
            "Basket",
            money2(report_metrics.get("basket", 0.0)),
            _delta_currency(
                float(report_metrics.get("basket", 0.0)),
                float(baseline_metrics.get("basket", 0.0)),
                baseline_label,
                compare_enabled,
            ),
        ),
        osnap.kpi_cell(
            styles,
            "Discount Rate",
            pct1(report_metrics.get("discount_rate", 0.0)),
            _delta_pp(
                float(report_metrics.get("discount_rate", 0.0)),
                float(baseline_metrics.get("discount_rate", 0.0)),
                baseline_label,
                compare_enabled,
            ),
        ),
        osnap.kpi_cell(styles, "Inventory Units", int0(inv_overview.get("units", 0.0)), note),
        osnap.kpi_cell(
            styles,
            "Inventory Value",
            inventory_value_with_units(inv_overview.get("inventory_value", 0.0), inv_overview.get("units", 0.0)),
            note,
        ),
        osnap.kpi_cell(styles, "Potential Revenue", money0(inv_overview.get("potential_revenue", 0.0)), note),
        osnap.kpi_cell(styles, "Potential Profit", money0(inv_overview.get("potential_profit", 0.0)), note),
        osnap.kpi_cell(styles, "Avg Inventory Margin", pct1(inv_overview.get("avg_margin", 0.0)), note),
        osnap.kpi_cell(styles, "Units Sold", int0(report_metrics.get("items", 0.0)), note),
        osnap.kpi_cell(styles, "Trend Units/Day (30d)", f"{float(inv_overview.get('trend_units_per_day_30d', inv_overview.get('trend_units_per_day_14d', 0.0))):,.1f}", note),
        osnap.kpi_cell(styles, "Trend Units/Day (14d)", f"{float(inv_overview.get('trend_units_per_day_14d', 0.0)):,.1f}", note),
        osnap.kpi_cell(styles, "Trend Units/Day (7d)", f"{float(inv_overview.get('trend_units_per_day_7d', 0.0)):,.1f}", note),
        osnap.kpi_cell(styles, "Days of Supply", days1(inv_overview.get("days_of_supply", np.nan)), "<font color='#374151'>at current 30d trend</font>"),
    ]
    return osnap.build_kpi_grid(styles, cells, cols=4)


def build_store_kpi_grid(
    styles: Dict[str, Any],
    report_metrics: Dict[str, float],
    baseline_metrics: Dict[str, float],
    baseline_label: str,
    compare_enabled: bool,
) -> Any:
    cells = [
        osnap.kpi_cell(
            styles,
            "Net Revenue",
            money0(report_metrics.get("net_revenue", 0.0)),
            _delta_currency(
                float(report_metrics.get("net_revenue", 0.0)),
                float(baseline_metrics.get("net_revenue", 0.0)),
                baseline_label,
                compare_enabled,
            ),
        ),
        osnap.kpi_cell(
            styles,
            "Profit",
            money0(report_metrics.get("profit_real", 0.0)),
            _delta_currency(
                float(report_metrics.get("profit_real", 0.0)),
                float(baseline_metrics.get("profit_real", 0.0)),
                baseline_label,
                compare_enabled,
            ),
        ),
        osnap.kpi_cell(
            styles,
            "Margin",
            pct1(report_metrics.get("margin_real", 0.0)),
            _delta_pp(
                float(report_metrics.get("margin_real", 0.0)),
                float(baseline_metrics.get("margin_real", 0.0)),
                baseline_label,
                compare_enabled,
            ),
        ),
        osnap.kpi_cell(
            styles,
            "Tickets",
            int0(report_metrics.get("tickets", 0.0)),
            _delta_int(
                float(report_metrics.get("tickets", 0.0)),
                float(baseline_metrics.get("tickets", 0.0)),
                baseline_label,
                compare_enabled,
            ),
        ),
        osnap.kpi_cell(styles, "Basket", money2(report_metrics.get("basket", 0.0)), "<font color='#374151'>Sales summary</font>"),
        osnap.kpi_cell(styles, "Units Sold", int0(report_metrics.get("items", 0.0)), "<font color='#374151'>Sales summary</font>"),
        osnap.kpi_cell(styles, "Discount Rate", pct1(report_metrics.get("discount_rate", 0.0)), "<font color='#374151'>Sales summary</font>"),
        osnap.kpi_cell(styles, "Avg Price / Unit", money2(report_metrics.get("net_price_per_item", 0.0)), "<font color='#374151'>Sales summary</font>"),
    ]
    return osnap.build_kpi_grid(styles, cells, cols=4)


def build_all_store_slow_mover_kpi_grid(
    styles: Dict[str, Any],
    store_summary: pd.DataFrame,
    store_brand_summary: pd.DataFrame,
    store_product_candidates: pd.DataFrame,
    selected_store_codes: Sequence[str],
) -> Any:
    stores_flagged = int(len(store_summary)) if store_summary is not None else 0
    brand_rows = int(len(store_brand_summary)) if store_brand_summary is not None else 0
    cut_sku_count = 0
    review_sku_count = 0
    no_sales = 0
    inv_risk = 0.0
    cut_inventory = 0.0
    healthy_trim_brands = 0
    if store_brand_summary is not None and not store_brand_summary.empty:
        inv_risk = float(pd.to_numeric(store_brand_summary["actionable_inventory_value"], errors="coerce").fillna(0.0).sum())
        cut_inventory = float(pd.to_numeric(store_brand_summary["cut_inventory_value"], errors="coerce").fillna(0.0).sum())
        if "report_status" in store_brand_summary.columns:
            healthy_trim_brands = int((
                store_brand_summary["report_status"].fillna("").astype(str) == "Healthy brand, trim a few SKUs"
            ).sum())
    if store_product_candidates is not None and not store_product_candidates.empty:
        action_series = store_product_candidates["action"].fillna("").astype(str) if "action" in store_product_candidates.columns else pd.Series("", index=store_product_candidates.index)
        sold_series = pd.to_numeric(store_product_candidates["units_sold_window"], errors="coerce").fillna(0.0) if "units_sold_window" in store_product_candidates.columns else pd.Series(0.0, index=store_product_candidates.index)
        cut_mask = action_series.str.startswith("Cut")
        review_mask = action_series.str.startswith("Review")
        cut_sku_count = int(cut_mask.sum())
        review_sku_count = int(review_mask.sum())
        no_sales = int((cut_mask & (sold_series <= 0)).sum())

    cells = [
        osnap.kpi_cell(styles, "Stores Flagged", int0(stores_flagged), "<font color='#374151'>stores with at least one brand to trim</font>"),
        osnap.kpi_cell(styles, "Store-Brand Rows", int0(brand_rows), "<font color='#374151'>brand/store combinations in the report</font>"),
        osnap.kpi_cell(styles, "SKUs To Cut", int0(cut_sku_count), "<font color='#374151'>high-confidence cut candidates</font>"),
        osnap.kpi_cell(styles, "SKUs To Review", int0(review_sku_count), "<font color='#374151'>slow SKUs worth checking first</font>"),
        osnap.kpi_cell(styles, "Inventory At Risk", money0(inv_risk), "<font color='#374151'>inventory tied to cut or review SKUs</font>"),
        osnap.kpi_cell(styles, "Cut Inventory", money0(cut_inventory), "<font color='#374151'>inventory tied only to cut SKUs</font>"),
        osnap.kpi_cell(styles, "No-Sales Cut SKUs", int0(no_sales), "<font color='#374151'>cut candidates with zero sales in window</font>"),
        osnap.kpi_cell(styles, "Healthy Brands w/ Trims", int0(healthy_trim_brands), "<font color='#374151'>good brands with only a few local cuts</font>"),
    ]
    return osnap.build_kpi_grid(styles, cells, cols=4)


def build_all_store_slow_mover_pdf(
    out_pdf: Path,
    start_day: date,
    end_day: date,
    selected_store_codes: Sequence[str],
    report_days: int,
    store_summary: pd.DataFrame,
    store_brand_summary: pd.DataFrame,
    store_category_candidates: pd.DataFrame,
    store_product_candidates: pd.DataFrame,
    thresholds: Dict[str, float],
) -> None:
    osnap.setup_fonts()
    styles = osnap.build_styles()
    generated_at = datetime.now(ZoneInfo(REPORT_TZ)).strftime("%B %d, %Y at %I:%M %p %Z")

    out_pdf.parent.mkdir(parents=True, exist_ok=True)
    doc = SimpleDocTemplate(
        str(out_pdf),
        pagesize=letter,
        leftMargin=osnap.PAGE_MARGINS["left"],
        rightMargin=osnap.PAGE_MARGINS["right"],
        topMargin=osnap.PAGE_MARGINS["top"],
        bottomMargin=osnap.PAGE_MARGINS["bottom"],
        pageCompression=1,
        title=f"Store SKU Cuts - {start_day.isoformat()} to {end_day.isoformat()}",
        author="Buzz Automation",
    )

    story: List[Any] = []
    story.append(Paragraph("Store SKU Cut Review", styles["TitleBig"]))
    story.append(osnap.build_report_day_band(end_day, width=7.6 * inch))
    story.append(Spacer(1, osnap.SPACER["xs"]))
    story.append(Paragraph(
        f"<b>Data Window:</b> {start_day.isoformat()} → {end_day.isoformat()}"
        f" &nbsp;&nbsp; <b>Generated:</b> {generated_at}"
        f" &nbsp;&nbsp; <b>Stores:</b> {', '.join(selected_store_codes)}",
        styles["Tiny"],
    ))
    story.append(Paragraph(
        f"<b>Thresholds:</b> Review DOS ≥ {days1(thresholds.get('review_days_of_supply', np.nan))}"
        f" &nbsp;&nbsp; Cut DOS ≥ {days1(thresholds.get('cut_days_of_supply', np.nan))}"
        f" &nbsp;&nbsp; Low Sell-Through ≤ {pct1(float(thresholds.get('low_sell_through', 0.0)))}",
        styles["Tiny"],
    ))
    story.append(Paragraph(
        "This report keeps every store separate. A brand can be healthy overall and still appear here when one store has stale or redundant SKUs worth trimming.",
        styles["Muted"],
    ))
    story.append(Paragraph(
        f"Window length: {int0(float(report_days))} days. “Healthy brand, trim a few SKUs” means keep the brand, but reduce the weaker SKUs listed for that store.",
        styles["Tiny"],
    ))
    story.append(Spacer(1, osnap.SPACER["sm"]))

    story.append(Paragraph("Executive Summary", styles["Section"]))
    story.append(build_all_store_slow_mover_kpi_grid(
        styles=styles,
        store_summary=store_summary,
        store_brand_summary=store_brand_summary,
        store_product_candidates=store_product_candidates,
        selected_store_codes=selected_store_codes,
    ))
    story.append(Spacer(1, 0.06 * inch))

    if store_summary is not None and not store_summary.empty:
        store_rows: List[List[Any]] = []
        for r in store_summary.itertuples(index=False):
            store_label = f"{str(getattr(r, 'store_name', '') or getattr(r, '_store_abbr', ''))} ({str(getattr(r, '_store_abbr', ''))})"
            store_rows.append([
                store_label,
                int0(float(getattr(r, "brands_to_trim", 0.0))),
                int0(float(getattr(r, "cut_sku_count", 0.0))),
                int0(float(getattr(r, "review_sku_count", 0.0))),
                money0(float(getattr(r, "inventory_at_risk", 0.0))),
                str(getattr(r, "top_brand", "") or "n/a"),
            ])
        story.append(Paragraph("Stores Needing Action", styles["Section"]))
        story.append(_build_table_fit(
            ["Store", "Brands", "Cut SKUs", "Review SKUs", "Inv At Risk", "Lead Brand"],
            store_rows,
            [2.18 * inch, 0.62 * inch, 0.74 * inch, 0.82 * inch, 1.00 * inch, 1.64 * inch],
        ))
        story.append(Spacer(1, 0.06 * inch))

    if store_brand_summary is None or store_brand_summary.empty:
        story.append(Spacer(1, 0.08 * inch))
        story.append(Paragraph("No store-specific SKU cut candidates were found for this window.", styles["Muted"]))
    else:
        top_rows: List[List[Any]] = []
        for r in store_brand_summary.head(18).itertuples(index=False):
            top_rows.append([
                str(getattr(r, "_store_abbr", "")),
                str(getattr(r, "brand_name", "")),
                str(getattr(r, "report_status", "")),
                int0(float(getattr(r, "cut_sku_count", 0.0))),
                int0(float(getattr(r, "review_sku_count", 0.0))),
                money0(float(getattr(r, "cut_inventory_value", 0.0))),
                str(getattr(r, "lead_sku", "") or "n/a"),
            ])
        story.append(Paragraph("Top Store-Brand Trim Rows", styles["Section"]))
        story.append(_build_table_fit(
            ["Store", "Brand", "Status", "Cut", "Review", "Cut Inv", "Lead SKU"],
            top_rows,
            [0.58 * inch, 1.24 * inch, 1.92 * inch, 0.42 * inch, 0.52 * inch, 0.76 * inch, 1.76 * inch],
        ))
        if len(store_brand_summary) > 18:
            story.append(Paragraph(f"Showing top 18 of {len(store_brand_summary)} store-brand rows. See the workbook for the full list.", styles["Tiny"]))

        for abbr in order_store_codes(list(selected_store_codes)):
            store_brands = store_brand_summary[
                store_brand_summary["_store_abbr"].fillna("").astype(str) == abbr
            ].copy()
            if store_brands.empty:
                continue

            store_row = store_summary[
                store_summary["_store_abbr"].fillna("").astype(str) == abbr
            ].head(1) if store_summary is not None and not store_summary.empty else pd.DataFrame()
            store_products = store_product_candidates[
                store_product_candidates["_store_abbr"].fillna("").astype(str) == abbr
            ].copy() if store_product_candidates is not None and not store_product_candidates.empty else pd.DataFrame()
            store_categories = store_category_candidates[
                store_category_candidates["_store_abbr"].fillna("").astype(str) == abbr
            ].copy() if store_category_candidates is not None and not store_category_candidates.empty else pd.DataFrame()

            store_name = str(store_brands["store_name"].iloc[0]) if "store_name" in store_brands.columns else _store_name_from_abbr(abbr)
            action_series = store_products["action"].fillna("").astype(str) if not store_products.empty and "action" in store_products.columns else pd.Series(dtype=str)
            sold_series = pd.to_numeric(store_products["units_sold_window"], errors="coerce").fillna(0.0) if not store_products.empty and "units_sold_window" in store_products.columns else pd.Series(dtype=float)
            cut_mask = action_series.str.startswith("Cut") if len(action_series) else pd.Series(dtype=bool)
            review_mask = action_series.str.startswith("Review") if len(action_series) else pd.Series(dtype=bool)

            story.append(PageBreak())
            story.append(Paragraph(f"{store_name} ({abbr})", styles["TitleBig"]))
            story.append(Paragraph(
                "Each brand below is evaluated inside this store only, so strong chain-wide brands can still show a few local cut candidates.",
                styles["Muted"],
            ))
            brands_to_trim = float(store_row["brands_to_trim"].iloc[0]) if not store_row.empty and "brands_to_trim" in store_row.columns else float(store_brands["store_brand_key"].nunique())
            inventory_at_risk = float(store_row["inventory_at_risk"].iloc[0]) if not store_row.empty and "inventory_at_risk" in store_row.columns else float(pd.to_numeric(store_brands["actionable_inventory_value"], errors="coerce").fillna(0.0).sum())
            cut_inventory = float(store_row["cut_inventory_value"].iloc[0]) if not store_row.empty and "cut_inventory_value" in store_row.columns else float(pd.to_numeric(store_brands["cut_inventory_value"], errors="coerce").fillna(0.0).sum())
            top_brand = str(store_row["top_brand"].iloc[0]) if not store_row.empty and "top_brand" in store_row.columns else ""
            healthy_trim_store = int((store_brands["report_status"].fillna("").astype(str) == "Healthy brand, trim a few SKUs").sum()) if "report_status" in store_brands.columns else 0

            store_cells = [
                osnap.kpi_cell(styles, "Brands To Trim", int0(brands_to_trim), "<font color='#374151'>store-brand rows with action</font>"),
                osnap.kpi_cell(styles, "Cut SKUs", int0(float(cut_mask.sum()) if len(cut_mask) else 0.0), "<font color='#374151'>high-confidence cuts in this store</font>"),
                osnap.kpi_cell(styles, "Review SKUs", int0(float(review_mask.sum()) if len(review_mask) else 0.0), "<font color='#374151'>slow SKUs worth a second look</font>"),
                osnap.kpi_cell(styles, "Inventory At Risk", money0(inventory_at_risk), "<font color='#374151'>cut + review inventory</font>"),
                osnap.kpi_cell(styles, "Cut Inventory", money0(cut_inventory), "<font color='#374151'>inventory tied to cut SKUs</font>"),
                osnap.kpi_cell(styles, "No-Sales Cuts", int0(float((cut_mask & (sold_series <= 0)).sum()) if len(cut_mask) else 0.0), "<font color='#374151'>cut SKUs with zero sales</font>"),
                osnap.kpi_cell(styles, "Healthy Brands w/ Trims", int0(float(healthy_trim_store)), "<font color='#374151'>good brands with only a few local cuts</font>"),
                osnap.kpi_cell(styles, "Lead Brand", top_brand or "n/a", "<font color='#374151'>largest cut-inventory brand in this store</font>"),
            ]
            story.append(osnap.build_kpi_grid(styles, store_cells, cols=4))
            story.append(Spacer(1, 0.05 * inch))

            brand_rows: List[List[Any]] = []
            for r in store_brands.itertuples(index=False):
                brand_rows.append([
                    str(getattr(r, "brand_name", "")),
                    str(getattr(r, "report_status", "")),
                    int0(float(getattr(r, "cut_sku_count", 0.0))),
                    int0(float(getattr(r, "review_sku_count", 0.0))),
                    int0(float(getattr(r, "no_sales_cut_skus", 0.0))),
                    money0(float(getattr(r, "cut_inventory_value", 0.0))),
                    pct1(float(getattr(r, "sell_through_ratio", 0.0))),
                    days1(getattr(r, "days_of_supply", np.nan)),
                    str(getattr(r, "lead_sku", "") or "n/a"),
                ])
            story.append(Paragraph("Brands With SKU Actions", styles["Section"]))
            story.append(_build_table_fit(
                ["Brand", "Status", "Cut", "Review", "No-Sales", "Cut Inv", "Sell-Thru", "DOS", "Lead SKU"],
                brand_rows,
                [1.10 * inch, 1.74 * inch, 0.40 * inch, 0.48 * inch, 0.58 * inch, 0.76 * inch, 0.63 * inch, 0.55 * inch, 1.36 * inch],
            ))

            for brand_row in store_brands.itertuples(index=False):
                brand_key = str(getattr(brand_row, "brand_key", ""))
                store_brand_key = str(getattr(brand_row, "store_brand_key", ""))
                brand_name = str(getattr(brand_row, "brand_name", "Unknown"))
                report_status = str(getattr(brand_row, "report_status", ""))

                brand_categories = store_categories[
                    store_categories["brand_key"].fillna("").astype(str) == brand_key
                ].copy() if not store_categories.empty else pd.DataFrame()
                brand_products = store_products[
                    store_products["store_brand_key"].fillna("").astype(str) == store_brand_key
                ].copy() if not store_products.empty else pd.DataFrame()
                if brand_categories.empty and brand_products.empty:
                    continue

                story.append(CondPageBreak(3.2 * inch))
                story.append(Spacer(1, 0.05 * inch))
                story.append(Paragraph(brand_name, styles["Section"]))
                status_note = (
                    "Brand sells well overall in this store. The rows below are the weaker SKUs to trim."
                    if report_status == "Healthy brand, trim a few SKUs"
                    else "This store-brand row has enough stale inventory to justify cutting or reducing the SKUs below."
                )
                story.append(Paragraph(
                    f"<b>Status:</b> {xml_escape(report_status)}"
                    f" &nbsp;&nbsp; <b>Cut SKUs:</b> {int0(float(getattr(brand_row, 'cut_sku_count', 0.0)))}"
                    f" &nbsp;&nbsp; <b>Review SKUs:</b> {int0(float(getattr(brand_row, 'review_sku_count', 0.0)))}"
                    f" &nbsp;&nbsp; <b>Cut Inventory:</b> {money0(float(getattr(brand_row, 'cut_inventory_value', 0.0)))}",
                    styles["Muted"],
                ))
                story.append(Paragraph(status_note, styles["Tiny"]))

                if not brand_categories.empty:
                    cat_rows: List[List[Any]] = []
                    for r in brand_categories.itertuples(index=False):
                        cat_rows.append([
                            str(getattr(r, "category_normalized", "")),
                            str(getattr(r, "status", "")),
                            int0(float(getattr(r, "cut_sku_count", 0.0))),
                            int0(float(getattr(r, "review_sku_count", 0.0))),
                            money0(float(getattr(r, "cut_inventory_value", 0.0))),
                            pct1(float(getattr(r, "sell_through_ratio", 0.0))),
                            days1(getattr(r, "days_of_supply", np.nan)),
                        ])
                    story.append(Paragraph("Category Pressure", styles["Small"]))
                    story.append(_build_table_fit(
                        ["Category", "Status", "Cut", "Review", "Cut Inv", "Sell-Thru", "DOS"],
                        cat_rows,
                        [1.25 * inch, 1.75 * inch, 0.45 * inch, 0.52 * inch, 0.85 * inch, 0.70 * inch, 0.60 * inch],
                    ))
                    story.append(Spacer(1, 0.03 * inch))

                if not brand_products.empty:
                    prod_rows: List[List[Any]] = []
                    for r in brand_products.itertuples(index=False):
                        prod_rows.append([
                            str(getattr(r, "display_product", "")),
                            str(getattr(r, "action", "")),
                            money0(float(getattr(r, "inventory_value", 0.0))),
                            int0(float(getattr(r, "units_available", 0.0))),
                            int0(float(getattr(r, "units_sold_window", 0.0))),
                            days1(getattr(r, "days_of_supply", np.nan)),
                            str(getattr(r, "last_sale_date", "") or "n/a"),
                        ])
                    story.append(Paragraph("SKU Candidates", styles["Small"]))
                    story.append(_build_table_fit(
                        ["SKU", "Action", "Inv Value", "Units", "Sold", "DOS", "Last Sale"],
                        prod_rows,
                        [2.65 * inch, 1.30 * inch, 0.80 * inch, 0.45 * inch, 0.45 * inch, 0.55 * inch, 0.78 * inch],
                    ))

    footer = _footer("Store SKU Cuts", end_day)
    doc.build(story, onFirstPage=footer, onLaterPages=footer)


def build_brand_packet_quick_pdf(
    out_pdf: Path,
    brand: str,
    start_day: date,
    end_day: date,
    options: PacketOptions,
    windows: Dict[str, Tuple[date, date]],
    window_metrics: Dict[str, Dict[str, float]],
    prior_window_covered: bool,
    daily_60: pd.DataFrame,
    store_60: pd.DataFrame,
    category_60: pd.DataFrame,
    product_60: pd.DataFrame,
    movers_store: pd.DataFrame,
    movers_category: pd.DataFrame,
    movers_product: pd.DataFrame,
    inv_overview: Dict[str, float],
    inv_products: pd.DataFrame,
    inv_category: pd.DataFrame,
    inv_store: pd.DataFrame,
    store_sales_packets: Dict[str, Dict[str, Any]],
    missing_sales_stores: Sequence[str],
    missing_catalog_stores: Sequence[str],
    credit_summary: Optional[Dict[str, Any]] = None,
    credit_reconciliation: Optional[pd.DataFrame] = None,
    action_items: Optional[Sequence[Dict[str, Any]]] = None,
    monthly_reference: Optional[Dict[str, Any]] = None,
    health_score: int = 0,
    health_status: str = "",
    health_reason: str = "",
    meeting_ask: str = "",
    store_credit_scorecard: Optional[pd.DataFrame] = None,
) -> None:
    osnap.setup_fonts()
    styles = osnap.build_styles()
    generated_at = datetime.now(ZoneInfo(REPORT_TZ)).strftime("%B %d, %Y at %I:%M %p %Z")
    credit_summary = credit_summary or {}
    credit_reconciliation = credit_reconciliation if credit_reconciliation is not None else pd.DataFrame()
    action_items = list(action_items or [])
    monthly_reference = monthly_reference or {}
    store_credit_scorecard = store_credit_scorecard if store_credit_scorecard is not None else pd.DataFrame()
    mode = str(options.packet_mode or "standard").lower()
    main_top_n = 6 if mode == "quick" else (15 if mode == "deep" else 10)
    main_top_n = min(max(5, int(options.top_n or main_top_n)), main_top_n)

    out_pdf.parent.mkdir(parents=True, exist_ok=True)
    doc = SimpleDocTemplate(
        str(out_pdf),
        pagesize=letter,
        leftMargin=osnap.PAGE_MARGINS["left"],
        rightMargin=osnap.PAGE_MARGINS["right"],
        topMargin=osnap.PAGE_MARGINS["top"],
        bottomMargin=osnap.PAGE_MARGINS["bottom"],
        pageCompression=1,
        title=f"Brand Meeting Packet - {brand} (Quick Combined)",
        author="Buzz Automation",
    )

    story: List[Any] = []
    story.append(Paragraph(f"{brand} • Quick Combined View", styles["TitleBig"]))
    story.append(osnap.build_report_day_band(end_day, width=7.6 * inch))
    story.append(Spacer(1, osnap.SPACER["xs"]))
    story.append(Paragraph(
        f"<b>Data Window:</b> {start_day.isoformat()} → {end_day.isoformat()}"
        f" &nbsp;&nbsp; <b>Generated:</b> {generated_at}",
        styles["Tiny"],
    ))
    if missing_sales_stores:
        story.append(Paragraph(
            f"<font color='#B91C1C'><b>Missing Sales Stores:</b> {', '.join(missing_sales_stores)}</font>",
            styles["Tiny"],
        ))
    if missing_catalog_stores:
        story.append(Paragraph(
            f"<font color='#B91C1C'><b>Missing Catalog Stores:</b> {', '.join(missing_catalog_stores)}</font>",
            styles["Tiny"],
        ))
    story.append(Spacer(1, osnap.SPACER["sm"]))

    report_metrics = window_metrics.get("report", {})
    baseline_metrics, baseline_label, baseline_key, compare_enabled = resolve_baseline_window(
        window_metrics,
        prior_window_covered,
    )
    baseline_label_with_dates = window_label_with_dates(baseline_label, baseline_key, windows)

    story.append(Paragraph("Executive Summary", styles["Section"]))
    if compare_enabled:
        delta_net = float(report_metrics.get("net_revenue", 0.0) - baseline_metrics.get("net_revenue", 0.0))
        delta_margin = float(report_metrics.get("margin_real", 0.0) - baseline_metrics.get("margin_real", 0.0))
        story.append(Paragraph(
            f"<b>Net Δ vs {baseline_label_with_dates}:</b> {osnap.fmt_signed_money(delta_net)}"
            f" &nbsp;&nbsp; <b>Margin Δ:</b> {osnap.pp1(delta_margin)}",
            styles["Muted"],
        ))
    else:
        prior_start, prior_end = windows["prior_report"]
        story.append(Paragraph(
            f"<b>Net Δ vs Prior Window:</b> n/a"
            f" &nbsp;&nbsp; <b>Reason:</b> no comparable baseline loaded "
            f"({prior_start.isoformat()} → {prior_end.isoformat()})",
            styles["Muted"],
        ))

    story.append(Spacer(1, 0.04 * inch))
    if options.include_credit_reconciliation:
        story.append(Paragraph(
            f"<b>Meeting Status:</b> {xml_escape(health_status or 'Review')} &nbsp;&nbsp; "
            f"<b>Brand Health:</b> {int(health_score or 0)}/100 &nbsp;&nbsp; "
            f"<b>Why:</b> {xml_escape(health_reason or 'No health flags generated.')}",
            styles["Muted"],
        ))
        story.append(Spacer(1, 0.04 * inch))
        story.append(_credit_metric_grid(styles, credit_summary, int(health_score or 0), health_status or "Review"))
        story.append(Spacer(1, 0.05 * inch))
        ask = meeting_ask or generate_meeting_ask(credit_summary, action_items)
        story.append(Paragraph(f"<b>Recommended Meeting Ask:</b> {xml_escape(ask)}", styles["Muted"]))
        if action_items:
            rows = _action_item_rows(action_items, top_n=3)
            story.append(Spacer(1, 0.04 * inch))
            story.append(_build_table_fit(
                ["Priority", "Category", "Problem", "Evidence", "Brand Action", "Affected", "$"],
                rows,
                [0.65 * inch, 1.0 * inch, 1.15 * inch, 1.85 * inch, 1.15 * inch, 1.0 * inch, 0.5 * inch],
            ))
        story.append(PageBreak())

        story.append(Paragraph("Margin Truth + Credit Reconciliation", styles["TitleBig"]))
        story.append(Paragraph(
            "Real margin uses actual sales and COGS only. Expected and received margins are shown separately so support is not double-counted.",
            styles["Tiny"],
        ))
        story.append(Spacer(1, 0.05 * inch))
        story.append(_credit_metric_grid(styles, credit_summary, int(health_score or 0), health_status or "Review"))
        rec_rows = _credit_reconciliation_rows(credit_reconciliation, top_n=main_top_n + 2)
        if rec_rows:
            story.append(Spacer(1, 0.06 * inch))
            story.append(_build_table_fit(
                ["Type", "Scope", "Expected", "Received", "Gap", "Status", "Lift Exp", "Lift Rec", "Notes"],
                rec_rows,
                [0.95 * inch, 0.9 * inch, 0.8 * inch, 0.8 * inch, 0.75 * inch, 0.7 * inch, 0.65 * inch, 0.65 * inch, 1.1 * inch],
            ))
        else:
            story.append(Paragraph("No manual credit ledger rows matched this brand/date window.", styles["Muted"]))
        story.append(Spacer(1, 0.05 * inch))
        story.append(Paragraph(f"<b>Recommended Ask:</b> {xml_escape(ask)}", styles["Muted"]))
        story.append(PageBreak())

        story.append(Paragraph("What The Brand Can Do Better", styles["TitleBig"]))
        action_rows = _action_item_rows(action_items, top_n=main_top_n)
        if action_rows:
            story.append(_build_table_fit(
                ["Priority", "Category", "Problem", "Evidence", "Brand Action", "Affected", "$"],
                action_rows,
                [0.65 * inch, 1.0 * inch, 1.15 * inch, 1.95 * inch, 1.15 * inch, 1.0 * inch, 0.45 * inch],
            ))
        else:
            story.append(Paragraph("No high-priority rule-based action items were triggered for this window.", styles["Muted"]))
        story.append(Spacer(1, 0.05 * inch))

        store_credit_rows = _store_credit_rows(store_credit_scorecard)
        if store_credit_rows:
            story.append(Paragraph("Store Scorecards", styles["Section"]))
            story.append(_build_table_fit(
                ["Store", "Net", "Units", "Real Margin", "Rec. Credit Margin", "Credit Gap", "Disc Rate", "Inventory", "Days Supply", "Status"],
                store_credit_rows,
                [0.45 * inch, 0.85 * inch, 0.55 * inch, 0.75 * inch, 0.95 * inch, 0.8 * inch, 0.7 * inch, 0.8 * inch, 0.75 * inch, 0.7 * inch],
            ))
            story.append(Spacer(1, 0.05 * inch))

        monthly_rows = _monthly_reference_rows(monthly_reference, top_n=main_top_n)
        if monthly_rows:
            story.append(Paragraph("Monthly Context", styles["Section"]))
            story.append(_build_table_fit(
                ["Context", "Store", "Revenue", "Margin", "Disc Rate", "Note"],
                monthly_rows,
                [1.0 * inch, 0.75 * inch, 1.0 * inch, 0.75 * inch, 0.75 * inch, 3.05 * inch],
            ))
        elif options.include_monthly_reference:
            story.append(Paragraph("Monthly owner-report reference was not available for this date window.", styles["Tiny"]))
        story.append(PageBreak())

    if category_60 is not None and not category_60.empty:
        story.append(Spacer(1, 0.05 * inch))
        story.append(Paragraph("Top Categories (All Stores, 60d)", styles["Section"]))
        if options.include_charts:
            ch_top_cat = chart_rank_barh(
                category_60,
                "category_normalized",
                "net_revenue",
                "Top Categories by Net Sales (All Stores)",
                value_kind="money",
            )
            if ch_top_cat.getbuffer().nbytes > 0:
                story.append(Image(ch_top_cat, width=7.3 * inch, height=2.75 * inch))
                story.append(Spacer(1, 0.04 * inch))

        cat_rows = []
        for r in category_60.head(main_top_n).itertuples(index=False):
            cat_rows.append([
                str(getattr(r, "category_normalized", "")),
                money0(float(getattr(r, "net_revenue", 0.0))),
                int0(float(getattr(r, "items", 0.0))),
                pct1(float(getattr(r, "margin_real", 0.0))),
            ])
        if cat_rows:
            story.append(_build_table_fit(
                ["Category", "Net", "Units", "Margin"],
                cat_rows,
                [3.5 * inch, 1.3 * inch, 1.1 * inch, 1.4 * inch],
            ))
            story.append(Spacer(1, 0.05 * inch))

    story.append(Paragraph("Quick Store Dashboards", styles["Section"]))
    story.append(Paragraph(
        "Each store is shown separately so performance differences are clear at a glance.",
        styles["Muted"],
    ))
    story.append(Spacer(1, 0.06 * inch))

    if not store_sales_packets:
        story.append(Paragraph("No store-level data found for this brand in the selected window.", styles["Muted"]))

    for idx, abbr in enumerate(order_store_codes(list(store_sales_packets.keys()))):
        pkt = store_sales_packets.get(abbr, {})
        if idx > 0:
            story.append(PageBreak())

        store_name = str(pkt.get("store_name", _store_name_from_abbr(abbr)))
        wm = pkt.get("window_metrics", {}) or {}
        m_report = wm.get("report", {}) or {}
        inv_store_metrics = pkt.get("inventory", {}) or {}
        s_prior_covered = bool(pkt.get("prior_window_covered", False))
        baseline_m, baseline_lbl, baseline_key, cmp_enabled = resolve_baseline_window(wm, s_prior_covered)
        baseline_lbl_dates = window_label_with_dates(baseline_lbl, baseline_key, windows)

        story.append(Paragraph(f"{store_name} ({abbr})", styles["TitleBig"]))
        story.append(build_store_kpi_grid(
            styles=styles,
            report_metrics=m_report,
            baseline_metrics=baseline_m,
            baseline_label=baseline_lbl_dates,
            compare_enabled=cmp_enabled,
        ))
        story.append(Spacer(1, 0.05 * inch))

        story.append(_build_table_fit(
            ["Window", "Net", "Tickets", "Basket", "Margin", "Disc Rate"],
            _window_comp_rows(wm, windows),
            [2.05 * inch, 1.25 * inch, 0.95 * inch, 1.05 * inch, 1.05 * inch, 1.15 * inch],
        ))
        story.append(Spacer(1, 0.04 * inch))

        story.append(_build_table_fit(
            ["Inventory Metric", "Value"],
            [
                ["Units Available", int0(inv_store_metrics.get("units_available", 0.0))],
                ["Inventory Value", inventory_value_with_units(inv_store_metrics.get("inventory_value", 0.0), inv_store_metrics.get("units_available", 0.0))],
                ["Potential Profit", money0(inv_store_metrics.get("potential_profit", 0.0))],
                ["Trend Units / Day (30d)", f"{float(inv_store_metrics.get('trend_units_per_day_30d', inv_store_metrics.get('trend_units_per_day_14d', 0.0))):,.1f}"],
                ["Trend Units / Day (14d)", f"{float(inv_store_metrics.get('trend_units_per_day_14d', 0.0)):,.1f}"],
                ["Trend Units / Day (7d)", f"{float(inv_store_metrics.get('trend_units_per_day_7d', 0.0)):,.1f}"],
                ["Days of Supply", days1(inv_store_metrics.get("days_of_supply", np.nan))],
            ],
            [3.9 * inch, 3.5 * inch],
        ))
        story.append(Spacer(1, 0.05 * inch))

        inv_products_store = pkt.get("inventory_products", pd.DataFrame())
        if inv_products_store is not None and not inv_products_store.empty:
            story.append(Paragraph("Inventory Value by Product Group", styles["Section"]))
            if options.include_charts:
                ch_inv_pg = chart_inventory_value_by_product_group(
                    inv_products_store,
                    f"{abbr} Inventory $ On Hand by Product Group",
                )
                if ch_inv_pg.getbuffer().nbytes > 0:
                    story.append(Image(ch_inv_pg, width=7.3 * inch, height=3.0 * inch))
                    story.append(Spacer(1, 0.04 * inch))
            story.append(Spacer(1, 0.03 * inch))

            story.append(Paragraph("Inventory Units On Hand by Product Group", styles["Section"]))
            inv_products_units_store = rollup_inventory_units_on_hand(inv_products_store)
            if options.include_charts:
                ch_inv_units = chart_inventory_units_by_product_group(
                    inv_products_units_store,
                    f"{abbr} Inventory Units On Hand by Product Group",
                )
                if ch_inv_units.getbuffer().nbytes > 0:
                    story.append(Image(ch_inv_units, width=7.3 * inch, height=3.0 * inch))
                    story.append(Spacer(1, 0.04 * inch))

            inv_unit_rows = []
            for r in inv_products_units_store.sort_values(["units_available", "display_product"], ascending=[False, True]).itertuples(index=False):
                inv_unit_rows.append([
                    str(getattr(r, "display_product", "")),
                    int0(float(getattr(r, "units_available", 0.0))),
                    money0(float(getattr(r, "inventory_value", 0.0))),
                    f"{float(getattr(r, 'trend_units_per_day_30d', getattr(r, 'trend_units_per_day_14d', 0.0))):,.1f}",
                    f"{float(getattr(r, 'trend_units_per_day_14d', 0.0)):,.1f}",
                    f"{float(getattr(r, 'trend_units_per_day_7d', 0.0)):,.1f}",
                    days1(getattr(r, "days_of_supply", np.nan)),
                ])
            story.append(_build_table_fit(
                ["Product Group", "Units", "Inv Value", "Units/Day 30d", "14d", "7d", "Days Supply"],
                inv_unit_rows,
                [2.85 * inch, 0.65 * inch, 0.95 * inch, 0.95 * inch, 0.55 * inch, 0.55 * inch, 0.85 * inch],
            ))
            story.append(Spacer(1, 0.05 * inch))

        daily_store = pkt.get("daily", pd.DataFrame())
        if options.include_charts and daily_store is not None and not daily_store.empty:
            ch_net = chart_daily_net(daily_store, f"{abbr} Daily Net Sales")
            ch_margin = chart_daily_margin(daily_store, f"{abbr} Daily Margin")
            if ch_net.getbuffer().nbytes > 0:
                story.append(Image(ch_net, width=7.3 * inch, height=2.7 * inch))
                story.append(Spacer(1, 0.04 * inch))
            if ch_margin.getbuffer().nbytes > 0:
                story.append(Image(ch_margin, width=7.3 * inch, height=2.5 * inch))
                story.append(Spacer(1, 0.04 * inch))

        category_60_store = pkt.get("category_60", pd.DataFrame())
        if category_60_store is not None and not category_60_store.empty:
            if options.include_charts:
                ch_cat_store = chart_rank_barh(
                    category_60_store,
                    "category_normalized",
                    "net_revenue",
                    f"{abbr} Top Categories by Net Sales",
                    value_kind="money",
                )
                if ch_cat_store.getbuffer().nbytes > 0:
                    story.append(Image(ch_cat_store, width=7.3 * inch, height=2.55 * inch))
                    story.append(Spacer(1, 0.04 * inch))
            c_rows = []
            for r in category_60_store.head(8).itertuples(index=False):
                c_rows.append([
                    str(getattr(r, "category_normalized", "")),
                    money0(float(getattr(r, "net_revenue", 0.0))),
                    pct1(float(getattr(r, "margin_real", 0.0))),
                ])
            story.append(_build_table_fit(
                ["Top Categories", "Net", "Margin"],
                c_rows,
                [4.2 * inch, 1.5 * inch, 1.3 * inch],
            ))
            story.append(Spacer(1, 0.04 * inch))

        product_60_store = pkt.get("product_60", pd.DataFrame())
        if product_60_store is not None and not product_60_store.empty:
            p_rows = []
            for r in product_60_store.sort_values("net_revenue", ascending=False).head(10).itertuples(index=False):
                cat_price_v = float(getattr(r, "catalog_price_per_item", getattr(r, "avg_price_per_item", 0.0)) or 0.0)
                cat_cost_v = float(getattr(r, "catalog_cost_per_item", getattr(r, "avg_cost_per_item", 0.0)) or 0.0)
                p_rows.append([
                    str(getattr(r, "product_group_display", getattr(r, "display_product", ""))),
                    money0(float(getattr(r, "net_revenue", 0.0))),
                    money2(cat_price_v),
                    money2(cat_cost_v),
                    pct1(float(getattr(r, "margin_real", 0.0))),
                    days1(getattr(r, "days_of_supply", np.nan)),
                ])
            story.append(_build_table_fit(
                ["Top Product Groups", "Net", "Catalog Price", "Catalog Cost", "Margin", "Days Supply"],
                p_rows,
                [3.25 * inch, 0.95 * inch, 0.90 * inch, 0.90 * inch, 0.75 * inch, 0.85 * inch],
            ))

    story.append(PageBreak())
    story.append(Paragraph("Deal Negotiation Scenarios", styles["TitleBig"]))
    story.append(Paragraph(
        "Inventory-weighted what-if for this brand. "
        "Assumptions: Effective Price = discounted shelf x 0.63, "
        "Out-The-Door = Effective Price x 1.33, and kickback applies to discounted shelf sales.",
        styles["Tiny"],
    ))
    target_margin_pct = DEAL_TARGET_MARGIN * 100.0
    story.append(Paragraph(
        f"<b>Decision Rule:</b> YES when projected Profit/Unit > $0 and projected Margin >= {target_margin_pct:.0f}%.",
        styles["Tiny"],
    ))
    story.append(Spacer(1, 0.05 * inch))

    scenario_df, break_even_df = build_deal_negotiation_data(inv_products, target_margin=DEAL_TARGET_MARGIN)
    if scenario_df.empty:
        story.append(Paragraph("No inventory pricing/cost data available for scenario modeling.", styles["Muted"]))
    else:
        if options.include_charts:
            ch_deal = chart_deal_scenario_margin(
                scenario_df,
                title="Projected Margin by Discount + Kickback Scenario",
                target_margin=DEAL_TARGET_MARGIN,
            )
            if ch_deal.getbuffer().nbytes > 0:
                story.append(Image(ch_deal, width=7.3 * inch, height=2.95 * inch))
                story.append(Spacer(1, 0.04 * inch))

        scen_rows_a: List[List[Any]] = []
        scen_rows_b: List[List[Any]] = []
        for _, r in scenario_df.iterrows():
            scen_rows_a.append([
                str(r.get("scenario", "")),
                pct1(float(r.get("discount_pct", 0.0))),
                pct1(float(r.get("kickback_pct", 0.0))),
                money2(float(r.get("avg_discounted_price", 0.0))),
                money2(float(r.get("avg_effective_price", 0.0))),
                money2(float(r.get("avg_out_the_door", 0.0))),
            ])
            scen_rows_b.append([
                str(r.get("scenario", "")),
                money2(float(r.get("avg_cost", 0.0))),
                money2(float(r.get("avg_kickback_per_unit", 0.0))),
                money2(float(r.get("avg_profit_per_unit", 0.0))),
                pct1(float(r.get("margin", 0.0))),
                money2(float(r.get("delta_profit_per_unit", 0.0))),
                osnap.pp1(float(r.get("delta_margin_pp", 0.0))),
                str(r.get("worth_it", "NO")),
            ])

        story.append(_build_table_fit(
            ["Scenario", "Discount", "Kickback", "Avg Shelf After Disc", "Effective Price", "Out-The-Door"],
            scen_rows_a,
            [2.30 * inch, 0.75 * inch, 0.78 * inch, 1.25 * inch, 1.10 * inch, 1.12 * inch],
        ))
        story.append(Spacer(1, 0.04 * inch))
        story.append(_build_table_fit(
            ["Scenario", "Avg Cost", "Kickback/u", "Profit/u", "Margin", "Δ Profit/u", "Δ Margin", "Worth It?"],
            scen_rows_b,
            [2.10 * inch, 0.82 * inch, 0.84 * inch, 0.84 * inch, 0.72 * inch, 0.82 * inch, 0.72 * inch, 0.74 * inch],
        ))

        if break_even_df is not None and not break_even_df.empty:
            story.append(Spacer(1, 0.05 * inch))
            be_rows: List[List[Any]] = []
            for _, r in break_even_df.iterrows():
                be_rows.append([
                    f"{pct1(float(r.get('discount_pct', 0.0)))} Off",
                    pct1(float(r.get("required_kickback_pct_break_even", 0.0))),
                    pct1(float(r.get("required_kickback_pct_target_margin", 0.0))),
                    str(r.get("within_30pct_target", "NO")),
                ])
            story.append(_build_table_fit(
                [
                    "Discount Plan",
                    "Kickback Needed (Break-Even)",
                    f"Kickback Needed ({int(round(DEAL_TARGET_MARGIN * 100.0))}% Margin)",
                    "Within 30% Back?",
                ],
                be_rows,
                [1.45 * inch, 1.95 * inch, 2.05 * inch, 1.85 * inch],
            ))

    footer = _footer(f"Brand Packet - {brand} - Quick Combined", end_day)
    doc.build(story, onFirstPage=footer, onLaterPages=footer)


def build_brand_packet_pdf(
    out_pdf: Path,
    brand: str,
    start_day: date,
    end_day: date,
    options: PacketOptions,
    windows: Dict[str, Tuple[date, date]],
    window_metrics: Dict[str, Dict[str, float]],
    prior_window_covered: bool,
    store_sales_packets: Dict[str, Dict[str, Any]],
    daily_60: pd.DataFrame,
    store_60: pd.DataFrame,
    store_14: pd.DataFrame,
    category_60: pd.DataFrame,
    category_14: pd.DataFrame,
    product_60: pd.DataFrame,
    product_14: pd.DataFrame,
    kickback_rules: pd.DataFrame,
    movers_store: pd.DataFrame,
    movers_category: pd.DataFrame,
    movers_product: pd.DataFrame,
    inv_overview: Dict[str, float],
    inv_products: pd.DataFrame,
    inv_category: pd.DataFrame,
    inv_store: pd.DataFrame,
    margin_risk: pd.DataFrame,
    best_candidates: pd.DataFrame,
    missing_sales_stores: Sequence[str],
    missing_catalog_stores: Sequence[str],
) -> None:
    osnap.setup_fonts()
    styles = osnap.build_styles()

    generated_at = datetime.now(ZoneInfo(REPORT_TZ)).strftime("%B %d, %Y at %I:%M %p %Z")

    out_pdf.parent.mkdir(parents=True, exist_ok=True)
    doc = SimpleDocTemplate(
        str(out_pdf),
        pagesize=letter,
        leftMargin=osnap.PAGE_MARGINS["left"],
        rightMargin=osnap.PAGE_MARGINS["right"],
        topMargin=osnap.PAGE_MARGINS["top"],
        bottomMargin=osnap.PAGE_MARGINS["bottom"],
        pageCompression=1,
        title=f"Brand Meeting Packet - {brand}",
        author="Buzz Automation",
    )

    story: List[Any] = []

    story.append(Paragraph(f"{brand} • Brand Meeting Packet", styles["TitleBig"]))
    story.append(osnap.build_report_day_band(end_day, width=7.6 * inch))
    story.append(Spacer(1, osnap.SPACER["xs"]))

    story.append(Paragraph(
        f"<b>Data Window:</b> {start_day.isoformat()} → {end_day.isoformat()}"
        f" &nbsp;&nbsp; <b>Generated:</b> {generated_at}",
        styles["Tiny"],
    ))

    if missing_sales_stores:
        story.append(Paragraph(
            f"<font color='#B91C1C'><b>Missing Sales Stores:</b> {', '.join(missing_sales_stores)}</font>",
            styles["Tiny"],
        ))
    if missing_catalog_stores:
        story.append(Paragraph(
            f"<font color='#B91C1C'><b>Missing Catalog Stores:</b> {', '.join(missing_catalog_stores)}</font>",
            styles["Tiny"],
        ))

    story.append(Spacer(1, osnap.SPACER["sm"]))

    # Executive summary
    story.append(Paragraph("Executive Summary", styles["TitleBig"]))
    report_metrics = window_metrics.get("report", {})
    baseline_metrics, baseline_label, baseline_key, compare_enabled = resolve_baseline_window(
        window_metrics,
        prior_window_covered,
    )
    baseline_label_with_dates = window_label_with_dates(baseline_label, baseline_key, windows)

    if compare_enabled:
        delta_net = float(report_metrics.get("net_revenue", 0.0) - baseline_metrics.get("net_revenue", 0.0))
        delta_margin = float(report_metrics.get("margin_real", 0.0) - baseline_metrics.get("margin_real", 0.0))
        story.append(Paragraph(
            f"<b>Window:</b> Last {window_days(*windows['report'])} days"
            f" &nbsp;&nbsp; <b>Net Δ vs {baseline_label_with_dates}:</b> {osnap.fmt_signed_money(delta_net)}"
            f" &nbsp;&nbsp; <b>Margin Δ:</b> {osnap.pp1(delta_margin)}",
            styles["Muted"],
        ))
    else:
        prior_start, prior_end = windows["prior_report"]
        story.append(Paragraph(
            f"<b>Window:</b> Last {window_days(*windows['report'])} days"
            f" &nbsp;&nbsp; <b>Net Δ vs Prior Window:</b> n/a"
            f" &nbsp;&nbsp; <b>Reason:</b> prior window not loaded "
            f"({prior_start.isoformat()} → {prior_end.isoformat()})",
            styles["Muted"],
        ))

    story.append(Paragraph("At-a-Glance Dashboard", styles["Section"]))
    story.append(build_exec_kpi_grid(
        styles=styles,
        report_metrics=report_metrics,
        baseline_metrics=baseline_metrics,
        baseline_label=baseline_label_with_dates,
        compare_enabled=compare_enabled,
        inv_overview=inv_overview,
    ))
    story.append(Spacer(1, osnap.SPACER["sm"]))

    inv_store_rows = _inventory_store_rows(inv_store, options.top_n)
    if inv_store_rows:
        story.append(Paragraph("Inventory by Store (Current)", styles["Section"]))
        story.append(_build_table_fit(
            ["Store", "Units", "Units/Day", "Days Supply", "Est OOS", "Inventory Value", "Potential Revenue", "Potential Profit", "Avg Margin"],
            inv_store_rows,
            [0.55 * inch, 0.55 * inch, 0.65 * inch, 0.75 * inch, 0.8 * inch, 1.05 * inch, 1.0 * inch, 1.0 * inch, 0.75 * inch],
        ))
        story.append(Spacer(1, osnap.SPACER["xs"]))

    story.append(Paragraph("Window Snapshot", styles["Section"]))
    story.append(_build_table_fit(
        ["Window", "Net", "Tickets", "Basket", "Margin", "Disc Rate"],
        _window_comp_rows(window_metrics, windows),
        [2.05 * inch, 1.25 * inch, 0.95 * inch, 1.05 * inch, 1.05 * inch, 1.15 * inch],
    ))
    story.append(Spacer(1, osnap.SPACER["sm"]))

    story.append(Paragraph("What Changed", styles["Section"]))

    if not movers_store.empty:
        rows = [[str(r["_store_abbr"]), money0(r["current_net"]), money0(r["prior_net"]), osnap.fmt_signed_money(float(r["delta"]))] for _, r in movers_store.iterrows()]
        story.append(_build_table_fit(["Store", "Current Net", "Prior Net", "Δ"], rows, [2.0 * inch, 1.8 * inch, 1.8 * inch, 1.7 * inch]))
        story.append(Spacer(1, 0.05 * inch))

    if not movers_category.empty:
        rows = [[str(r["category_normalized"]), money0(r["current_net"]), money0(r["prior_net"]), osnap.fmt_signed_money(float(r["delta"]))] for _, r in movers_category.iterrows()]
        story.append(_build_table_fit(["Category", "Current Net", "Prior Net", "Δ"], rows, [2.5 * inch, 1.6 * inch, 1.6 * inch, 1.6 * inch]))
        story.append(Spacer(1, 0.05 * inch))

    if not movers_product.empty:
        rows = [[str(r["product_group_display"]), money0(r["current_net"]), money0(r["prior_net"]), osnap.fmt_signed_money(float(r["delta"]))] for _, r in movers_product.iterrows()]
        story.append(_build_table_fit(["Product Group", "Current Net", "Prior Net", "Δ"], rows, [3.2 * inch, 1.3 * inch, 1.3 * inch, 1.5 * inch]))

    # Sales by location (all sales sections are per-store, not combined)
    if options.include_store_sections and store_sales_packets:
        story.append(PageBreak())
        story.append(Paragraph("Sales by Location", styles["TitleBig"]))
        story.append(Paragraph("All sales sections below are calculated per location (not combined).", styles["Tiny"]))

        store_order = [abbr for abbr in store_60.get("_store_abbr", pd.Series(dtype=str)).astype(str).tolist() if abbr in store_sales_packets]
        for abbr in store_sales_packets.keys():
            if abbr not in store_order:
                store_order.append(abbr)

        first_store = True
        for abbr in store_order:
            pkt = store_sales_packets.get(abbr, {})
            wm = pkt.get("window_metrics", {})
            m_report = wm.get("report", {})
            daily_store = pkt.get("daily", pd.DataFrame())
            category_60_store = pkt.get("category_60", pd.DataFrame())
            category_14_store = pkt.get("category_14", pd.DataFrame())
            product_60_store = pkt.get("product_60", pd.DataFrame())
            product_14_store = pkt.get("product_14", pd.DataFrame())
            kickback_rules_store = pkt.get("kickback_rules", pd.DataFrame())
            movers_category_store = pkt.get("movers_category", pd.DataFrame())
            movers_product_store = pkt.get("movers_product", pd.DataFrame())
            prior_cov_store = bool(pkt.get("prior_window_covered", False))
            inv_store_metrics = pkt.get("inventory", {})
            s_base_metrics, s_base_label, s_base_key, s_compare_enabled = resolve_baseline_window(wm, prior_cov_store)
            s_base_label_with_dates = window_label_with_dates(s_base_label, s_base_key, windows)

            if not first_store:
                story.append(PageBreak())
            first_store = False

            story.append(Paragraph(f"{abbr} • {osnap.store_label(pkt.get('store_name', _store_name_from_abbr(abbr)))}", styles["TitleBig"]))

            if s_compare_enabled:
                s_delta_net = float(m_report.get("net_revenue", 0.0) - s_base_metrics.get("net_revenue", 0.0))
                s_delta_margin = float(m_report.get("margin_real", 0.0) - s_base_metrics.get("margin_real", 0.0))
                story.append(Paragraph(
                    f"<b>Net Δ vs {s_base_label_with_dates}:</b> {osnap.fmt_signed_money(s_delta_net)}"
                    f" &nbsp;&nbsp; <b>Margin Δ:</b> {osnap.pp1(s_delta_margin)}",
                    styles["Muted"],
                ))
            else:
                prior_start, prior_end = windows["prior_report"]
                story.append(Paragraph(
                    f"<b>Net Δ vs Prior Window:</b> n/a"
                    f" &nbsp;&nbsp; <b>Reason:</b> no comparable baseline loaded "
                    f"({prior_start.isoformat()} → {prior_end.isoformat()})",
                    styles["Muted"],
                ))

            story.append(Paragraph("Store KPI Dashboard", styles["Section"]))
            story.append(build_store_kpi_grid(
                styles=styles,
                report_metrics=m_report,
                baseline_metrics=s_base_metrics,
                baseline_label=s_base_label_with_dates,
                compare_enabled=s_compare_enabled,
            ))
            story.append(Spacer(1, 0.05 * inch))

            story.append(_build_table_fit(
                ["Inventory Snapshot", "Value"],
                [
                    ["Units Available", int0(inv_store_metrics.get("units_available", 0.0))],
                    ["Trend Units / Day (30d)", f"{float(inv_store_metrics.get('trend_units_per_day_30d', inv_store_metrics.get('trend_units_per_day_14d', 0.0))):,.1f}"],
                    ["Days of Supply", days1(inv_store_metrics.get("days_of_supply", np.nan))],
                    ["Est. OOS Date", str(inv_store_metrics.get("est_oos_date", "n/a"))],
                    ["Inventory Value", inventory_value_with_units(inv_store_metrics.get("inventory_value", 0.0), inv_store_metrics.get("units_available", 0.0))],
                    ["Potential Revenue", money0(inv_store_metrics.get("potential_revenue", 0.0))],
                    ["Potential Profit", money0(inv_store_metrics.get("potential_profit", 0.0))],
                    ["Average Margin", pct1(inv_store_metrics.get("avg_margin", 0.0))],
                ],
                [3.9 * inch, 3.35 * inch],
            ))
            story.append(Spacer(1, 0.05 * inch))

            inv_products_store = pkt.get("inventory_products", pd.DataFrame())
            if inv_products_store is not None and not inv_products_store.empty:
                story.append(Paragraph("Inventory Units On Hand by Product Group", styles["Section"]))
                inv_products_units_store = rollup_inventory_units_on_hand(inv_products_store)
                if options.include_charts:
                    ch_inv_units_store = chart_inventory_units_by_product_group(
                        inv_products_units_store,
                        f"{abbr} Inventory Units On Hand by Product Group",
                    )
                    if ch_inv_units_store.getbuffer().nbytes > 0:
                        story.append(Image(ch_inv_units_store, width=7.3 * inch, height=3.0 * inch))
                        story.append(Spacer(1, 0.04 * inch))

                inv_unit_rows_store = []
                for r in inv_products_units_store.sort_values(["units_available", "display_product"], ascending=[False, True]).itertuples(index=False):
                    inv_unit_rows_store.append([
                        str(getattr(r, "display_product", "")),
                        int0(float(getattr(r, "units_available", 0.0))),
                        money0(float(getattr(r, "inventory_value", 0.0))),
                        f"{float(getattr(r, 'trend_units_per_day_30d', getattr(r, 'trend_units_per_day_14d', 0.0))):,.1f}",
                        days1(getattr(r, "days_of_supply", np.nan)),
                    ])
                story.append(_build_table_fit(
                    ["Product Group", "Units", "Inv Value", "Units/Day 30d", "Days Supply"],
                    inv_unit_rows_store,
                    [3.35 * inch, 0.75 * inch, 1.0 * inch, 1.0 * inch, 1.0 * inch],
                ))
                story.append(Spacer(1, 0.05 * inch))

            story.append(_build_table_fit(
                ["Window", "Net", "Tickets", "Basket", "Margin", "Disc Rate"],
                _window_comp_rows(wm, windows),
                [2.05 * inch, 1.25 * inch, 0.95 * inch, 1.05 * inch, 1.05 * inch, 1.15 * inch],
            ))
            story.append(Spacer(1, 0.06 * inch))

            if not movers_category_store.empty:
                mc_rows = [[str(r["category_normalized"]), money0(r["current_net"]), money0(r["prior_net"]), osnap.fmt_signed_money(float(r["delta"]))] for _, r in movers_category_store.iterrows()]
                story.append(Paragraph("Category Movers", styles["Section"]))
                story.append(_build_table_fit(["Category", "Current Net", "Prior Net", "Δ"], mc_rows, [2.5 * inch, 1.6 * inch, 1.6 * inch, 1.6 * inch]))
                story.append(Spacer(1, 0.05 * inch))

            if not movers_product_store.empty:
                mp_rows = [[str(r["product_group_display"]), money0(r["current_net"]), money0(r["prior_net"]), osnap.fmt_signed_money(float(r["delta"]))] for _, r in movers_product_store.iterrows()]
                story.append(Paragraph("Product Movers", styles["Section"]))
                story.append(_build_table_fit(["Product Group", "Current Net", "Prior Net", "Δ"], mp_rows, [3.2 * inch, 1.3 * inch, 1.3 * inch, 1.5 * inch]))
                story.append(Spacer(1, 0.05 * inch))

            if options.include_charts and daily_store is not None and not daily_store.empty:
                ch_net_store = chart_daily_net(daily_store, f"{abbr} Daily Net Sales")
                ch_margin_store = chart_daily_margin(daily_store, f"{abbr} Daily Margin")
                if ch_net_store.getbuffer().nbytes > 0:
                    story.append(Image(ch_net_store, width=7.3 * inch, height=3.1 * inch))
                    story.append(Spacer(1, 0.06 * inch))
                if ch_margin_store.getbuffer().nbytes > 0:
                    story.append(Image(ch_margin_store, width=7.3 * inch, height=2.9 * inch))
                    story.append(Spacer(1, 0.06 * inch))

            story.append(Paragraph("Category Performance", styles["Section"]))
            if category_60_store is not None and not category_60_store.empty:
                rows = []
                for r in category_60_store.head(options.top_n).itertuples(index=False):
                    rows.append([
                        str(getattr(r, "category_normalized")),
                        money0(float(getattr(r, "net_revenue"))),
                        money0(float(getattr(r, "profit"))),
                        pct1(float(getattr(r, "margin_real"))),
                        int0(float(getattr(r, "items"))),
                        pct1(float(getattr(r, "discount_rate"))),
                    ])
                story.append(_build_table_fit(
                    ["Category", "Net", "Profit", "Margin", "Units", "Disc Rate"],
                    rows,
                    [2.35 * inch, 1.15 * inch, 1.15 * inch, 1.05 * inch, 0.85 * inch, 1.0 * inch],
                ))
                story.append(Spacer(1, 0.05 * inch))

            if category_14_store is not None and not category_14_store.empty:
                rows_14 = []
                for r in category_14_store.head(min(options.top_n, 12)).itertuples(index=False):
                    rows_14.append([
                        str(getattr(r, "category_normalized")),
                        money0(float(getattr(r, "net_revenue"))),
                        money0(float(getattr(r, "profit"))),
                        pct1(float(getattr(r, "margin_real"))),
                        int0(float(getattr(r, "items"))),
                        pct1(float(getattr(r, "discount_rate"))),
                    ])
                story.append(_build_table_fit(
                    ["Category (14d)", "Net", "Profit", "Margin", "Units", "Disc Rate"],
                    rows_14,
                    [2.35 * inch, 1.15 * inch, 1.15 * inch, 1.05 * inch, 0.85 * inch, 1.0 * inch],
                ))
                story.append(Spacer(1, 0.05 * inch))

            story.append(Paragraph("Product Groups", styles["Section"]))
            if product_60_store is not None and not product_60_store.empty:
                top_rev = product_60_store.sort_values("net_revenue", ascending=False).head(options.top_n)
                rows = []
                for r in top_rev.itertuples(index=False):
                    cat_price_v = float(getattr(r, "catalog_price_per_item", getattr(r, "avg_price_per_item", 0.0)) or 0.0)
                    cat_cost_v = float(getattr(r, "catalog_cost_per_item", getattr(r, "avg_cost_per_item", 0.0)) or 0.0)
                    rows.append([
                        str(getattr(r, "product_group_display", getattr(r, "display_product", ""))),
                        money0(float(r.net_revenue)),
                        int0(float(r.units)),
                        pct1(float(r.margin_real)),
                        money2(cat_price_v),
                        money2(cat_cost_v),
                        days1(getattr(r, "days_of_supply", np.nan)),
                    ])
                story.append(_build_table_fit(
                    ["Product Group", "Net", "Units", "Margin", "Catalog Price", "Catalog Cost", "Days Supply"],
                    rows,
                    [2.55 * inch, 0.92 * inch, 0.70 * inch, 0.75 * inch, 0.84 * inch, 0.84 * inch, 0.92 * inch],
                ))
                story.append(Spacer(1, 0.05 * inch))

            if product_14_store is not None and not product_14_store.empty:
                p14 = product_14_store.sort_values("net_revenue", ascending=False).head(min(options.top_n, 15))
                rows_14 = []
                for r in p14.itertuples(index=False):
                    cat_price_v = float(getattr(r, "catalog_price_per_item", getattr(r, "avg_price_per_item", 0.0)) or 0.0)
                    cat_cost_v = float(getattr(r, "catalog_cost_per_item", getattr(r, "avg_cost_per_item", 0.0)) or 0.0)
                    rows_14.append([
                        str(getattr(r, "product_group_display", getattr(r, "display_product", ""))),
                        money0(float(r.net_revenue)),
                        int0(float(r.units)),
                        pct1(float(r.margin_real)),
                        money2(cat_price_v),
                        money2(cat_cost_v),
                        days1(getattr(r, "days_of_supply", np.nan)),
                    ])
                story.append(_build_table_fit(
                    ["Product Group (14d)", "Net", "Units", "Margin", "Catalog Price", "Catalog Cost", "Days Supply"],
                    rows_14,
                    [2.50 * inch, 0.90 * inch, 0.65 * inch, 0.72 * inch, 0.84 * inch, 0.84 * inch, 0.90 * inch],
                ))
                story.append(Spacer(1, 0.05 * inch))

            if options.include_kickback_adjustments:
                story.append(Paragraph("Deals / Kickbacks", styles["Section"]))
                kb_total_store = float(m_report.get("kickback_total", 0.0))
                kb_rows_store = int(m_report.get("kickback_rows", 0.0))
                story.append(Paragraph(
                    f"<b>Total Kickback Applied:</b> {money0(kb_total_store)} &nbsp;&nbsp; <b>Rows:</b> {kb_rows_store:,}",
                    styles["Muted"],
                ))
                if kickback_rules_store is not None and not kickback_rules_store.empty:
                    kb_rows = [[str(r.rule), money0(float(r.kickback_total)), int0(float(r.rows))] for r in kickback_rules_store.head(12).itertuples(index=False)]
                    story.append(_build_table_fit(["Rule", "Kickback Total", "Rows"], kb_rows, [4.9 * inch, 1.5 * inch, 0.9 * inch]))

    # Inventory overview
    story.append(PageBreak())
    story.append(Paragraph("Inventory Overview", styles["TitleBig"]))
    story.append(_build_table_fit(
        ["Metric", "Value"],
        _inventory_summary_rows(inv_overview),
        [3.9 * inch, 3.35 * inch],
    ))
    story.append(Spacer(1, 0.08 * inch))

    inv_store_rows_overview = _inventory_store_rows(inv_store, options.top_n)
    if inv_store_rows_overview:
        story.append(Paragraph("Inventory by Store", styles["Section"]))
        story.append(_build_table_fit(
            ["Store", "Units", "Units/Day", "Days Supply", "Est OOS", "Inventory Value", "Potential Revenue", "Potential Profit", "Avg Margin"],
            inv_store_rows_overview,
            [0.55 * inch, 0.55 * inch, 0.65 * inch, 0.75 * inch, 0.8 * inch, 1.05 * inch, 1.0 * inch, 1.0 * inch, 0.75 * inch],
        ))
        story.append(Spacer(1, 0.08 * inch))

    if options.include_charts:
        ch_inv_cat = chart_inventory_value_by_category(inv_category)
        ch_inv_margin_hist = chart_margin_distribution(inv_products)
        ch_inv_top_profit = chart_top_inventory_profit(inv_products)

        if ch_inv_cat.getbuffer().nbytes > 0:
            story.append(Image(ch_inv_cat, width=7.3 * inch, height=3.25 * inch))
            story.append(Spacer(1, 0.07 * inch))
        if ch_inv_margin_hist.getbuffer().nbytes > 0:
            story.append(Image(ch_inv_margin_hist, width=7.3 * inch, height=2.9 * inch))
            story.append(Spacer(1, 0.07 * inch))
        if ch_inv_top_profit.getbuffer().nbytes > 0:
            story.append(Image(ch_inv_top_profit, width=7.3 * inch, height=3.25 * inch))

    # Inventory by product
    story.append(CondPageBreak(5.2 * inch))
    story.append(Spacer(1, 0.08 * inch))
    story.append(Paragraph("Inventory by Product", styles["TitleBig"]))
    if not inv_products.empty:
        rows = []
        for r in inv_products.head(options.top_n).itertuples(index=False):
            rows.append([
                str(r.display_product),
                int0(float(r.units_available)),
                f"{float(getattr(r, 'trend_units_per_day_30d', getattr(r, 'trend_units_per_day_14d', 0.0))):,.1f}",
                days1(getattr(r, "days_of_supply", np.nan)),
                money2(float(r.shelf_price)),
                money2(float(r.cost)),
                money2(float(r.effective_price)),
                pct1(float(r.margin_current)),
                money0(float(r.inventory_value)),
                money0(float(r.potential_profit)),
            ])
        story.append(_build_table_fit(
            ["Product", "Units", "Units/Day", "Days Supply", "Shelf", "Cost", "Eff Price", "Margin", "Inv Value", "Potential Profit"],
            rows,
            [2.1 * inch, 0.45 * inch, 0.5 * inch, 0.55 * inch, 0.55 * inch, 0.55 * inch, 0.6 * inch, 0.5 * inch, 0.7 * inch, 0.7 * inch],
        ))

    # Margin risk
    story.append(CondPageBreak(4.8 * inch))
    story.append(Spacer(1, 0.08 * inch))
    story.append(Paragraph("Margin Risk", styles["TitleBig"]))
    story.append(Paragraph("SKUs where Margin Current < 35%", styles["Tiny"]))
    if not margin_risk.empty:
        rows = []
        for r in margin_risk.head(options.top_n).itertuples(index=False):
            rows.append([
                str(r.display_product),
                money2(float(r.shelf_price)),
                money2(float(r.cost)),
                pct1(float(r.margin_current)),
                int0(float(r.units_available)),
            ])
        story.append(_build_table_fit(["Product", "Price", "Cost", "Margin", "Units"], rows, [4.0 * inch, 1.0 * inch, 1.0 * inch, 0.9 * inch, 0.7 * inch]))
    else:
        story.append(Paragraph("No SKUs currently below the 35% threshold.", styles["Muted"]))

    # Best deal candidates
    story.append(CondPageBreak(4.8 * inch))
    story.append(Spacer(1, 0.08 * inch))
    story.append(Paragraph("Best Deal Candidates", styles["TitleBig"]))
    story.append(Paragraph("SKUs where Margin Current > 55% and Available > 10", styles["Tiny"]))
    if not best_candidates.empty:
        rows = []
        for r in best_candidates.head(options.top_n).itertuples(index=False):
            rows.append([
                str(r.display_product),
                int0(float(r.units_available)),
                pct1(float(r.margin_current)),
                money0(float(r.potential_profit)),
            ])
        story.append(_build_table_fit(["Product", "Units", "Margin", "Potential Profit"], rows, [4.35 * inch, 1.0 * inch, 1.0 * inch, 1.0 * inch]))
    else:
        story.append(Paragraph("No current SKUs meet the best-candidate threshold.", styles["Muted"]))

    # Appendix
    if options.include_product_appendix:
        story.append(PageBreak())
        story.append(Paragraph("Appendix — Product Group Mapping", styles["TitleBig"]))
        if not product_60.empty:
            rows = []
            for r in product_60.itertuples(index=False):
                rows.append([
                    str(getattr(r, "product_group_display", getattr(r, "display_product", ""))),
                    str(getattr(r, "product_group_key", getattr(r, "merge_key", ""))),
                    int0(float(r.merged_count)),
                    str(r.raw_names_top5),
                ])
            story.append(_build_table_fit(
                ["Group Display", "Group Key", "Merged Raw Names", "Top Raw Names (up to 5)"],
                rows,
                [1.6 * inch, 2.0 * inch, 0.9 * inch, 3.1 * inch],
            ))

        story.append(Spacer(1, 0.10 * inch))
        story.append(Paragraph("Inventory Appendix", styles["TitleBig"]))
        if not inv_products.empty:
            rows = []
            for r in inv_products.itertuples(index=False):
                rows.append([
                    str(r.display_product),
                    str(r.merge_key),
                    int0(float(r.units_available)),
                    money2(float(r.shelf_price)),
                    money2(float(r.cost)),
                    pct1(float(r.margin_current)),
                    money0(float(r.inventory_value)),
                    money0(float(r.potential_profit)),
                    str(r.raw_names_top5),
                ])
            story.append(_build_table_fit(
                ["Product", "Merge Key", "Units", "Price", "Cost", "Margin", "Inv Value", "Potential Profit", "Raw Names"],
                rows,
                [1.35 * inch, 1.5 * inch, 0.55 * inch, 0.6 * inch, 0.6 * inch, 0.6 * inch, 0.75 * inch, 0.85 * inch, 0.95 * inch],
            ))

    footer = _footer(f"Brand Packet - {brand}", end_day)
    doc.build(story, onFirstPage=footer, onLaterPages=footer)


# ---------------------------------------------------------------------------
# XLSX
# ---------------------------------------------------------------------------
def build_brand_packet_xlsx(
    out_xlsx: Path,
    window_metrics: Dict[str, Dict[str, float]],
    store_60: pd.DataFrame,
    store_14: pd.DataFrame,
    category_60: pd.DataFrame,
    category_14: pd.DataFrame,
    product_60: pd.DataFrame,
    product_14: pd.DataFrame,
    daily_60: pd.DataFrame,
    weekly: pd.DataFrame,
    kickback_rules: pd.DataFrame,
    inv_overview: Dict[str, float],
    inv_products: pd.DataFrame,
    inv_category: pd.DataFrame,
    margin_risk: pd.DataFrame,
    best_candidates: pd.DataFrame,
    credit_summary: Optional[Dict[str, Any]] = None,
    credit_reconciliation: Optional[pd.DataFrame] = None,
    credit_ledger_rows: Optional[pd.DataFrame] = None,
    store_credit_scorecard: Optional[pd.DataFrame] = None,
    action_items: Optional[Sequence[Dict[str, Any]]] = None,
    monthly_reference: Optional[Dict[str, Any]] = None,
    inventory_risk: Optional[pd.DataFrame] = None,
    slow_movers: Optional[pd.DataFrame] = None,
    fast_movers: Optional[pd.DataFrame] = None,
) -> None:
    out_xlsx.parent.mkdir(parents=True, exist_ok=True)

    summary_rows: List[Dict[str, Any]] = []
    for key, vals in window_metrics.items():
        row = {"window": key}
        row.update(vals)
        summary_rows.append(row)

    summary_df = pd.DataFrame(summary_rows)
    inv_summary_df = pd.DataFrame([inv_overview])
    inventory_risk = inventory_risk if inventory_risk is not None else compute_inventory_risk_v2(product_60, inv_products)
    slow_movers = slow_movers if slow_movers is not None else compute_slow_movers_v2(product_60, inv_products)
    fast_movers = fast_movers if fast_movers is not None else compute_fast_movers_v2(product_60, inv_products)

    with pd.ExcelWriter(out_xlsx, engine="openpyxl") as writer:
        summary_df.to_excel(writer, sheet_name="Meeting Summary", index=False)
        pd.DataFrame([credit_summary or {}]).to_excel(writer, sheet_name="Credit Summary", index=False)
        _credit_source_summary(credit_reconciliation).to_excel(writer, sheet_name="Credit Source Summary", index=False)
        (credit_reconciliation if credit_reconciliation is not None else pd.DataFrame()).to_excel(writer, sheet_name="Credit Reconciliation", index=False)
        (credit_ledger_rows if credit_ledger_rows is not None else pd.DataFrame()).to_excel(writer, sheet_name="Credit Ledger", index=False)
        (store_credit_scorecard if store_credit_scorecard is not None else pd.DataFrame()).to_excel(writer, sheet_name="Store Scorecards", index=False)
        pd.DataFrame(list(action_items or [])).to_excel(writer, sheet_name="Action Items", index=False)
        product_60.to_excel(writer, sheet_name="Product Performance", index=False)
        category_60.to_excel(writer, sheet_name="Category Performance", index=False)
        (inventory_risk if inventory_risk is not None else pd.DataFrame()).to_excel(writer, sheet_name="Inventory Risk", index=False)
        (slow_movers if slow_movers is not None else pd.DataFrame()).to_excel(writer, sheet_name="Slow Movers", index=False)
        (fast_movers if fast_movers is not None else pd.DataFrame()).to_excel(writer, sheet_name="Fast Movers", index=False)
        if monthly_reference and isinstance(monthly_reference.get("brand_rows"), pd.DataFrame):
            monthly_reference["brand_rows"].to_excel(writer, sheet_name="Monthly Reference", index=False)
        else:
            pd.DataFrame().to_excel(writer, sheet_name="Monthly Reference", index=False)
        inv_products.to_excel(writer, sheet_name="Raw Product Detail", index=False)

        summary_df.to_excel(writer, sheet_name="Summary", index=False)
        inv_summary_df.to_excel(writer, sheet_name="Inventory_Summary", index=False)

        store_60.to_excel(writer, sheet_name="Store_60d", index=False)
        store_14.to_excel(writer, sheet_name="Store_14d", index=False)
        category_60.to_excel(writer, sheet_name="Category_60d", index=False)
        category_14.to_excel(writer, sheet_name="Category_14d", index=False)
        product_60.to_excel(writer, sheet_name="Product_60d", index=False)
        product_14.to_excel(writer, sheet_name="Product_14d", index=False)
        daily_60.to_excel(writer, sheet_name="Daily_60d", index=False)
        weekly.to_excel(writer, sheet_name="Weekly", index=False)
        kickback_rules.to_excel(writer, sheet_name="Kickbacks", index=False)

        inv_products.to_excel(writer, sheet_name="Inv_Products", index=False)
        inv_category.to_excel(writer, sheet_name="Inv_Category", index=False)
        margin_risk.to_excel(writer, sheet_name="Margin_Risk", index=False)
        best_candidates.to_excel(writer, sheet_name="Best_Candidates", index=False)

        pd.DataFrame([credit_summary or {}]).to_excel(writer, sheet_name="Credit_Summary", index=False)
        _credit_source_summary(credit_reconciliation).to_excel(writer, sheet_name="Credit_Source_Summary", index=False)
        (credit_reconciliation if credit_reconciliation is not None else pd.DataFrame()).to_excel(
            writer,
            sheet_name="Credit_Reconciliation",
            index=False,
        )
        (credit_ledger_rows if credit_ledger_rows is not None else pd.DataFrame()).to_excel(
            writer,
            sheet_name="Credit_Ledger",
            index=False,
        )
        (store_credit_scorecard if store_credit_scorecard is not None else pd.DataFrame()).to_excel(
            writer,
            sheet_name="Store_Scorecards",
            index=False,
        )
        pd.DataFrame(list(action_items or [])).to_excel(writer, sheet_name="Action_Items", index=False)
        if monthly_reference and isinstance(monthly_reference.get("brand_rows"), pd.DataFrame):
            monthly_reference["brand_rows"].to_excel(writer, sheet_name="Monthly_Reference", index=False)
        else:
            pd.DataFrame().to_excel(writer, sheet_name="Monthly_Reference", index=False)
        if monthly_reference and isinstance(monthly_reference.get("inventory_rows"), pd.DataFrame):
            monthly_reference["inventory_rows"].to_excel(writer, sheet_name="Monthly_Inventory_Risk", index=False)
        else:
            pd.DataFrame().to_excel(writer, sheet_name="Monthly_Inventory_Risk", index=False)


# ---------------------------------------------------------------------------
# Email
# ---------------------------------------------------------------------------
def _build_gmail_service():
    if not GMAIL_TOKEN.exists():
        raise RuntimeError("token_gmail.json not found. Run Gmail auth first.")

    creds = Credentials.from_authorized_user_file(str(GMAIL_TOKEN), GMAIL_SCOPES)
    if creds.expired and creds.refresh_token:
        creds.refresh(Request())
        GMAIL_TOKEN.write_text(creds.to_json())
    return build("gmail", "v1", credentials=creds)


def _attach_file_to_email(msg: EmailMessage, file_path: Path) -> None:
    path = Path(file_path)
    if not path.exists():
        return

    guessed, _encoding = mimetypes.guess_type(str(path))
    if guessed and "/" in guessed:
        maintype, subtype = guessed.split("/", 1)
    elif path.suffix.lower() == ".xlsx":
        maintype, subtype = "application", "vnd.openxmlformats-officedocument.spreadsheetml.sheet"
    else:
        maintype, subtype = "application", "octet-stream"

    with open(path, "rb") as f:
        data = f.read()
    msg.add_attachment(data, maintype=maintype, subtype=subtype, filename=path.name)


def send_brand_packet_email(
    pdf_paths: Sequence[Path],
    brand: str,
    start_day: date,
    end_day: date,
    report_metrics: Dict[str, float],
    inv_overview: Dict[str, float],
    top_products: pd.DataFrame,
    top_store_name: str,
    to_email: str,
    logger: Optional[Callable[[str], None]],
    credit_summary: Optional[Dict[str, Any]] = None,
    meeting_ask: str = "",
) -> None:
    service = _build_gmail_service()

    subject = f"Brand Packet — {brand} — {start_day.isoformat()} to {end_day.isoformat()}"

    top_lines: List[str] = []
    for r in top_products.head(3).itertuples(index=False):
        name = str(getattr(r, "product_group_display", getattr(r, "display_product", "")))
        top_lines.append(f"- {name}: {money0(float(r.net_revenue))}")
    credit_summary = credit_summary or {}
    credit_plain = ""
    credit_html = ""
    if credit_summary:
        credit_plain = (
            f"\nCredit + Margin Truth:\n"
            f"Real Margin: {pct1(credit_summary.get('real_margin', report_metrics.get('margin_real', 0.0)))}\n"
            f"Expected Credit Margin: {pct1(credit_summary.get('expected_credit_margin', 0.0))}\n"
            f"Received Credit Margin: {pct1(credit_summary.get('received_credit_margin', 0.0))}\n"
            f"Credit Gap: {money0(credit_summary.get('credit_gap', 0.0))}\n"
            f"Meeting Ask: {meeting_ask or 'Confirm margin support and next steps.'}\n"
        )
        credit_html = f"""
      <br/>
      <div><b>Credit + Margin Truth:</b></div>
      <div>Real Margin: {pct1(credit_summary.get('real_margin', report_metrics.get('margin_real', 0.0)))}</div>
      <div>Expected Credit Margin: {pct1(credit_summary.get('expected_credit_margin', 0.0))}</div>
      <div>Received Credit Margin: {pct1(credit_summary.get('received_credit_margin', 0.0))}</div>
      <div>Credit Gap: {money0(credit_summary.get('credit_gap', 0.0))}</div>
      <div>Meeting Ask: {xml_escape(meeting_ask or 'Confirm margin support and next steps.')}</div>
        """

    plain = (
        f"Brand Meeting Packet\n"
        f"Brand: {brand}\n"
        f"Window: {start_day.isoformat()} to {end_day.isoformat()}\n\n"
        f"Top KPIs:\n"
        f"Net Revenue: {money0(report_metrics.get('net_revenue', 0.0))}\n"
        f"Tickets: {int0(report_metrics.get('tickets', 0.0))}\n"
        f"Margin: {pct1(report_metrics.get('margin_real', 0.0))}\n"
        f"Top Store: {top_store_name}\n\n"
        f"Top Product Groups:\n" + ("\n".join(top_lines) if top_lines else "- None") + "\n\n"
        f"Brand Inventory Snapshot:\n"
        f"Units: {int0(inv_overview.get('units', 0.0))}\n"
        f"Inventory Cost: {money0(inv_overview.get('inventory_value', 0.0))}\n"
        f"Potential Revenue: {money0(inv_overview.get('potential_revenue', 0.0))}\n"
        f"Potential Profit: {money0(inv_overview.get('potential_profit', 0.0))}\n"
        f"Average Margin: {pct1(inv_overview.get('avg_margin', 0.0))}\n"
        f"Trend Units/Day (30d): {float(inv_overview.get('trend_units_per_day_30d', inv_overview.get('trend_units_per_day_14d', 0.0))):,.1f}\n"
        f"Trend Units/Day (14d): {float(inv_overview.get('trend_units_per_day_14d', 0.0)):,.1f}\n"
        f"Trend Units/Day (7d): {float(inv_overview.get('trend_units_per_day_7d', 0.0)):,.1f}\n"
        f"Days of Supply: {days1(inv_overview.get('days_of_supply', np.nan))}\n"
        f"Est. OOS Date: {inv_overview.get('est_oos_date', 'n/a')}\n"
        f"{credit_plain}"
    )

    html = f"""
    <div style=\"font-family:Arial,sans-serif;color:#111827;\">
      <h2 style=\"margin:0 0 8px 0;\">Brand Packet — {brand}</h2>
      <div style=\"margin-bottom:10px;color:#4B5563;\">{start_day.isoformat()} to {end_day.isoformat()}</div>
      <div><b>Net Revenue:</b> {money0(report_metrics.get('net_revenue', 0.0))}</div>
      <div><b>Tickets:</b> {int0(report_metrics.get('tickets', 0.0))}</div>
      <div><b>Margin:</b> {pct1(report_metrics.get('margin_real', 0.0))}</div>
      <div><b>Top Store:</b> {top_store_name}</div>
      <br/>
      <div><b>Top Product Groups:</b></div>
      <ul>{''.join(f'<li>{line[2:]}</li>' for line in top_lines) if top_lines else '<li>None</li>'}</ul>
      <br/>
      <div><b>Brand Inventory Snapshot:</b></div>
      <div>Units: {int0(inv_overview.get('units', 0.0))}</div>
      <div>Inventory Cost: {money0(inv_overview.get('inventory_value', 0.0))}</div>
      <div>Potential Revenue: {money0(inv_overview.get('potential_revenue', 0.0))}</div>
      <div>Potential Profit: {money0(inv_overview.get('potential_profit', 0.0))}</div>
      <div>Average Margin: {pct1(inv_overview.get('avg_margin', 0.0))}</div>
      <div>Trend Units/Day (30d): {float(inv_overview.get('trend_units_per_day_30d', inv_overview.get('trend_units_per_day_14d', 0.0))):,.1f}</div>
      <div>Trend Units/Day (14d): {float(inv_overview.get('trend_units_per_day_14d', 0.0)):,.1f}</div>
      <div>Trend Units/Day (7d): {float(inv_overview.get('trend_units_per_day_7d', 0.0)):,.1f}</div>
      <div>Days of Supply: {days1(inv_overview.get('days_of_supply', np.nan))}</div>
      <div>Est. OOS Date: {inv_overview.get('est_oos_date', 'n/a')}</div>
      {credit_html}
    </div>
    """

    msg = EmailMessage()
    msg["To"] = to_email
    msg["From"] = "me"
    msg["Subject"] = subject
    msg.set_content(plain)
    msg.add_alternative(html, subtype="html")

    for pdf_path in pdf_paths:
        if not pdf_path:
            continue
        _attach_file_to_email(msg, Path(pdf_path))

    raw = base64.urlsafe_b64encode(msg.as_bytes()).decode()
    service.users().messages().send(userId="me", body={"raw": raw}).execute()

    names = ", ".join(Path(p).name for p in pdf_paths if p and Path(p).exists())
    _log(f"[EMAIL] Sent packet to {to_email}: {names}", logger)


def send_all_store_slow_mover_email(
    attachments: Sequence[Path],
    start_day: date,
    end_day: date,
    store_summary: pd.DataFrame,
    store_brand_summary: pd.DataFrame,
    store_product_candidates: pd.DataFrame,
    to_email: str,
    logger: Optional[Callable[[str], None]],
) -> None:
    service = _build_gmail_service()

    subject = f"Store SKU Cuts — {start_day.isoformat()} to {end_day.isoformat()}"
    cut_sku_count = 0
    review_sku_count = 0
    no_sales_cut_skus = 0
    if store_product_candidates is not None and not store_product_candidates.empty:
        action_series = store_product_candidates["action"].fillna("").astype(str) if "action" in store_product_candidates.columns else pd.Series("", index=store_product_candidates.index)
        sold_series = pd.to_numeric(store_product_candidates["units_sold_window"], errors="coerce").fillna(0.0) if "units_sold_window" in store_product_candidates.columns else pd.Series(0.0, index=store_product_candidates.index)
        cut_mask = action_series.str.startswith("Cut")
        review_mask = action_series.str.startswith("Review")
        cut_sku_count = int(cut_mask.sum())
        review_sku_count = int(review_mask.sum())
        no_sales_cut_skus = int((cut_mask & (sold_series <= 0)).sum())

    top_lines: List[str] = []
    if store_brand_summary is not None and not store_brand_summary.empty:
        for r in store_brand_summary.head(8).itertuples(index=False):
            store_label = str(getattr(r, "_store_abbr", ""))
            top_lines.append(
                f"- {store_label} - {str(getattr(r, 'brand_name', ''))}: "
                f"{int0(float(getattr(r, 'cut_sku_count', 0.0)))} cut, "
                f"{int0(float(getattr(r, 'review_sku_count', 0.0)))} review, "
                f"{money0(float(getattr(r, 'cut_inventory_value', 0.0)))} cut inventory, "
                f"{str(getattr(r, 'report_status', ''))}"
            )

    plain = (
        f"Store SKU Cuts\n"
        f"Window: {start_day.isoformat()} to {end_day.isoformat()}\n\n"
        f"Stores Flagged: {int(len(store_summary)) if store_summary is not None else 0}\n"
        f"Store-Brand Rows: {int(len(store_brand_summary)) if store_brand_summary is not None else 0}\n"
        f"SKUs To Cut: {cut_sku_count}\n"
        f"SKUs To Review: {review_sku_count}\n"
        f"No-Sales Cut SKUs: {no_sales_cut_skus}\n\n"
        f"Top Store-Brand Rows:\n" + ("\n".join(top_lines) if top_lines else "- None") + "\n"
    )
    html = f"""
    <div style=\"font-family:Arial,sans-serif;color:#111827;\">
      <h2 style=\"margin:0 0 8px 0;\">Store SKU Cuts</h2>
      <div style=\"margin-bottom:10px;color:#4B5563;\">{start_day.isoformat()} to {end_day.isoformat()}</div>
      <div><b>Stores Flagged:</b> {int(len(store_summary)) if store_summary is not None else 0}</div>
      <div><b>Store-Brand Rows:</b> {int(len(store_brand_summary)) if store_brand_summary is not None else 0}</div>
      <div><b>SKUs To Cut:</b> {cut_sku_count}</div>
      <div><b>SKUs To Review:</b> {review_sku_count}</div>
      <div><b>No-Sales Cut SKUs:</b> {no_sales_cut_skus}</div>
      <br/>
      <div><b>Top Store-Brand Rows:</b></div>
      <ul>{''.join(f'<li>{xml_escape(line[2:])}</li>' for line in top_lines) if top_lines else '<li>None</li>'}</ul>
    </div>
    """

    msg = EmailMessage()
    msg["To"] = to_email
    msg["From"] = "me"
    msg["Subject"] = subject
    msg.set_content(plain)
    msg.add_alternative(html, subtype="html")

    sent_names: List[str] = []
    for attachment in attachments:
        path = Path(attachment)
        if not path.exists():
            continue
        _attach_file_to_email(msg, path)
        sent_names.append(path.name)

    raw = base64.urlsafe_b64encode(msg.as_bytes()).decode()
    service.users().messages().send(userId="me", body={"raw": raw}).execute()
    _log(f"[EMAIL] Sent store SKU cut report to {to_email}: {', '.join(sent_names)}", logger)


# ---------------------------------------------------------------------------
# Pipeline
# ---------------------------------------------------------------------------
def generate_brand_meeting_packet(
    brand: str,
    start_day: date,
    end_day: date,
    selected_store_codes: Sequence[str],
    output_root: Path = DEFAULT_OUTPUT_ROOT,
    options: Optional[PacketOptions] = None,
    logger: Optional[Callable[[str], None]] = None,
) -> PacketArtifacts:
    options = options or PacketOptions()

    _log(f"[START] Building Brand Meeting Packet for '{brand}'", logger)
    _log(f"[WINDOW] {start_day.isoformat()} -> {end_day.isoformat()}", logger)
    _log(f"[STORES] {', '.join(selected_store_codes)}", logger)
    if options.use_api:
        effective_workers = resolve_worker_count(options.api_workers, len(selected_store_codes))
        _log(f"[API] Store workers requested={options.api_workers}, effective={effective_workers}", logger)

    paths = build_run_paths(Path(output_root), brand, start_day, end_day)

    # Windows (report + recency + prior comparable)
    report_start, report_end = start_day, end_day
    last14_start = max(report_start, report_end - timedelta(days=13))
    last30_start = max(report_start, report_end - timedelta(days=29))
    last7_start = max(report_start, report_end - timedelta(days=6))
    mtd_start = osnap.month_start(report_end)
    prev_mtd_start, prev_mtd_end = osnap.prev_month_same_window(report_end)

    prior_report_start, prior_report_end = compute_prior_report_window(report_start, report_end)

    windows = {
        "report": (report_start, report_end),
        "last14": (last14_start, report_end),
        "last30": (last30_start, report_end),
        "last7": (last7_start, report_end),
        "mtd": (mtd_start, report_end),
        "prev_mtd": (prev_mtd_start, prev_mtd_end),
        "prior_report": (prior_report_start, prior_report_end),
    }

    acquisition_start = prior_report_start if options.include_prior_window_data else report_start
    acquisition_end = report_end

    # --- sales exports
    sales_paths, missing_sales_stores, _did_export_sales = prepare_sales_exports(
        paths=paths,
        brand=brand,
        selected_store_codes=selected_store_codes,
        acquisition_start=acquisition_start,
        acquisition_end=acquisition_end,
        allow_export=bool(options.run_export),
        force_refresh=bool(options.force_refresh_data),
        use_api=bool(options.use_api),
        api_env_file=options.api_env_file,
        api_workers=options.api_workers,
        logger=logger,
    )

    # --- catalog exports
    _copied_catalog, missing_catalog_stores, _did_export_catalog = prepare_catalog_exports(
        paths,
        selected_store_codes,
        run_export=bool(options.run_catalog_export),
        force_refresh=bool(options.force_refresh_data),
        use_api=bool(options.use_api),
        api_env_file=options.api_env_file,
        api_workers=options.api_workers,
        logger=logger,
    )

    # Load catalog first so its Brand column can inform sales-brand matching.
    catalog_raw = _load_catalog_exports(paths, selected_store_codes, logger)
    if not catalog_raw.empty and "_store_abbr" in catalog_raw.columns:
        present_catalog_codes = {
            str(code).upper()
            for code in catalog_raw["_store_abbr"].dropna().astype(str).tolist()
            if str(code).strip()
        }
        if present_catalog_codes:
            missing_catalog_stores = [abbr for abbr in selected_store_codes if abbr not in present_catalog_codes]
    brand_aliases = build_brand_aliases_from_catalog(brand, catalog_raw, logger)
    catalog_brand = prepare_catalog_for_brand(catalog_raw, brand, selected_store_codes, brand_aliases=brand_aliases)
    catalog_merge_maps = build_catalog_merge_maps(catalog_brand)

    # Load + prepare sales data
    brand_frames: List[pd.DataFrame] = []
    sales_source_coverage: Dict[str, Tuple[date, date]] = {}
    for abbr, path in sales_paths.items():
        try:
            raw = _read_sales_source_file(path)
        except Exception as exc:
            _log(f"[WARN] Could not read sales export for {abbr} ({path.name}): {exc}", logger)
            continue

        date_col = osnap.find_col(raw, osnap.COLUMN_CANDIDATES["date"])
        if date_col:
            dt_series = pd.to_datetime(raw[date_col], errors="coerce").dropna()
            if not dt_series.empty:
                sales_source_coverage[abbr] = (dt_series.min().date(), dt_series.max().date())

        dfb = _prepare_sales_df_for_brand(
            raw,
            store_code=abbr,
            brand=brand,
            brand_aliases=brand_aliases,
            include_kickbacks=options.include_kickback_adjustments,
            logger=logger,
            catalog_merge_maps=catalog_merge_maps,
        )
        if dfb.empty:
            _log(f"[WARN] No brand rows found for {abbr}.", logger)
            continue
        brand_frames.append(dfb)

    sales_brand = pd.concat(brand_frames, ignore_index=True) if brand_frames else _empty_prepared_brand_sales_df()
    if sales_brand.empty:
        _log("[WARN] No matching brand sales rows found in selected window/stores.", logger)

    # Catalog
    inv_overview = summarize_inventory_overview(catalog_brand)
    inv_products = summarize_inventory_products(catalog_brand)
    inv_category = summarize_inventory_by_category(catalog_brand)
    inv_store = summarize_inventory_by_store(catalog_brand)

    window_metrics: Dict[str, Dict[str, float]] = {}
    for key, (ws, we) in windows.items():
        window_metrics[key] = summarize_metrics(_date_filter(sales_brand, ws, we))

    report_df = _date_filter(sales_brand, *windows["report"])
    last14_df = _date_filter(sales_brand, *windows["last14"])
    last30_df = _date_filter(sales_brand, *windows["last30"])
    last7_df = _date_filter(sales_brand, *windows["last7"])
    prior_df = _date_filter(sales_brand, *windows["prior_report"])

    trend_days_30 = window_days(*windows["last30"])
    all_pg_units_day_map, all_pg_dos_map, store_pg_units_day_map, store_pg_dos_map = build_product_group_supply_maps(
        catalog_brand_df=catalog_brand,
        last14_sales_df=last30_df,
        trend_days=trend_days_30,
    )

    inv_over_30, inv_store_30, inv_products_30 = add_inventory_supply_metrics(
        inv_overview=inv_overview,
        inv_store=inv_store,
        inv_products=inv_products,
        catalog_brand_df=catalog_brand,
        last14_sales_df=last30_df,
        trend_start=windows["last30"][0],
        trend_end=windows["last30"][1],
        as_of_day=report_end,
    )
    inv_over_14, inv_store_14, inv_products_14 = add_inventory_supply_metrics(
        inv_overview=inv_overview,
        inv_store=inv_store,
        inv_products=inv_products,
        catalog_brand_df=catalog_brand,
        last14_sales_df=last14_df,
        trend_start=windows["last14"][0],
        trend_end=windows["last14"][1],
        as_of_day=report_end,
    )
    inv_over_7, inv_store_7, inv_products_7 = add_inventory_supply_metrics(
        inv_overview=inv_overview,
        inv_store=inv_store,
        inv_products=inv_products,
        catalog_brand_df=catalog_brand,
        last14_sales_df=last7_df,
        trend_start=windows["last7"][0],
        trend_end=windows["last7"][1],
        as_of_day=report_end,
    )

    inv_overview = _merge_inventory_overview_trends(inv_over_30, inv_over_14, inv_over_7)
    inv_store = _merge_inventory_store_trend_columns(inv_store_30, inv_store_14, inv_store_7)
    inv_products = _merge_inventory_trend_columns(inv_products_30, inv_products_14, inv_products_7)

    inv_units_by_store: Dict[str, float] = {}
    if catalog_brand is not None and not catalog_brand.empty and "_store_abbr" in catalog_brand.columns:
        inv_tmp = _inventory_reporting_rows(catalog_brand)
        inv_tmp["_store_abbr"] = inv_tmp["_store_abbr"].fillna("").astype(str).str.upper()
        inv_store_units_df = inv_tmp.groupby("_store_abbr", as_index=False).agg(units=("Available", "sum"))
        inv_units_by_store = {
            str(r["_store_abbr"]).upper(): float(r["units"])
            for _, r in inv_store_units_df.iterrows()
            if str(r["_store_abbr"]).strip()
        }

    # Trend-window supply-key coverage diagnostics.
    sales_cov = last30_df.copy()
    if not sales_cov.empty:
        if "_is_return" in sales_cov.columns:
            sales_cov = sales_cov[~sales_cov["_is_return"]].copy()
        sales_cov["supply_merge_key"] = sales_cov.get("supply_merge_key", "").fillna("").astype(str)
        sales_cov["supply_base_key"] = sales_cov.get("supply_base_key", "").fillna("").astype(str)
        missing_base = sales_cov["supply_base_key"].eq("")
        if missing_base.any():
            sales_cov.loc[missing_base, "supply_base_key"] = sales_cov.loc[missing_base, "supply_merge_key"].map(_supply_base_from_merge_key)
        sales_cov = sales_cov[sales_cov["supply_merge_key"] != ""].copy()

    catalog_cov = _inventory_reporting_rows(catalog_brand.copy() if catalog_brand is not None else pd.DataFrame())
    if not catalog_cov.empty:
        catalog_cov["supply_merge_key"] = catalog_cov.get("supply_merge_key", "").fillna("").astype(str)
        catalog_cov["supply_base_key"] = catalog_cov.get("supply_base_key", "").fillna("").astype(str)
        missing_base = catalog_cov["supply_base_key"].eq("")
        if missing_base.any():
            catalog_cov.loc[missing_base, "supply_base_key"] = catalog_cov.loc[missing_base, "supply_merge_key"].map(_supply_base_from_merge_key)
        catalog_cov = catalog_cov[(catalog_cov["supply_merge_key"] != "") & (catalog_cov["Available"] >= MIN_REPORTABLE_INVENTORY_UNITS)].copy()

    sales_unique_merge = int(sales_cov["supply_merge_key"].nunique()) if not sales_cov.empty else 0
    catalog_unique_merge = int(catalog_cov["supply_merge_key"].nunique()) if not catalog_cov.empty else 0
    sales_rows_n = int(len(sales_cov))

    basis_match_rows = 0
    basis_match_pct = 0.0
    if sales_rows_n > 0 and "_catalog_basis_matched" in sales_cov.columns:
        basis_match_rows = int(pd.to_numeric(sales_cov["_catalog_basis_matched"], errors="coerce").fillna(0).astype(int).sum())
        basis_match_pct = (basis_match_rows / float(sales_rows_n)) * 100.0

    catalog_merge_key_set = set(catalog_cov["supply_merge_key"].astype(str).tolist()) if not catalog_cov.empty else set()
    merge_match_rows = int(sales_cov["supply_merge_key"].isin(catalog_merge_key_set).sum()) if sales_rows_n > 0 else 0
    merge_match_pct = ((merge_match_rows / float(sales_rows_n)) * 100.0) if sales_rows_n > 0 else 0.0

    catalog_base_key_set = set(catalog_cov["supply_base_key"].astype(str).tolist()) if not catalog_cov.empty else set()
    family_match_rows = int(sales_cov["supply_base_key"].isin(catalog_base_key_set).sum()) if sales_rows_n > 0 else 0
    family_match_pct = ((family_match_rows / float(sales_rows_n)) * 100.0) if sales_rows_n > 0 else 0.0

    _log(
        f"[SUPPLY] Trend key coverage (30d): sales_keys={sales_unique_merge}, catalog_keys={catalog_unique_merge}, "
        f"sales_rows={sales_rows_n}, basis_match={basis_match_rows}/{sales_rows_n} ({basis_match_pct:.1f}%), "
        f"merge_match={merge_match_rows}/{sales_rows_n} ({merge_match_pct:.1f}%), "
        f"family_match={family_match_rows}/{sales_rows_n} ({family_match_pct:.1f}%)",
        logger,
    )

    prior_window_covered = False
    if options.include_prior_window_data:
        prior_window_covered = bool(sales_source_coverage)
        if prior_window_covered:
            for abbr in sales_paths.keys():
                cov = sales_source_coverage.get(abbr)
                if cov is None:
                    prior_window_covered = False
                    break
                cov_start, cov_end = cov
                if not (cov_start <= prior_report_start and cov_end >= prior_report_end):
                    prior_window_covered = False
                    break

        if not prior_window_covered:
            _log(
                f"[WARN] Prior comparable window is not fully covered by loaded exports. "
                f"Need data from {prior_report_start.isoformat()} -> {prior_report_end.isoformat()}.",
                logger,
            )
            # Avoid misleading deltas/movers when prior window isn't loaded.
            prior_df = prior_df.iloc[0:0].copy()
            window_metrics["prior_report"] = summarize_metrics(prior_df)
    else:
        prior_df = prior_df.iloc[0:0].copy()
        empty_metrics = summarize_metrics(prior_df)
        window_metrics["prior_report"] = empty_metrics
        window_metrics["prev_mtd"] = empty_metrics

    store_60 = summarize_group(report_df, "_store_abbr")
    store_14 = summarize_group(last14_df, "_store_abbr")
    category_60 = summarize_group(report_df, "category_normalized")
    category_14 = summarize_group(last14_df, "category_normalized")
    product_60 = summarize_product_groups(report_df)
    product_14 = summarize_product_groups(last14_df)
    prior_product = summarize_product_groups(prior_df) if options.include_prior_window_data and not prior_df.empty else pd.DataFrame()
    product_60 = add_supply_to_product_groups(product_60, all_pg_units_day_map, all_pg_dos_map)
    product_14 = add_supply_to_product_groups(product_14, all_pg_units_day_map, all_pg_dos_map)
    product_60 = attach_catalog_price_cost_to_product_groups(product_60, catalog_brand)
    product_14 = attach_catalog_price_cost_to_product_groups(product_14, catalog_brand)
    report_df_pg = _filter_product_group_rows(report_df)
    prior_df_pg = _filter_product_group_rows(prior_df)

    daily_60 = summarize_daily(report_df)
    weekly_60 = summarize_weekly(report_df)

    kickback_rules = summarize_kickback_rules(report_df)

    movers_store = compute_movers(report_df, prior_df, "_store_abbr", top_n=2)
    movers_category = compute_movers(report_df, prior_df, "category_normalized", top_n=2)
    movers_product = relabel_product_group_movers(
        compute_movers(report_df_pg, prior_df_pg, "product_group_key", top_n=3),
        product_60,
    )

    store_sales_packets: Dict[str, Dict[str, Any]] = {}
    for abbr in selected_store_codes:
        s_catalog = pd.DataFrame()
        if catalog_brand is not None and not catalog_brand.empty and "_store_abbr" in catalog_brand.columns:
            s_catalog = catalog_brand[
                catalog_brand["_store_abbr"].fillna("").astype(str).str.upper() == str(abbr).upper()
            ].copy()

        s_report = report_df[report_df["_store_abbr"] == abbr].copy()
        inv_units = float(inv_units_by_store.get(str(abbr).upper(), 0.0))
        has_sales = not s_report.empty
        has_inventory = inv_units > 0.0
        if not has_sales and not has_inventory:
            _log(f"[STORE] Skipping {abbr}: no sales and no inventory.", logger)
            continue

        s_last30 = last30_df[last30_df["_store_abbr"] == abbr].copy()
        s_last14 = last14_df[last14_df["_store_abbr"] == abbr].copy()
        s_last7 = _date_filter(sales_brand[sales_brand["_store_abbr"] == abbr], *windows["last7"])
        s_mtd = _date_filter(sales_brand[sales_brand["_store_abbr"] == abbr], *windows["mtd"])
        s_prev_mtd = _date_filter(sales_brand[sales_brand["_store_abbr"] == abbr], *windows["prev_mtd"])
        s_prior = _date_filter(sales_brand[sales_brand["_store_abbr"] == abbr], *windows["prior_report"])

        if options.include_prior_window_data:
            cov = sales_source_coverage.get(abbr)
            s_prior_covered = bool(cov and cov[0] <= prior_report_start and cov[1] >= prior_report_end)
            if not s_prior_covered:
                s_prior = s_prior.iloc[0:0].copy()
        else:
            s_prev_mtd = s_prev_mtd.iloc[0:0].copy()
            s_prior = s_prior.iloc[0:0].copy()
            s_prior_covered = False

        s_window_metrics = {
            "report": summarize_metrics(s_report),
            "last14": summarize_metrics(s_last14),
            "last7": summarize_metrics(s_last7),
            "mtd": summarize_metrics(s_mtd),
            "prev_mtd": summarize_metrics(s_prev_mtd),
            "prior_report": summarize_metrics(s_prior),
        }

        inv_row = inv_store[inv_store["_store_abbr"] == str(abbr).upper()] if not inv_store.empty else pd.DataFrame()
        inv_metrics = {
            "units_available": float(inv_row["units_available"].iloc[0]) if not inv_row.empty else 0.0,
            "inventory_value": float(inv_row["inventory_value"].iloc[0]) if not inv_row.empty else 0.0,
            "potential_revenue": float(inv_row["potential_revenue"].iloc[0]) if not inv_row.empty else 0.0,
            "potential_profit": float(inv_row["potential_profit"].iloc[0]) if not inv_row.empty else 0.0,
            "avg_margin": float(inv_row["avg_margin"].iloc[0]) if not inv_row.empty else 0.0,
            "trend_units_per_day_30d": float(inv_row["trend_units_per_day_30d"].iloc[0]) if (not inv_row.empty and "trend_units_per_day_30d" in inv_row.columns) else (
                float(inv_row["trend_units_per_day_14d"].iloc[0]) if (not inv_row.empty and "trend_units_per_day_14d" in inv_row.columns) else 0.0
            ),
            "trend_units_per_day_14d": float(inv_row["trend_units_per_day_14d"].iloc[0]) if (not inv_row.empty and "trend_units_per_day_14d" in inv_row.columns) else 0.0,
            "trend_units_per_day_7d": float(inv_row["trend_units_per_day_7d"].iloc[0]) if (not inv_row.empty and "trend_units_per_day_7d" in inv_row.columns) else 0.0,
            "days_of_supply": float(inv_row["days_of_supply"].iloc[0]) if (not inv_row.empty and "days_of_supply" in inv_row.columns and pd.notna(inv_row["days_of_supply"].iloc[0])) else np.nan,
            "est_oos_date": str(inv_row["est_oos_date"].iloc[0]) if (not inv_row.empty and "est_oos_date" in inv_row.columns) else "n/a",
        }

        s_product_60 = summarize_product_groups(s_report)
        s_product_14 = summarize_product_groups(s_last14)
        s_units_day_map = store_pg_units_day_map.get(str(abbr).upper(), {})
        s_dos_map = store_pg_dos_map.get(str(abbr).upper(), {})
        s_product_60 = add_supply_to_product_groups(s_product_60, s_units_day_map, s_dos_map)
        s_product_14 = add_supply_to_product_groups(s_product_14, s_units_day_map, s_dos_map)
        s_product_60 = attach_catalog_price_cost_to_product_groups(s_product_60, s_catalog)
        s_product_14 = attach_catalog_price_cost_to_product_groups(s_product_14, s_catalog)
        s_report_pg = _filter_product_group_rows(s_report)
        s_prior_pg = _filter_product_group_rows(s_prior)

        s_inv_products = pd.DataFrame()
        if not s_catalog.empty:
            s_inv_over_base = summarize_inventory_overview(s_catalog)
            s_inv_store_base = summarize_inventory_by_store(s_catalog)
            s_inv_products_base = summarize_inventory_products(s_catalog)
            _s_o30, _s_s30, _s_p30 = add_inventory_supply_metrics(
                inv_overview=s_inv_over_base,
                inv_store=s_inv_store_base,
                inv_products=s_inv_products_base,
                catalog_brand_df=s_catalog,
                last14_sales_df=s_last30,
                trend_start=windows["last30"][0],
                trend_end=windows["last30"][1],
                as_of_day=report_end,
            )
            _s_o14, _s_s14, _s_p14 = add_inventory_supply_metrics(
                inv_overview=s_inv_over_base,
                inv_store=s_inv_store_base,
                inv_products=s_inv_products_base,
                catalog_brand_df=s_catalog,
                last14_sales_df=s_last14,
                trend_start=windows["last14"][0],
                trend_end=windows["last14"][1],
                as_of_day=report_end,
            )
            _s_o7, _s_s7, _s_p7 = add_inventory_supply_metrics(
                inv_overview=s_inv_over_base,
                inv_store=s_inv_store_base,
                inv_products=s_inv_products_base,
                catalog_brand_df=s_catalog,
                last14_sales_df=s_last7,
                trend_start=windows["last7"][0],
                trend_end=windows["last7"][1],
                as_of_day=report_end,
            )
            s_inv_products = _merge_inventory_trend_columns(_s_p30, _s_p14, _s_p7)

        store_sales_packets[abbr] = {
            "store_name": _store_name_from_abbr(abbr),
            "prior_window_covered": s_prior_covered,
            "window_metrics": s_window_metrics,
            "inventory": inv_metrics,
            "inventory_products": s_inv_products,
            "daily": summarize_daily(s_report),
            "category_60": summarize_group(s_report, "category_normalized"),
            "category_14": summarize_group(s_last14, "category_normalized"),
            "product_60": s_product_60,
            "product_14": s_product_14,
            "kickback_rules": summarize_kickback_rules(s_report),
            "movers_category": compute_movers(s_report, s_prior, "category_normalized", top_n=2),
            "movers_product": relabel_product_group_movers(
                compute_movers(s_report_pg, s_prior_pg, "product_group_key", top_n=3),
                s_product_60,
            ),
        }
    margin_risk = inv_products[inv_products["margin_current"] < 0.35].copy() if not inv_products.empty else pd.DataFrame()
    best_candidates = inv_products[(inv_products["margin_current"] > 0.55) & (inv_products["units_available"] > 10)].copy() if not inv_products.empty else pd.DataFrame()

    targets_payload = load_targets(THIS_DIR / "brand_meeting_targets.json")
    brand_targets = get_brand_targets(targets_payload, brand)
    target_margin = float(options.target_margin if options.target_margin is not None else brand_targets.get("target_margin", DEAL_TARGET_MARGIN))
    brand_targets["target_margin"] = target_margin

    all_credit_rows: List[Dict[str, Any]] = []
    credit_ledger_df = pd.DataFrame()
    credit_summary: Dict[str, Any] = {
        "brand": brand,
        "target_margin": target_margin,
        "net_revenue": float(window_metrics.get("report", {}).get("net_revenue", 0.0)),
        "real_profit": float(window_metrics.get("report", {}).get("profit_real", 0.0)),
        "real_margin": float(window_metrics.get("report", {}).get("margin_real", 0.0)),
        "system_expected_credit": 0.0,
        "manual_expected_credit": 0.0,
        "manual_received_credit": 0.0,
        "expected_credit_amount": 0.0,
        "received_credit_amount": 0.0,
        "credit_gap": 0.0,
        "expected_credit_margin": float(window_metrics.get("report", {}).get("margin_real", 0.0)),
        "received_credit_margin": float(window_metrics.get("report", {}).get("margin_real", 0.0)),
        "credit_needed_to_hit_target": 0.0,
    }
    credit_reconciliation = pd.DataFrame()
    if options.include_credit_reconciliation:
        ledger_path = Path(options.credit_ledger_path or "brand_credit_ledger.json")
        if not ledger_path.is_absolute():
            ledger_path = THIS_DIR / ledger_path
        manual_credit_rows = load_credit_ledger(ledger_path)
        all_credit_rows = list(manual_credit_rows)
        creditflow_rows: List[Dict[str, Any]] = []
        if options.include_creditflow_credits:
            creditflow_rows, creditflow_meta = fetch_creditflow_credits_for_brand(
                brand=brand,
                start_day=start_day,
                end_day=end_day,
                env_file=THIS_DIR / options.api_env_file,
                base_url=options.creditflow_base_url,
                aliases=brand_aliases,
            )
            try:
                write_creditflow_cache(paths.cache_dir / "creditflow_credits_cache.json", creditflow_rows, creditflow_meta)
            except Exception:
                pass
            _print_creditflow_pull_audit(brand, creditflow_rows, creditflow_meta)
            if creditflow_meta.get("warning"):
                _log(f"[CREDITFLOW] {creditflow_meta.get('warning')}", logger)
            else:
                _log(
                    f"[CREDITFLOW] matched {len(creditflow_rows)} of {creditflow_meta.get('raw_credits', 0)} credits "
                    f"for {brand} ({creditflow_meta.get('brands', 0)} brands, {creditflow_meta.get('stores', 0)} stores loaded).",
                    logger,
                )
            all_credit_rows.extend(creditflow_rows)
        credit_ledger_df = ledger_to_dataframe(all_credit_rows)
        system_expected_credit = float(window_metrics.get("report", {}).get("kickback_total", 0.0) or 0.0)
        credit_summary, credit_reconciliation = summarize_credit_reconciliation(
            all_credit_rows,
            report_df,
            brand=brand,
            start_day=start_day,
            end_day=end_day,
            target_margin=target_margin,
            system_expected_credit=system_expected_credit,
        )
        if not credit_reconciliation.empty:
            src = credit_reconciliation.get("Source", pd.Series("", index=credit_reconciliation.index)).astype(str).str.lower()
            cf_mask = src.eq("creditflow")
            ledger_mask = src.eq("manual")
            credit_summary["creditflow_expected_credit"] = float(pd.to_numeric(credit_reconciliation.loc[cf_mask, "Expected"], errors="coerce").fillna(0.0).sum())
            credit_summary["creditflow_received_credit"] = float(pd.to_numeric(credit_reconciliation.loc[cf_mask, "Received"], errors="coerce").fillna(0.0).sum())
            credit_summary["creditflow_gap"] = float(pd.to_numeric(credit_reconciliation.loc[cf_mask, "Gap"], errors="coerce").fillna(0.0).sum())
            credit_summary["creditflow_open_rows"] = int(
                (cf_mask & credit_reconciliation.get("Status", pd.Series("", index=credit_reconciliation.index)).astype(str).str.lower().isin({"expected", "partial", "overdue"})).sum()
            )
            credit_summary["ledger_expected_credit"] = float(pd.to_numeric(credit_reconciliation.loc[ledger_mask, "Expected"], errors="coerce").fillna(0.0).sum())
            credit_summary["ledger_received_credit"] = float(pd.to_numeric(credit_reconciliation.loc[ledger_mask, "Received"], errors="coerce").fillna(0.0).sum())
            credit_summary["ledger_gap"] = float(pd.to_numeric(credit_reconciliation.loc[ledger_mask, "Gap"], errors="coerce").fillna(0.0).sum())
        else:
            credit_summary["creditflow_expected_credit"] = 0.0
            credit_summary["creditflow_received_credit"] = 0.0
            credit_summary["creditflow_gap"] = 0.0
            credit_summary["creditflow_open_rows"] = 0
            credit_summary["ledger_expected_credit"] = 0.0
            credit_summary["ledger_received_credit"] = 0.0
            credit_summary["ledger_gap"] = 0.0
        window_metrics["report"].update({
            "target_margin": target_margin,
            "expected_credit_amount": float(credit_summary.get("expected_credit_amount", 0.0)),
            "received_credit_amount": float(credit_summary.get("received_credit_amount", 0.0)),
            "credit_gap": float(credit_summary.get("credit_gap", 0.0)),
            "expected_credit_margin": float(credit_summary.get("expected_credit_margin", 0.0)),
            "received_credit_margin": float(credit_summary.get("received_credit_margin", 0.0)),
        })
        try:
            export_credit_csv(paths.cache_dir / "brand_credit_ledger_export.csv", all_credit_rows)
        except Exception:
            pass
        _log(
            f"[CREDITS] rows={len(all_credit_rows)} manual={len(manual_credit_rows)} creditflow={len(creditflow_rows)} "
            f"expected={money0(credit_summary.get('expected_credit_amount', 0.0))} "
            f"received={money0(credit_summary.get('received_credit_amount', 0.0))} gap={money0(credit_summary.get('credit_gap', 0.0))}",
            logger,
        )

    store_credit_scorecard = _store_credit_scorecard(
        store_60,
        inv_store,
        credit_summary,
        target_margin,
        top_n=max(options.top_n, 12),
        credit_reconciliation=credit_reconciliation,
    )
    monthly_reference: Dict[str, Any] = {}
    if options.include_monthly_reference:
        monthly_reference = load_monthly_reference(brand, start_day, end_day)
        if monthly_reference.get("available"):
            _log(f"[MONTHLY] Loaded owner monthly reference for {monthly_reference.get('month')}.", logger)
        else:
            _log(f"[MONTHLY] Owner monthly reference not found for {monthly_reference.get('month')}.", logger)
    action_items = generate_brand_action_items(
        metrics=window_metrics.get("report", {}),
        credit_summary=credit_summary,
        inv_products=inv_products,
        store_df=store_60,
        targets=brand_targets,
        max_items=12,
    )
    health_score, health_status, health_reason = generate_brand_health_score(
        metrics=window_metrics.get("report", {}),
        credit_summary=credit_summary,
        inv_overview=inv_overview,
        store_df=store_60,
        targets=brand_targets,
    )
    meeting_ask = generate_meeting_ask(credit_summary, action_items)
    dashboard_data: Dict[str, Any] = {}
    if str(options.packet_layout or "classic").lower() == "dashboard":
        dashboard_data = build_dashboard_packet_data(
            product_60=product_60,
            inv_products=inv_products,
            prior_product=prior_product,
            report_df=report_df,
            report_days=window_days(report_start, report_end),
            selected_store_codes=selected_store_codes,
            store_60=store_60,
            store_sales_packets=store_sales_packets,
            inv_store=inv_store,
            category_60=category_60,
            inv_category=inv_category,
            window_metrics=window_metrics,
            inv_overview=inv_overview,
            credit_summary=credit_summary,
            target_margin=target_margin,
            max_products=max(1, int(options.max_products or 20)),
            include_prior_data=bool(options.include_prior_window_data and prior_window_covered),
            meeting_ask=meeting_ask,
        )

    # Persist cache CSVs for debugging / quick QA
    try:
        p_sales = paths.cache_dir / "sales_brand_rows.csv"
        p_sales_14 = paths.cache_dir / "sales_brand_rows_14d.csv"
        p_sales_30 = paths.cache_dir / "sales_brand_rows_30d.csv"
        p_pg60 = paths.cache_dir / "product_groups_60d.csv"
        p_pg14 = paths.cache_dir / "product_groups_14d.csv"
        p_pg30 = paths.cache_dir / "product_groups_30d.csv"
        p_inv = paths.cache_dir / "inventory_products.csv"
        p_credit = paths.cache_dir / "credit_reconciliation.csv"
        p_credit_source = paths.cache_dir / "credit_source_summary.csv"
        p_actions = paths.cache_dir / "brand_action_items.csv"
        p_store_credit = paths.cache_dir / "store_credit_scorecards.csv"

        report_df.to_csv(p_sales, index=False)
        last14_df.to_csv(p_sales_14, index=False)
        last30_df.to_csv(p_sales_30, index=False)
        product_60.to_csv(p_pg60, index=False)
        product_14.to_csv(p_pg14, index=False)
        add_supply_to_product_groups(summarize_product_groups(last30_df), all_pg_units_day_map, all_pg_dos_map).to_csv(p_pg30, index=False)
        inv_products.to_csv(p_inv, index=False)
        credit_reconciliation.to_csv(p_credit, index=False)
        _credit_source_summary(credit_reconciliation).to_csv(p_credit_source, index=False)
        pd.DataFrame(action_items).to_csv(p_actions, index=False)
        store_credit_scorecard.to_csv(p_store_credit, index=False)

        # DOS QA files: shows the trend keys and how sales/inventory align.
        sales_q_grp = pd.DataFrame(columns=["supply_base_key", "supply_merge_key", "units_sold"])
        inv_q_grp = pd.DataFrame(columns=["supply_base_key", "supply_merge_key", "units_available"])
        if not last30_df.empty:
            sales_q = last30_df.copy()
            if "_is_return" in sales_q.columns:
                sales_q = sales_q[~sales_q["_is_return"]].copy()
            sales_q["supply_merge_key"] = sales_q.get("supply_merge_key", "")
            sales_q["supply_base_key"] = sales_q.get("supply_base_key", sales_q["supply_merge_key"].map(_supply_base_from_merge_key))
            sales_q_grp = sales_q.groupby(["_store_abbr", "supply_base_key", "supply_merge_key"], as_index=False).agg(
                units_30d=("_qty", "sum"),
                net_30d=("_net", "sum"),
                rows=("supply_merge_key", "size"),
            )
            p_dos_sales = paths.cache_dir / "dos_sales_families_30d.csv"
            sales_q_grp.to_csv(p_dos_sales, index=False)
            _log(f"[QA] Wrote DOS sales families: {p_dos_sales}", logger)
            sales_q_grp = sales_q.groupby(["supply_base_key", "supply_merge_key"], as_index=False).agg(
                units_sold=("_qty", "sum"),
            )

        if catalog_brand is not None and not catalog_brand.empty:
            inv_q = _inventory_reporting_rows(catalog_brand)
            inv_q["supply_merge_key"] = inv_q.get("supply_merge_key", "")
            inv_q["supply_base_key"] = inv_q.get("supply_base_key", inv_q["supply_merge_key"].map(_supply_base_from_merge_key))
            inv_q_grp = inv_q.groupby(["_store_abbr", "supply_base_key", "supply_merge_key"], as_index=False).agg(
                units_available=("Available", "sum"),
            )
            p_dos_inv = paths.cache_dir / "dos_inventory_families.csv"
            inv_q_grp.to_csv(p_dos_inv, index=False)
            _log(f"[QA] Wrote DOS inventory families: {p_dos_inv}", logger)

            inv_q_grp = inv_q.groupby(["supply_base_key", "supply_merge_key"], as_index=False).agg(
                units_available=("Available", "sum"),
            )

        dos_key_audit = sales_q_grp.merge(inv_q_grp, on=["supply_base_key", "supply_merge_key"], how="outer")
        if not dos_key_audit.empty:
            dos_key_audit["units_sold"] = pd.to_numeric(dos_key_audit.get("units_sold", 0.0), errors="coerce").fillna(0.0).astype(float)
            dos_key_audit["units_available"] = pd.to_numeric(dos_key_audit.get("units_available", 0.0), errors="coerce").fillna(0.0).astype(float)
            dos_key_audit["units_per_day"] = dos_key_audit["units_sold"] / float(max(trend_days_30, 1))
            dos_key_audit["days_of_supply"] = dos_key_audit.apply(
                lambda r: _safe_dos(float(r.get("units_available", 0.0)), float(r.get("units_per_day", 0.0))),
                axis=1,
            )
            dos_key_audit = dos_key_audit.sort_values(["units_sold", "units_available"], ascending=False)
        p_dos_audit = paths.cache_dir / "dos_trend_key_audit_30d.csv"
        dos_key_audit.to_csv(p_dos_audit, index=False)
        _log(f"[QA] Wrote DOS key audit: {p_dos_audit}", logger)
        _log(f"[QA] Cache files: {p_sales}, {p_pg60}, {p_inv}", logger)
        if dashboard_data:
            write_dashboard_cache(paths.cache_dir, dashboard_data, logger)
    except Exception:
        pass

    if str(options.packet_layout or "classic").lower() == "dashboard":
        quick_pdf_name = safe_filename(
            f"Brand Dashboard - {brand} - {start_day.isoformat()}_to_{end_day.isoformat()}.pdf"
        )
    else:
        quick_pdf_name = safe_filename(
            f"Brand Packet - {brand} - {start_day.isoformat()}_to_{end_day.isoformat()} - Quick Store Dashboards.pdf"
        )
    out_quick_pdf = paths.pdf_dir / quick_pdf_name

    layout = str(options.packet_layout or "classic").lower()
    if layout == "dashboard":
        build_brand_packet_dashboard_pdf(
            out_pdf=out_quick_pdf,
            brand=brand,
            start_day=start_day,
            end_day=end_day,
            selected_store_codes=selected_store_codes,
            options=options,
            dashboard_data=dashboard_data,
            credit_reconciliation=credit_reconciliation,
            daily_60=daily_60,
            store_sales_packets=store_sales_packets,
            missing_sales_stores=missing_sales_stores,
            missing_catalog_stores=missing_catalog_stores,
        )
        _log(f"[PDF] Created (Dashboard Easy Read): {out_quick_pdf}", logger)
    else:
        pdf_builder = build_brand_packet_premium_pdf if options.compact_pdf_mode else build_brand_packet_quick_pdf
        pdf_builder(
            out_pdf=out_quick_pdf,
            brand=brand,
            start_day=start_day,
            end_day=end_day,
            options=options,
            windows=windows,
            window_metrics=window_metrics,
            prior_window_covered=prior_window_covered,
            daily_60=daily_60,
            store_60=store_60,
            category_60=category_60,
            product_60=product_60,
            movers_store=movers_store,
            movers_category=movers_category,
            movers_product=movers_product,
            inv_overview=inv_overview,
            inv_products=inv_products,
            inv_category=inv_category,
            inv_store=inv_store,
            store_sales_packets=store_sales_packets,
            missing_sales_stores=missing_sales_stores,
            missing_catalog_stores=missing_catalog_stores,
            credit_summary=credit_summary,
            credit_reconciliation=credit_reconciliation,
            action_items=action_items,
            monthly_reference=monthly_reference,
            health_score=health_score,
            health_status=health_status,
            health_reason=health_reason,
            meeting_ask=meeting_ask,
            store_credit_scorecard=store_credit_scorecard,
        )
        _log(f"[PDF] Created ({'Premium Compact' if options.compact_pdf_mode else 'Quick Store Dashboards'}): {out_quick_pdf}", logger)

    out_xlsx: Optional[Path] = None
    if options.generate_xlsx:
        xlsx_name = safe_filename(f"Brand Packet - {brand} - {start_day.isoformat()}_to_{end_day.isoformat()}.xlsx")
        out_xlsx = paths.pdf_dir / xlsx_name
        build_brand_packet_xlsx(
            out_xlsx,
            window_metrics,
            store_60,
            store_14,
            category_60,
            category_14,
            product_60,
            product_14,
            daily_60,
            weekly_60,
            kickback_rules,
            inv_overview,
            inv_products,
            inv_category,
            margin_risk,
            best_candidates,
            credit_summary=credit_summary,
            credit_reconciliation=credit_reconciliation,
            credit_ledger_rows=credit_ledger_df,
            store_credit_scorecard=store_credit_scorecard,
            action_items=action_items,
            monthly_reference=monthly_reference,
        )
        _log(f"[XLSX] Created: {out_xlsx}", logger)

    followup_notes_path: Optional[Path] = None
    if options.generate_followup_notes:
        followup_name = safe_filename(f"Brand Meeting Follow-Up - {brand} - {start_day.isoformat()}_to_{end_day.isoformat()}.txt")
        followup_notes_path = paths.pdf_dir / followup_name
        followup_notes_path.write_text(
            build_followup_text(
                brand=brand,
                start_day=start_day,
                end_day=end_day,
                metrics=window_metrics.get("report", {}),
                credit_summary=credit_summary,
                action_items=action_items,
                meeting_ask=meeting_ask,
            ),
            encoding="utf-8",
        )
        _log(f"[NOTES] Created: {followup_notes_path}", logger)

    if options.email_results:
        top_store = "N/A"
        if not store_60.empty:
            r0 = store_60.iloc[0]
            top_store = f"{r0['_store_abbr']} ({money0(float(r0['net_revenue']))})"

        send_brand_packet_email(
            pdf_paths=[out_quick_pdf],
            brand=brand,
            start_day=start_day,
            end_day=end_day,
            report_metrics=window_metrics.get("report", {}),
            inv_overview=inv_overview,
            top_products=product_60,
            top_store_name=top_store,
            to_email=DEFAULT_REPORT_EMAIL,
            logger=logger,
            credit_summary=credit_summary,
            meeting_ask=meeting_ask,
        )

    _log("Done ✅", logger)

    return PacketArtifacts(
        quick_pdf_path=out_quick_pdf,
        detail_pdf_path=out_quick_pdf,
        pdf_path=out_quick_pdf,
        xlsx_path=out_xlsx,
        followup_notes_path=followup_notes_path,
        run_paths=paths,
        missing_sales_stores=missing_sales_stores,
        missing_catalog_stores=missing_catalog_stores,
    )


# ---------------------------------------------------------------------------
# CLI
# ---------------------------------------------------------------------------
def parse_cli_args() -> argparse.Namespace:
    p = argparse.ArgumentParser(description="Generate a Brand Meeting Packet PDF/XLSX across Buzz stores.")
    p.add_argument("--brand", help="Brand name (example: 'Cold Fire'). Not required with --owner-rollup.")
    p.add_argument("--days", type=int, default=DEFAULT_DAYS, help=f"Default rolling window length. Default: {DEFAULT_DAYS}")
    p.add_argument("--start-date", type=parse_iso_date, help="Override start date (YYYY-MM-DD).")
    p.add_argument("--end-date", type=parse_iso_date, help="Override end date (YYYY-MM-DD).")
    p.add_argument("--stores", type=str, default="", help="Comma-separated store codes, e.g. MV,LM,SV")

    p.add_argument("--use-api", action="store_true", help="Fetch sales and catalog/inventory data from the Dutchie POS API instead of browser exports.")
    p.add_argument("--env-file", type=str, default=DEFAULT_API_ENV_FILE, help=f"Env file containing Dutchie API keys when --use-api is enabled. Default: {DEFAULT_API_ENV_FILE}")
    p.add_argument("--workers", type=int, default=DEFAULT_PACKET_API_WORKERS, help=f"Store workers for Dutchie API downloads. Default: {DEFAULT_PACKET_API_WORKERS}.")
    p.add_argument("--run-export", action="store_true", help="Run Dutchie sales export before building packet.")
    p.add_argument("--no-export", action="store_true", help="Reuse latest archived exports instead of running exporter.")
    p.add_argument("--no-catalog-export", action="store_true", help="Skip running getCatalog.py (debug/fast runs).")

    p.add_argument("--email", dest="email_results", action="store_true", help="Email packet after build (default).")
    p.add_argument("--no-email", dest="email_results", action="store_false", help="Skip email.")
    p.set_defaults(email_results=True)

    p.add_argument("--output-dir", type=str, default=str(DEFAULT_OUTPUT_ROOT), help="Output root dir.")
    p.add_argument("--top-n", type=int, default=20, help="Top-N rows for packet tables.")
    p.add_argument("--owner-rollup", action="store_true", help="Build one owner-facing top-brands review instead of a single-brand packet.")
    p.add_argument("--top-brands", type=int, default=20, help="Number of brands in the owner top-brands review. Default: 20.")
    p.add_argument("--owner-brand-cards", dest="owner_brand_cards", action="store_true", help="Include compact brand cards in the owner rollup (default).")
    p.add_argument("--no-owner-brand-cards", dest="owner_brand_cards", action="store_false", help="Skip compact brand cards in the owner rollup.")
    p.set_defaults(owner_brand_cards=True)
    p.add_argument("--owner-email", action="store_true", help="Email the owner top-brands review after it is built (default; use --no-email to skip).")
    p.add_argument("--owner-output-root", type=str, default="", help="Output root for owner rollups. Defaults to --output-dir.")
    p.add_argument("--owner-creditflow", action="store_true", help="Pull CreditFlow credits for reviewed owner-rollup brands.")
    p.add_argument("--sort-by", choices=["sales"], default="sales", help="Owner rollup sort field. Currently only sales is supported.")

    p.add_argument("--no-charts", action="store_true", help="Skip charts in PDF.")
    p.add_argument("--no-store-sections", action="store_true", help="Skip store-level sections.")
    p.add_argument("--no-product-appendix", action="store_true", help="Skip full product appendix.")
    p.add_argument("--packet-layout", choices=["dashboard", "classic"], default="classic", help="Single-brand PDF layout. Use dashboard for the landscape easy-read packet. Default: classic.")
    p.add_argument("--dashboard", dest="packet_layout", action="store_const", const="dashboard", help="Shortcut for --packet-layout dashboard.")
    p.add_argument("--include-appendix", dest="appendix_enabled", action="store_true", help="Include appendix tables in dashboard/classic packet output.")
    p.add_argument("--no-appendix", dest="appendix_enabled", action="store_false", help="Skip appendix tables in dashboard/classic packet output.")
    p.set_defaults(appendix_enabled=None)
    p.add_argument("--max-products", type=int, default=20, help="Maximum product rows in dashboard product sections. Default: 20.")
    p.add_argument("--max-store-products", type=int, default=10, help="Maximum product rows in dashboard store sections. Default: 10.")
    p.add_argument("--with-kickbacks", action="store_true", help="Enable deal kickback adjustments (default: OFF).")
    p.add_argument("--no-kickbacks", action="store_true", help=argparse.SUPPRESS)
    p.add_argument("--xlsx", action="store_true", help="Also generate XLSX workbook.")
    p.add_argument("--force-refresh", action="store_true", help="Ignore cached inputs for this run and download fresh data.")
    p.add_argument("--no-prior-data", action="store_true", help="Use only the selected report window and disable prior-comparison data.")
    p.add_argument("--include-credit-reconciliation", dest="include_credit_reconciliation", action="store_true", help="Include manual credit/support reconciliation (default).")
    p.add_argument("--no-credit-reconciliation", dest="include_credit_reconciliation", action="store_false", help="Skip manual credit/support reconciliation.")
    p.set_defaults(include_credit_reconciliation=True)
    p.add_argument("--credit-ledger", type=str, default="brand_credit_ledger.json", help="Path to manual brand credit ledger JSON.")
    p.add_argument("--fetch-creditflow-credits", dest="include_creditflow_credits", action="store_true", help="Pull CreditFlow ERP credits from the API when an API key is configured (default).")
    p.add_argument("--no-creditflow-credits", dest="include_creditflow_credits", action="store_false", help="Do not pull CreditFlow ERP credits.")
    p.set_defaults(include_creditflow_credits=True)
    p.add_argument("--creditflow-base-url", type=str, default="https://creditflow.replit.app/api/v1", help="CreditFlow API base URL.")
    p.add_argument("--target-margin", type=float, default=None, help="Target margin as decimal or percent (0.35 or 35).")
    p.add_argument("--include-monthly-reference", dest="include_monthly_reference", action="store_true", help="Use monthly owner report exports as context (default).")
    p.add_argument("--no-monthly-reference", dest="include_monthly_reference", action="store_false", help="Skip monthly owner report context.")
    p.set_defaults(include_monthly_reference=True)
    p.add_argument("--packet-mode", choices=["quick", "standard", "deep"], default="standard", help="Packet depth. Default: standard.")
    p.add_argument("--generate-followup-notes", dest="generate_followup_notes", action="store_true", help="Write meeting follow-up notes text file (default).")
    p.add_argument("--no-followup-notes", dest="generate_followup_notes", action="store_false", help="Skip meeting follow-up notes.")
    p.set_defaults(generate_followup_notes=True)
    p.add_argument("--compact-pdf-mode", dest="compact_pdf_mode", action="store_true", help="Use compact PDF layout (default).")
    p.add_argument("--no-compact-pdf-mode", dest="compact_pdf_mode", action="store_false", help="Use legacy spacing where still available.")
    p.set_defaults(compact_pdf_mode=True)

    return p.parse_args()


def _resolve_dates(args: argparse.Namespace) -> Tuple[date, date]:
    if args.start_date and not args.end_date:
        raise ValueError("--start-date requires --end-date")
    if args.end_date and not args.start_date:
        raise ValueError("--end-date requires --start-date")

    if args.start_date and args.end_date:
        if args.end_date < args.start_date:
            raise ValueError("--end-date must be >= --start-date")
        return args.start_date, args.end_date

    return compute_default_window(args.days, REPORT_TZ)


def main() -> None:
    args = parse_cli_args()

    if args.run_export and args.no_export:
        raise SystemExit("Choose only one of --run-export or --no-export")
    if not args.owner_rollup and not args.brand:
        raise SystemExit("--brand is required unless --owner-rollup is used")

    start_day, end_day = _resolve_dates(args)
    stores = parse_store_codes_arg(args.stores)
    target_margin = args.target_margin
    if target_margin is not None and target_margin > 1:
        target_margin = target_margin / 100.0

    if args.owner_rollup:
        owner_root = Path(args.owner_output_root).resolve() if str(args.owner_output_root or "").strip() else Path(args.output_dir).resolve()
        generate_owner_brand_rollup_packet(
            start_date=start_day,
            end_date=end_day,
            stores=stores,
            top_n=max(1, int(args.top_brands or 20)),
            use_api=bool(args.use_api),
            run_export=bool(args.run_export),
            no_export=bool(args.no_export),
            no_catalog_export=bool(args.no_catalog_export),
            force_refresh=bool(args.force_refresh),
            include_prior_data=not args.no_prior_data,
            include_creditflow=bool(args.owner_creditflow),
            target_margin=target_margin,
            output_root=owner_root,
            email=bool(args.email_results),
            compact=bool(args.compact_pdf_mode),
            include_brand_cards=bool(args.owner_brand_cards),
            api_env_file=str(args.env_file or DEFAULT_API_ENV_FILE),
            api_workers=max(1, int(args.workers or DEFAULT_PACKET_API_WORKERS)),
            credit_ledger_path=str(args.credit_ledger),
            creditflow_base_url=str(args.creditflow_base_url),
            logger=_default_logger,
        )
        return

    packet_layout = str(args.packet_layout or "classic").lower()
    if args.appendix_enabled is None:
        include_product_appendix = bool(not args.no_product_appendix and packet_layout != "dashboard")
    else:
        include_product_appendix = bool(args.appendix_enabled and not args.no_product_appendix)

    options = PacketOptions(
        run_export=bool((args.use_api or args.run_export) and not args.no_export),
        run_catalog_export=bool((not args.no_catalog_export) and ((not args.use_api) or not args.no_export)),
        use_api=bool(args.use_api),
        api_env_file=str(args.env_file or DEFAULT_API_ENV_FILE),
        include_store_sections=not args.no_store_sections,
        include_product_appendix=include_product_appendix,
        include_charts=not args.no_charts,
        include_prior_window_data=not args.no_prior_data,
        include_kickback_adjustments=bool(args.with_kickbacks and not args.no_kickbacks),
        email_results=bool(args.email_results),
        generate_xlsx=bool(args.xlsx),
        top_n=max(5, int(args.top_n)),
        force_refresh_data=bool(args.force_refresh),
        api_workers=max(1, int(args.workers or DEFAULT_PACKET_API_WORKERS)),
        include_credit_reconciliation=bool(args.include_credit_reconciliation),
        credit_ledger_path=str(args.credit_ledger),
        include_creditflow_credits=bool(args.include_creditflow_credits),
        creditflow_base_url=str(args.creditflow_base_url),
        target_margin=target_margin,
        include_monthly_reference=bool(args.include_monthly_reference),
        packet_mode=str(args.packet_mode or "standard"),
        generate_followup_notes=bool(args.generate_followup_notes),
        compact_pdf_mode=bool(args.compact_pdf_mode),
        packet_layout=packet_layout,
        max_products=max(1, int(args.max_products or 20)),
        max_store_products=max(1, int(args.max_store_products or 10)),
    )

    generate_brand_meeting_packet(
        brand=str(args.brand),
        start_day=start_day,
        end_day=end_day,
        selected_store_codes=stores,
        output_root=Path(args.output_dir).resolve(),
        options=options,
        logger=_default_logger,
    )


if __name__ == "__main__":
    main()
