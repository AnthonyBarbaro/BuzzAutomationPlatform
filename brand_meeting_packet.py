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
from collections import Counter
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
from reportlab.lib.styles import ParagraphStyle
from reportlab.lib.pagesizes import letter
from reportlab.lib.units import inch
from reportlab.platypus import (
    CondPageBreak,
    Image,
    KeepTogether,
    PageBreak,
    Paragraph,
    SimpleDocTemplate,
    Spacer,
)

import owner_snapshot as osnap
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
THIS_DIR = Path(__file__).resolve().parent
FILES_DIR = THIS_DIR / "files"

GMAIL_SCOPES = ["https://www.googleapis.com/auth/gmail.send"]
GMAIL_TOKEN = THIS_DIR / "token_gmail.json"
DEFAULT_REPORT_EMAIL = "anthony@buzzcannabis.com"

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


# ---------------------------------------------------------------------------
# Logging
# ---------------------------------------------------------------------------
def _default_logger(msg: str) -> None:
    print(msg)


def _log(msg: str, logger: Optional[Callable[[str], None]]) -> None:
    (logger or _default_logger)(msg)


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
            "avg_cost_per_item", "tickets", "merged_count", "raw_names_top5", "product_list",
        ])

    df = _filter_product_group_rows(df)
    if df.empty:
        return pd.DataFrame(columns=[
            "product_group_key", "product_group_display", "category_normalized", "size_normalized", "variant_type",
            "net_revenue", "units", "profit", "profit_real", "margin", "margin_real", "avg_price_per_item",
            "avg_cost_per_item", "tickets", "merged_count", "raw_names_top5", "product_list",
        ])

    group_col = "product_group_key" if "product_group_key" in df.columns else "merge_key"
    display_col = "product_group_display" if "product_group_display" in df.columns else "display_product"

    grouped = df.groupby(group_col, as_index=False).agg(
        product_group_display=(display_col, lambda s: s.mode().iloc[0] if not s.mode().empty else str(s.iloc[0])),
        category_normalized=("category_normalized", lambda s: s.mode().iloc[0] if not s.mode().empty else str(s.iloc[0])),
        size_normalized=("size_normalized", lambda s: s.mode().iloc[0] if not s.mode().empty else str(s.iloc[0])),
        variant_type=("variant_type", lambda s: s.mode().iloc[0] if not s.mode().empty else str(s.iloc[0])),
        net_revenue=("_net", "sum"),
        units=("_qty", "sum"),
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
        if not names:
            display_name = ""
        elif len(names) == 1:
            display_name = names[0]
        else:
            display_name = f"{names[0]} (+{len(names) - 1} more)"

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
    for c in ["product_group_key", "supply_merge_key", "merge_key"]:
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
    tmp["effective_total"] = tmp["Effective_Price"] * tmp["Available"]
    if "supply_merge_key" not in tmp.columns:
        tmp["supply_merge_key"] = ""
    if "supply_base_key" not in tmp.columns:
        tmp["supply_base_key"] = tmp["supply_merge_key"].map(_supply_base_from_merge_key)

    grouped = tmp.groupby("merge_key", as_index=False).agg(
        display_product=("display_product", lambda s: s.mode().iloc[0] if not s.mode().empty else str(s.iloc[0])),
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
    for mk, part in tmp.groupby("merge_key"):
        cnt = Counter(part["_product_raw"].astype(str).tolist())
        raw_map[mk] = " | ".join([name for name, _n in cnt.most_common(5)])
        merged_count_map[mk] = int(len(cnt))

    grouped["raw_names_top5"] = grouped["merge_key"].map(raw_map).fillna("")
    grouped["merged_count"] = grouped["merge_key"].map(merged_count_map).fillna(1).astype(int)

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
        canvas.drawRightString(letter[0] - doc.rightMargin, 0.30 * inch, f"Page {canvas.getPageNumber()}")
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
        for r in category_60.head(10).itertuples(index=False):
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
) -> None:
    out_xlsx.parent.mkdir(parents=True, exist_ok=True)

    summary_rows: List[Dict[str, Any]] = []
    for key, vals in window_metrics.items():
        row = {"window": key}
        row.update(vals)
        summary_rows.append(row)

    summary_df = pd.DataFrame(summary_rows)
    inv_summary_df = pd.DataFrame([inv_overview])

    with pd.ExcelWriter(out_xlsx, engine="openpyxl") as writer:
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
) -> None:
    service = _build_gmail_service()

    subject = f"Brand Packet — {brand} — {start_day.isoformat()} to {end_day.isoformat()}"

    top_lines: List[str] = []
    for r in top_products.head(3).itertuples(index=False):
        name = str(getattr(r, "product_group_display", getattr(r, "display_product", "")))
        top_lines.append(f"- {name}: {money0(float(r.net_revenue))}")

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

    # Persist cache CSVs for debugging / quick QA
    try:
        p_sales = paths.cache_dir / "sales_brand_rows.csv"
        p_sales_14 = paths.cache_dir / "sales_brand_rows_14d.csv"
        p_sales_30 = paths.cache_dir / "sales_brand_rows_30d.csv"
        p_pg60 = paths.cache_dir / "product_groups_60d.csv"
        p_pg14 = paths.cache_dir / "product_groups_14d.csv"
        p_pg30 = paths.cache_dir / "product_groups_30d.csv"
        p_inv = paths.cache_dir / "inventory_products.csv"

        report_df.to_csv(p_sales, index=False)
        last14_df.to_csv(p_sales_14, index=False)
        last30_df.to_csv(p_sales_30, index=False)
        product_60.to_csv(p_pg60, index=False)
        product_14.to_csv(p_pg14, index=False)
        add_supply_to_product_groups(summarize_product_groups(last30_df), all_pg_units_day_map, all_pg_dos_map).to_csv(p_pg30, index=False)
        inv_products.to_csv(p_inv, index=False)

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
    except Exception:
        pass

    quick_pdf_name = safe_filename(
        f"Brand Packet - {brand} - {start_day.isoformat()}_to_{end_day.isoformat()} - Quick Store Dashboards.pdf"
    )
    out_quick_pdf = paths.pdf_dir / quick_pdf_name

    build_brand_packet_quick_pdf(
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
    )
    _log(f"[PDF] Created (Quick Store Dashboards): {out_quick_pdf}", logger)

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
        )
        _log(f"[XLSX] Created: {out_xlsx}", logger)

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
        )

    _log("Done ✅", logger)

    return PacketArtifacts(
        quick_pdf_path=out_quick_pdf,
        detail_pdf_path=out_quick_pdf,
        pdf_path=out_quick_pdf,
        xlsx_path=out_xlsx,
        run_paths=paths,
        missing_sales_stores=missing_sales_stores,
        missing_catalog_stores=missing_catalog_stores,
    )


# ---------------------------------------------------------------------------
# CLI
# ---------------------------------------------------------------------------
def parse_cli_args() -> argparse.Namespace:
    p = argparse.ArgumentParser(description="Generate a Brand Meeting Packet PDF/XLSX across Buzz stores.")
    p.add_argument("--brand", required=True, help="Brand name (example: 'Cold Fire').")
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

    p.add_argument("--no-charts", action="store_true", help="Skip charts in PDF.")
    p.add_argument("--no-store-sections", action="store_true", help="Skip store-level sections.")
    p.add_argument("--no-product-appendix", action="store_true", help="Skip full product appendix.")
    p.add_argument("--with-kickbacks", action="store_true", help="Enable deal kickback adjustments (default: OFF).")
    p.add_argument("--no-kickbacks", action="store_true", help=argparse.SUPPRESS)
    p.add_argument("--xlsx", action="store_true", help="Also generate XLSX workbook.")
    p.add_argument("--force-refresh", action="store_true", help="Ignore cached inputs for this run and download fresh data.")
    p.add_argument("--no-prior-data", action="store_true", help="Use only the selected report window and disable prior-comparison data.")

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

    start_day, end_day = _resolve_dates(args)
    stores = parse_store_codes_arg(args.stores)

    options = PacketOptions(
        run_export=bool((args.use_api or args.run_export) and not args.no_export),
        run_catalog_export=bool((not args.no_catalog_export) and ((not args.use_api) or not args.no_export)),
        use_api=bool(args.use_api),
        api_env_file=str(args.env_file or DEFAULT_API_ENV_FILE),
        include_store_sections=not args.no_store_sections,
        include_product_appendix=not args.no_product_appendix,
        include_charts=not args.no_charts,
        include_prior_window_data=not args.no_prior_data,
        include_kickback_adjustments=bool(args.with_kickbacks and not args.no_kickbacks),
        email_results=bool(args.email_results),
        generate_xlsx=bool(args.xlsx),
        top_n=max(5, int(args.top_n)),
        force_refresh_data=bool(args.force_refresh),
        api_workers=max(1, int(args.workers or DEFAULT_PACKET_API_WORKERS)),
    )

    generate_brand_meeting_packet(
        brand=args.brand,
        start_day=start_day,
        end_day=end_day,
        selected_store_codes=stores,
        output_root=Path(args.output_dir).resolve(),
        options=options,
        logger=_default_logger,
    )


if __name__ == "__main__":
    main()
