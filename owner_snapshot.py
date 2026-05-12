import re
import shutil
import argparse
from io import BytesIO
from pathlib import Path
from datetime import datetime, timedelta, date
from zoneinfo import ZoneInfo
from typing import Dict, List, Optional, Tuple, Any
import calendar
import json
import numpy as np
import importlib
import pandas as pd
from concurrent.futures import ThreadPoolExecutor, as_completed
from zipfile import is_zipfile

OWNER_EMAILER_IMPORT_ERROR: Optional[Exception] = None
try:
    from owner_emailer import send_owner_snapshot_email
except Exception as exc:
    OWNER_EMAILER_IMPORT_ERROR = exc

    def send_owner_snapshot_email(*args, **kwargs):
        raise RuntimeError(
            "Owner emailer is unavailable in this environment. "
            f"Import error: {OWNER_EMAILER_IMPORT_ERROR}"
        )

# Charts (for PDFs)
import matplotlib
matplotlib.use("Agg")
import matplotlib.pyplot as plt

# PDF rendering
from reportlab.lib import colors
from reportlab.lib.pagesizes import letter
from reportlab.lib.units import inch
from reportlab.lib.styles import getSampleStyleSheet, ParagraphStyle
from reportlab.platypus import (
    SimpleDocTemplate,
    Paragraph,
    Spacer,
    Table,
    TableStyle,
    Image,
    PageBreak,
    CondPageBreak,
    KeepTogether,
    Flowable,
)

# Optional nicer fonts
from reportlab.pdfbase import pdfmetrics
from reportlab.pdfbase.ttfonts import TTFont

from dutchie_api_reports import (
    DEFAULT_ENV_FILE as DUTCHIE_DEFAULT_ENV_FILE,
    STORE_CODES as DUTCHIE_STORE_CODES,
    canonical_env_map as dutchie_canonical_env_map,
    create_session as dutchie_create_session,
    local_date_range_to_utc_strings as dutchie_local_date_range_to_utc_strings,
    parse_store_codes as dutchie_parse_store_codes,
    request_json as dutchie_request_json,
    resolve_integrator_key as dutchie_resolve_integrator_key,
    resolve_store_keys as dutchie_resolve_store_keys,
    resolve_worker_count as dutchie_resolve_worker_count,
)

# --- IMPORTANT: uses your existing exporter when Selenium exports are selected ---
DEFAULT_STORE_ABBR_MAP = {
    "Buzz Cannabis - Mission Valley": "MV",
    "Buzz Cannabis-La Mesa": "LM",
    "Buzz Cannabis - SORRENTO VALLEY": "SV",
    "Buzz Cannabis - Lemon Grove": "LG",
    "Buzz Cannabis (National City)": "NC",
    "Buzz Cannabis Wildomar Palomar": "WP",
}

GET_SALES_REPORT_IMPORT_ERROR: Optional[Exception] = None
try:
    import getSalesReport as gsr
    from getSalesReport import run_sales_report, store_abbr_map  # store_name -> "MV"
except Exception as exc:
    gsr = None
    run_sales_report = None
    store_abbr_map = DEFAULT_STORE_ABBR_MAP.copy()
    GET_SALES_REPORT_IMPORT_ERROR = exc

###############################################################################
# CONFIG (easy to change)
###############################################################################

REPORT_TZ = "America/Los_Angeles"

# Backfill window used only when RUN_EXPORT=True
BACKFILL_DAYS = 61

REPORTS_ROOT = Path("reports").resolve()
RAW_ROOT = REPORTS_ROOT / "raw_sales"
PDF_ROOT = REPORTS_ROOT / "pdf"

# If True: run the selected export source and archive fresh files
# If False: reuse latest RAW folder
RUN_EXPORT = True
SHOW_BOTH_MARGINS = True
# If RUN_EXPORT=True: delete existing /files downloads first?
CLEANUP_FILES_BEFORE_EXPORT = True

# If RUN_EXPORT=True: do you want to "move" files out of /files, or "copy" them?
ARCHIVE_ACTION = "move"  # "move" or "copy"

# Build combined PDF summary as well as per-store PDFs
GENERATE_ALL_STORES_SUMMARY_PDF = True

# Charts / tables
TREND_DAYS = 14
TOP_N = 20
CATEGORY_TOP_N = 10
DAILY_UNITS_ROWS = 10
CHART_DPI = 170
CHART_JPEG_QUALITY = 86

# Cart value distribution buckets (transaction-level net cart total)
CART_VALUE_BUCKETS: List[Dict[str, Any]] = [
    {"label": "$0-$1", "lower": 0.0, "upper": 1.0, "upper_inclusive": False},
    {"label": "$1-$10", "lower": 1.0, "upper": 10.0, "upper_inclusive": False},
    {"label": "$10-$20", "lower": 10.0, "upper": 20.0, "upper_inclusive": False},
    {"label": "$20-$40", "lower": 20.0, "upper": 40.0, "upper_inclusive": False},
    {"label": "$40-$60", "lower": 40.0, "upper": 60.0, "upper_inclusive": False},
    {"label": "$60-$99", "lower": 60.0, "upper": 100.0, "upper_inclusive": False},
    {"label": "$100-$200", "lower": 100.0, "upper": 200.0, "upper_inclusive": True},
    {"label": "$200+", "lower": 200.0, "upper": None, "lower_inclusive": False},
]

# --- Dutchie export header row ---
FORCE_HEADER_ROW = True
EXPORT_HEADER_ROW_INDEX = 4  # Excel row 5

# Discover getSalesReport /files directory
FILES_DIR = (Path(gsr.__file__).resolve().parent if gsr is not None else Path(__file__).resolve().parent) / "files"

# Dutchie API export support
DEFAULT_EXPORT_SOURCE = "api"
DEFAULT_API_ENV_FILE = DUTCHIE_DEFAULT_ENV_FILE
DEFAULT_OWNER_API_WORKERS = 6
API_EXPORT_MAX_WINDOW_DAYS = 31

# Loyalty detail audit appended at the bottom of PDFs and exported as XLSX
LOYALTY_DISCOUNT_MATCH_TEXT = "Loyalty Points Adjustment"
LOYALTY_DETAIL_ROOT = REPORTS_ROOT / "discount_details"
LOYALTY_ADJUSTMENT_ROOT = REPORTS_ROOT / "loyalty_adjustments"
LOYALTY_ADJUSTMENT_REPORT_URL = "https://dusk.backoffice.dutchie.com/reports/marketing/reports/loyalty-adjustment-report"
DISCOUNT_DETAIL_EXPORT_ROOT = REPORTS_ROOT / "discount_detail_exports"
DISCOUNT_DETAIL_REPORT_URL = "https://dusk.backoffice.dutchie.com/reports/marketing/reports/discount-detail-report"
DEFAULT_LOYALTY_ADJUSTMENT_SOURCE = "auto"
DEFAULT_DISCOUNT_DETAIL_SOURCE = "auto"
DEFAULT_LOYALTY_BROWSER_WORKERS = 2
DEFAULT_DISCOUNT_DETAIL_BROWSER_WORKERS = 2
MAX_LOYALTY_BROWSER_WORKERS = 3
MAX_DISCOUNT_DETAIL_BROWSER_WORKERS = 3
LOYALTY_PDF_MAX_ROWS = 60
API_FIELD_UNAVAILABLE = "Not provided by API"
NO_DATA_MARKER_SUFFIX = ".NO_DATA.json"


# -------------------------------------------------------------------
# ✅ DEAL / KICKBACK ADJUSTMENTS (brand-based)
# -------------------------------------------------------------------
APPLY_DEAL_KICKBACKS = True

# Your deals config file (same directory). Must expose: brand_criteria dict
DEALS_MODULE_NAME = "deals"

# If a deal rule doesn't specify kickback, infer from discount:
DEFAULT_KICKBACK_BY_DISCOUNT = {
    0.50: 0.30,  # 50% off => 30% back on cost
    0.40: 0.20,  # 40% off => 20% back on cost
}

# Debug prints per store
DEBUG_DEAL_KICKBACKS = False

###############################################################################
# ✅ MONTH-END FORECAST (self-learning)
###############################################################################

FORECAST_ENABLED = True

FORECAST_DIR = REPORTS_ROOT / "forecast"
FORECAST_HISTORY_PATH = FORECAST_DIR / "daily_history.csv.gz"
FORECAST_MODEL_PATH = FORECAST_DIR / "month_end_forecaster.joblib"
FORECAST_META_PATH = FORECAST_DIR / "month_end_forecaster_meta.json"

# Training / data rules
FORECAST_MIN_ASOF_DAY = 4                  # don’t train/predict on day 1-3 (too noisy)
FORECAST_MONTH_COVERAGE_THRESHOLD = 0.90   # month must have >= 90% days to be "complete"
FORECAST_MIN_COMPLETE_MONTHS = 2           # minimum complete months before ML trains
FORECAST_RETRAIN_EVERY_RUN = True          # simplest "keeps learning" behavior

# Baseline fallback (also used for features)
FORECAST_WEEKDAY_WINDOW_DAYS = 56          # last 8 weeks weekday profile

# If sklearn is available, also train P10/P90 bands for net & profit
FORECAST_USE_QUANTILES = True
###############################################################################
# Column candidates
###############################################################################

COLUMN_CANDIDATES = {
    "date": ["Order Time", "Transaction Date", "Transaction Date (Local)", "Date", "Sold At", "Created At", "Order Date"],
    "transaction_id": ["Order ID", "Transaction ID", "Order Number", "Receipt ID", "Ticket", "Ticket Number", "Sale ID", "Cart ID"],
    "employee": ["Budtender Name", "Budtender", "Employee", "Employee Name", "Cashier"],
    "customer_type": ["Customer Type"],
    "product": ["Product Name", "Product", "Item Name", "Item"],
    "category": ["Major Category", "Category", "Product Category", "Product Category Name"],  # prefer Major Category first
    "quantity": ["Total Inventory Sold", "Quantity", "Qty", "Items", "Item Quantity"],
    "gross_sales": ["Gross Sales", "Gross Revenue", "Subtotal", "Total", "Gross"],
    "net_sales": ["Net Sales", "Net Revenue", "Net Total", "Net", "Net Amount", "Total (Net)"],
    "discount_main": ["Discounted Amount", "Discount Amount", "Discount", "Total Discount"],
    "discount_loyalty": ["Loyalty as Discount"],
    "cogs": ["Inventory Cost", "COGS", "Cost of Goods Sold", "Cost"],
    "profit": ["Order Profit", "Profit", "Gross Profit", "Net Profit"],
    "return_date": ["Return Date"],
    "total_weight_sold": ["Total Weight Sold", "Total Weight", "Weight Sold"],
    "discount_name": ["Discount Name", "Discount Names", "Loyalty Discount Names"],
    "discount_reason": ["Discount Description", "Discount Reason", "Loyalty Discount Reasons"],
    "discount_approved_by": ["Discount Approved By"],
    "points_added_by": ["Points Added By"],
    "loyalty_adjustment_discount": ["Loyalty Points Adjustment Discount"],
    "completed_by": ["Completed By", "Budtender Name", "Budtender", "Employee"],
    "customer_id": ["Customer ID"],
}


###############################################################################
# Brand Theme (your palette)
###############################################################################
THEME = {
    "yellow": colors.HexColor("#FFF200"),
    "green": colors.HexColor("#00AE6F"),
    "black": colors.HexColor("#000000"),
    "muted": colors.HexColor("#374151"),
    "light_bg": colors.HexColor("#F7F7F7"),
    "border": colors.HexColor("#E5E7EB"),
    "row_alt": colors.HexColor("#FAFAFA"),
    "soft_gray": colors.HexColor("#F3F4F6"),
}

# Compact layout
PAGE_MARGINS = {
    "left": 0.45 * inch,
    "right": 0.45 * inch,
    "top": 0.42 * inch,
    "bottom": 0.42 * inch,
}
SPACER = {"xs": 0.04 * inch, "sm": 0.07 * inch, "md": 0.10 * inch}

# Chart color hex
HEX_GREEN = "#00AE6F"
HEX_YELLOW = "#FFF200"
HEX_BLACK = "#000000"
HEX_GRAY_SHADOW = "#9CA3AF"


###############################################################################
# Font setup (nicer font if available)
###############################################################################

BASE_FONT = "Helvetica"
BASE_FONT_BOLD = "Helvetica-Bold"
USE_UNICODE_ARROWS = False

def _try_register_font(name: str, path: str) -> bool:
    try:
        p = Path(path)
        if p.exists():
            pdfmetrics.registerFont(TTFont(name, str(p)))
            return True
    except Exception:
        return False
    return False

def setup_fonts() -> None:
    """Try to use DejaVuSans (nice, readable, supports more glyphs)."""
    global BASE_FONT, BASE_FONT_BOLD, USE_UNICODE_ARROWS

    regular_candidates = [
        "/usr/share/fonts/truetype/dejavu/DejaVuSans.ttf",
        "/usr/share/fonts/TTF/DejaVuSans.ttf",
    ]
    bold_candidates = [
        "/usr/share/fonts/truetype/dejavu/DejaVuSans-Bold.ttf",
        "/usr/share/fonts/TTF/DejaVuSans-Bold.ttf",
    ]

    reg_ok = False
    bold_ok = False

    for p in regular_candidates:
        if _try_register_font("BuzzSans", p):
            reg_ok = True
            break
    for p in bold_candidates:
        if _try_register_font("BuzzSans-Bold", p):
            bold_ok = True
            break

    if reg_ok and bold_ok:
        BASE_FONT = "BuzzSans"
        BASE_FONT_BOLD = "BuzzSans-Bold"
        USE_UNICODE_ARROWS = True
    else:
        BASE_FONT = "Helvetica"
        BASE_FONT_BOLD = "Helvetica-Bold"
        USE_UNICODE_ARROWS = False


###############################################################################
# Formatting helpers
###############################################################################
def pctN(x: float, n: int = 1) -> str:
    try:
        return f"{x*100:,.{n}f}%"
    except Exception:
        return f"{0:.{n}f}%"

def fmt_margin_display(kb_margin: float, real_margin: float, *, compact: bool = False, decimals: int = 1) -> str:
    """
    kb_margin   = kickback-adjusted margin (includes kickback effect)
    real_margin = real margin (no kickback)

    compact=True => no spaces "52.3%/40.1%"
    compact=False => "52.3% / 40.1%"
    decimals controls % precision.
    """
    if not SHOW_BOTH_MARGINS:
        return pctN(kb_margin, decimals)

    sep = "/" if compact else " / "
    return f"{pctN(kb_margin, decimals)}{sep}{pctN(real_margin, decimals)}"
def delta_html_pp_pair(current_kb: float, baseline_kb: float, current_real: float, baseline_real: float, label: str) -> str:
    """
    Two-line delta for margins:
      line1 = KB delta
      line2 = Real delta
    """
    if not SHOW_BOTH_MARGINS:
        return delta_html_pp(current_kb, baseline_kb, label)

    line1 = delta_html_pp(current_kb, baseline_kb, f"{label} (KB)")
    line2 = delta_html_pp(current_real, baseline_real, f"{label} (Real)")
    return f"{line1}<br/>{line2}"
def money(x: float) -> str:
    try:
        return f"${x:,.0f}"
    except Exception:
        return "$0"

def money_compact(x: float) -> str:
    try:
        v = float(x)
    except Exception:
        return "$0"
    sign = "-" if v < 0 else ""
    av = abs(v)
    if av >= 1_000_000:
        return f"{sign}${av / 1_000_000:.2f}M"
    if av >= 1_000:
        return f"{sign}${av / 1_000:.1f}k"
    return f"{sign}${av:,.0f}"

def money2(x: float) -> str:
    try:
        return f"${x:,.2f}"
    except Exception:
        return "$0.00"

def pct1(x: float) -> str:
    try:
        return f"{x*100:,.1f}%"
    except Exception:
        return "0.0%"

def pp1(x: float) -> str:
    try:
        return f"{x*100:,.1f}pp"
    except Exception:
        return "0.0pp"

def fmt_signed_money(x: float) -> str:
    sign = "+" if x >= 0 else "-"
    return f"{sign}${abs(x):,.0f}"

def fmt_signed_int(x: float) -> str:
    sign = "+" if x >= 0 else "-"
    return f"{sign}{int(abs(x)):,}"

def safe_filename(s: str) -> str:
    s = s.strip()
    s = re.sub(r"\s+", " ", s)
    s = re.sub(r"[^a-zA-Z0-9 _\-\(\)\.]", "_", s)
    return s

def store_label(store_name: str) -> str:
    label = store_name.replace("Buzz Cannabis", "").strip()
    label = label.replace("(", "").replace(")", "")
    label = re.sub(r"^\-+", "", label).strip()
    return (label or store_name).upper()

def to_number(series: pd.Series) -> pd.Series:
    if series is None:
        return series
    if pd.api.types.is_numeric_dtype(series):
        return series.astype(float)

    s = series.astype(str)
    s = s.str.replace("$", "", regex=False).str.replace(",", "", regex=False)
    s = s.str.replace("(", "-", regex=False).str.replace(")", "", regex=False)
    s = s.replace({"nan": None, "None": None, "": None})
    return pd.to_numeric(s, errors="coerce")

def find_col(df: pd.DataFrame, candidates: List[str]) -> Optional[str]:
    cols = {str(c).strip().lower(): c for c in df.columns}
    for cand in candidates:
        key = cand.strip().lower()
        if key in cols:
            return cols[key]
    return None

def dow_short(d: date) -> str:
    return d.strftime("%a")  # Sun, Mon...

def fmt_hour_ampm(h: int) -> str:
    """0..23 -> '12a', '1a', ..., '12p', '1p'"""
    h = int(h)
    if h == 0:
        return "12a"
    if 1 <= h <= 11:
        return f"{h}a"
    if h == 12:
        return "12p"
    return f"{h-12}p"

def parse_brand_from_product(product_name: Any) -> str:
    """
    Brand is the part before the first '|'
    Example: "Cold Fire | Cart 1g Pineapple" -> "Cold Fire"
             "Dab Daddy | Flower 14g | | LA Pop Rocks" -> "Dab Daddy"
    """
    s = str(product_name or "").strip()
    if not s:
        return "Unknown"
    parts = [p.strip() for p in s.split("|")]
    for p in parts:
        if p:
            return p
    return "Unknown"


###############################################################################
# ✅ Deals (brand-based) integration
###############################################################################

_DEALS_MOD = None

def _canon(s: Any) -> str:
    """Canonical compare key for brand/category strings."""
    return re.sub(r"[^a-z0-9]+", "", str(s or "").lower())

def _load_deals_module():
    global _DEALS_MOD
    if not APPLY_DEAL_KICKBACKS:
        return None
    if _DEALS_MOD is not None:
        return _DEALS_MOD
    try:
        _DEALS_MOD = importlib.import_module(DEALS_MODULE_NAME)
        return _DEALS_MOD
    except Exception as e:
        print(f"[WARN] Could not import {DEALS_MODULE_NAME}.py; skipping deal kickbacks. Error: {e}")
        _DEALS_MOD = None
        return None

def _normalize_rules(criteria: Any, default_stores: List[str]) -> List[Dict[str, Any]]:
    """
    Same schema as your deals script supports:
      - dict with base keys (+ optional rules list)
      - list of rules (no base)
    """
    if isinstance(criteria, list):
        base = {}
        rules = criteria
    else:
        base = dict(criteria or {})
        rules = base.pop("rules", None) or [{}]

    out = []
    for i, r in enumerate(rules, 1):
        effective = dict(base)
        effective.update(r or {})
        effective.setdefault("rule_name", f"Rule {i}")
        effective.setdefault("stores", base.get("stores", default_stores))
        # Keep the rest (days/categories/include/exclude/brands/discount/kickback)
        out.append(effective)
    return out

def _kickback_pct_from_rule(rule: Dict[str, Any]) -> float:
    """
    Priority:
      1) Use explicit rule['kickback'] if present (even if 0.0)
      2) Else infer from rule['discount'] via DEFAULT_KICKBACK_BY_DISCOUNT
    """
    if rule is None:
        return 0.0

    if "kickback" in rule and rule["kickback"] is not None:
        try:
            return float(rule["kickback"])
        except Exception:
            return 0.0

    # infer
    try:
        d = float(rule.get("discount", 0.0) or 0.0)
    except Exception:
        d = 0.0
    d = round(d, 2)
    return float(DEFAULT_KICKBACK_BY_DISCOUNT.get(d, 0.0))

def _discount_from_rule(rule: Dict[str, Any]) -> float:
    try:
        return float(rule.get("discount", 0.0) or 0.0)
    except Exception:
        return 0.0

def enrich_with_deal_kickbacks_by_brand(df: pd.DataFrame, store_code: str) -> pd.DataFrame:
    """
    Adds:
      _deal_kickback_pct, _deal_kickback_amt
      _cogs_raw, _cogs_adj
      _profit_adj  (profit_base + kickback_amt)
      _deal_brand, _deal_rule, _deal_discount

    Matching:
      - brand parsed from product name (before '|')
      - rule days/categories/include/excluded respected
      - vendor ignored entirely
    """
    deals_mod = _load_deals_module()
    if deals_mod is None:
        return df

    brand_criteria = getattr(deals_mod, "brand_criteria", None)
    if not isinstance(brand_criteria, dict) or not brand_criteria:
        print("[WARN] deals.py does not expose brand_criteria dict; skipping deal kickbacks.")
        return df

    prod_col = find_col(df, COLUMN_CANDIDATES["product"])
    date_col = find_col(df, COLUMN_CANDIDATES["date"])
    cat_col = find_col(df, COLUMN_CANDIDATES["category"])
    cogs_col = find_col(df, COLUMN_CANDIDATES["cogs"])
    net_col = find_col(df, COLUMN_CANDIDATES["net_sales"])
    profit_col = find_col(df, COLUMN_CANDIDATES["profit"])

    if not prod_col or not date_col or not cogs_col or not net_col:
        return df

    out = df.copy()

    # Core series
    dt = pd.to_datetime(out[date_col], errors="coerce")
    day_series = dt.dt.strftime("%A").fillna("")

    prod_series = out[prod_col].fillna("").astype(str)
    prod_lower = prod_series.str.lower()

    brand_series = prod_series.apply(parse_brand_from_product)
    brand_key = brand_series.apply(_canon)

    cat_series = out[cat_col].fillna("").astype(str) if cat_col else pd.Series("", index=out.index)
    cat_key = cat_series.apply(_canon)

    cogs_raw = to_number(out[cogs_col]).fillna(0.0).astype(float)
    net_sales = to_number(out[net_col]).fillna(0.0).astype(float)

    if profit_col:
        profit_base = to_number(out[profit_col]).fillna(0.0).astype(float)
    else:
        profit_base = (net_sales - cogs_raw).astype(float)

    # Defaults
    kickback_pct = pd.Series(0.0, index=out.index, dtype="float")
    deal_brand = pd.Series("", index=out.index, dtype="object")
    deal_rule = pd.Series("", index=out.index, dtype="object")
    deal_discount = pd.Series(0.0, index=out.index, dtype="float")

    default_stores = ["MV", "LM", "SV", "LG", "NC", "WP"]

    # Apply all rules: keep the highest kickback_pct if overlaps
    for brand_name, criteria in brand_criteria.items():
        rules = _normalize_rules(criteria, default_stores=default_stores)

        for rule in rules:
            allowed = set(rule.get("stores", default_stores) or default_stores)
            if store_code not in allowed:
                continue

            # Days
            days = rule.get("days") or []
            mask = pd.Series(True, index=out.index)
            if days:
                mask &= day_series.isin(days)

            # Categories
            categories = rule.get("categories") or []
            if categories:
                cat_allowed = set(_canon(c) for c in categories)
                mask &= cat_key.isin(cat_allowed)

            # Brand match (primary): parsed brand equality against rule brands
            rule_brands = rule.get("brands") or []
            if not rule_brands:
                # fallback: use the dict key name as brand if rule didn't specify
                rule_brands = [str(brand_name)]

            rule_brand_keys = set()
            for b in rule_brands:
                # if they wrote "Made |" etc, parse brand portion too
                rule_brand_keys.add(_canon(parse_brand_from_product(b)))

            mask_brand = brand_key.isin(rule_brand_keys)

            # Fallback brand match: substring in full product name (covers weird formatting)
            if not mask_brand.any():
                # build a contains mask from rule brand raw tokens
                token_mask = pd.Series(False, index=out.index)
                for b in rule_brands:
                    b2 = str(b or "").strip()
                    if not b2:
                        continue
                    token_mask |= prod_lower.str.contains(re.escape(b2.lower()), na=False)
                mask_brand = token_mask

            mask &= mask_brand

            # include_phrases / excluded_phrases
            include_phrases = rule.get("include_phrases") or []
            if include_phrases:
                inc = pd.Series(False, index=out.index)
                for p in include_phrases:
                    p2 = str(p or "").strip()
                    if not p2:
                        continue
                    inc |= prod_lower.str.contains(re.escape(p2.lower()), na=False)
                mask &= inc

            excluded_phrases = rule.get("excluded_phrases") or []
            if excluded_phrases:
                exc = pd.Series(False, index=out.index)
                for p in excluded_phrases:
                    p2 = str(p or "").strip()
                    if not p2:
                        continue
                    exc |= prod_lower.str.contains(re.escape(p2.lower()), na=False)
                mask &= ~exc

            if not mask.any():
                continue

            k = _kickback_pct_from_rule(rule)
            if k <= 0:
                # Even if it matches, no kickback effect -> ignore for margin adjustments
                continue

            idx = mask[mask].index
            override = k > kickback_pct.loc[idx]
            if not override.any():
                continue

            idx2 = override[override].index
            kickback_pct.loc[idx2] = float(k)
            deal_brand.loc[idx2] = str(brand_name)
            deal_rule.loc[idx2] = str(rule.get("rule_name", brand_name))
            deal_discount.loc[idx2] = float(_discount_from_rule(rule))

    out["_deal_kickback_pct"] = kickback_pct
    out["_deal_kickback_amt"] = (cogs_raw * kickback_pct).astype(float)

    out["_cogs_raw"] = cogs_raw
    out["_cogs_adj"] = (cogs_raw - out["_deal_kickback_amt"]).astype(float)

    # ✅ keep Dutchie profit if present, then add kickback back in
    out["_profit_adj"] = (profit_base + out["_deal_kickback_amt"]).astype(float)

    out["_deal_brand"] = deal_brand
    out["_deal_rule"] = deal_rule
    out["_deal_discount"] = deal_discount

    if DEBUG_DEAL_KICKBACKS:
        rows = int((out["_deal_kickback_pct"] > 0).sum())
        tot = float(out["_deal_kickback_amt"].sum())
        print(f"[DEALS] {store_code}: kickback rows={rows:,}, total kickback=${tot:,.2f}")

    return out

###############################################################################
# ✅ Month-End Forecasting Engine (Self-learning)
###############################################################################

def _ensure_forecast_dir() -> None:
    FORECAST_DIR.mkdir(parents=True, exist_ok=True)

def _last_day_of_month(d: date) -> date:
    _, n = calendar.monthrange(d.year, d.month)
    return date(d.year, d.month, n)

def _normalize_dt(s: pd.Series) -> pd.Series:
    # store dates as midnight timestamps for stable grouping/joins
    return pd.to_datetime(s, errors="coerce").dt.normalize()

def _history_keep_cols() -> List[str]:
    # Keep a rich daily feature set so the model can learn “factors”
    # (discounting, tickets, margin, basket, etc.)
    return [
        "date",
        "net_revenue",
        "gross_sales",
        "tickets",
        "items",
        "discount",
        "discount_main",
        "loyalty_discount",
        "discount_rate",
        "basket",
        "items_per_ticket",
        "net_price_per_item",
        "profit",
        "profit_real",
        "margin",
        "margin_real",
        "cogs",
        "cogs_real",
        "returns_net",
        "returns_tickets",
        "weight_sold",
    ]

def _load_history() -> pd.DataFrame:
    _ensure_forecast_dir()
    if not FORECAST_HISTORY_PATH.exists():
        return pd.DataFrame(columns=["store_code"] + _history_keep_cols())

    try:
        df = pd.read_csv(FORECAST_HISTORY_PATH, compression="gzip")
        if "date" in df.columns:
            df["date"] = _normalize_dt(df["date"])
        return df
    except Exception as e:
        print(f"[FORECAST] WARN: Could not load history file: {e}")
        return pd.DataFrame(columns=["store_code"] + _history_keep_cols())

def _save_history(df: pd.DataFrame) -> None:
    _ensure_forecast_dir()
    try:
        df2 = df.copy()
        df2.to_csv(FORECAST_HISTORY_PATH, index=False, compression="gzip")
    except Exception as e:
        print(f"[FORECAST] WARN: Could not save history file: {e}")

def _daily_to_history_rows(store_code: str, daily_df: pd.DataFrame) -> pd.DataFrame:
    if daily_df is None or daily_df.empty:
        return pd.DataFrame(columns=["store_code"] + _history_keep_cols())

    keep = _history_keep_cols()
    out = daily_df.copy()
    if "date" not in out.columns:
        return pd.DataFrame(columns=["store_code"] + keep)

    for c in keep:
        if c not in out.columns:
            out[c] = 0.0

    out = out[keep].copy()
    out["date"] = _normalize_dt(out["date"])
    out.insert(0, "store_code", store_code)
    return out

def _aggregate_all_stores_daily(store_daily_map: Dict[str, pd.DataFrame]) -> pd.DataFrame:
    frames = []
    for abbr, d in (store_daily_map or {}).items():
        if d is None or d.empty:
            continue
        frames.append(d.copy())

    if not frames:
        return pd.DataFrame(columns=_history_keep_cols())

    big = pd.concat(frames, ignore_index=True)
    big["date"] = _normalize_dt(big["date"])

    # Sum numeric columns; then recompute ratio metrics from totals
    num_cols = [c for c in _history_keep_cols() if c != "date"]
    agg = big.groupby("date", as_index=False)[num_cols].sum(numeric_only=True)

    # Recompute derived fields (avoid summing ratios)
    agg["basket"] = agg["net_revenue"] / agg["tickets"].replace({0: np.nan})
    agg["items_per_ticket"] = agg["items"] / agg["tickets"].replace({0: np.nan})
    agg["net_price_per_item"] = agg["net_revenue"] / agg["items"].replace({0: np.nan})
    agg["margin"] = agg["profit"] / agg["net_revenue"].replace({0: np.nan})
    agg["margin_real"] = agg["profit_real"] / agg["net_revenue"].replace({0: np.nan})

    # discount_rate: prefer gross
    approx_g = (agg["net_revenue"] + agg["discount"]).replace({0: np.nan})
    agg["discount_rate"] = np.where(
        agg["gross_sales"] > 0,
        agg["discount"] / agg["gross_sales"].replace({0: np.nan}),
        agg["discount"] / approx_g,
    )

    agg = agg.fillna(0.0)
    return agg[_history_keep_cols()].copy()

def forecast_upsert_history(store_daily_map: Dict[str, pd.DataFrame]) -> pd.DataFrame:
    """
    Appends current run daily data into the long-term history and dedupes.
    Also writes an ALL store_code row per day so the model can learn ALL STORES directly.
    Returns the updated history DF.
    """
    hist = _load_history()

    new_rows = []
    for abbr, daily in (store_daily_map or {}).items():
        if daily is None or daily.empty:
            continue
        new_rows.append(_daily_to_history_rows(abbr, daily))

    # Add ALL STORES aggregate rows
    all_daily = _aggregate_all_stores_daily(store_daily_map)
    if all_daily is not None and not all_daily.empty:
        new_rows.append(_daily_to_history_rows("ALL", all_daily))

    if not new_rows:
        return hist

    add = pd.concat(new_rows, ignore_index=True)
    add["date"] = _normalize_dt(add["date"])

    # Coerce numeric
    for c in _history_keep_cols():
        if c == "date":
            continue
        add[c] = pd.to_numeric(add[c], errors="coerce").fillna(0.0)

    combined = pd.concat([hist, add], ignore_index=True)
    combined["date"] = _normalize_dt(combined["date"])
    combined["store_code"] = combined["store_code"].fillna("").astype(str)

    # Dedupe: keep latest row per store/date
    combined = combined.sort_values(["store_code", "date"])
    combined = combined.drop_duplicates(subset=["store_code", "date"], keep="last").reset_index(drop=True)

    _save_history(combined)
    return combined

def _slope(values: List[float]) -> float:
    # simple slope estimate without sklearn
    n = len(values)
    if n < 2:
        return 0.0
    x = np.arange(n, dtype=float)
    y = np.array(values, dtype=float)
    x = x - x.mean()
    y = y - y.mean()
    denom = float((x * x).sum())
    if denom == 0:
        return 0.0
    return float((x * y).sum() / denom)

def _weekday_counts(start_d: date, end_d: date) -> Dict[str, int]:
    # counts weekdays in inclusive range
    if end_d < start_d:
        return {f"wd_{i}": 0 for i in range(7)}

    cur = start_d
    out = {f"wd_{i}": 0 for i in range(7)}
    while cur <= end_d:
        out[f"wd_{cur.weekday()}"] += 1
        cur += timedelta(days=1)
    return out

def _build_asof_features(hist: pd.DataFrame, store_code: str, as_of: date) -> Dict[str, Any]:
    """
    Build a single feature row for a given store + "as-of" date.
    This is what the model learns from over time.
    """
    if hist is None or hist.empty:
        hist = pd.DataFrame(columns=["store_code"] + _history_keep_cols())

    as_of_ts = pd.Timestamp(as_of)
    store = str(store_code)

    # Pull store history up to as_of
    h = hist[(hist["store_code"] == store) & (hist["date"] <= as_of_ts)].copy()
    h = h.sort_values("date")

    # Month slice (MTD)
    mtd_start = pd.Timestamp(date(as_of.year, as_of.month, 1))
    mtd = h[h["date"] >= mtd_start].copy()

    # last X windows (trend/pace signals)
    lb7_start = as_of_ts - pd.Timedelta(days=6)
    lb14_start = as_of_ts - pd.Timedelta(days=13)
    lbW_start = as_of_ts - pd.Timedelta(days=FORECAST_WEEKDAY_WINDOW_DAYS - 1)

    last7 = h[h["date"] >= lb7_start]
    last14 = h[h["date"] >= lb14_start]
    winW = h[h["date"] >= lbW_start]

    # Month context
    last_dom = _last_day_of_month(as_of)
    days_in_month = last_dom.day
    day_of_month = as_of.day
    remaining_days = max((last_dom - as_of).days, 0)

    # Previous month totals (seasonal baseline)
    if as_of.month == 1:
        prev_y, prev_m = as_of.year - 1, 12
    else:
        prev_y, prev_m = as_of.year, as_of.month - 1
    prev_start = pd.Timestamp(date(prev_y, prev_m, 1))
    prev_end = pd.Timestamp(_last_day_of_month(date(prev_y, prev_m, 1)))

    prev_month = hist[(hist["store_code"] == store) & (hist["date"] >= prev_start) & (hist["date"] <= prev_end)]
    prev_net = float(prev_month["net_revenue"].sum()) if not prev_month.empty else 0.0
    prev_profit = float(prev_month["profit"].sum()) if not prev_month.empty else 0.0
    prev_tickets = float(prev_month["tickets"].sum()) if not prev_month.empty else 0.0

    # MTD sums
    mtd_net = float(mtd["net_revenue"].sum()) if not mtd.empty else 0.0
    mtd_profit = float(mtd["profit"].sum()) if not mtd.empty else 0.0
    mtd_tickets = float(mtd["tickets"].sum()) if not mtd.empty else 0.0
    mtd_discount = float(mtd["discount"].sum()) if not mtd.empty else 0.0
    mtd_gross = float(mtd["gross_sales"].sum()) if not mtd.empty else 0.0

    mtd_margin = (mtd_profit / mtd_net) if mtd_net else 0.0
    mtd_basket = (mtd_net / mtd_tickets) if mtd_tickets else 0.0
    mtd_disc_rate = (mtd_discount / mtd_gross) if mtd_gross else ((mtd_discount / (mtd_net + mtd_discount)) if (mtd_net + mtd_discount) else 0.0)

    # last7/14 sums
    last7_net = float(last7["net_revenue"].sum()) if not last7.empty else 0.0
    last7_profit = float(last7["profit"].sum()) if not last7.empty else 0.0
    last7_tickets = float(last7["tickets"].sum()) if not last7.empty else 0.0
    last7_disc = float(last7["discount"].sum()) if not last7.empty else 0.0

    last14_net = float(last14["net_revenue"].sum()) if not last14.empty else 0.0
    last14_profit = float(last14["profit"].sum()) if not last14.empty else 0.0
    last14_tickets = float(last14["tickets"].sum()) if not last14.empty else 0.0

    # trend slope (pace)
    last7_daily = last7.sort_values("date")["net_revenue"].astype(float).tolist() if not last7.empty else []
    net_slope_7 = _slope(last7_daily)

    # Weekday profile (baseline)
    weekday_avg_net = {i: 0.0 for i in range(7)}
    weekday_avg_profit = {i: 0.0 for i in range(7)}
    if winW is not None and not winW.empty:
        tmp = winW.copy()
        tmp["wd"] = tmp["date"].dt.weekday
        g = tmp.groupby("wd").agg(
            net=("net_revenue", "mean"),
            profit=("profit", "mean"),
        )
        for i in range(7):
            if i in g.index:
                weekday_avg_net[i] = float(g.loc[i, "net"])
                weekday_avg_profit[i] = float(g.loc[i, "profit"])

    # Remaining weekday counts (calendar factor)
    rem_counts = _weekday_counts(as_of + timedelta(days=1), last_dom)

    feats = {
        "store_code": store,
        "year": int(as_of.year),
        "month": int(as_of.month),
        "dow": int(as_of.weekday()),
        "day_of_month": int(day_of_month),
        "days_in_month": int(days_in_month),
        "pct_elapsed": float(day_of_month / days_in_month) if days_in_month else 0.0,
        "remaining_days": int(remaining_days),

        "mtd_net": mtd_net,
        "mtd_profit": mtd_profit,
        "mtd_tickets": mtd_tickets,
        "mtd_margin": mtd_margin,
        "mtd_basket": mtd_basket,
        "mtd_discount": mtd_discount,
        "mtd_discount_rate": mtd_disc_rate,

        "last7_net": last7_net,
        "last7_profit": last7_profit,
        "last7_tickets": last7_tickets,
        "last7_discount": last7_disc,

        "last14_net": last14_net,
        "last14_profit": last14_profit,
        "last14_tickets": last14_tickets,

        "net_slope_7": net_slope_7,

        "prev_month_net": prev_net,
        "prev_month_profit": prev_profit,
        "prev_month_tickets": prev_tickets,
    }

    # Add weekday remaining counts
    feats.update(rem_counts)

    # Add weekday profile features (what’s a “typical” Mon/Tue/etc)
    for i in range(7):
        feats[f"wd_avg_net_{i}"] = float(weekday_avg_net[i])
        feats[f"wd_avg_profit_{i}"] = float(weekday_avg_profit[i])

    return feats

def _complete_month_groups(hist: pd.DataFrame) -> List[Tuple[str, pd.Period, pd.DataFrame]]:
    """
    Returns list of (store_code, month_period, df_month) for months with enough coverage.
    """
    if hist is None or hist.empty:
        return []

    df = hist.copy()
    df = df[df["store_code"].astype(str).str.len() > 0].copy()
    df["date"] = _normalize_dt(df["date"])
    df["ym"] = df["date"].dt.to_period("M")

    out = []
    for (store, ym), g in df.groupby(["store_code", "ym"]):
        g = g.sort_values("date")
        if g.empty:
            continue

        # month coverage
        days_in_month = int(g["date"].dt.daysinmonth.iloc[0])
        coverage = (g["date"].nunique() / float(days_in_month)) if days_in_month else 0.0
        if coverage < FORECAST_MONTH_COVERAGE_THRESHOLD:
            continue

        out.append((str(store), ym, g))

    return out

def _build_training_data(hist: pd.DataFrame) -> Tuple[pd.DataFrame, Dict[str, pd.Series], Dict[str, Any]]:
    """
    Builds a supervised dataset:
      X = as-of features inside complete historical months
      y = month-end totals (net, profit, tickets, discount)

    Returns:
      X_df,
      y_dict (target_name -> Series),
      meta dict
    """
    groups = _complete_month_groups(hist)
    if not groups:
        return pd.DataFrame(), {}, {"n_complete_months": 0, "n_samples": 0}

    # Month-end targets per (store, ym)
    month_targets = {}
    for store, ym, g in groups:
        month_targets[(store, ym)] = {
            "y_net": float(g["net_revenue"].sum()),
            "y_profit": float(g["profit"].sum()),
            "y_tickets": float(g["tickets"].sum()),
            "y_discount": float(g["discount"].sum()),
        }

    X_rows = []
    y_net = []
    y_profit = []
    y_tickets = []
    y_discount = []

    for store, ym, g in groups:
        target = month_targets[(store, ym)]
        # build as-of samples inside that month
        dates = g["date"].dt.date.tolist()

        for d in dates:
            if d.day < FORECAST_MIN_ASOF_DAY:
                continue
            # don’t create sample on the final day (no forecasting needed)
            if d == _last_day_of_month(d):
                continue

            feats = _build_asof_features(hist, store, d)
            X_rows.append(feats)
            y_net.append(target["y_net"])
            y_profit.append(target["y_profit"])
            y_tickets.append(target["y_tickets"])
            y_discount.append(target["y_discount"])

    X_df = pd.DataFrame(X_rows)
    y_dict = {
        "net": pd.Series(y_net, name="y_net"),
        "profit": pd.Series(y_profit, name="y_profit"),
        "tickets": pd.Series(y_tickets, name="y_tickets"),
        "discount": pd.Series(y_discount, name="y_discount"),
    }

    meta = {
        "n_complete_months": len({(s, m) for s, m, _ in groups}),
        "n_samples": len(X_df),
    }
    return X_df, y_dict, meta

def _try_import_sklearn():
    try:
        import joblib
        from sklearn.compose import ColumnTransformer
        from sklearn.pipeline import Pipeline
        from sklearn.preprocessing import OneHotEncoder
        from sklearn.impute import SimpleImputer
        from sklearn.ensemble import HistGradientBoostingRegressor, GradientBoostingRegressor
        return {
            "ok": True,
            "joblib": joblib,
            "ColumnTransformer": ColumnTransformer,
            "Pipeline": Pipeline,
            "OneHotEncoder": OneHotEncoder,
            "SimpleImputer": SimpleImputer,
            "HistGradientBoostingRegressor": HistGradientBoostingRegressor,
            "GradientBoostingRegressor": GradientBoostingRegressor,
        }
    except Exception:
        return {"ok": False}

def _weekday_profile_baseline(hist: pd.DataFrame, store_code: str, as_of: date) -> Dict[str, float]:
    """
    Data-driven projection anchored to available daily data.
    Prioritizes MTD weekday behavior, then recent trend, then wider weekday profile.
    """
    as_of_ts = pd.Timestamp(as_of)
    last_dom = _last_day_of_month(as_of)
    store = str(store_code)
    days_in_month = last_dom.day
    days_elapsed = max(as_of.day, 1)

    h = hist[(hist["store_code"] == store) & (hist["date"] <= as_of_ts)].copy().sort_values("date")
    mtd_start = pd.Timestamp(date(as_of.year, as_of.month, 1))
    mtd = h[h["date"] >= mtd_start].copy()

    mtd_net = float(mtd["net_revenue"].sum()) if not mtd.empty else 0.0
    mtd_profit = float(mtd["profit"].sum()) if not mtd.empty else 0.0
    mtd_tickets = float(mtd["tickets"].sum()) if not mtd.empty else 0.0
    mtd_discount = float(mtd["discount"].sum()) if not mtd.empty else 0.0

    if h.empty:
        return {
            "net_pred": mtd_net,
            "profit_pred": mtd_profit,
            "tickets_pred": mtd_tickets,
            "discount_pred": mtd_discount,
        }

    recent = h[h["date"] >= (as_of_ts - pd.Timedelta(days=13))].copy()
    profile = h[h["date"] >= (as_of_ts - pd.Timedelta(days=FORECAST_WEEKDAY_WINDOW_DAYS - 1))].copy()

    def _weekday_means(df: pd.DataFrame) -> pd.DataFrame:
        if df is None or df.empty:
            return pd.DataFrame()
        tmp = df.copy()
        tmp["wd"] = tmp["date"].dt.weekday
        return tmp.groupby("wd").agg(
            net=("net_revenue", "mean"),
            profit=("profit", "mean"),
            tickets=("tickets", "mean"),
            discount=("discount", "mean"),
        )

    wd_mtd = _weekday_means(mtd)
    wd_recent = _weekday_means(recent)
    wd_profile = _weekday_means(profile)

    mtd_obs_days = int(mtd["date"].nunique()) if not mtd.empty else 0
    recent_obs_days = int(recent["date"].nunique()) if not recent.empty else 0
    profile_obs_days = int(profile["date"].nunique()) if not profile.empty else 0

    mtd_avg = {
        "net": (mtd_net / mtd_obs_days) if mtd_obs_days else 0.0,
        "profit": (mtd_profit / mtd_obs_days) if mtd_obs_days else 0.0,
        "tickets": (mtd_tickets / mtd_obs_days) if mtd_obs_days else 0.0,
        "discount": (mtd_discount / mtd_obs_days) if mtd_obs_days else 0.0,
    }
    recent_avg = {
        "net": (float(recent["net_revenue"].sum()) / recent_obs_days) if recent_obs_days else 0.0,
        "profit": (float(recent["profit"].sum()) / recent_obs_days) if recent_obs_days else 0.0,
        "tickets": (float(recent["tickets"].sum()) / recent_obs_days) if recent_obs_days else 0.0,
        "discount": (float(recent["discount"].sum()) / recent_obs_days) if recent_obs_days else 0.0,
    }
    profile_avg = {
        "net": (float(profile["net_revenue"].sum()) / profile_obs_days) if profile_obs_days else 0.0,
        "profit": (float(profile["profit"].sum()) / profile_obs_days) if profile_obs_days else 0.0,
        "tickets": (float(profile["tickets"].sum()) / profile_obs_days) if profile_obs_days else 0.0,
        "discount": (float(profile["discount"].sum()) / profile_obs_days) if profile_obs_days else 0.0,
    }

    def _pick_metric_for_weekday(metric_key: str, wd: int) -> float:
        vals: List[Tuple[float, float]] = []
        if wd in wd_mtd.index:
            w = 0.58 if mtd_obs_days >= 7 else 0.40
            vals.append((float(wd_mtd.loc[wd, metric_key]), w))
        if wd in wd_recent.index:
            vals.append((float(wd_recent.loc[wd, metric_key]), 0.24))
        if wd in wd_profile.index:
            vals.append((float(wd_profile.loc[wd, metric_key]), 0.18))

        if vals:
            w_sum = sum(w for _, w in vals)
            if w_sum > 0:
                return sum(v * w for v, w in vals) / w_sum

        # Fallback to observed daily averages.
        if recent_obs_days:
            return recent_avg[metric_key]
        if mtd_obs_days:
            return mtd_avg[metric_key]
        return profile_avg[metric_key]

    # remaining dates
    rem_net = rem_profit = rem_tickets = rem_discount = 0.0
    cur = as_of + timedelta(days=1)
    while cur <= last_dom:
        wd = cur.weekday()
        rem_net += _pick_metric_for_weekday("net", wd)
        rem_profit += _pick_metric_for_weekday("profit", wd)
        rem_tickets += _pick_metric_for_weekday("tickets", wd)
        rem_discount += _pick_metric_for_weekday("discount", wd)
        cur += timedelta(days=1)

    # Trend adjustment from MTD acceleration/deceleration (bounded).
    trend_factor = 1.0
    if mtd_obs_days >= 10:
        mtd_sorted = mtd.sort_values("date")
        last7 = mtd_sorted.tail(7)
        prev7 = mtd_sorted.iloc[-14:-7]
        prev_avg_net = float(prev7["net_revenue"].mean()) if not prev7.empty else 0.0
        last_avg_net = float(last7["net_revenue"].mean()) if not last7.empty else 0.0
        if prev_avg_net > 0:
            trend_factor = float(np.clip(last_avg_net / prev_avg_net, 0.85, 1.15))

    rem_net *= trend_factor
    rem_profit *= trend_factor
    rem_tickets *= trend_factor
    rem_discount *= trend_factor

    # Blend weekday-pattern projection with straight MTD pace.
    # Weight increases as month observation depth improves.
    pattern_weight = float(np.clip(0.35 + (mtd_obs_days * 0.035), 0.35, 0.80))
    pace_net = (mtd_net / days_elapsed) * days_in_month
    pace_profit = (mtd_profit / days_elapsed) * days_in_month
    pace_tickets = (mtd_tickets / days_elapsed) * days_in_month
    pace_discount = (mtd_discount / days_elapsed) * days_in_month

    weekday_net = mtd_net + rem_net
    weekday_profit = mtd_profit + rem_profit
    weekday_tickets = mtd_tickets + rem_tickets
    weekday_discount = mtd_discount + rem_discount

    return {
        "net_pred": max((pattern_weight * weekday_net) + ((1.0 - pattern_weight) * pace_net), mtd_net),
        "profit_pred": max((pattern_weight * weekday_profit) + ((1.0 - pattern_weight) * pace_profit), mtd_profit),
        "tickets_pred": max((pattern_weight * weekday_tickets) + ((1.0 - pattern_weight) * pace_tickets), mtd_tickets),
        "discount_pred": max((pattern_weight * weekday_discount) + ((1.0 - pattern_weight) * pace_discount), mtd_discount),
    }

class MonthEndForecaster:
    """
    Trains & predicts month-end totals.
    Persists to disk so it “learns” as history grows.
    """
    def __init__(self):
        self.sklearn = _try_import_sklearn()
        self.models = {}          # point models
        self.q_models = {}        # quantile models (optional)
        self.meta = {}

    def train(self, hist: pd.DataFrame) -> None:
        X, y_dict, meta = _build_training_data(hist)
        self.meta = dict(meta)

        # Not enough data -> no ML model
        complete_months = int(meta.get("n_complete_months", 0))
        if complete_months < FORECAST_MIN_COMPLETE_MONTHS or X.empty or not y_dict:
            self.meta["model_name"] = "baseline_weekday_profile"
            self.models = {}
            self.q_models = {}
            return

        if not self.sklearn.get("ok"):
            self.meta["model_name"] = "baseline_weekday_profile"
            self.models = {}
            self.q_models = {}
            return

        # Build preprocess
        ColumnTransformer = self.sklearn["ColumnTransformer"]
        Pipeline = self.sklearn["Pipeline"]
        OneHotEncoder = self.sklearn["OneHotEncoder"]
        SimpleImputer = self.sklearn["SimpleImputer"]
        HistGBR = self.sklearn["HistGradientBoostingRegressor"]
        GBR = self.sklearn["GradientBoostingRegressor"]

        cat_cols = ["store_code", "month"]
        num_cols = [c for c in X.columns if c not in cat_cols]

        preprocess = ColumnTransformer(
            transformers=[
                ("cat", OneHotEncoder(handle_unknown="ignore"), cat_cols),
                ("num", Pipeline([("imputer", SimpleImputer(strategy="median"))]), num_cols),
            ],
            remainder="drop",
        )

        # Point model (strong non-linear learner)
        def make_point_model():
            return HistGBR(
                max_depth=6,
                learning_rate=0.05,
                max_iter=600,
                l2_regularization=0.01,
                random_state=42,
            )

        def make_quantile_model(alpha: float):
            # More compatible across sklearn versions than HistGBR quantile
            return GBR(
                loss="quantile",
                alpha=alpha,
                n_estimators=700,
                learning_rate=0.03,
                max_depth=3,
                random_state=42,
            )

        self.models = {}
        for target_name in ["net", "profit", "tickets", "discount"]:
            y = y_dict[target_name]
            pipe = Pipeline([("prep", preprocess), ("model", make_point_model())])
            pipe.fit(X, y)
            self.models[target_name] = pipe

        # Optional quantile bands for net & profit
        self.q_models = {}
        if FORECAST_USE_QUANTILES:
            for target_name in ["net", "profit"]:
                y = y_dict[target_name]
                p10 = Pipeline([("prep", preprocess), ("model", make_quantile_model(0.10))])
                p90 = Pipeline([("prep", preprocess), ("model", make_quantile_model(0.90))])
                p10.fit(X, y)
                p90.fit(X, y)
                self.q_models[target_name] = {"p10": p10, "p90": p90}

        self.meta["model_name"] = "HistGradientBoosting (self-learning)"
        self.meta["trained_at"] = datetime.now().isoformat(timespec="seconds")

    def save(self) -> None:
        if not self.sklearn.get("ok"):
            return
        _ensure_forecast_dir()
        try:
            joblib = self.sklearn["joblib"]
            joblib.dump({"models": self.models, "q_models": self.q_models, "meta": self.meta}, FORECAST_MODEL_PATH)
            with open(FORECAST_META_PATH, "w") as f:
                json.dump(self.meta, f, indent=2)
        except Exception as e:
            print(f"[FORECAST] WARN: Could not save model: {e}")

    def load(self) -> bool:
        if not self.sklearn.get("ok"):
            return False
        if not FORECAST_MODEL_PATH.exists():
            return False
        try:
            joblib = self.sklearn["joblib"]
            blob = joblib.load(FORECAST_MODEL_PATH)
            self.models = blob.get("models", {})
            self.q_models = blob.get("q_models", {})
            self.meta = blob.get("meta", {})
            return True
        except Exception as e:
            print(f"[FORECAST] WARN: Could not load model: {e}")
            return False

    def predict(self, hist: pd.DataFrame, store_code: str, as_of: date) -> Dict[str, Any]:
        """
        Predict month-end totals as-of a given date.
        Always clamps predicted totals >= MTD actual totals.
        """
        store = str(store_code)

        # Build current feature row
        feats = _build_asof_features(hist, store, as_of)
        X1 = pd.DataFrame([feats])

        # Pull MTD actual (for clamping + reporting)
        as_of_ts = pd.Timestamp(as_of)
        mtd_start = pd.Timestamp(date(as_of.year, as_of.month, 1))
        h_store = hist[(hist["store_code"] == store) & (hist["date"] >= mtd_start) & (hist["date"] <= as_of_ts)]
        mtd_net = float(h_store["net_revenue"].sum()) if not h_store.empty else 0.0
        mtd_profit = float(h_store["profit"].sum()) if not h_store.empty else 0.0
        mtd_tickets = float(h_store["tickets"].sum()) if not h_store.empty else 0.0
        mtd_discount = float(h_store["discount"].sum()) if not h_store.empty else 0.0

        # If ML model exists, use it; else baseline.
        if not self.models:
            base = _weekday_profile_baseline(hist, store, as_of)
            net_pred = float(base["net_pred"])
            profit_pred = float(base["profit_pred"])
            tickets_pred = float(base["tickets_pred"])
            discount_pred = float(base["discount_pred"])
            p10_net = p90_net = None
            p10_profit = p90_profit = None
            model_name = self.meta.get("model_name", "baseline_weekday_profile")
        else:
            net_pred = float(self.models["net"].predict(X1)[0])
            profit_pred = float(self.models["profit"].predict(X1)[0])
            tickets_pred = float(self.models["tickets"].predict(X1)[0])
            discount_pred = float(self.models["discount"].predict(X1)[0])

            # Quantiles if available
            p10_net = p90_net = None
            p10_profit = p90_profit = None
            if self.q_models.get("net"):
                p10_net = float(self.q_models["net"]["p10"].predict(X1)[0])
                p90_net = float(self.q_models["net"]["p90"].predict(X1)[0])
            if self.q_models.get("profit"):
                p10_profit = float(self.q_models["profit"]["p10"].predict(X1)[0])
                p90_profit = float(self.q_models["profit"]["p90"].predict(X1)[0])

            model_name = f"{self.meta.get('model_name', 'ML')} + adaptive pace"

            # Anchor ML outputs to current observed pace so projections stay responsive.
            base = _weekday_profile_baseline(hist, store, as_of)
            pct_elapsed = float(np.clip(feats.get("pct_elapsed", 0.0), 0.0, 1.0))
            data_w = 0.45 + (0.30 * pct_elapsed)   # 45% early month -> 75% late month
            ml_w = 1.0 - data_w

            net_pred = (ml_w * net_pred) + (data_w * float(base["net_pred"]))
            profit_pred = (ml_w * profit_pred) + (data_w * float(base["profit_pred"]))
            tickets_pred = (ml_w * tickets_pred) + (data_w * float(base["tickets_pred"]))
            discount_pred = (ml_w * discount_pred) + (data_w * float(base["discount_pred"]))

        # Clamp totals >= MTD actuals
        net_pred = max(net_pred, mtd_net)
        profit_pred = max(profit_pred, mtd_profit)
        tickets_pred = max(tickets_pred, mtd_tickets)
        discount_pred = max(discount_pred, mtd_discount)

        # Derived
        margin_pred = (profit_pred / net_pred) if net_pred else 0.0
        basket_pred = (net_pred / tickets_pred) if tickets_pred else 0.0

        last_dom = _last_day_of_month(as_of)
        remaining_days = max((last_dom - as_of).days, 0)
        remaining_net = net_pred - mtd_net
        remaining_profit = profit_pred - mtd_profit

        req_net_per_day = (remaining_net / remaining_days) if remaining_days else 0.0
        req_profit_per_day = (remaining_profit / remaining_days) if remaining_days else 0.0

        return {
            "store_code": store,
            "as_of": as_of.isoformat(),
            "model": model_name,

            "mtd_net": mtd_net,
            "mtd_profit": mtd_profit,
            "mtd_tickets": mtd_tickets,
            "mtd_discount": mtd_discount,

            "net_pred": net_pred,
            "profit_pred": profit_pred,
            "tickets_pred": tickets_pred,
            "discount_pred": discount_pred,

            "margin_pred": margin_pred,
            "basket_pred": basket_pred,

            "remaining_days": int(remaining_days),
            "remaining_net": float(remaining_net),
            "remaining_profit": float(remaining_profit),
            "req_net_per_day": float(req_net_per_day),
            "req_profit_per_day": float(req_profit_per_day),

            "net_p10": p10_net,
            "net_p90": p90_net,
            "profit_p10": p10_profit,
            "profit_p90": p90_profit,
        }

def run_month_end_forecast_pipeline(store_daily_map: Dict[str, pd.DataFrame], as_of: date) -> Dict[str, Any]:
    """
    1) Upsert latest run data into history
    2) Train / load model (retrain every run if configured)
    3) Predict ALL + each store
    Returns a bundle safe to print / embed in PDFs.
    """
    hist = forecast_upsert_history(store_daily_map)

    engine = MonthEndForecaster()
    loaded = engine.load()

    if FORECAST_RETRAIN_EVERY_RUN or not loaded:
        engine.train(hist)
        engine.save()

    # Predict ALL + stores
    by_store = {}
    by_store["ALL"] = engine.predict(hist, "ALL", as_of)

    for store_name, abbr in store_abbr_map.items():
        by_store[abbr] = engine.predict(hist, abbr, as_of)

    bundle = {
        "as_of": as_of.isoformat(),
        "meta": engine.meta,
        "stores": by_store,
    }
    return bundle

def print_forecast_bundle(bundle: Dict[str, Any]) -> None:
    if not bundle:
        return
    meta = bundle.get("meta", {})
    stores = bundle.get("stores", {})

    print("\n================ MONTH-END PROJECTION ================")
    print(f"As of: {bundle.get('as_of')} • Model: {meta.get('model_name','')} • "
          f"Complete months: {meta.get('n_complete_months',0)} • Samples: {meta.get('n_samples',0)}")

    all_fc = stores.get("ALL", {})
    if all_fc:
        print("\n[ALL STORES]")
        print(f"  MTD Net: {money(all_fc['mtd_net'])}  ->  Projected Month Net: {money(all_fc['net_pred'])}")
        print(f"  MTD Profit: {money(all_fc['mtd_profit'])}  ->  Projected Month Profit: {money(all_fc['profit_pred'])}")
        if all_fc.get("net_p10") is not None and all_fc.get("net_p90") is not None:
            print(f"  Net Band (P10–P90): {money(all_fc['net_p10'])} – {money(all_fc['net_p90'])}")
        if all_fc.get("profit_p10") is not None and all_fc.get("profit_p90") is not None:
            print(f"  Profit Band (P10–P90): {money(all_fc['profit_p10'])} – {money(all_fc['profit_p90'])}")
        print(f"  Margin (proj): {pct1(all_fc['margin_pred'])} • Remaining days: {all_fc['remaining_days']} • "
              f"Req Net/Day: {money(all_fc['req_net_per_day'])}")

    print("======================================================\n")
###############################################################################
# Reading exports robustly (Row 5 header fix)
###############################################################################

def guess_header_row(path: Path, tokens: List[str], scan_rows: int = 60) -> int:
    preview = pd.read_excel(path, header=None, nrows=scan_rows, engine="openpyxl")
    token_lc = [t.lower() for t in tokens]
    for i in range(len(preview)):
        row_vals = [
            str(x).strip().lower()
            for x in preview.iloc[i].tolist()
            if str(x).strip() != "nan"
        ]
        joined = " | ".join(row_vals)
        if any(t in joined for t in token_lc):
            return i
    return 0

def _clean_df(df: pd.DataFrame) -> pd.DataFrame:
    df = df.loc[:, ~df.columns.astype(str).str.contains("^Unnamed", case=False, regex=True)]
    df.columns = [str(c).strip() for c in df.columns]
    return df

def read_export(path: Path) -> pd.DataFrame:
    if FORCE_HEADER_ROW:
        try:
            df_try = pd.read_excel(path, header=EXPORT_HEADER_ROW_INDEX, engine="openpyxl")
            df_try = _clean_df(df_try)
            if any(c in df_try.columns for c in ["Order ID", "Order Time", "Net Sales", "Gross Sales"]):
                return df_try
        except Exception:
            pass

    header_row = guess_header_row(
        path,
        tokens=["Order ID", "Order Time", "Net Sales", "Gross Sales", "Category", "Budtender Name"],
        scan_rows=80,
    )
    df = pd.read_excel(path, header=header_row, engine="openpyxl")
    return _clean_df(df)


###############################################################################
# Date helpers
###############################################################################

def compute_date_window(backfill_days: int, tz_name: str) -> Tuple[date, date]:
    tz = ZoneInfo(tz_name)
    today = datetime.now(tz).date()
    end_d = today - timedelta(days=1)
    start_d = end_d - timedelta(days=backfill_days - 1)
    return start_d, end_d

def parse_iso_date(value: str) -> date:
    try:
        return datetime.strptime(value, "%Y-%m-%d").date()
    except ValueError as e:
        raise argparse.ArgumentTypeError(f"Invalid date '{value}'. Use YYYY-MM-DD.") from e

def parse_cli_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        description="Generate owner snapshot PDFs and email summary."
    )
    parser.add_argument(
        "--report-day",
        type=parse_iso_date,
        help="Report day/end date in YYYY-MM-DD (example: 2026-01-31).",
    )
    parser.add_argument(
        "--start-date",
        type=parse_iso_date,
        help="Explicit data-window start date (YYYY-MM-DD).",
    )
    parser.add_argument(
        "--end-date",
        type=parse_iso_date,
        help="Explicit data-window end date (YYYY-MM-DD).",
    )
    parser.add_argument(
        "--backfill-days",
        type=int,
        default=BACKFILL_DAYS,
        help=f"Window length when start date is omitted. Default: {BACKFILL_DAYS}.",
    )
    parser.add_argument(
        "--run-export",
        dest="run_export",
        action="store_true",
        help="Run export for the selected date window. Enabled by default.",
    )
    parser.add_argument(
        "--reuse-latest",
        dest="run_export",
        action="store_false",
        help="Reuse archived raw exports instead of running Selenium.",
    )
    parser.add_argument(
        "--no-email",
        action="store_true",
        help="Generate PDFs only; skip sending email.",
    )
    parser.add_argument(
        "-email",
        "--email",
        action="append",
        default=[],
        help="Recipient email address. Can be repeated or comma-separated. Defaults to the owner snapshot list.",
    )
    parser.add_argument(
        "--export-source",
        choices=["selenium", "api"],
        default=DEFAULT_EXPORT_SOURCE,
        help=f"Data source when exports run. Default: {DEFAULT_EXPORT_SOURCE}.",
    )
    parser.add_argument(
        "--use-api",
        dest="export_source",
        action="store_const",
        const="api",
        help="Shortcut for --export-source api.",
    )
    parser.add_argument(
        "--api-env-file",
        default=DEFAULT_API_ENV_FILE,
        help=f"Path to Dutchie API .env file. Default: {DEFAULT_API_ENV_FILE}.",
    )
    parser.add_argument(
        "--workers",
        type=int,
        default=DEFAULT_OWNER_API_WORKERS,
        help=f"Dutchie API store workers. Default: {DEFAULT_OWNER_API_WORKERS}.",
    )
    parser.add_argument(
        "--stores",
        nargs="*",
        help="Optional store codes to include, for example: MV LG or 'MV,LG'.",
    )
    parser.add_argument(
        "--loyalty-discount-name",
        default=LOYALTY_DISCOUNT_MATCH_TEXT,
        help=f"Discount name text to track in the loyalty detail table. Default: {LOYALTY_DISCOUNT_MATCH_TEXT}.",
    )
    parser.add_argument(
        "--loyalty-person",
        default="",
        help="Optional person/name text to highlight in loyalty register adjustments, for example 'Anthony Barbaro'.",
    )
    parser.add_argument(
        "--loyalty-adjustment-source",
        choices=["auto", "browser", "none"],
        default=DEFAULT_LOYALTY_ADJUSTMENT_SOURCE,
        help=(
            "How to add Dutchie Backoffice loyalty point adjustment audit rows. "
            "auto tries the Backoffice report and continues if unavailable; browser requires it; none skips it."
        ),
    )
    parser.add_argument(
        "--no-loyalty-adjustment-report",
        dest="loyalty_adjustment_source",
        action="store_const",
        const="none",
        help="Skip the Dutchie Backoffice loyalty adjustment report.",
    )
    parser.add_argument(
        "--loyalty-browser-workers",
        type=int,
        default=DEFAULT_LOYALTY_BROWSER_WORKERS,
        help=(
            "Parallel browser sessions for the Dutchie Backoffice loyalty adjustment report. "
            f"Default: {DEFAULT_LOYALTY_BROWSER_WORKERS}; max: {MAX_LOYALTY_BROWSER_WORKERS}."
        ),
    )
    parser.add_argument(
        "--discount-detail-source",
        choices=["auto", "browser", "none"],
        default=DEFAULT_DISCOUNT_DETAIL_SOURCE,
        help=(
            "How to fill Discount Approved By from the Dutchie Backoffice Discount Detail Report. "
            "auto tries the browser report and continues if unavailable; browser requires it; none keeps API-only data."
        ),
    )
    parser.add_argument(
        "--no-discount-detail-report",
        dest="discount_detail_source",
        action="store_const",
        const="none",
        help="Skip the Dutchie Backoffice Discount Detail Report approval enrichment.",
    )
    parser.add_argument(
        "--discount-detail-browser-workers",
        type=int,
        default=DEFAULT_DISCOUNT_DETAIL_BROWSER_WORKERS,
        help=(
            "Parallel browser sessions for the Dutchie Backoffice Discount Detail Report. "
            f"Default: {DEFAULT_DISCOUNT_DETAIL_BROWSER_WORKERS}; max: {MAX_DISCOUNT_DETAIL_BROWSER_WORKERS}."
        ),
    )
    parser.add_argument(
        "--no-forecast",
        action="store_true",
        help="Skip the month-end forecast step for faster test runs.",
    )
    parser.set_defaults(run_export=RUN_EXPORT)
    return parser.parse_args()

def parse_email_recipients(values: List[str]) -> List[str]:
    recipients: List[str] = []
    seen = set()
    for value in values or []:
        for piece in str(value).replace(",", " ").split():
            email = piece.strip()
            if not email:
                continue
            key = email.lower()
            if key in seen:
                continue
            seen.add(key)
            recipients.append(email)
    return recipients

def month_start(d: date) -> date:
    return date(d.year, d.month, 1)

def prev_month_same_window(end_d: date) -> Tuple[date, date]:
    """Previous month: day 1 -> same day-of-month (clamped)."""
    if end_d.month == 1:
        py, pm = end_d.year - 1, 12
    else:
        py, pm = end_d.year, end_d.month - 1

    start = date(py, pm, 1)
    first_this_month = date(end_d.year, end_d.month, 1)
    last_prev = first_this_month - timedelta(days=1)
    end_day = min(end_d.day, last_prev.day)
    end = date(py, pm, end_day)
    return start, end

def parse_range_from_folder_name(folder: Path) -> Optional[Tuple[date, date]]:
    """
    Expects folder name like: 2025-12-10_to_2026-02-08
    """
    m = re.match(r"^(\d{4}-\d{2}-\d{2})_to_(\d{4}-\d{2}-\d{2})$", folder.name.strip())
    if not m:
        return None
    try:
        a = datetime.strptime(m.group(1), "%Y-%m-%d").date()
        b = datetime.strptime(m.group(2), "%Y-%m-%d").date()
        return a, b
    except Exception:
        return None


###############################################################################
# Export -> archive
###############################################################################

def cleanup_files_dir(files_dir: Path) -> None:
    files_dir.mkdir(parents=True, exist_ok=True)
    for p in files_dir.iterdir():
        try:
            if p.is_file():
                p.unlink()
        except Exception as e:
            print(f"[WARN] Could not delete {p}: {e}")

def run_export_for_range(start_day: date, end_day: date) -> None:
    if run_sales_report is None:
        raise SystemExit(
            "Selenium sales export is unavailable in this environment. "
            f"Import error: {GET_SALES_REPORT_IMPORT_ERROR}. "
            "Use --export-source api (or --use-api) to fetch from Dutchie API."
        )

    print(f"[EXPORT] Running run_sales_report({start_day} -> {end_day})")
    FILES_DIR.mkdir(parents=True, exist_ok=True)

    if CLEANUP_FILES_BEFORE_EXPORT:
        cleanup_files_dir(FILES_DIR)
    else:
        print("[EXPORT] Skipping /files cleanup (CLEANUP_FILES_BEFORE_EXPORT=False)")

    start_dt = datetime(start_day.year, start_day.month, start_day.day)
    end_dt = datetime(end_day.year, end_day.month, end_day.day)

    run_sales_report(start_dt, end_dt)
    print("[EXPORT] Done.")

def archive_exports(start_day: date, end_day: date) -> Tuple[Path, Dict[str, Path]]:
    range_dir = RAW_ROOT / f"{start_day.isoformat()}_to_{end_day.isoformat()}"
    range_dir.mkdir(parents=True, exist_ok=True)

    abbr_to_path: Dict[str, Path] = {}

    for store_name, abbr in store_abbr_map.items():
        src = FILES_DIR / f"sales{abbr}.xlsx"
        if not src.exists():
            print(f"[WARN] Missing export for {store_name} ({abbr}): {src}")
            continue

        nice = store_label(store_name)
        dst_name = f"{abbr} - Sales Export - {nice} - {start_day.isoformat()}_to_{end_day.isoformat()}.xlsx"
        dst = range_dir / safe_filename(dst_name)

        if ARCHIVE_ACTION.lower() == "copy":
            shutil.copy2(str(src), str(dst))
        else:
            shutil.move(str(src), str(dst))

        abbr_to_path[abbr] = dst
        print(f"[ARCHIVE] {abbr}: {dst}")

    return range_dir, abbr_to_path

def as_float(value: Any, default: float = 0.0) -> float:
    try:
        if value is None or value == "":
            return float(default)
        return float(value)
    except Exception:
        return float(default)

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

def parse_api_nested(value: Any) -> Any:
    if isinstance(value, str):
        text = value.strip()
        if not text:
            return []
        try:
            return json.loads(text)
        except json.JSONDecodeError:
            return []
    return value if value is not None else []

def api_payload_records(payload: Any) -> List[Dict[str, Any]]:
    if isinstance(payload, list):
        return [row for row in payload if isinstance(row, dict)]
    if isinstance(payload, dict):
        for key in ("data", "results", "items", "transactions", "products", "registerTransactions"):
            rows = payload.get(key)
            if isinstance(rows, list):
                return [row for row in rows if isinstance(row, dict)]
        return [payload]
    return []

def _selected_store_codes(values: Optional[List[str]]) -> List[str]:
    if not values:
        return list(dict.fromkeys(store_abbr_map.values()))
    return dutchie_parse_store_codes(values)

def _store_name_from_abbr(abbr: str) -> str:
    for store_name, code in store_abbr_map.items():
        if code == abbr:
            return store_name
    return DUTCHIE_STORE_CODES.get(abbr, abbr)

def _store_iter(selected_store_codes: Optional[List[str]] = None) -> List[Tuple[str, str]]:
    allowed = set(selected_store_codes or list(dict.fromkeys(store_abbr_map.values())))
    return [(store_name, abbr) for store_name, abbr in store_abbr_map.items() if abbr in allowed]

def _iter_api_date_chunks(start_day: date, end_day: date, max_days: int = API_EXPORT_MAX_WINDOW_DAYS) -> List[Tuple[date, date]]:
    chunks: List[Tuple[date, date]] = []
    cur = start_day
    window_days = max(1, int(max_days))
    while cur <= end_day:
        chunk_end = min(cur + timedelta(days=window_days - 1), end_day)
        chunks.append((cur, chunk_end))
        cur = chunk_end + timedelta(days=1)
    return chunks

def normalize_api_product_label(product_name: Any, brand_name: Any) -> str:
    product = str(product_name or "").strip() or "Unknown Product"
    brand = str(brand_name or "").strip()
    if not brand:
        return product
    parsed = parse_brand_from_product(product)
    if parsed.strip().lower() == brand.lower():
        return product
    return f"{brand} | {product}"

def _product_lookup_by_id(products_payload: Any) -> Dict[str, Dict[str, Any]]:
    lookup: Dict[str, Dict[str, Any]] = {}
    for row in api_payload_records(products_payload):
        product_id = _first_nonempty(row.get("productId"), row.get("id"), row.get("globalProductId"))
        if product_id in (None, ""):
            continue
        lookup[str(product_id)] = row
    return lookup

def _contains_text(value: Any, needle: str) -> bool:
    if not needle:
        return False
    return needle.lower() in str(value or "").lower()

def _discount_matches(discount: Dict[str, Any], match_text: str) -> bool:
    haystack = " ".join(
        str(_first_nonempty(discount.get(key), ""))
        for key in ("discountName", "discountReason", "name", "reason", "description")
    )
    return _contains_text(haystack, match_text)

def _join_unique(values: List[Any]) -> str:
    out: List[str] = []
    seen = set()
    for value in values:
        text = str(value or "").strip()
        if not text:
            continue
        key = text.lower()
        if key in seen:
            continue
        seen.add(key)
        out.append(text)
    return "; ".join(out)

PERSON_FIELD_CANDIDATES = (
    "discountApprovedBy",
    "approvedBy",
    "approvedByUser",
    "approvedByUserName",
    "approvedByEmployee",
    "approvedByEmployeeName",
    "managerName",
    "manager",
)

POINTS_ADDER_FIELD_CANDIDATES = (
    "pointsAddedBy",
    "pointsAddedByUser",
    "loyaltyAddedBy",
    "loyaltyAddedByUser",
    "addedBy",
    "addedByUser",
    "createdBy",
    "createdByUser",
)

CUSTOMER_NAME_FIELDS = (
    "name",
    "customerName",
    "fullName",
    "displayName",
)

def _customer_display_name(row: Dict[str, Any]) -> str:
    for key in CUSTOMER_NAME_FIELDS:
        value = row.get(key)
        if value not in (None, ""):
            return str(value).strip()
    first = str(row.get("firstName") or "").strip()
    middle = str(row.get("middleName") or "").strip()
    last = str(row.get("lastName") or "").strip()
    return " ".join(part for part in [first, middle, last] if part).strip()

def _first_person_from_records(records: List[Dict[str, Any]], keys: Tuple[str, ...]) -> str:
    for record in records:
        if not isinstance(record, dict):
            continue
        for key in keys:
            value = record.get(key)
            if value not in (None, ""):
                return str(value).strip()
    return ""

def _transaction_item_id(value: Any) -> str:
    if value in (None, ""):
        return ""
    try:
        return str(int(float(value)))
    except Exception:
        return str(value).strip()

def _matching_transaction_discounts_by_item(tx_discounts: List[Dict[str, Any]], match_text: str) -> Tuple[Dict[str, List[Dict[str, Any]]], List[Dict[str, Any]]]:
    by_item: Dict[str, List[Dict[str, Any]]] = {}
    unassigned: List[Dict[str, Any]] = []
    for discount in tx_discounts:
        if not isinstance(discount, dict) or not _discount_matches(discount, match_text):
            continue
        item_id = _transaction_item_id(discount.get("transactionItemId"))
        if item_id and item_id not in {"0", "0.0"}:
            by_item.setdefault(item_id, []).append(discount)
        else:
            unassigned.append(discount)
    return by_item, unassigned

def _discount_amount(discount: Dict[str, Any]) -> float:
    return as_float(_first_nonempty(discount.get("amount"), discount.get("discountAmount"), discount.get("totalDiscount")))

def _discount_names(discounts: List[Dict[str, Any]]) -> str:
    return _join_unique([
        _first_nonempty(d.get("discountName"), d.get("name"), d.get("discountReason"), d.get("description"))
        for d in discounts
        if isinstance(d, dict)
    ])

def _discount_reasons(discounts: List[Dict[str, Any]]) -> str:
    return _join_unique([
        _first_nonempty(d.get("discountReason"), d.get("reason"), d.get("description"), d.get("discountName"))
        for d in discounts
        if isinstance(d, dict)
    ])

def build_customer_lookup(customer_rows: List[Dict[str, Any]]) -> Dict[str, Dict[str, Any]]:
    lookup: Dict[str, Dict[str, Any]] = {}
    for row in customer_rows:
        if not isinstance(row, dict):
            continue
        customer_id = _first_nonempty(row.get("customerId"), row.get("customerID"), row.get("id"))
        if customer_id in (None, ""):
            continue
        lookup[str(customer_id)] = row
    return lookup

def fetch_customers_for_ids(session: Any, customer_ids: List[Any]) -> Dict[str, Dict[str, Any]]:
    lookup: Dict[str, Dict[str, Any]] = {}
    seen: set[str] = set()
    for raw_id in customer_ids:
        customer_id = str(raw_id or "").strip()
        if not customer_id or customer_id.lower() in {"nan", "none"} or customer_id in seen:
            continue
        seen.add(customer_id)
        try:
            payload = dutchie_request_json(
                session,
                "/customer/customers",
                params={"customerID": customer_id, "includeAnonymous": False},
                timeout=60,
                max_attempts=2,
            )
        except Exception as exc:
            print(f"[EXPORT API] WARN: customer lookup skipped for {customer_id}: {exc}")
            continue
        lookup.update(build_customer_lookup(api_payload_records(payload)))
    return lookup

def build_register_transaction_lookup(register_rows: List[Dict[str, Any]]) -> Dict[str, Dict[str, Any]]:
    lookup: Dict[str, Dict[str, Any]] = {}
    for row in register_rows:
        if not isinstance(row, dict):
            continue
        tx_id = _first_nonempty(row.get("transactionId"), row.get("transactionID"))
        if tx_id in (None, ""):
            continue
        lookup[str(tx_id)] = row
    return lookup

def normalize_api_sales_rows(
    store_code: str,
    transactions: List[Dict[str, Any]],
    products_payload: Any,
    loyalty_discount_name: str = LOYALTY_DISCOUNT_MATCH_TEXT,
) -> pd.DataFrame:
    rows: List[Dict[str, Any]] = []
    product_lookup = _product_lookup_by_id(products_payload)
    store_name = _store_name_from_abbr(store_code)

    for tx in transactions:
        if not isinstance(tx, dict) or bool(tx.get("isVoid")):
            continue

        items = [item for item in parse_api_nested(tx.get("items")) if isinstance(item, dict) and not bool(item.get("isCoupon"))]
        tx_discounts = [d for d in parse_api_nested(tx.get("discounts")) if isinstance(d, dict)]
        tx_matches_by_item, tx_unassigned_matches = _matching_transaction_discounts_by_item(tx_discounts, loyalty_discount_name)
        unassigned_total = sum(_discount_amount(d) for d in tx_unassigned_matches)
        item_discount_total = sum(abs(as_float(item.get("totalDiscount"))) for item in items)
        item_gross_total = sum(abs(as_float(item.get("totalPrice"))) for item in items)

        transaction_id = _first_nonempty(tx.get("transactionId"), tx.get("globalId"), tx.get("referenceId"), tx.get("invoiceNumber"))
        tx_date = _first_nonempty(tx.get("transactionDateLocalTime"), tx.get("transactionDate"), tx.get("lastModifiedDateUTC"))
        employee = _first_nonempty(tx.get("completedByUser"), tx.get("employeeName"), tx.get("employeeId"), "Unknown")
        customer_type = _first_nonempty(tx.get("customerTypeName"), tx.get("customerType"), tx.get("customerTypeId"), "")
        is_return = bool(tx.get("isReturn"))
        tx_total_before_tax = as_float(tx.get("totalBeforeTax"))

        if not items:
            gross = as_float(tx.get("subtotal"))
            discount = as_float(tx.get("totalDiscount"))
            net = as_float(tx.get("totalBeforeTax"), gross - discount)
            if is_return and net > 0 and tx_total_before_tax >= 0:
                gross = -abs(gross)
                discount = -abs(discount)
                net = -abs(net)

            matched = [d for d in tx_discounts if _discount_matches(d, loyalty_discount_name)]
            rows.append({
                "Order ID": transaction_id,
                "Order Time": tx_date,
                "Transaction Date": tx_date,
                "Budtender Name": employee,
                "Completed By": employee,
                "Customer ID": tx.get("customerId"),
                "Customer Type": customer_type,
                "Product Name": "Unknown Product",
                "Brand": "Unknown",
                "Major Category": "Unknown",
                "Category": "Unknown",
                "Product Category": "Unknown",
                "Total Inventory Sold": as_float(tx.get("totalItems"), 1.0),
                "Gross Sales": gross,
                "Discounted Amount": discount,
                "Loyalty as Discount": 0.0,
                "Net Sales": net,
                "Inventory Cost": 0.0,
                "Order Profit": net,
                "Return Date": tx.get("voidDate") if is_return else None,
                "Total Weight Sold": 0.0,
                "Store": store_code,
                "Store Name": store_name,
                "Discount Names": _discount_names(tx_discounts),
                "Discount Reasons": _discount_reasons(tx_discounts),
                "Loyalty Discount Names": _discount_names(matched),
                "Loyalty Discount Reasons": _discount_reasons(matched),
                "Loyalty Points Adjustment Discount": sum(_discount_amount(d) for d in matched),
                "Discount Approved By": _first_person_from_records(matched + [tx], PERSON_FIELD_CANDIDATES) or API_FIELD_UNAVAILABLE,
                "Points Added By": _first_person_from_records(matched + [tx], POINTS_ADDER_FIELD_CANDIDATES) or API_FIELD_UNAVAILABLE,
                "Loyalty Earned": as_float(tx.get("loyaltyEarned")),
                "Loyalty Spent": as_float(tx.get("loyaltySpent")),
                "API Detail Note": "No item detail returned by API",
            })
            continue

        for item in items:
            product_id = item.get("productId")
            catalog = product_lookup.get(str(product_id), {})
            product_name = _first_nonempty(
                catalog.get("productName"),
                catalog.get("internalName"),
                catalog.get("alternateName"),
                item.get("productName"),
                item.get("name"),
                f"Product {product_id or 'Unknown'}",
            )
            brand_name = _first_nonempty(catalog.get("brandName"), catalog.get("brand"), item.get("brandName"), "")
            category = _first_nonempty(catalog.get("masterCategory"), catalog.get("category"), item.get("masterCategory"), item.get("category"), "Unknown")
            product_category = _first_nonempty(catalog.get("category"), category)

            qty = as_float(item.get("quantity"), 1.0)
            gross = as_float(item.get("totalPrice"), as_float(item.get("unitPrice")) * qty)
            discount = as_float(item.get("totalDiscount"))
            net = gross - discount
            return_date = item.get("returnDate") or tx.get("voidDate")
            row_is_return = is_return or bool(item.get("isReturned")) or bool(return_date)
            if row_is_return and net > 0 and tx_total_before_tax >= 0:
                gross = -abs(gross)
                discount = -abs(discount)
                net = -abs(net)
                qty = -abs(qty)

            cogs = as_float(item.get("unitCost")) * qty
            item_discounts = [d for d in parse_api_nested(item.get("discounts")) if isinstance(d, dict)]
            item_matches = [d for d in item_discounts if _discount_matches(d, loyalty_discount_name)]
            tx_item_matches = tx_matches_by_item.get(_transaction_item_id(item.get("transactionItemId")), [])
            direct_matches = item_matches + tx_item_matches
            loyalty_amount = sum(_discount_amount(d) for d in direct_matches)
            note = ""

            if loyalty_amount == 0.0 and unassigned_total:
                if item_discount_total:
                    ratio = abs(as_float(item.get("totalDiscount"))) / item_discount_total
                elif item_gross_total:
                    ratio = abs(as_float(item.get("totalPrice"))) / item_gross_total
                else:
                    ratio = 1.0 / max(1, len(items))
                loyalty_amount = unassigned_total * ratio
                direct_matches = tx_unassigned_matches
                note = "Allocated from transaction-level discount"

            all_discounts = item_discounts + tx_discounts
            rows.append({
                "Order ID": transaction_id,
                "Order Time": tx_date,
                "Transaction Date": tx_date,
                "Budtender Name": employee,
                "Completed By": employee,
                "Customer ID": tx.get("customerId"),
                "Customer Type": customer_type,
                "Vendor Name": _first_nonempty(item.get("vendor"), catalog.get("vendorName"), catalog.get("producerName"), ""),
                "Product Name": normalize_api_product_label(product_name, brand_name),
                "Brand": str(brand_name or parse_brand_from_product(product_name) or "Unknown").strip(),
                "Major Category": category,
                "Category": category,
                "Product Category": product_category,
                "Package ID": item.get("packageId"),
                "Batch ID": _first_nonempty(item.get("batchName"), ""),
                "External Package ID": _first_nonempty(item.get("sourcePackageId"), item.get("packageId"), ""),
                "Inventory ID": item.get("inventoryId"),
                "Product ID": product_id,
                "Transaction Item ID": item.get("transactionItemId"),
                "Total Inventory Sold": qty,
                "Unit Weight Sold": as_float(item.get("unitWeight")),
                "Total Weight Sold": as_float(item.get("unitWeight")) * abs(qty),
                "Unit Price": as_float(_first_nonempty(item.get("unitPrice"), catalog.get("price"), catalog.get("recPrice"), catalog.get("medPrice"))),
                "Gross Sales": gross,
                "Inventory Cost": cogs,
                "Discounted Amount": discount,
                "Loyalty as Discount": 0.0,
                "Net Sales": net,
                "Order Profit": net - cogs,
                "Return Date": return_date if row_is_return else None,
                "Store": store_code,
                "Store Name": store_name,
                "Discount Names": _discount_names(all_discounts),
                "Discount Reasons": _discount_reasons(all_discounts),
                "Loyalty Discount Names": _discount_names(direct_matches),
                "Loyalty Discount Reasons": _discount_reasons(direct_matches),
                "Loyalty Points Adjustment Discount": loyalty_amount,
                "Discount Approved By": _first_person_from_records(direct_matches + [tx], PERSON_FIELD_CANDIDATES) or API_FIELD_UNAVAILABLE,
                "Points Added By": _first_person_from_records(direct_matches + [tx], POINTS_ADDER_FIELD_CANDIDATES) or API_FIELD_UNAVAILABLE,
                "Loyalty Earned": as_float(tx.get("loyaltyEarned")),
                "Loyalty Spent": as_float(tx.get("loyaltySpent")),
                "API Detail Note": note or ("Approval/add-point user fields only populate if Dutchie sends them"),
            })

    out = pd.DataFrame(rows)
    if out.empty:
        return pd.DataFrame(columns=[
            "Order ID", "Order Time", "Budtender Name", "Product Name", "Major Category",
            "Category", "Total Inventory Sold", "Gross Sales", "Discounted Amount",
            "Loyalty as Discount", "Net Sales", "Inventory Cost", "Order Profit",
            "Return Date", "Total Weight Sold", "Store", "Loyalty Points Adjustment Discount",
        ])

    out = _clean_df(out)
    for date_col in ["Order Time", "Transaction Date", "Return Date"]:
        if date_col in out.columns:
            out[date_col] = pd.to_datetime(out[date_col], errors="coerce")
    return out

def compute_loyalty_points_adjustment_detail(
    df: pd.DataFrame,
    start: date,
    end: date,
    match_text: str = LOYALTY_DISCOUNT_MATCH_TEXT,
    customer_lookup: Optional[Dict[str, Dict[str, Any]]] = None,
    register_lookup: Optional[Dict[str, Dict[str, Any]]] = None,
) -> pd.DataFrame:
    if df is None or df.empty:
        return pd.DataFrame()

    tmp = _filter_df_date_range(df, start, end)
    if tmp.empty:
        return pd.DataFrame()

    amount_col = find_col(tmp, COLUMN_CANDIDATES["loyalty_adjustment_discount"])
    name_col = find_col(tmp, COLUMN_CANDIDATES["discount_name"])
    reason_col = find_col(tmp, COLUMN_CANDIDATES["discount_reason"])
    if amount_col:
        amount = to_number(tmp[amount_col]).fillna(0.0).astype(float)
    else:
        amount = pd.Series(0.0, index=tmp.index)

    text_match = pd.Series(False, index=tmp.index)
    for col in [name_col, reason_col]:
        if col:
            text_match |= tmp[col].fillna("").astype(str).str.contains(re.escape(match_text), case=False, na=False)

    detail = tmp[(amount.abs() > 1e-9) | text_match].copy()
    if detail.empty:
        return pd.DataFrame()

    date_col = find_col(detail, COLUMN_CANDIDATES["date"])
    tx_col = find_col(detail, COLUMN_CANDIDATES["transaction_id"])
    product_col = find_col(detail, COLUMN_CANDIDATES["product"])
    category_col = find_col(detail, COLUMN_CANDIDATES["category"])
    qty_col = find_col(detail, COLUMN_CANDIDATES["quantity"])
    gross_col = find_col(detail, COLUMN_CANDIDATES["gross_sales"])
    net_col = find_col(detail, COLUMN_CANDIDATES["net_sales"])
    disc_col = find_col(detail, COLUMN_CANDIDATES["discount_main"])
    approved_col = find_col(detail, COLUMN_CANDIDATES["discount_approved_by"])
    added_col = find_col(detail, COLUMN_CANDIDATES["points_added_by"])
    completed_col = find_col(detail, COLUMN_CANDIDATES["completed_by"])
    customer_col = find_col(detail, COLUMN_CANDIDATES["customer_id"])

    out = pd.DataFrame({
        "store": detail.get("Store", ""),
        "store_name": detail.get("Store Name", ""),
        "order_time": pd.to_datetime(detail[date_col], errors="coerce") if date_col else pd.NaT,
        "order_id": detail[tx_col].astype(str) if tx_col else "",
        "customer_id": detail[customer_col].astype(str) if customer_col else "",
        "product": detail[product_col].astype(str) if product_col else "",
        "category": detail[category_col].astype(str) if category_col else "",
        "quantity": to_number(detail[qty_col]).fillna(0.0).astype(float) if qty_col else 0.0,
        "gross_sales": to_number(detail[gross_col]).fillna(0.0).astype(float) if gross_col else 0.0,
        "item_discount": to_number(detail[disc_col]).fillna(0.0).astype(float) if disc_col else 0.0,
        "loyalty_adjustment_discount": amount.loc[detail.index].astype(float),
        "net_sales": to_number(detail[net_col]).fillna(0.0).astype(float) if net_col else 0.0,
        "discount_name": detail[name_col].astype(str) if name_col else match_text,
        "discount_reason": detail[reason_col].astype(str) if reason_col else "",
        "discount_approved_by": detail[approved_col].fillna(API_FIELD_UNAVAILABLE).astype(str) if approved_col else API_FIELD_UNAVAILABLE,
        "points_added_by": detail[added_col].fillna(API_FIELD_UNAVAILABLE).astype(str) if added_col else API_FIELD_UNAVAILABLE,
        "completed_by": detail[completed_col].fillna("").astype(str) if completed_col else "",
        "loyalty_earned": to_number(detail["Loyalty Earned"]).fillna(0.0).astype(float) if "Loyalty Earned" in detail.columns else 0.0,
        "loyalty_spent": to_number(detail["Loyalty Spent"]).fillna(0.0).astype(float) if "Loyalty Spent" in detail.columns else 0.0,
        "api_note": detail["API Detail Note"].fillna("").astype(str) if "API Detail Note" in detail.columns else "",
    })
    out["store"] = out["store"].fillna("").astype(str)
    if "Store" not in detail.columns:
        out["store"] = ""

    customer_lookup = customer_lookup or {}
    register_lookup = register_lookup or {}

    customer_names: List[str] = []
    customer_phones: List[str] = []
    customer_emails: List[str] = []
    edited_by: List[str] = []
    edited_by_employee_id: List[str] = []

    for row in out.to_dict("records"):
        customer_id = str(row.get("customer_id") or "").strip()
        customer = customer_lookup.get(customer_id, {})
        tx_id = str(row.get("order_id") or "").strip()
        register = register_lookup.get(tx_id, {})
        register_by = str(_first_nonempty(register.get("transactionBy"), register.get("adjustedBy"), "")).strip()
        register_employee_id = str(_first_nonempty(register.get("transactionByEmployeeId"), register.get("adjustedByEmployeeId"), "")).strip()
        points_added_by = str(row.get("points_added_by") or "").strip()
        completed_by = str(row.get("completed_by") or "").strip()

        customer_names.append(_customer_display_name(customer))
        customer_phones.append(str(_first_nonempty(customer.get("phone"), customer.get("cellPhone"), "")).strip())
        customer_emails.append(str(_first_nonempty(customer.get("emailAddress"), customer.get("email"), "")).strip())
        if points_added_by and points_added_by != API_FIELD_UNAVAILABLE:
            edited_by.append(points_added_by)
        elif register_by:
            edited_by.append(register_by)
        elif completed_by:
            edited_by.append(completed_by)
        else:
            edited_by.append(API_FIELD_UNAVAILABLE)
        edited_by_employee_id.append(register_employee_id)

    out["customer_name"] = customer_names
    out["customer_phone"] = customer_phones
    out["customer_email"] = customer_emails
    out["edited_or_processed_by"] = edited_by
    out["edited_or_processed_by_employee_id"] = edited_by_employee_id
    out = out.sort_values(["order_time", "order_id", "product"], ascending=[True, True, True])
    return out.reset_index(drop=True)

def normalize_register_loyalty_adjustments(
    store_code: str,
    payload: Any,
    match_text: str,
    person_text: str = "",
    customer_lookup: Optional[Dict[str, Dict[str, Any]]] = None,
    sales_customer_by_transaction: Optional[Dict[str, Any]] = None,
) -> pd.DataFrame:
    rows: List[Dict[str, Any]] = []
    needles = [match_text, "loyalty", "points"]
    customer_lookup = customer_lookup or {}
    sales_customer_by_transaction = sales_customer_by_transaction or {}

    for row in api_payload_records(payload):
        text = " ".join(str(v or "") for v in row.values() if not isinstance(v, (dict, list)))
        if not any(_contains_text(text, needle) for needle in needles if needle):
            continue
        transaction_id = _first_nonempty(row.get("transactionId"), row.get("id"), row.get("globalId"), row.get("referenceId"))
        customer_id = _first_nonempty(row.get("customerId"), sales_customer_by_transaction.get(str(transaction_id)), "")
        customer = customer_lookup.get(str(customer_id), {})
        employee = _first_nonempty(
            row.get("transactionBy"),
            row.get("adjustedBy"),
            row.get("completedByUser"),
            row.get("employeeName"),
            row.get("createdByUser"),
            row.get("userName"),
            row.get("employeeId"),
            API_FIELD_UNAVAILABLE,
        )
        rows.append({
            "store": store_code,
            "store_name": _store_name_from_abbr(store_code),
            "transaction_id": transaction_id,
            "receipt_number": transaction_id,
            "customer_id": customer_id,
            "customer_name": _customer_display_name(customer),
            "customer_phone": _first_nonempty(customer.get("phone"), customer.get("cellPhone"), ""),
            "date": _first_nonempty(
                row.get("transactionDateLocalTime"),
                row.get("transactionDate"),
                row.get("transactionDateUTC"),
                row.get("adjustedOn"),
                row.get("lastModifiedDateUTC"),
                row.get("createdDateUTC"),
            ),
            "type": _first_nonempty(row.get("transactionType"), row.get("adjustmentType"), row.get("type"), row.get("reason"), ""),
            "description": _first_nonempty(row.get("description"), row.get("adjustmentReason"), row.get("reason"), row.get("memo"), row.get("note"), row.get("comment"), ""),
            "points_delta": "",
            "amount": as_float(_first_nonempty(row.get("amount"), row.get("transactionAmount"), row.get("total"), row.get("adjustmentAmount"))),
            "adjusted_by": employee,
            "points_added_by": employee,
            "employee": employee,
            "employee_id": _first_nonempty(row.get("transactionByEmployeeId"), row.get("adjustedByEmployeeId"), ""),
            "source": "Dutchie API register activity",
            "raw_match": text[:500],
        })

    out = pd.DataFrame(rows)
    if not out.empty and "date" in out.columns:
        out["date"] = pd.to_datetime(out["date"], errors="coerce")
    return out

LOYALTY_ADJUSTMENT_EXPORT_CANDIDATES = {
    "date": [
        "Adjustment Date", "Adjusted On", "Date", "Created Date", "Created On", "Order Time",
        "Transaction Date", "Time", "Updated On",
    ],
    "receipt": [
        "Receipt", "Receipt No", "Receipt Number", "Receipt #", "Transaction ID",
        "Transaction Id", "Order ID", "Order Id", "Invoice", "Invoice Number",
    ],
    "customer_id": ["Customer ID", "Customer Id", "CustomerID", "Patient ID", "Patient Id"],
    "customer": ["Customer", "Customer Name", "Patient", "Patient Name", "Name", "Member"],
    "first_name": ["First Name", "Customer First Name", "Patient First Name"],
    "last_name": ["Last Name", "Customer Last Name", "Patient Last Name"],
    "phone": ["Phone", "Phone Number", "Customer Phone", "Patient Phone"],
    "email": ["Email", "Email Address", "Customer Email"],
    "points": [
        "Points", "Point Adjustment", "Points Adjustment", "Adjustment", "Adjusted Points",
        "Points Adjusted", "Loyalty Points", "Points Added", "Points Removed", "Delta",
        "Change", "Amount", "Loyalty Adjustment", "LoyaltyAdjustment",
    ],
    "balance": ["Balance", "New Balance", "Loyalty Balance", "Ending Balance"],
    "type": ["Type", "Adjustment Type", "Action", "Activity"],
    "reason": ["Reason", "Adjustment Reason", "AdjustmentReason", "Description", "Comment", "Comments", "Note", "Notes"],
    "adjusted_by": [
        "Adjusted By", "Edited By", "Added By", "Points Added By", "Created By", "Transaction By", "TransactionBy",
        "Employee", "Employee Name", "User", "User Name", "Performed By",
    ],
    "approved_by": ["Approved By", "Manager", "Manager Name", "Approving Manager", "ApprovingManager"],
}

def _normalized_col_key(value: Any) -> str:
    return re.sub(r"[^a-z0-9]+", "", str(value or "").lower())

def find_col_fuzzy(df: pd.DataFrame, candidates: List[str]) -> Optional[str]:
    exact = find_col(df, candidates)
    if exact:
        return exact
    normalized = {_normalized_col_key(c): c for c in df.columns}
    for candidate in candidates:
        key = _normalized_col_key(candidate)
        if key in normalized:
            return normalized[key]
    for candidate in candidates:
        key = _normalized_col_key(candidate)
        for col_key, col in normalized.items():
            if key and key in col_key:
                return col
    return None

def _series_or_blank(df: pd.DataFrame, col: Optional[str]) -> pd.Series:
    if col and col in df.columns:
        return df[col].fillna("").astype(str)
    return pd.Series([""] * len(df), index=df.index, dtype="object")

def _combine_customer_name(df: pd.DataFrame, customer_col: Optional[str], first_col: Optional[str], last_col: Optional[str]) -> pd.Series:
    customer = _series_or_blank(df, customer_col).str.strip()
    if customer.str.len().gt(0).any():
        return customer
    first = _series_or_blank(df, first_col).str.strip()
    last = _series_or_blank(df, last_col).str.strip()
    return (first + " " + last).str.strip()

def read_loyalty_adjustment_export(path: Path) -> pd.DataFrame:
    suffix = path.suffix.lower()
    if suffix == ".csv":
        return _clean_df(pd.read_csv(path))
    if suffix in {".xlsx", ".xlsm", ".xls"}:
        tokens = [
            "Adjustment Date", "Adjusted On", "Customer", "Patient", "Points",
            "Adjusted By", "Employee", "Reason", "Receipt",
        ]
        try:
            header_row = guess_header_row(path, tokens=tokens, scan_rows=80)
            df = pd.read_excel(path, header=header_row, engine="openpyxl")
        except Exception:
            df = pd.read_excel(path, header=0, engine="openpyxl")
        return _clean_df(df).dropna(how="all").reset_index(drop=True)
    raise ValueError(f"Unsupported loyalty adjustment export file type: {path}")

def normalize_backoffice_loyalty_adjustments(
    store_code: str,
    data: pd.DataFrame,
    start: date,
    end: date,
    source_path: Optional[Path] = None,
) -> pd.DataFrame:
    if data is None or data.empty:
        return pd.DataFrame()

    df = _clean_df(data.copy()).dropna(how="all")
    if df.empty:
        return pd.DataFrame()

    date_col = find_col_fuzzy(df, LOYALTY_ADJUSTMENT_EXPORT_CANDIDATES["date"])
    receipt_col = find_col_fuzzy(df, LOYALTY_ADJUSTMENT_EXPORT_CANDIDATES["receipt"])
    customer_id_col = find_col_fuzzy(df, LOYALTY_ADJUSTMENT_EXPORT_CANDIDATES["customer_id"])
    customer_col = find_col_fuzzy(df, LOYALTY_ADJUSTMENT_EXPORT_CANDIDATES["customer"])
    first_col = find_col_fuzzy(df, LOYALTY_ADJUSTMENT_EXPORT_CANDIDATES["first_name"])
    last_col = find_col_fuzzy(df, LOYALTY_ADJUSTMENT_EXPORT_CANDIDATES["last_name"])
    phone_col = find_col_fuzzy(df, LOYALTY_ADJUSTMENT_EXPORT_CANDIDATES["phone"])
    email_col = find_col_fuzzy(df, LOYALTY_ADJUSTMENT_EXPORT_CANDIDATES["email"])
    points_col = find_col_fuzzy(df, LOYALTY_ADJUSTMENT_EXPORT_CANDIDATES["points"])
    balance_col = find_col_fuzzy(df, LOYALTY_ADJUSTMENT_EXPORT_CANDIDATES["balance"])
    type_col = find_col_fuzzy(df, LOYALTY_ADJUSTMENT_EXPORT_CANDIDATES["type"])
    reason_col = find_col_fuzzy(df, LOYALTY_ADJUSTMENT_EXPORT_CANDIDATES["reason"])
    adjusted_by_col = find_col_fuzzy(df, LOYALTY_ADJUSTMENT_EXPORT_CANDIDATES["adjusted_by"])
    approved_by_col = find_col_fuzzy(df, LOYALTY_ADJUSTMENT_EXPORT_CANDIDATES["approved_by"])

    if date_col:
        dates = pd.to_datetime(df[date_col], errors="coerce")
        mask = dates.dt.date.between(start, end)
        df = df[mask | dates.isna()].copy()
        dates = dates.loc[df.index]
    else:
        dates = pd.Series(pd.NaT, index=df.index)

    if df.empty:
        return pd.DataFrame()

    points = to_number(df[points_col]).astype(float) if points_col else pd.Series(np.nan, index=df.index)
    balance = to_number(df[balance_col]).astype(float) if balance_col else pd.Series(np.nan, index=df.index)
    adjusted_by = _series_or_blank(df, adjusted_by_col).str.strip()
    approved_by = _series_or_blank(df, approved_by_col).str.strip()
    display_adjusted_by = approved_by.where(approved_by.str.len() > 0, adjusted_by)
    customer_name = _combine_customer_name(df, customer_col, first_col, last_col)

    out = pd.DataFrame({
        "store": store_code,
        "store_name": _store_name_from_abbr(store_code),
        "date": dates,
        "receipt_number": _series_or_blank(df, receipt_col),
        "transaction_id": _series_or_blank(df, receipt_col),
        "customer_id": _series_or_blank(df, customer_id_col),
        "customer_name": customer_name,
        "customer_phone": _series_or_blank(df, phone_col),
        "customer_email": _series_or_blank(df, email_col),
        "type": _series_or_blank(df, type_col),
        "description": _series_or_blank(df, reason_col),
        "points_delta": points,
        "ending_balance": balance,
        "amount": np.nan,
        "adjusted_by": display_adjusted_by.replace("", API_FIELD_UNAVAILABLE),
        "approving_manager": approved_by.replace("", API_FIELD_UNAVAILABLE),
        "transaction_by": adjusted_by.replace("", API_FIELD_UNAVAILABLE),
        "points_added_by": display_adjusted_by.replace("", API_FIELD_UNAVAILABLE),
        "discount_approved_by": approved_by.replace("", API_FIELD_UNAVAILABLE),
        "employee": display_adjusted_by.replace("", API_FIELD_UNAVAILABLE),
        "employee_id": "",
        "source": "Dutchie Backoffice loyalty adjustment report",
        "source_file": str(source_path or ""),
    })
    return out.sort_values(["date", "receipt_number"], na_position="last").reset_index(drop=True)

DISCOUNT_DETAIL_EXPORT_CANDIDATES = {
    "date": ["Order Time", "Transaction Date", "Date", "Sold At", "Created At"],
    "order_id": [
        "Order ID", "Order Number", "Order", "Receipt", "Receipt No",
        "Receipt Number", "Transaction ID", "Transaction Id",
    ],
    "customer": ["Customer Name", "Customer", "Patient", "Patient Name"],
    "product": ["Product Name", "Product", "Item Name", "Item"],
    "amount": ["Discounted Amount", "Discount Amount", "Discount", "Total Discount"],
    "discount_name": ["Discount Name", "Discount Names"],
    "discount_description": ["Discount Description", "Discount Reason", "Reason"],
    "approved_by": ["Discount Approved By", "ApprovingManager", "Approved By", "Manager", "Manager Name"],
    "budtender": ["Budtender Name", "Budtender", "Employee", "Employee Name", "Cashier"],
}

def _clean_report_person(value: Any) -> str:
    text = str(value if value is not None else "").strip()
    if not text:
        return ""
    if text.lower() in {"nan", "none", "api n/a", API_FIELD_UNAVAILABLE.lower()}:
        return ""
    return text

def _approval_order_key(value: Any) -> str:
    text = str(value if value is not None else "").strip()
    if not text or text.lower() in {"nan", "none"}:
        return ""
    try:
        return str(int(float(text)))
    except Exception:
        return re.sub(r"\s+", "", text)

def _approval_product_key(value: Any) -> str:
    return re.sub(r"[^a-z0-9]+", "", str(value or "").lower())

def _approval_amount_key(value: Any) -> str:
    return f"{abs(as_float(value)):.2f}"

def normalize_discount_detail_approvals(
    store_code: str,
    data: pd.DataFrame,
    start: date,
    end: date,
    match_text: str = LOYALTY_DISCOUNT_MATCH_TEXT,
    source_path: Optional[Path] = None,
) -> pd.DataFrame:
    if data is None or data.empty:
        return pd.DataFrame()

    df = _clean_df(data.copy()).dropna(how="all")
    if df.empty:
        return pd.DataFrame()

    date_col = find_col_fuzzy(df, DISCOUNT_DETAIL_EXPORT_CANDIDATES["date"])
    order_col = find_col_fuzzy(df, DISCOUNT_DETAIL_EXPORT_CANDIDATES["order_id"])
    customer_col = find_col_fuzzy(df, DISCOUNT_DETAIL_EXPORT_CANDIDATES["customer"])
    product_col = find_col_fuzzy(df, DISCOUNT_DETAIL_EXPORT_CANDIDATES["product"])
    amount_col = find_col_fuzzy(df, DISCOUNT_DETAIL_EXPORT_CANDIDATES["amount"])
    discount_name_col = find_col_fuzzy(df, DISCOUNT_DETAIL_EXPORT_CANDIDATES["discount_name"])
    discount_desc_col = find_col_fuzzy(df, DISCOUNT_DETAIL_EXPORT_CANDIDATES["discount_description"])
    approved_col = find_col_fuzzy(df, DISCOUNT_DETAIL_EXPORT_CANDIDATES["approved_by"])
    budtender_col = find_col_fuzzy(df, DISCOUNT_DETAIL_EXPORT_CANDIDATES["budtender"])

    if date_col:
        dates = pd.to_datetime(df[date_col], errors="coerce")
        mask = dates.dt.date.between(start, end)
        df = df[mask].copy()
        dates = dates.loc[df.index]
    else:
        dates = pd.Series(pd.NaT, index=df.index)

    if df.empty:
        return pd.DataFrame()

    name_series = _series_or_blank(df, discount_name_col)
    desc_series = _series_or_blank(df, discount_desc_col)
    match_mask = (
        name_series.str.contains(re.escape(match_text), case=False, na=False)
        | desc_series.str.contains(re.escape(match_text), case=False, na=False)
    )
    df = df[match_mask].copy()
    dates = dates.loc[df.index]
    if df.empty:
        return pd.DataFrame()

    out = pd.DataFrame({
        "store": store_code,
        "store_name": _store_name_from_abbr(store_code),
        "order_time": dates,
        "order_id": _series_or_blank(df, order_col),
        "customer_name": _series_or_blank(df, customer_col),
        "product": _series_or_blank(df, product_col),
        "loyalty_adjustment_discount": to_number(df[amount_col]).fillna(0.0).astype(float) if amount_col else 0.0,
        "discount_name": _series_or_blank(df, discount_name_col),
        "discount_reason": _series_or_blank(df, discount_desc_col),
        "discount_approved_by": _series_or_blank(df, approved_col).map(_clean_report_person) if approved_col else "",
        "completed_by": _series_or_blank(df, budtender_col),
        "approval_source": "Dutchie Backoffice Discount Detail Report",
        "source_file": str(source_path or ""),
    })
    out = out[out["discount_approved_by"].astype(str).str.strip().ne("")]
    return out.sort_values(["order_time", "order_id", "product"], na_position="last").reset_index(drop=True)

def _add_unique_approval(lookup: Dict[Tuple[str, ...], str], key: Tuple[str, ...], approval: str) -> None:
    if not all(key) or not approval:
        return
    existing = lookup.get(key)
    if existing is None:
        lookup[key] = approval
    elif existing != approval:
        lookup[key] = ""

def _discount_approval_lookups(approval_df: pd.DataFrame) -> Dict[str, Dict[Tuple[str, ...], str]]:
    lookups: Dict[str, Dict[Tuple[str, ...], str]] = {
        "order_product_amount": {},
        "order_product": {},
        "order_amount": {},
        "order": {},
    }
    if approval_df is None or approval_df.empty:
        return lookups

    for row in approval_df.to_dict("records"):
        approval = _clean_report_person(row.get("discount_approved_by"))
        if not approval:
            continue
        order_key = _approval_order_key(row.get("order_id"))
        product_key = _approval_product_key(row.get("product"))
        amount_key = _approval_amount_key(row.get("loyalty_adjustment_discount"))
        _add_unique_approval(lookups["order_product_amount"], (order_key, product_key, amount_key), approval)
        _add_unique_approval(lookups["order_product"], (order_key, product_key), approval)
        _add_unique_approval(lookups["order_amount"], (order_key, amount_key), approval)
        _add_unique_approval(lookups["order"], (order_key,), approval)
    return lookups

def enrich_loyalty_detail_with_discount_approvals(
    detail_df: pd.DataFrame,
    approval_df: pd.DataFrame,
) -> pd.DataFrame:
    if detail_df is None or detail_df.empty or approval_df is None or approval_df.empty:
        return detail_df

    out = detail_df.copy()
    if "discount_approved_by" not in out.columns:
        out["discount_approved_by"] = API_FIELD_UNAVAILABLE
    if "discount_approval_source" not in out.columns:
        out["discount_approval_source"] = ""

    lookups = _discount_approval_lookups(approval_df)
    filled = 0
    for idx, row in out.iterrows():
        current = _clean_report_person(row.get("discount_approved_by"))
        if current:
            continue

        order_key = _approval_order_key(row.get("order_id"))
        product_key = _approval_product_key(row.get("product"))
        amount_key = _approval_amount_key(row.get("loyalty_adjustment_discount"))
        approval = (
            lookups["order_product_amount"].get((order_key, product_key, amount_key))
            or lookups["order_product"].get((order_key, product_key))
            or lookups["order_amount"].get((order_key, amount_key))
            or lookups["order"].get((order_key,))
            or ""
        )
        approval = _clean_report_person(approval)
        if not approval:
            continue

        out.at[idx, "discount_approved_by"] = approval
        out.at[idx, "discount_approval_source"] = "Dutchie Backoffice Discount Detail Report"
        if "api_note" in out.columns:
            note = str(out.at[idx, "api_note"] or "").strip()
            addition = "Discount Approved By filled from Backoffice Discount Detail Report"
            out.at[idx, "api_note"] = f"{note}; {addition}" if note else addition
        filled += 1

    if filled:
        print(f"[DISCOUNT DETAIL] Filled Discount Approved By for {filled:,} loyalty line item(s).")
    return out

def enrich_loyalty_detail_maps_with_discount_approvals(
    detail_by_store: Dict[str, pd.DataFrame],
    approvals_by_store: Dict[str, pd.DataFrame],
) -> Dict[str, pd.DataFrame]:
    if not detail_by_store or not approvals_by_store:
        return detail_by_store
    enriched: Dict[str, pd.DataFrame] = {}
    for abbr, detail in detail_by_store.items():
        enriched[abbr] = enrich_loyalty_detail_with_discount_approvals(detail, approvals_by_store.get(abbr, pd.DataFrame()))
    return enriched

def merge_discount_approval_maps(*maps: Optional[Dict[str, pd.DataFrame]]) -> Dict[str, pd.DataFrame]:
    merged: Dict[str, pd.DataFrame] = {}
    keys: List[str] = []
    for mapping in maps:
        for key in (mapping or {}).keys():
            if key not in keys:
                keys.append(key)

    for key in keys:
        frames = [
            mapping[key]
            for mapping in maps
            if mapping and key in mapping and mapping[key] is not None and not mapping[key].empty
        ]
        if not frames:
            continue
        combined = pd.concat(frames, ignore_index=True)
        subset = [
            c for c in [
                "store",
                "order_id",
                "product",
                "loyalty_adjustment_discount",
                "discount_approved_by",
            ]
            if c in combined.columns
        ]
        if subset:
            combined = combined.drop_duplicates(subset=subset, keep="first")
        merged[key] = combined.reset_index(drop=True)
    return merged

def _discount_detail_export_dirs(start_day: date, end_day: date, root: Path = DISCOUNT_DETAIL_EXPORT_ROOT) -> List[Path]:
    candidates = [root / f"{start_day.isoformat()}_to_{end_day.isoformat()}"]
    if start_day == end_day:
        candidates.extend([
            root / end_day.isoformat(),
            root / f"{end_day.isoformat()}_to_{end_day.isoformat()}",
        ])
    out: List[Path] = []
    seen = set()
    for path in candidates:
        key = str(path.resolve())
        if key not in seen:
            seen.add(key)
            out.append(path)
    return out

def _marker_date(value: Any) -> Optional[date]:
    try:
        return date.fromisoformat(str(value or "")[:10])
    except Exception:
        return None

def _valid_no_data_marker(
    path: Path,
    report_name: str,
    store_code: str,
    start_day: date,
    end_day: date,
) -> bool:
    try:
        payload = json.loads(path.read_text(encoding="utf-8"))
    except Exception:
        return False
    if str(payload.get("status", "")).strip().lower() != "no_data":
        return False
    if str(payload.get("report", "")).strip() != report_name:
        return False
    if str(payload.get("store_code", "")).strip().upper() != store_code.upper():
        return False
    marker_start = _marker_date(payload.get("start_date"))
    marker_end = _marker_date(payload.get("end_date"))
    if not marker_start or not marker_end:
        return False
    return marker_start <= start_day and marker_end >= end_day

def _no_data_marker_store_codes(
    search_dirs: List[Path],
    selected_store_codes: List[str],
    report_name: str,
    start_day: date,
    end_day: date,
) -> set[str]:
    existing: set[str] = set()
    for abbr in selected_store_codes:
        for directory in search_dirs:
            if not directory.exists():
                continue
            for marker in directory.glob(f"{abbr} - *{NO_DATA_MARKER_SUFFIX}"):
                if _valid_no_data_marker(marker, report_name, abbr, start_day, end_day):
                    existing.add(abbr)
                    break
            if abbr in existing:
                break
    return existing

def existing_discount_detail_export_store_codes(
    start_day: date,
    end_day: date,
    selected_store_codes: List[str],
    root: Path = DISCOUNT_DETAIL_EXPORT_ROOT,
) -> set[str]:
    existing: set[str] = set()
    search_dirs = [path for path in _discount_detail_export_dirs(start_day, end_day, root) if path.exists()]
    for abbr in selected_store_codes:
        for directory in search_dirs:
            matches = [
                p for p in directory.glob(f"{abbr} - Discount Detail Report -*")
                if p.suffix.lower() in {".xlsx", ".xlsm", ".xls", ".csv"}
            ]
            if matches:
                existing.add(abbr)
                break
    return existing

def existing_discount_detail_no_data_store_codes(
    start_day: date,
    end_day: date,
    selected_store_codes: List[str],
    root: Path = DISCOUNT_DETAIL_EXPORT_ROOT,
) -> set[str]:
    search_dirs = [path for path in _discount_detail_export_dirs(start_day, end_day, root) if path.exists()]
    return _no_data_marker_store_codes(
        search_dirs,
        selected_store_codes,
        "Discount Detail Report",
        start_day,
        end_day,
    )

def load_discount_detail_approvals_for_range(
    start_day: date,
    end_day: date,
    selected_store_codes: List[str],
    match_text: str = LOYALTY_DISCOUNT_MATCH_TEXT,
    root: Path = DISCOUNT_DETAIL_EXPORT_ROOT,
) -> Dict[str, pd.DataFrame]:
    try:
        from getDiscountDetail import read_discount_detail_export
    except Exception as exc:
        print(f"[DISCOUNT DETAIL] WARN: Discount Detail reader is unavailable: {exc}")
        return {}

    out: Dict[str, pd.DataFrame] = {}
    search_dirs = [path for path in _discount_detail_export_dirs(start_day, end_day, root) if path.exists()]
    if not search_dirs:
        return out

    for abbr in selected_store_codes:
        matches: List[Path] = []
        for directory in search_dirs:
            matches.extend([
                p for p in directory.glob(f"{abbr} - Discount Detail Report -*")
                if p.suffix.lower() in {".xlsx", ".xlsm", ".xls", ".csv"}
            ])
        if not matches:
            continue
        latest = sorted(matches, key=lambda p: p.stat().st_mtime, reverse=True)[0]
        try:
            raw = read_discount_detail_export(latest)
            normalized = normalize_discount_detail_approvals(
                abbr,
                raw,
                start_day,
                end_day,
                match_text=match_text,
                source_path=latest,
            )
            if normalized is not None and not normalized.empty:
                out[abbr] = normalized
        except Exception as exc:
            print(f"[DISCOUNT DETAIL] WARN: Could not read {latest}: {exc}")
    return out

def run_discount_detail_export_for_range(
    start_day: date,
    end_day: date,
    selected_store_codes: List[str],
    match_text: str = LOYALTY_DISCOUNT_MATCH_TEXT,
    required: bool = False,
    workers: int = DEFAULT_DISCOUNT_DETAIL_BROWSER_WORKERS,
) -> Dict[str, pd.DataFrame]:
    range_dir = DISCOUNT_DETAIL_EXPORT_ROOT / f"{start_day.isoformat()}_to_{end_day.isoformat()}"
    range_dir.mkdir(parents=True, exist_ok=True)
    start_dt = datetime(start_day.year, start_day.month, start_day.day)
    end_dt = datetime(end_day.year, end_day.month, end_day.day)

    try:
        from getDiscountDetail import read_discount_detail_export, run_discount_detail_report
    except Exception as exc:
        message = f"Dutchie Backoffice Discount Detail exporter is unavailable: {exc}"
        if required:
            raise RuntimeError(message) from exc
        print(f"[DISCOUNT DETAIL] WARN: {message}")
        return {}

    try:
        exported = run_discount_detail_report(
            start_dt,
            end_dt,
            output_dir=range_dir,
            stores=selected_store_codes,
            fail_on_error=required,
            workers=workers,
        )
    except Exception as exc:
        if required:
            raise
        print(f"[DISCOUNT DETAIL] WARN: Backoffice Discount Detail export skipped: {exc}")
        return {}

    out: Dict[str, pd.DataFrame] = {}
    for abbr, path in exported.items():
        try:
            raw = read_discount_detail_export(path)
            normalized = normalize_discount_detail_approvals(
                abbr,
                raw,
                start_day,
                end_day,
                match_text=match_text,
                source_path=path,
            )
            if normalized is not None and not normalized.empty:
                out[abbr] = normalized
        except Exception as exc:
            print(f"[DISCOUNT DETAIL] WARN: Could not normalize {path}: {exc}")
    if out:
        print(f"[DISCOUNT DETAIL] Approval rows loaded for: {', '.join(sorted(out))}")
    else:
        print("[DISCOUNT DETAIL] Discount Detail report returned no approval rows.")
    return out

def _loyalty_adjustment_export_dirs(start_day: date, end_day: date, root: Path = LOYALTY_ADJUSTMENT_ROOT) -> List[Path]:
    candidates = [root / f"{start_day.isoformat()}_to_{end_day.isoformat()}"]
    if start_day == end_day:
        candidates.extend([
            root / end_day.isoformat(),
            root / f"{end_day.isoformat()}_to_{end_day.isoformat()}",
        ])
    out: List[Path] = []
    seen = set()
    for path in candidates:
        key = str(path.resolve())
        if key not in seen:
            seen.add(key)
            out.append(path)
    return out

def existing_loyalty_adjustment_no_data_store_codes(
    loyalty_start_day: date,
    end_day: date,
    selected_store_codes: List[str],
    root: Path = LOYALTY_ADJUSTMENT_ROOT,
    data_start_day: Optional[date] = None,
) -> set[str]:
    search_dirs = _loyalty_adjustment_export_dirs(loyalty_start_day, end_day, root)
    if data_start_day and data_start_day != loyalty_start_day:
        search_dirs.extend(_loyalty_adjustment_export_dirs(data_start_day, end_day, root))
    return _no_data_marker_store_codes(
        [path for path in search_dirs if path.exists()],
        selected_store_codes,
        "Loyalty Adjustment Report",
        loyalty_start_day,
        end_day,
    )

def load_backoffice_loyalty_adjustments_for_range(
    start_day: date,
    end_day: date,
    selected_store_codes: List[str],
    root: Path = LOYALTY_ADJUSTMENT_ROOT,
) -> Dict[str, pd.DataFrame]:
    range_dir = root / f"{start_day.isoformat()}_to_{end_day.isoformat()}"
    out: Dict[str, pd.DataFrame] = {}
    if not range_dir.exists():
        return out

    for abbr in selected_store_codes:
        matches = sorted(
            [
                p for p in range_dir.glob(f"{abbr} - Loyalty Adjustment Report -*")
                if p.suffix.lower() in {".xlsx", ".xlsm", ".xls", ".csv"}
            ],
            key=lambda p: p.stat().st_mtime,
            reverse=True,
        )
        if not matches:
            continue
        try:
            raw = read_loyalty_adjustment_export(matches[0])
            out[abbr] = normalize_backoffice_loyalty_adjustments(abbr, raw, start_day, end_day, source_path=matches[0])
        except Exception as exc:
            print(f"[LOYALTY] WARN: Could not read {matches[0]}: {exc}")
    return out

def load_cached_backoffice_loyalty_adjustments(
    loyalty_start_day: date,
    end_day: date,
    selected_store_codes: List[str],
    data_start_day: Optional[date] = None,
) -> Dict[str, pd.DataFrame]:
    cached = load_backoffice_loyalty_adjustments_for_range(
        loyalty_start_day,
        end_day,
        selected_store_codes,
    )
    missing = [abbr for abbr in selected_store_codes if abbr not in cached]
    if data_start_day and data_start_day != loyalty_start_day and missing:
        wider_cached = load_backoffice_loyalty_adjustments_for_range(
            data_start_day,
            end_day,
            missing,
        )
        if wider_cached:
            cached = merge_loyalty_adjustment_maps(cached, wider_cached)
    return {
        abbr: filter_loyalty_adjustment_df(df, loyalty_start_day, end_day)
        for abbr, df in cached.items()
        if df is not None and not filter_loyalty_adjustment_df(df, loyalty_start_day, end_day).empty
    }

def run_backoffice_loyalty_adjustment_export_for_range(
    start_day: date,
    end_day: date,
    selected_store_codes: List[str],
    required: bool = False,
    workers: int = DEFAULT_LOYALTY_BROWSER_WORKERS,
) -> Dict[str, pd.DataFrame]:
    range_dir = LOYALTY_ADJUSTMENT_ROOT / f"{start_day.isoformat()}_to_{end_day.isoformat()}"
    range_dir.mkdir(parents=True, exist_ok=True)
    start_dt = datetime(start_day.year, start_day.month, start_day.day)
    end_dt = datetime(end_day.year, end_day.month, end_day.day)

    try:
        from getLoyaltyAdjustmentReport import run_loyalty_adjustment_report
    except Exception as exc:
        message = f"Dutchie Backoffice loyalty adjustment exporter is unavailable: {exc}"
        if required:
            raise RuntimeError(message) from exc
        print(f"[LOYALTY] WARN: {message}")
        return {}

    try:
        exported = run_loyalty_adjustment_report(
            start_dt,
            end_dt,
            output_dir=range_dir,
            stores=selected_store_codes,
            fail_on_error=required,
            workers=workers,
        )
    except Exception as exc:
        if required:
            raise
        print(f"[LOYALTY] WARN: Backoffice loyalty adjustment export skipped: {exc}")
        return {}

    out: Dict[str, pd.DataFrame] = {}
    for abbr, path in exported.items():
        try:
            raw = read_loyalty_adjustment_export(path)
            out[abbr] = normalize_backoffice_loyalty_adjustments(abbr, raw, start_day, end_day, source_path=path)
        except Exception as exc:
            print(f"[LOYALTY] WARN: Could not normalize {path}: {exc}")
    if out:
        print(f"[LOYALTY] Backoffice adjustment report rows loaded for: {', '.join(sorted(out))}")
    else:
        print("[LOYALTY] Backoffice adjustment report returned no rows.")
    return out

def merge_loyalty_adjustment_maps(*maps: Optional[Dict[str, pd.DataFrame]]) -> Dict[str, pd.DataFrame]:
    merged: Dict[str, pd.DataFrame] = {}
    keys: List[str] = []
    for mapping in maps:
        for key in (mapping or {}).keys():
            if key not in keys:
                keys.append(key)
    for key in keys:
        frames = [mapping[key] for mapping in maps if mapping and key in mapping and mapping[key] is not None and not mapping[key].empty]
        if not frames:
            merged[key] = pd.DataFrame()
            continue
        combined = pd.concat(frames, ignore_index=True)
        subset = [c for c in ["store", "date", "receipt_number", "customer_id", "customer_name", "points_delta", "amount", "adjusted_by", "source"] if c in combined.columns]
        if subset:
            combined = combined.drop_duplicates(subset=subset, keep="first")
        merged[key] = combined.reset_index(drop=True)
    return merged

def _filter_date_column(df: pd.DataFrame, column: str, start: date, end: date) -> pd.DataFrame:
    if df is None or df.empty or column not in df.columns:
        return pd.DataFrame(columns=(df.columns if df is not None else []))
    tmp = df.copy()
    dates = pd.to_datetime(tmp[column], errors="coerce")
    mask = dates.dt.date.between(start, end)
    return tmp[mask].copy().reset_index(drop=True)

def filter_loyalty_detail_df(df: pd.DataFrame, start: date, end: date) -> pd.DataFrame:
    if df is None or df.empty:
        return pd.DataFrame(columns=(df.columns if df is not None else []))
    for column in ("order_time", "date", "transaction_date"):
        if column in df.columns:
            return _filter_date_column(df, column, start, end)
    return pd.DataFrame(columns=df.columns)

def filter_loyalty_adjustment_df(df: pd.DataFrame, start: date, end: date) -> pd.DataFrame:
    if df is None or df.empty:
        return pd.DataFrame(columns=(df.columns if df is not None else []))
    for column in ("date", "order_time", "adjustment_date"):
        if column in df.columns:
            return _filter_date_column(df, column, start, end)
    return pd.DataFrame(columns=df.columns)

def filter_loyalty_maps_to_range(
    detail_by_store: Dict[str, pd.DataFrame],
    register_by_store: Dict[str, pd.DataFrame],
    start: date,
    end: date,
) -> Tuple[Dict[str, pd.DataFrame], Dict[str, pd.DataFrame]]:
    detail_filtered = {
        abbr: filter_loyalty_detail_df(df, start, end)
        for abbr, df in (detail_by_store or {}).items()
    }
    register_filtered = {
        abbr: filter_loyalty_adjustment_df(df, start, end)
        for abbr, df in (register_by_store or {}).items()
    }
    return detail_filtered, register_filtered

def write_loyalty_detail_workbook(
    output_dir: Path,
    start_day: date,
    end_day: date,
    detail_by_store: Dict[str, pd.DataFrame],
    register_by_store: Optional[Dict[str, pd.DataFrame]] = None,
    match_text: str = LOYALTY_DISCOUNT_MATCH_TEXT,
) -> Optional[Path]:
    filtered_detail, filtered_register = filter_loyalty_maps_to_range(
        detail_by_store or {},
        register_by_store or {},
        start_day,
        end_day,
    )
    frames = [df for df in filtered_detail.values() if df is not None and not df.empty]
    register_frames = [df for df in filtered_register.values() if df is not None and not df.empty]
    if not frames and not register_frames:
        return None

    output_dir.mkdir(parents=True, exist_ok=True)
    out_path = output_dir / safe_filename(
        f"Detailed Discount Report - {match_text} - {start_day.isoformat()}_to_{end_day.isoformat()}.xlsx"
    )

    all_detail = pd.concat(frames, ignore_index=True) if frames else pd.DataFrame()
    all_register = pd.concat(register_frames, ignore_index=True) if register_frames else pd.DataFrame()

    with pd.ExcelWriter(out_path, engine="openpyxl") as writer:
        if not all_detail.empty:
            first_cols = [
                "store",
                "store_name",
                "order_time",
                "order_id",
                "customer_id",
                "customer_name",
                "customer_phone",
                "product",
                "category",
                "quantity",
                "loyalty_adjustment_discount",
                "edited_or_processed_by",
                "edited_or_processed_by_employee_id",
                "completed_by",
                "discount_approved_by",
                "discount_approval_source",
                "points_added_by",
            ]
            ordered_cols = [c for c in first_cols if c in all_detail.columns] + [c for c in all_detail.columns if c not in first_cols]
            all_detail = all_detail[ordered_cols]
            all_detail.to_excel(writer, sheet_name="Line Items", index=False)
            by_approver = all_detail.groupby("discount_approved_by", dropna=False).agg(
                line_items=("order_id", "size"),
                orders=("order_id", "nunique"),
                loyalty_discount=("loyalty_adjustment_discount", "sum"),
            ).reset_index().sort_values("loyalty_discount", ascending=False)
            by_approver.to_excel(writer, sheet_name="By Approver", index=False)

            by_added = all_detail.groupby("points_added_by", dropna=False).agg(
                line_items=("order_id", "size"),
                orders=("order_id", "nunique"),
                loyalty_discount=("loyalty_adjustment_discount", "sum"),
            ).reset_index().sort_values("loyalty_discount", ascending=False)
            by_added.to_excel(writer, sheet_name="By Points Added By", index=False)

            if "edited_or_processed_by" in all_detail.columns:
                by_editor = all_detail.groupby("edited_or_processed_by", dropna=False).agg(
                    line_items=("order_id", "size"),
                    orders=("order_id", "nunique"),
                    loyalty_discount=("loyalty_adjustment_discount", "sum"),
                ).reset_index().sort_values("loyalty_discount", ascending=False)
                by_editor.to_excel(writer, sheet_name="By Edited By", index=False)

            by_store = all_detail.groupby("store", dropna=False).agg(
                line_items=("order_id", "size"),
                orders=("order_id", "nunique"),
                loyalty_discount=("loyalty_adjustment_discount", "sum"),
            ).reset_index().sort_values("loyalty_discount", ascending=False)
            by_store.to_excel(writer, sheet_name="By Store", index=False)
        else:
            pd.DataFrame({"message": [f"No {match_text} line items found."]}).to_excel(writer, sheet_name="Line Items", index=False)

        if not all_register.empty:
            first_cols = [
                "store",
                "store_name",
                "date",
                "receipt_number",
                "transaction_id",
                "customer_id",
                "customer_name",
                "customer_phone",
                "points_delta",
                "ending_balance",
                "approving_manager",
                "adjusted_by",
                "points_added_by",
                "discount_approved_by",
                "employee",
                "employee_id",
                "type",
                "description",
                "amount",
                "source",
                "source_file",
            ]
            ordered_cols = [c for c in first_cols if c in all_register.columns] + [c for c in all_register.columns if c not in first_cols]
            all_register = all_register[ordered_cols]
            all_register_export = all_register.rename(columns={"approving_manager": "ApprovingManager"})
            all_register_export.to_excel(writer, sheet_name="Adjustment Audit", index=False)
            user_col = (
                "approving_manager"
                if "approving_manager" in all_register.columns
                else "adjusted_by"
                if "adjusted_by" in all_register.columns
                else "employee"
            )
            if user_col in all_register.columns:
                summary = all_register.copy()
                summary["_points_delta"] = to_number(summary["points_delta"]).fillna(0.0) if "points_delta" in summary.columns else 0.0
                summary["_customer_id"] = summary["customer_id"].fillna("").astype(str) if "customer_id" in summary.columns else ""
                by_user = summary.groupby(user_col, dropna=False).agg(
                    adjustments=(user_col, "size"),
                    customers=("_customer_id", "nunique"),
                    points_delta=("_points_delta", "sum"),
                ).reset_index()
                if user_col == "approving_manager":
                    by_user = by_user.rename(columns={user_col: "ApprovingManager"})
                by_user.to_excel(writer, sheet_name="By ApprovingManager", index=False)

    try:
        from openpyxl import load_workbook
        wb = load_workbook(out_path)
        for sheet in wb.worksheets:
            sheet.freeze_panes = "A2"
            for column_cells in sheet.columns:
                letter = column_cells[0].column_letter
                width = min(48, max(10, max(len(str(cell.value or "")) for cell in column_cells) + 2))
                sheet.column_dimensions[letter].width = width
        wb.save(out_path)
    except Exception:
        pass

    return out_path

def run_api_export_for_range(
    start_day: date,
    end_day: date,
    selected_store_codes: List[str],
    env_file: str,
    workers: int,
    loyalty_discount_name: str,
    loyalty_person: str = "",
    write_detail_workbook: bool = True,
) -> Tuple[Path, Dict[str, Path], Dict[str, pd.DataFrame], Dict[str, pd.DataFrame], Optional[Path]]:
    range_dir = RAW_ROOT / f"{start_day.isoformat()}_to_{end_day.isoformat()}"
    range_dir.mkdir(parents=True, exist_ok=True)
    detail_dir = LOYALTY_DETAIL_ROOT / f"{start_day.isoformat()}_to_{end_day.isoformat()}"

    env_map = dutchie_canonical_env_map(env_file)
    store_keys = dutchie_resolve_store_keys(env_map, selected_store_codes)
    integrator_key = dutchie_resolve_integrator_key(env_map)
    missing = [abbr for abbr in selected_store_codes if abbr not in store_keys]
    if missing:
        raise SystemExit(
            "Missing Dutchie API location key(s) for: "
            f"{', '.join(missing)}. Add them to {env_file} using names like DUTCHIE_API_KEY_MV or MV."
        )

    chunks = _iter_api_date_chunks(start_day, end_day)
    worker_count = dutchie_resolve_worker_count(workers, len(selected_store_codes))
    worker_label = "serial mode" if worker_count == 1 else f"{worker_count} store worker threads"
    print(f"[EXPORT API] Fetching Dutchie sales with {worker_label}")
    loyalty_start_day = month_start(end_day)

    def fetch_store(abbr: str) -> Tuple[str, Optional[Path], pd.DataFrame, pd.DataFrame, Optional[str]]:
        session = dutchie_create_session(store_keys[abbr], integrator_key)
        try:
            products_payload = dutchie_request_json(session, "/reporting/products", timeout=240)
            transactions_payload: List[Dict[str, Any]] = []
            register_payload: List[Dict[str, Any]] = []

            for idx, (chunk_start, chunk_end) in enumerate(chunks, start=1):
                from_utc, to_utc = dutchie_local_date_range_to_utc_strings(chunk_start.isoformat(), chunk_end.isoformat(), REPORT_TZ)
                print(f"[EXPORT API] {abbr} sales chunk {idx}/{len(chunks)}: {chunk_start.isoformat()} -> {chunk_end.isoformat()}")
                sales_payload = dutchie_request_json(
                    session,
                    "/reporting/transactions",
                    params={
                        "FromDateUTC": from_utc,
                        "ToDateUTC": to_utc,
                        "IncludeDetail": True,
                        "IncludeTaxes": True,
                        "IncludeOrderIds": True,
                        "IncludeFeesAndDonations": True,
                    },
                    timeout=240,
                )
                transactions_payload.extend(api_payload_records(sales_payload))

                try:
                    register_response = dutchie_request_json(
                        session,
                        "/reporting/register-transactions",
                        params={
                            "fromLastModifiedDateUTC": from_utc,
                            "toLastModifiedDateUTC": to_utc,
                        },
                        timeout=240,
                        max_attempts=2,
                    )
                    register_payload.extend(api_payload_records(register_response))
                except Exception as exc:
                    print(f"[EXPORT API] WARN: {abbr} register-transactions lookup skipped: {exc}")

                try:
                    register_adjustment_response = dutchie_request_json(
                        session,
                        "/reporting/register-adjustments",
                        params={
                            "fromLastModifiedDateUTC": from_utc,
                            "toLastModifiedDateUTC": to_utc,
                        },
                        timeout=240,
                        max_attempts=2,
                    )
                    register_payload.extend(api_payload_records(register_adjustment_response))
                except Exception as exc:
                    print(f"[EXPORT API] WARN: {abbr} register-adjustments lookup skipped: {exc}")

            df = normalize_api_sales_rows(abbr, transactions_payload, products_payload, loyalty_discount_name)
            if df.empty:
                return abbr, None, pd.DataFrame(), pd.DataFrame(), "no rows returned"

            register_lookup = build_register_transaction_lookup(register_payload)
            sales_customer_by_transaction = {
                str(_first_nonempty(tx.get("transactionId"), tx.get("globalId"), tx.get("referenceId"))): tx.get("customerId")
                for tx in transactions_payload
                if _first_nonempty(tx.get("transactionId"), tx.get("globalId"), tx.get("referenceId")) not in (None, "")
            }
            loyalty_customer_ids: List[Any] = []
            base_loyalty_detail = compute_loyalty_points_adjustment_detail(df, loyalty_start_day, end_day, loyalty_discount_name)
            if not base_loyalty_detail.empty and "customer_id" in base_loyalty_detail.columns:
                loyalty_customer_ids.extend(base_loyalty_detail["customer_id"].dropna().astype(str).tolist())

            for register_row in register_payload:
                tx_id = str(_first_nonempty(register_row.get("transactionId"), "")).strip()
                if tx_id and tx_id in sales_customer_by_transaction:
                    text = " ".join(str(v or "") for v in register_row.values() if not isinstance(v, (dict, list)))
                    if any(_contains_text(text, needle) for needle in [loyalty_discount_name, "loyalty", "points"] if needle):
                        loyalty_customer_ids.append(sales_customer_by_transaction.get(tx_id))

            customer_lookup = fetch_customers_for_ids(session, loyalty_customer_ids)

            dst_name = f"{abbr} - Sales API Export - {store_label(_store_name_from_abbr(abbr))} - {start_day.isoformat()}_to_{end_day.isoformat()}.xlsx"
            dst = range_dir / safe_filename(dst_name)
            with pd.ExcelWriter(dst, engine="openpyxl") as writer:
                df.to_excel(writer, index=False, sheet_name="Sales")

            detail = compute_loyalty_points_adjustment_detail(
                df,
                loyalty_start_day,
                end_day,
                loyalty_discount_name,
                customer_lookup=customer_lookup,
                register_lookup=register_lookup,
            )
            register_detail = normalize_register_loyalty_adjustments(
                abbr,
                register_payload,
                loyalty_discount_name,
                loyalty_person,
                customer_lookup=customer_lookup,
                sales_customer_by_transaction=sales_customer_by_transaction,
            )
            return abbr, dst, detail, register_detail, None
        except Exception as exc:
            return abbr, None, pd.DataFrame(), pd.DataFrame(), str(exc)
        finally:
            session.close()

    if worker_count == 1:
        results = [fetch_store(abbr) for abbr in selected_store_codes]
    else:
        results = []
        with ThreadPoolExecutor(max_workers=worker_count) as executor:
            future_map = {executor.submit(fetch_store, abbr): abbr for abbr in selected_store_codes}
            for future in as_completed(future_map):
                abbr = future_map[future]
                try:
                    results.append(future.result())
                except Exception as exc:
                    results.append((abbr, None, pd.DataFrame(), pd.DataFrame(), str(exc)))

    abbr_to_path: Dict[str, Path] = {}
    detail_by_store: Dict[str, pd.DataFrame] = {}
    register_by_store: Dict[str, pd.DataFrame] = {}
    failures: List[str] = []

    for abbr, path, detail, register_detail, error in sorted(results, key=lambda item: selected_store_codes.index(item[0]) if item[0] in selected_store_codes else 999):
        if error:
            failures.append(f"{abbr}: {error}")
            print(f"[EXPORT API] WARN: {abbr}: {error}")
            continue
        if path is not None:
            abbr_to_path[abbr] = path
            print(f"[EXPORT API] {abbr}: {path}")
        detail_by_store[abbr] = detail
        register_by_store[abbr] = register_detail

    if not abbr_to_path:
        raise SystemExit("Dutchie API export completed, but no usable sales exports were created.")
    if failures:
        print("[EXPORT API] WARN: " + "; ".join(failures))

    detail_workbook = None
    if write_detail_workbook:
        detail_workbook = write_loyalty_detail_workbook(
            detail_dir,
            loyalty_start_day,
            end_day,
            detail_by_store,
            register_by_store,
            match_text=loyalty_discount_name,
        )
        if detail_workbook:
            print(f"[EXPORT API] Detailed discount report: {detail_workbook}")
        else:
            print(f"[EXPORT API] No {loyalty_discount_name} rows found for detailed discount workbook.")

    return range_dir, abbr_to_path, detail_by_store, register_by_store, detail_workbook

def find_latest_raw_folder() -> Optional[Path]:
    if not RAW_ROOT.exists():
        return None
    folders = [p for p in RAW_ROOT.iterdir() if p.is_dir()]
    if not folders:
        return None
    return sorted(folders, key=lambda p: p.stat().st_mtime, reverse=True)[0]

def is_valid_excel_export(path: Path) -> bool:
    try:
        if not path.exists() or path.stat().st_size <= 0:
            return False
        if path.suffix.lower() == ".xlsx":
            return is_zipfile(path)
        return True
    except OSError:
        return False


###############################################################################
# Metrics
###############################################################################

METRIC_KEYS = [
    "net_revenue",
    "gross_sales",
    "tickets",
    "basket",
    "items",
    "items_per_ticket",
    "net_price_per_item",
    "discount",
    "discount_main",
    "loyalty_discount",
    "discount_rate",
    "profit",
    "margin",
    "cogs",
    "profit_real",
    "margin_real",
    "cogs_real",
    "returns_net",
    "returns_tickets",
    "weight_sold",
]

def normalize(df: pd.DataFrame) -> pd.DataFrame:
    date_col = find_col(df, COLUMN_CANDIDATES["date"])
    if not date_col:
        raise RuntimeError(f"Could not find date column. Columns: {list(df.columns)}")

    df = df.copy()
    df[date_col] = pd.to_datetime(df[date_col], errors="coerce")
    df = df[df[date_col].notna()]
    return df

def compute_daily_metrics(df: pd.DataFrame) -> pd.DataFrame:
    df = normalize(df)

    date_col = find_col(df, COLUMN_CANDIDATES["date"])
    tx_col = find_col(df, COLUMN_CANDIDATES["transaction_id"])

    qty_col = find_col(df, COLUMN_CANDIDATES["quantity"])
    net_col = find_col(df, COLUMN_CANDIDATES["net_sales"])
    gross_col = find_col(df, COLUMN_CANDIDATES["gross_sales"])

    disc_main_col = find_col(df, COLUMN_CANDIDATES["discount_main"])
    disc_loyal_col = find_col(df, COLUMN_CANDIDATES["discount_loyalty"])

    cogs_col = find_col(df, COLUMN_CANDIDATES["cogs"])
    profit_col = find_col(df, COLUMN_CANDIDATES["profit"])

    return_col = find_col(df, COLUMN_CANDIDATES["return_date"])
    weight_col = find_col(df, COLUMN_CANDIDATES["total_weight_sold"])

    if not net_col:
        raise RuntimeError(f"Could not find Net Sales column. Columns: {list(df.columns)}")

    df["_date"] = df[date_col].dt.date

    df["_net"] = to_number(df[net_col]).fillna(0).astype(float)
    df["_gross"] = to_number(df[gross_col]).fillna(0).astype(float) if gross_col else 0.0
    df["_qty"] = to_number(df[qty_col]).fillna(0).astype(float) if qty_col else 1.0

    df["_disc_main"] = to_number(df[disc_main_col]).fillna(0).astype(float) if disc_main_col else 0.0
    df["_disc_loyal"] = to_number(df[disc_loyal_col]).fillna(0).astype(float) if disc_loyal_col else 0.0
    df["_disc_total"] = (df["_disc_main"] + df["_disc_loyal"]).astype(float)

    # Kickback amount per row (if present)
    if "_deal_kickback_amt" in df.columns:
        df["_kickback_amt"] = to_number(df["_deal_kickback_amt"]).fillna(0).astype(float)
    else:
        df["_kickback_amt"] = 0.0

    # -------------------------
    # COGS: Real vs Kickback
    # -------------------------
    if "_cogs_raw" in df.columns:
        df["_cogs_real"] = to_number(df["_cogs_raw"]).fillna(0).astype(float)
    else:
        df["_cogs_real"] = to_number(df[cogs_col]).fillna(0).astype(float) if cogs_col else 0.0

    if "_cogs_adj" in df.columns:
        df["_cogs_kb"] = to_number(df["_cogs_adj"]).fillna(0).astype(float)
    else:
        df["_cogs_kb"] = df["_cogs_real"]

    # -------------------------
    # Profit: Real vs Kickback
    # -------------------------
    # Real profit: no kickback benefit
    if profit_col:
        df["_profit_real"] = to_number(df[profit_col]).fillna(0).astype(float)
    elif "_profit_adj" in df.columns and "_deal_kickback_amt" in df.columns:
        # reverse the kickback if we only have adjusted profit
        df["_profit_real"] = (to_number(df["_profit_adj"]).fillna(0) - df["_kickback_amt"]).astype(float)
    else:
        df["_profit_real"] = (df["_net"] - df["_cogs_real"]).astype(float)

    # Kickback profit: includes kickback benefit
    if "_profit_adj" in df.columns:
        df["_profit_kb"] = to_number(df["_profit_adj"]).fillna(0).astype(float)
    else:
        df["_profit_kb"] = (df["_profit_real"] + df["_kickback_amt"]).astype(float)

    # Keep legacy downstream behavior = kickback-adjusted
    df["_cogs"] = df["_cogs_kb"]
    df["_profit"] = df["_profit_kb"]

    df["_weight"] = to_number(df[weight_col]).fillna(0).astype(float) if weight_col else 0.0

    # Tickets
    if tx_col:
        tickets = df.groupby("_date")[tx_col].nunique().rename("tickets")
    else:
        tickets = df.groupby("_date").size().rename("tickets")
        print("[WARN] No Order ID column found; ticket count may be inaccurate.")

    daily = df.groupby("_date").agg(
        net_revenue=("_net", "sum"),
        gross_sales=("_gross", "sum"),
        items=("_qty", "sum"),
        discount=("_disc_total", "sum"),
        discount_main=("_disc_main", "sum"),
        loyalty_discount=("_disc_loyal", "sum"),

        # kickback-adjusted
        cogs=("_cogs", "sum"),
        profit=("_profit", "sum"),

        # real
        cogs_real=("_cogs_real", "sum"),
        profit_real=("_profit_real", "sum"),

        weight_sold=("_weight", "sum"),
    ).join(tickets)

    daily = daily.reset_index().rename(columns={"_date": "date"})

    # Returns
    if return_col:
        ret = df[df[return_col].notna()].copy()
        if not ret.empty:
            if tx_col:
                returns = ret.groupby("_date").agg(
                    returns_net=("_net", "sum"),
                    returns_tickets=(tx_col, "nunique"),
                ).reset_index().rename(columns={"_date": "date"})
            else:
                returns = ret.groupby("_date").agg(
                    returns_net=("_net", "sum"),
                    returns_tickets=("_net", "size"),
                ).reset_index().rename(columns={"_date": "date"})
            daily = daily.merge(returns, on="date", how="left")

    daily["returns_net"] = daily.get("returns_net", 0.0)
    daily["returns_tickets"] = daily.get("returns_tickets", 0.0)
    daily["returns_net"] = daily["returns_net"].fillna(0.0)
    daily["returns_tickets"] = daily["returns_tickets"].fillna(0.0)

    # Derived
    daily["basket"] = daily.apply(lambda r: r["net_revenue"] / r["tickets"] if r["tickets"] else 0.0, axis=1)
    daily["items_per_ticket"] = daily.apply(lambda r: r["items"] / r["tickets"] if r["tickets"] else 0.0, axis=1)
    daily["net_price_per_item"] = daily.apply(lambda r: r["net_revenue"] / r["items"] if r["items"] else 0.0, axis=1)

    # ✅ Both margins
    daily["margin"] = daily.apply(lambda r: r["profit"] / r["net_revenue"] if r["net_revenue"] else 0.0, axis=1)
    daily["margin_real"] = daily.apply(lambda r: r["profit_real"] / r["net_revenue"] if r["net_revenue"] else 0.0, axis=1)

    # discount_rate: prefer gross if available, else approximate gross = net + discount
    def _disc_rate(row):
        g = row["gross_sales"]
        if g:
            return row["discount"] / g
        approx_g = row["net_revenue"] + row["discount"]
        return row["discount"] / approx_g if approx_g else 0.0

    daily["discount_rate"] = daily.apply(_disc_rate, axis=1)

    for k in METRIC_KEYS:
        if k not in daily.columns:
            daily[k] = 0.0

    return daily.sort_values("date")

def slice_range(daily: pd.DataFrame, start: date, end: date) -> pd.DataFrame:
    if daily is None or daily.empty or "date" not in daily.columns:
        return pd.DataFrame(columns=(daily.columns if daily is not None else []))
    tmp = daily.copy()
    cmp_dates = pd.to_datetime(tmp["date"], errors="coerce").dt.date
    return tmp[(cmp_dates >= start) & (cmp_dates <= end)].copy()

def metrics_for_day(daily: pd.DataFrame, day: date) -> Dict[str, float]:
    row = slice_range(daily, day, day)
    if row.empty:
        return {k: 0.0 for k in METRIC_KEYS}
    r = row.iloc[0]
    return {k: float(r.get(k)) if pd.notna(r.get(k)) else 0.0 for k in METRIC_KEYS}

def metrics_for_range(daily: pd.DataFrame, start: date, end: date) -> Dict[str, float]:
    sub = slice_range(daily, start, end)
    if sub.empty:
        return {k: 0.0 for k in METRIC_KEYS}

    sum_fields = [
        "net_revenue", "gross_sales", "tickets", "items", "discount",
        "discount_main", "loyalty_discount",
        "cogs", "profit",
        "cogs_real", "profit_real",
        "returns_net", "returns_tickets",
        "weight_sold",
    ]
    out = {k: float(sub[k].sum()) if k in sub.columns else 0.0 for k in sum_fields}

    net = out["net_revenue"]
    gross = out["gross_sales"]
    tickets = out["tickets"]
    items = out["items"]
    profit_kb = out["profit"]
    profit_real = out.get("profit_real", profit_kb)
    disc = out["discount"]

    out["basket"] = net / tickets if tickets else 0.0
    out["items_per_ticket"] = items / tickets if tickets else 0.0
    out["net_price_per_item"] = net / items if items else 0.0

    # ✅ Both margins
    out["margin"] = profit_kb / net if net else 0.0
    out["margin_real"] = profit_real / net if net else 0.0

    if gross:
        out["discount_rate"] = disc / gross
    else:
        approx_g = net + disc
        out["discount_rate"] = disc / approx_g if approx_g else 0.0

    for k in METRIC_KEYS:
        out.setdefault(k, 0.0)

    return out


###############################################################################
# Breakdowns & summaries
###############################################################################

def _filter_df_date_range(df: pd.DataFrame, start: date, end: date) -> pd.DataFrame:
    date_col = find_col(df, COLUMN_CANDIDATES["date"])
    if not date_col:
        return df.iloc[0:0].copy()
    tmp = df.copy()
    tmp[date_col] = pd.to_datetime(tmp[date_col], errors="coerce")
    tmp = tmp[tmp[date_col].notna()]
    tmp["_date"] = tmp[date_col].dt.date
    return tmp[(tmp["_date"] >= start) & (tmp["_date"] <= end)].copy()

def _customer_activity_frame(df: pd.DataFrame) -> pd.DataFrame:
    if df is None or df.empty:
        return pd.DataFrame(columns=["_date", "_customer_id"])
    date_col = find_col(df, COLUMN_CANDIDATES["date"])
    customer_col = find_col(df, COLUMN_CANDIDATES["customer_id"])
    if not date_col or not customer_col:
        return pd.DataFrame(columns=["_date", "_customer_id"])

    tmp = df[[date_col, customer_col]].copy()
    tmp[date_col] = pd.to_datetime(tmp[date_col], errors="coerce")
    tmp = tmp[tmp[date_col].notna()]
    tmp["_date"] = tmp[date_col].dt.date
    tmp["_customer_id"] = tmp[customer_col].fillna("").astype(str).str.strip()
    invalid = {"", "0", "0.0", "nan", "none", "null", "anonymous"}
    tmp = tmp[~tmp["_customer_id"].str.lower().isin(invalid)]
    return tmp[["_date", "_customer_id"]].drop_duplicates()

def compute_customer_counts(df: pd.DataFrame, start: date, end: date) -> Dict[str, int]:
    activity = _customer_activity_frame(df)
    if activity.empty:
        return {"new": 0, "total": 0}

    period = activity[(activity["_date"] >= start) & (activity["_date"] <= end)]
    if period.empty:
        return {"new": 0, "total": 0}

    first_seen = activity.groupby("_customer_id")["_date"].min()
    period_customers = set(period["_customer_id"].unique())
    new_customers = {
        customer_id
        for customer_id in period_customers
        if start <= first_seen.get(customer_id, end) <= end
    }
    return {"new": len(new_customers), "total": len(period_customers)}

def fmt_new_total(counts: Dict[str, int]) -> str:
    return f"{int(counts.get('new', 0)):,} / {int(counts.get('total', 0)):,}"

def compute_breakdown_net(
    df: pd.DataFrame,
    group_candidates: List[str],
    start: date,
    end: date,
    top_n: Optional[int] = 10,
) -> Optional[pd.DataFrame]:
    group_col = find_col(df, group_candidates)
    net_col = find_col(df, COLUMN_CANDIDATES["net_sales"])
    if not group_col or not net_col:
        return None

    tmp = _filter_df_date_range(df, start, end)
    if tmp.empty:
        return pd.DataFrame(columns=[group_col, "net_revenue"])

    tmp["_net"] = to_number(tmp[net_col]).fillna(0)
    tmp[group_col] = tmp[group_col].fillna("Unknown").astype(str)

    out = tmp.groupby(group_col, as_index=False)["_net"].sum().rename(columns={"_net": "net_revenue"})
    out = out.sort_values("net_revenue", ascending=False)
    if top_n is not None:
        out = out.head(top_n)
    return out

def compute_breakdown_units(
    df: pd.DataFrame,
    group_candidates: List[str],
    start: date,
    end: date,
    top_n: Optional[int] = 10,
) -> Optional[pd.DataFrame]:
    group_col = find_col(df, group_candidates)
    qty_col = find_col(df, COLUMN_CANDIDATES["quantity"])
    if not group_col:
        return None

    tmp = _filter_df_date_range(df, start, end)
    if tmp.empty:
        return pd.DataFrame(columns=[group_col, "units_sold"])

    if qty_col:
        tmp["_qty"] = to_number(tmp[qty_col]).fillna(0).astype(float)
    else:
        # Fallback when quantity is missing in export.
        tmp["_qty"] = 1.0
    tmp[group_col] = tmp[group_col].fillna("Unknown").astype(str)

    out = tmp.groupby(group_col, as_index=False)["_qty"].sum().rename(columns={"_qty": "units_sold"})
    out = out.sort_values("units_sold", ascending=False)
    if top_n is not None:
        out = out.head(top_n)
    return out

def compute_brand_summary(
    df: pd.DataFrame,
    start: date,
    end: date,
    top_n: int = 10,
) -> Optional[pd.DataFrame]:
    """
    Brand is parsed from product name (before first '|').

    Returns:
      - margin        = kickback-adjusted margin
      - margin_real   = real margin (no kickback)
    """
    prod_col = find_col(df, COLUMN_CANDIDATES["product"])
    net_col = find_col(df, COLUMN_CANDIDATES["net_sales"])
    profit_col = find_col(df, COLUMN_CANDIDATES["profit"])
    cogs_col = find_col(df, COLUMN_CANDIDATES["cogs"])

    if not prod_col or not net_col:
        return None

    tmp = _filter_df_date_range(df, start, end)
    if tmp.empty:
        return None

    tmp["_net"] = to_number(tmp[net_col]).fillna(0).astype(float)

    # kickback amt
    if "_deal_kickback_amt" in tmp.columns:
        tmp["_kickback_amt"] = to_number(tmp["_deal_kickback_amt"]).fillna(0).astype(float)
    else:
        tmp["_kickback_amt"] = 0.0

    # cogs real
    if "_cogs_raw" in tmp.columns:
        tmp["_cogs_real"] = to_number(tmp["_cogs_raw"]).fillna(0).astype(float)
    else:
        tmp["_cogs_real"] = to_number(tmp[cogs_col]).fillna(0).astype(float) if cogs_col else 0.0

    # profit real
    if profit_col:
        tmp["_profit_real"] = to_number(tmp[profit_col]).fillna(0).astype(float)
    elif "_profit_adj" in tmp.columns and "_deal_kickback_amt" in tmp.columns:
        tmp["_profit_real"] = (to_number(tmp["_profit_adj"]).fillna(0) - tmp["_kickback_amt"]).astype(float)
    else:
        tmp["_profit_real"] = (tmp["_net"] - tmp["_cogs_real"]).astype(float)

    # profit kb
    if "_profit_adj" in tmp.columns:
        tmp["_profit_kb"] = to_number(tmp["_profit_adj"]).fillna(0).astype(float)
    else:
        tmp["_profit_kb"] = (tmp["_profit_real"] + tmp["_kickback_amt"]).astype(float)

    tmp["_brand"] = tmp[prod_col].apply(parse_brand_from_product)

    out = tmp.groupby("_brand", as_index=False).agg(
        net_revenue=("_net", "sum"),
        profit=("_profit_kb", "sum"),
        profit_real=("_profit_real", "sum"),
    )

    out["margin"] = out["profit"] / out["net_revenue"].replace({0: None})
    out["margin_real"] = out["profit_real"] / out["net_revenue"].replace({0: None})
    out["margin"] = out["margin"].fillna(0.0)
    out["margin_real"] = out["margin_real"].fillna(0.0)

    out = out.sort_values("net_revenue", ascending=False).head(top_n)
    out = out.rename(columns={"_brand": "brand"})
    return out

def compute_customer_type_summary(df: pd.DataFrame, start: date, end: date) -> Optional[pd.DataFrame]:
    type_col = find_col(df, COLUMN_CANDIDATES["customer_type"])
    net_col = find_col(df, COLUMN_CANDIDATES["net_sales"])
    tx_col = find_col(df, COLUMN_CANDIDATES["transaction_id"])
    if not type_col or not net_col or not tx_col:
        return None

    tmp = _filter_df_date_range(df, start, end)
    if tmp.empty:
        return None

    tmp["_net"] = to_number(tmp[net_col]).fillna(0)
    tmp[type_col] = tmp[type_col].fillna("Unknown").astype(str)

    out = tmp.groupby(type_col, as_index=False).agg(
        net_revenue=("_net", "sum"),
        tickets=(tx_col, "nunique"),
    )
    out["basket"] = out["net_revenue"] / out["tickets"].replace({0: None})
    out["basket"] = out["basket"].fillna(0.0)
    return out.sort_values("net_revenue", ascending=False)

def compute_budtender_summary(df: pd.DataFrame, start: date, end: date) -> Optional[pd.DataFrame]:
    emp_col = find_col(df, COLUMN_CANDIDATES["employee"])
    net_col = find_col(df, COLUMN_CANDIDATES["net_sales"])
    tx_col = find_col(df, COLUMN_CANDIDATES["transaction_id"])
    gross_col = find_col(df, COLUMN_CANDIDATES["gross_sales"])
    disc_main_col = find_col(df, COLUMN_CANDIDATES["discount_main"])
    disc_loyal_col = find_col(df, COLUMN_CANDIDATES["discount_loyalty"])

    if not emp_col or not net_col or not tx_col:
        return None

    tmp = _filter_df_date_range(df, start, end)
    if tmp.empty:
        return None

    tmp["_net"] = to_number(tmp[net_col]).fillna(0)
    tmp["_gross"] = to_number(tmp[gross_col]).fillna(0) if gross_col else 0.0
    tmp["_disc_main"] = to_number(tmp[disc_main_col]).fillna(0) if disc_main_col else 0.0
    tmp["_disc_loyal"] = to_number(tmp[disc_loyal_col]).fillna(0) if disc_loyal_col else 0.0
    tmp["_disc_total"] = tmp["_disc_main"] + tmp["_disc_loyal"]

    tmp[emp_col] = tmp[emp_col].fillna("Unknown").astype(str)

    out = tmp.groupby(emp_col, as_index=False).agg(
        net_revenue=("_net", "sum"),
        gross_sales=("_gross", "sum"),
        discount=("_disc_total", "sum"),
        tickets=(tx_col, "nunique"),
    )
    out["basket"] = out["net_revenue"] / out["tickets"].replace({0: None})
    out["basket"] = out["basket"].fillna(0.0)

    out["discount_rate"] = out.apply(
        lambda r: (r["discount"] / r["gross_sales"]) if r["gross_sales"]
        else (r["discount"] / (r["net_revenue"] + r["discount"]) if (r["net_revenue"] + r["discount"]) else 0.0),
        axis=1
    )

    out = out.sort_values("net_revenue", ascending=False).rename(columns={emp_col: "budtender"})
    return out

def compute_cart_value_distribution(df: pd.DataFrame, start: date, end: date) -> Optional[pd.DataFrame]:
    """
    Returns transaction cart counts grouped by cart-value buckets.
    Uses transaction-level net sales (sum of line items per transaction ID).
    """
    net_col = find_col(df, COLUMN_CANDIDATES["net_sales"])
    tx_col = find_col(df, COLUMN_CANDIDATES["transaction_id"])
    if not net_col:
        return None

    tmp = _filter_df_date_range(df, start, end)
    if tmp.empty:
        return pd.DataFrame(columns=["bucket", "count", "pct"])

    tmp["_net"] = to_number(tmp[net_col]).fillna(0.0).astype(float)

    if tx_col:
        raw_tx = tmp[tx_col]
        tx_txt = raw_tx.astype(str).str.strip()
        tx_missing = raw_tx.isna() | tx_txt.eq("") | tx_txt.str.lower().isin({"nan", "none"})
        tmp["_tx_key"] = tx_txt
        # Keep missing IDs unique so one bad export cell does not collapse unrelated carts.
        tmp.loc[tx_missing, "_tx_key"] = "__row_" + tmp.loc[tx_missing].index.astype(str)
        cart_totals = tmp.groupby("_tx_key", as_index=False)["_net"].sum()["_net"].astype(float)
    else:
        # Fallback: treat each row as a cart when transaction ID is unavailable.
        cart_totals = tmp["_net"].astype(float)

    cart_totals = cart_totals[cart_totals >= 0.0]
    total_carts = int(cart_totals.shape[0])

    rows: List[Dict[str, Any]] = []
    for bucket in CART_VALUE_BUCKETS:
        lower = float(bucket["lower"])
        upper = bucket.get("upper")
        lower_inclusive = bool(bucket.get("lower_inclusive", True))
        upper_inclusive = bool(bucket.get("upper_inclusive", False))

        lower_mask = (cart_totals >= lower) if lower_inclusive else (cart_totals > lower)
        if upper is None:
            mask = lower_mask
        elif upper_inclusive:
            mask = lower_mask & (cart_totals <= float(upper))
        else:
            mask = lower_mask & (cart_totals < float(upper))

        count = int(mask.sum())
        rows.append({
            "bucket": str(bucket["label"]),
            "count": count,
            "pct": (count / total_carts) if total_carts else 0.0,
        })

    out = pd.DataFrame(rows, columns=["bucket", "count", "pct"])
    out.attrs["total_carts"] = total_carts
    return out

def compute_category_summary(df: pd.DataFrame, start: date, end: date) -> Optional[pd.DataFrame]:
    """
    Category-level metrics.
    profit/margin use kickback-adjusted values.
    Also includes profit_real/margin_real (no kickback).
    """
    cat_col = find_col(df, COLUMN_CANDIDATES["category"])
    date_col = find_col(df, COLUMN_CANDIDATES["date"])
    net_col = find_col(df, COLUMN_CANDIDATES["net_sales"])
    gross_col = find_col(df, COLUMN_CANDIDATES["gross_sales"])
    qty_col = find_col(df, COLUMN_CANDIDATES["quantity"])
    disc_main_col = find_col(df, COLUMN_CANDIDATES["discount_main"])
    disc_loyal_col = find_col(df, COLUMN_CANDIDATES["discount_loyalty"])
    cogs_col = find_col(df, COLUMN_CANDIDATES["cogs"])
    profit_col = find_col(df, COLUMN_CANDIDATES["profit"])

    if not cat_col or not date_col or not net_col:
        return None

    tmp = _filter_df_date_range(df, start, end)
    if tmp.empty:
        return None

    tmp["_net"] = to_number(tmp[net_col]).fillna(0).astype(float)
    tmp["_gross"] = to_number(tmp[gross_col]).fillna(0).astype(float) if gross_col else 0.0
    tmp["_qty"] = to_number(tmp[qty_col]).fillna(0).astype(float) if qty_col else 1.0

    tmp["_disc_main"] = to_number(tmp[disc_main_col]).fillna(0).astype(float) if disc_main_col else 0.0
    tmp["_disc_loyal"] = to_number(tmp[disc_loyal_col]).fillna(0).astype(float) if disc_loyal_col else 0.0
    tmp["_disc"] = (tmp["_disc_main"] + tmp["_disc_loyal"]).astype(float)

    # kickback amt
    if "_deal_kickback_amt" in tmp.columns:
        tmp["_kickback_amt"] = to_number(tmp["_deal_kickback_amt"]).fillna(0).astype(float)
    else:
        tmp["_kickback_amt"] = 0.0

    # cogs real vs kb
    if "_cogs_raw" in tmp.columns:
        tmp["_cogs_real"] = to_number(tmp["_cogs_raw"]).fillna(0).astype(float)
    else:
        tmp["_cogs_real"] = to_number(tmp[cogs_col]).fillna(0).astype(float) if cogs_col else 0.0

    if "_cogs_adj" in tmp.columns:
        tmp["_cogs_kb"] = to_number(tmp["_cogs_adj"]).fillna(0).astype(float)
    else:
        tmp["_cogs_kb"] = tmp["_cogs_real"]

    # profit real vs kb
    if profit_col:
        tmp["_profit_real"] = to_number(tmp[profit_col]).fillna(0).astype(float)
    elif "_profit_adj" in tmp.columns and "_deal_kickback_amt" in tmp.columns:
        tmp["_profit_real"] = (to_number(tmp["_profit_adj"]).fillna(0) - tmp["_kickback_amt"]).astype(float)
    else:
        tmp["_profit_real"] = (tmp["_net"] - tmp["_cogs_real"]).astype(float)

    if "_profit_adj" in tmp.columns:
        tmp["_profit_kb"] = to_number(tmp["_profit_adj"]).fillna(0).astype(float)
    else:
        tmp["_profit_kb"] = (tmp["_net"] - tmp["_cogs_kb"]).astype(float)

    tmp[cat_col] = tmp[cat_col].fillna("Unknown").astype(str)

    out = tmp.groupby(cat_col, as_index=False).agg(
        net_revenue=("_net", "sum"),
        gross_sales=("_gross", "sum"),

        # kickback-adjusted
        profit=("_profit_kb", "sum"),
        cogs=("_cogs_kb", "sum"),

        # real
        profit_real=("_profit_real", "sum"),
        cogs_real=("_cogs_real", "sum"),

        discount=("_disc", "sum"),
        items=("_qty", "sum"),
    ).rename(columns={cat_col: "category"})

    total_net = float(out["net_revenue"].sum()) if not out.empty else 0.0
    total_profit = float(out["profit"].sum()) if not out.empty else 0.0

    out["pct_revenue"] = out["net_revenue"] / (total_net if total_net else 1.0)
    out["pct_profit"] = out["profit"] / (total_profit if total_profit else 1.0) if total_profit else 0.0

    def _disc_rate_row(r):
        if r["gross_sales"]:
            return r["discount"] / r["gross_sales"]
        approx_g = r["net_revenue"] + r["discount"]
        return r["discount"] / approx_g if approx_g else 0.0

    out["discount_rate"] = out.apply(_disc_rate_row, axis=1)

    # ✅ Both margins
    out["margin"] = out["profit"] / out["net_revenue"].replace({0: None})
    out["margin_real"] = out["profit_real"] / out["net_revenue"].replace({0: None})
    out["margin"] = out["margin"].fillna(0.0)
    out["margin_real"] = out["margin_real"].fillna(0.0)

    out["price_per_item"] = out["net_revenue"] / out["items"].replace({0: None})
    out["price_per_item"] = out["price_per_item"].fillna(0.0)

    out["profit_per_item"] = out["profit"] / out["items"].replace({0: None})
    out["profit_per_item"] = out["profit_per_item"].fillna(0.0)

    out["cogs_pct"] = out["cogs"] / out["net_revenue"].replace({0: None})
    out["cogs_pct"] = out["cogs_pct"].fillna(0.0)

    out = out.sort_values("net_revenue", ascending=False)
    return out

def compute_hourly_metrics(df: pd.DataFrame, day: date) -> Optional[pd.DataFrame]:
    """
    Hourly metrics for one day:
      - net_revenue, profit (kickback), profit_real, tickets, basket, margin (kickback), margin_real
    """
    date_col = find_col(df, COLUMN_CANDIDATES["date"])
    tx_col = find_col(df, COLUMN_CANDIDATES["transaction_id"])
    net_col = find_col(df, COLUMN_CANDIDATES["net_sales"])
    profit_col = find_col(df, COLUMN_CANDIDATES["profit"])
    cogs_col = find_col(df, COLUMN_CANDIDATES["cogs"])

    if not date_col or not net_col:
        return None

    tmp = df.copy()
    tmp[date_col] = pd.to_datetime(tmp[date_col], errors="coerce")
    tmp = tmp[tmp[date_col].notna()]
    tmp["_date"] = tmp[date_col].dt.date
    tmp = tmp[tmp["_date"] == day]
    if tmp.empty:
        return pd.DataFrame(columns=["hour", "net_revenue", "profit", "profit_real", "tickets", "basket", "margin", "margin_real"])

    tmp["_hour"] = tmp[date_col].dt.hour
    tmp["_net"] = to_number(tmp[net_col]).fillna(0).astype(float)

    # kickback amt
    if "_deal_kickback_amt" in tmp.columns:
        tmp["_kickback_amt"] = to_number(tmp["_deal_kickback_amt"]).fillna(0).astype(float)
    else:
        tmp["_kickback_amt"] = 0.0

    # cogs real
    if "_cogs_raw" in tmp.columns:
        tmp["_cogs_real"] = to_number(tmp["_cogs_raw"]).fillna(0).astype(float)
    else:
        tmp["_cogs_real"] = to_number(tmp[cogs_col]).fillna(0).astype(float) if cogs_col else 0.0

    # cogs kb
    if "_cogs_adj" in tmp.columns:
        tmp["_cogs_kb"] = to_number(tmp["_cogs_adj"]).fillna(0).astype(float)
    else:
        tmp["_cogs_kb"] = tmp["_cogs_real"]

    # profit real
    if profit_col:
        tmp["_profit_real"] = to_number(tmp[profit_col]).fillna(0).astype(float)
    elif "_profit_adj" in tmp.columns and "_deal_kickback_amt" in tmp.columns:
        tmp["_profit_real"] = (to_number(tmp["_profit_adj"]).fillna(0) - tmp["_kickback_amt"]).astype(float)
    else:
        tmp["_profit_real"] = (tmp["_net"] - tmp["_cogs_real"]).astype(float)

    # profit kb
    if "_profit_adj" in tmp.columns:
        tmp["_profit_kb"] = to_number(tmp["_profit_adj"]).fillna(0).astype(float)
    else:
        tmp["_profit_kb"] = (tmp["_net"] - tmp["_cogs_kb"]).astype(float)

    if tx_col:
        agg = tmp.groupby("_hour").agg(
            net_revenue=("_net", "sum"),
            profit=("_profit_kb", "sum"),
            profit_real=("_profit_real", "sum"),
            tickets=(tx_col, "nunique"),
        ).reset_index().rename(columns={"_hour": "hour"})
    else:
        agg = tmp.groupby("_hour").agg(
            net_revenue=("_net", "sum"),
            profit=("_profit_kb", "sum"),
            profit_real=("_profit_real", "sum"),
            tickets=("_net", "size"),
        ).reset_index().rename(columns={"_hour": "hour"})

    agg["basket"] = agg["net_revenue"] / agg["tickets"].replace({0: None})
    agg["basket"] = agg["basket"].fillna(0.0)

    agg["margin"] = agg["profit"] / agg["net_revenue"].replace({0: None})
    agg["margin_real"] = agg["profit_real"] / agg["net_revenue"].replace({0: None})
    agg["margin"] = agg["margin"].fillna(0.0)
    agg["margin_real"] = agg["margin_real"].fillna(0.0)

    return agg.sort_values("hour")


###############################################################################
# Charts (compact + visual)
###############################################################################

def _mpl_setup():
    plt.rcParams.update({
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

def _save_chart_image(buf: BytesIO) -> None:
    """
    Save chart bytes with lightweight defaults so PDFs open/download faster.
    Falls back to PNG if JPEG save is unavailable.
    """
    try:
        plt.savefig(
            buf,
            format="jpeg",
            dpi=CHART_DPI,
            bbox_inches="tight",
            pad_inches=0.14,
            pil_kwargs={
                "quality": CHART_JPEG_QUALITY,
                "optimize": True,
                "progressive": True,
                "subsampling": 0,
            },
        )
    except TypeError:
        # Older matplotlib versions may not support pil_kwargs.
        plt.savefig(buf, format="jpeg", dpi=CHART_DPI, bbox_inches="tight", pad_inches=0.14)
    except Exception:
        buf.seek(0)
        buf.truncate(0)
        plt.savefig(buf, format="png", dpi=CHART_DPI, bbox_inches="tight", pad_inches=0.14)

def chart_trend_bar_with_labels(
    daily: pd.DataFrame,
    value_col: str,
    title: str,
    days: int = 14,
    kind: str = "money",
    figsize: Tuple[float, float] = (7.3, 3.2),
) -> BytesIO:
    _mpl_setup()
    buf = BytesIO()
    if daily is None or daily.empty or value_col not in daily.columns:
        return buf

    tail = daily.tail(days).copy()
    labels = [f"{d.month}/{d.day} {dow_short(d)}" for d in tail["date"].tolist()]
    values = tail[value_col].fillna(0).astype(float).tolist()
    dense_labels = len(values) >= 10

    fig, ax = plt.subplots(figsize=figsize)
    x = list(range(len(labels)))
    ax.bar(
        x,
        values,
        color=HEX_GREEN,
        edgecolor="#047857",
        linewidth=0.45,
        alpha=0.95,
        zorder=2,
    )
    if values:
        ax.plot(x, values, color="#111827", linewidth=1.25, alpha=0.78, marker="o", markersize=3.2, zorder=3)

    ax.set_title(title, pad=22)
    label_font = 7.2 if len(labels) > 10 else 7.6
    ax.set_xticks(x)
    ax.set_xticklabels(labels, rotation=35, ha="right", fontsize=label_font)
    ax.tick_params(axis="x", pad=4)
    ax.tick_params(axis="y", pad=4)
    ax.grid(True, axis="y", zorder=0)
    ax.spines["top"].set_visible(False)
    ax.spines["right"].set_visible(False)
    ax.margins(x=0.01)
    if values:
        vmin = min(values)
        vmax = max(values)
        y_low = min(0.0, vmin * 1.08)
        y_high = (vmax * (1.62 if dense_labels else 1.44)) if vmax > 0 else 1.0
        if y_high <= y_low:
            y_high = y_low + 1.0
        ax.set_ylim(y_low, y_high)

    fig.subplots_adjust(left=0.06, right=0.992, bottom=0.31, top=0.76)

    if values:
        vmax = max(values)
        base_pad = (vmax * 0.03) if vmax else 1.0
        alt_pad = base_pad * 2.0 if dense_labels else base_pad * 1.35
        for i, v in enumerate(values):
            if kind == "money":
                txt = money_compact(v) if dense_labels else money(v)
            elif kind == "int":
                txt = f"{int(v):,}"
            else:
                txt = pct1(v)
            label_y = v + (alt_pad if dense_labels and (i % 2 == 1) else base_pad)
            ax.text(
                i,
                label_y,
                txt,
                ha="center",
                va="bottom",
                fontsize=7.0 if dense_labels else 8.1,
                fontweight="bold",
                color="#111827",
                clip_on=False,
            )

    _save_chart_image(buf)
    plt.close(fig)
    buf.seek(0)
    return buf

def chart_rank_barh(
    df: pd.DataFrame,
    label_col: str,
    value_col: str,
    title: str,
    top_n: int = 10,
    figsize: Tuple[float, float] = (7.3, 3.15),
) -> BytesIO:
    _mpl_setup()
    buf = BytesIO()
    if df is None or df.empty:
        return buf

    d = df.head(top_n).copy()
    labels = d[label_col].astype(str).tolist()[::-1]
    values = d[value_col].astype(float).tolist()[::-1]

    fig, ax = plt.subplots(figsize=figsize)
    y = list(range(len(labels)))
    bars = ax.barh(
        y,
        values,
        color=HEX_GREEN,
        edgecolor="#047857",
        linewidth=0.45,
        alpha=0.95,
        zorder=2,
    )
    ax.set_title(title, pad=16)
    ax.set_yticks(y)
    ax.set_yticklabels(labels, fontsize=8.2)
    ax.tick_params(axis="x", pad=5)
    ax.grid(True, axis="x", zorder=0)
    ax.spines["top"].set_visible(False)
    ax.spines["right"].set_visible(False)
    ax.spines["left"].set_visible(False)
    ax.margins(y=0.08)

    max_val = max(values) if values else 0.0
    pad = max_val * 0.012 if max_val else 1.0
    for bar, val in zip(bars, values):
        ax.text(
            val + pad,
            bar.get_y() + (bar.get_height() / 2.0),
            money(val),
            va="center",
            ha="left",
            fontsize=7.4,
            fontweight="bold",
            color="#111827",
            clip_on=False,
            bbox={"facecolor": "#FFFFFF", "edgecolor": "#E5E7EB", "boxstyle": "round,pad=0.12", "alpha": 0.72},
        )
    if max_val > 0:
        # Keep bars visually fuller; avoid excess empty right-side space.
        ax.set_xlim(0, max_val * 1.18)

    max_label_len = max((len(lbl) for lbl in labels), default=10)
    left_margin = min(max(0.14, 0.095 + (max_label_len * 0.006)), 0.24)
    fig.subplots_adjust(left=left_margin, right=0.97, bottom=0.16, top=0.80)

    _save_chart_image(buf)
    plt.close(fig)
    buf.seek(0)
    return buf

def chart_cart_value_distribution(
    dist_df: pd.DataFrame,
    title: str,
    figsize: Tuple[float, float] = (7.3, 4.35),
) -> BytesIO:
    _mpl_setup()
    buf = BytesIO()
    if dist_df is None or dist_df.empty:
        return buf

    d = dist_df.copy()
    labels = d["bucket"].astype(str).tolist()
    counts = d["count"].fillna(0).astype(float).tolist()
    shares = d["pct"].fillna(0).astype(float).tolist() if "pct" in d.columns else [0.0] * len(labels)
    if not labels:
        return buf

    palette = [
        "#D1FAE5", "#A7F3D0", "#6EE7B7", "#34D399",
        "#10B981", "#059669", "#047857", "#065F46",
    ]
    bar_colors = [palette[i % len(palette)] for i in range(len(labels))]

    fig, ax = plt.subplots(figsize=figsize)
    x = np.arange(len(labels))
    bars = ax.bar(
        x,
        counts,
        width=0.74,
        color=bar_colors,
        edgecolor="#065F46",
        linewidth=0.55,
        zorder=2,
    )

    ax.set_title(title, pad=18)
    ax.set_ylabel("Cart Count", fontsize=8.1)
    ax.set_xticks(x)
    ax.set_xticklabels(labels, fontsize=8.2)
    ax.tick_params(axis="x", pad=5)
    ax.tick_params(axis="y", pad=4)
    ax.grid(True, axis="y", zorder=0)
    ax.spines["top"].set_visible(False)
    ax.spines["right"].set_visible(False)
    ax.margins(x=0.01)

    y_max = max(counts) if counts else 0.0
    if y_max > 0:
        ax.set_ylim(0, y_max * 1.33)
    else:
        ax.set_ylim(0, 1.0)

    label_pad = (y_max * 0.02) if y_max else 0.07
    for bar, val, pct in zip(bars, counts, shares):
        ax.text(
            bar.get_x() + (bar.get_width() / 2.0),
            val + label_pad,
            f"{int(val):,}\n{pct1(pct)}",
            ha="center",
            va="bottom",
            fontsize=7.9,
            fontweight="bold",
            color="#111827",
            clip_on=False,
        )

    fig.subplots_adjust(left=0.07, right=0.992, bottom=0.18, top=0.82)
    _save_chart_image(buf)
    plt.close(fig)
    buf.seek(0)
    return buf

def chart_hourly_shadow_compare(
    this_day: pd.DataFrame,
    last_week: pd.DataFrame,
    metric_col: str,
    title: str,
    kind: str,
    figsize: Tuple[float, float] = (7.3, 2.85),
) -> BytesIO:
    _mpl_setup()
    buf = BytesIO()

    if this_day is None or last_week is None:
        return buf
    if this_day.empty and last_week.empty:
        return buf

    hours = sorted(set(this_day["hour"].tolist()) | set(last_week["hour"].tolist()))
    if not hours:
        return buf

    this_map = {int(h): float(v) for h, v in zip(this_day["hour"], this_day[metric_col])} if (metric_col in this_day.columns) else {}
    last_map = {int(h): float(v) for h, v in zip(last_week["hour"], last_week[metric_col])} if (metric_col in last_week.columns) else {}

    this_vals = [this_map.get(h, 0.0) for h in hours]
    last_vals = [last_map.get(h, 0.0) for h in hours]

    if kind == "pct":
        this_vals_plot = [v * 100.0 for v in this_vals]
        last_vals_plot = [v * 100.0 for v in last_vals]
    else:
        this_vals_plot = this_vals
        last_vals_plot = last_vals

    x = list(range(len(hours)))

    fig, ax = plt.subplots(figsize=figsize)
    ax.bar(
        x, last_vals_plot, width=0.72, color=HEX_YELLOW, alpha=0.35,
        edgecolor=HEX_BLACK, linewidth=0.4, label="Last Week", zorder=1,
    )
    ax.bar(
        x, this_vals_plot, width=0.46, color=HEX_GREEN, alpha=1.0,
        edgecolor=HEX_BLACK, linewidth=0.3, label="Report Day", zorder=2,
    )

    if len(hours) > 14:
        tick_idx = [i for i, h in enumerate(hours) if (h % 2 == 0) or (i == len(hours) - 1)]
        ax.set_xticks(tick_idx)
        ax.set_xticklabels([fmt_hour_ampm(hours[i]) for i in tick_idx], fontsize=8.0)
    else:
        ax.set_xticks(x)
        ax.set_xticklabels([fmt_hour_ampm(h) for h in hours], fontsize=8.0)
    ax.set_title(title, pad=18)
    ax.grid(True, axis="y", zorder=0)
    ax.spines["top"].set_visible(False)
    ax.spines["right"].set_visible(False)
    y_max = max(this_vals_plot + last_vals_plot + [0.0])
    if y_max > 0:
        ax.set_ylim(0, y_max * 1.24)
    ax.legend(
        loc="lower left",
        bbox_to_anchor=(0.0, 1.03),
        ncol=2,
        frameon=False,
        fontsize=7.0,
        borderaxespad=0.0,
    )
    fig.subplots_adjust(left=0.06, right=0.992, bottom=0.24, top=0.78)

    _save_chart_image(buf)
    plt.close(fig)
    buf.seek(0)
    return buf


###############################################################################
# PDF visuals: KPI + tables + category "bar cells"
###############################################################################

def build_styles() -> Dict[str, Any]:
    styles = getSampleStyleSheet()

    styles.add(ParagraphStyle(
        name="TitleBig",
        parent=styles["Title"],
        fontName=BASE_FONT_BOLD,
        fontSize=16.5,
        textColor=THEME["black"],
        spaceAfter=3,
    ))
    styles.add(ParagraphStyle(
        name="Muted",
        parent=styles["Normal"],
        fontName=BASE_FONT,
        fontSize=8.6,
        textColor=THEME["muted"],
        leading=10.2,
    ))
    styles.add(ParagraphStyle(
        name="Section",
        parent=styles["Heading2"],
        fontName=BASE_FONT_BOLD,
        fontSize=11.0,
        textColor=THEME["black"],
        spaceBefore=5,
        spaceAfter=3,
    ))
    styles.add(ParagraphStyle(
        name="KpiLabel",
        parent=styles["Normal"],
        fontName=BASE_FONT_BOLD,
        fontSize=8.3,
        textColor=THEME["black"],
        leading=9.6,
    ))
    styles.add(ParagraphStyle(
        name="KpiValue",
        parent=styles["Normal"],
        fontName=BASE_FONT_BOLD,
        fontSize=13.6,
        textColor=THEME["black"],
        leading=14.6,
    ))
    styles.add(ParagraphStyle(
        name="KpiDelta",
        parent=styles["Normal"],
        fontName=BASE_FONT,
        fontSize=8.2,
        textColor=THEME["muted"],
        leading=9.6,
    ))
    styles.add(ParagraphStyle(
        name="Small",
        parent=styles["Normal"],
        fontName=BASE_FONT,
        fontSize=8.5,
        leading=10.2,
        textColor=THEME["black"],
    ))
    styles.add(ParagraphStyle(
        name="Tiny",
        parent=styles["Normal"],
        fontName=BASE_FONT,
        fontSize=7.8,
        leading=9.4,
        textColor=THEME["muted"],
    ))
    styles.add(ParagraphStyle(
        name="CustomerBlockTitle",
        parent=styles["Normal"],
        fontName=BASE_FONT_BOLD,
        fontSize=9.1,
        leading=10.5,
        textColor=THEME["yellow"],
    ))
    styles.add(ParagraphStyle(
        name="CustomerBlockNote",
        parent=styles["Normal"],
        fontName=BASE_FONT_BOLD,
        fontSize=8.1,
        leading=10,
        textColor=colors.white,
        alignment=2,
    ))
    styles.add(ParagraphStyle(
        name="CustomerMetricLabel",
        parent=styles["Normal"],
        fontName=BASE_FONT_BOLD,
        fontSize=7.4,
        leading=8.8,
        textColor=THEME["muted"],
    ))
    styles.add(ParagraphStyle(
        name="CustomerMetricValue",
        parent=styles["Normal"],
        fontName=BASE_FONT_BOLD,
        fontSize=18.5,
        leading=20.0,
        textColor=THEME["black"],
    ))
    styles.add(ParagraphStyle(
        name="CustomerMetricValueSmall",
        parent=styles["Normal"],
        fontName=BASE_FONT_BOLD,
        fontSize=14.0,
        leading=15.5,
        textColor=THEME["black"],
    ))
    styles.add(ParagraphStyle(
        name="CustomerMetricDetail",
        parent=styles["Normal"],
        fontName=BASE_FONT,
        fontSize=7.5,
        leading=9.0,
        textColor=THEME["muted"],
    ))
    return styles

def _arrow(diff: float) -> str:
    if USE_UNICODE_ARROWS:
        return "▲" if diff >= 0 else "▼"
    return "+" if diff >= 0 else "-"

def delta_html_currency(current: float, baseline: float, label: str) -> str:
    if baseline == 0:
        return f"<font color='#374151'>vs {label}: n/a</font>"
    diff = current - baseline
    pct = diff / baseline
    arrow = _arrow(diff)
    color = "#00AE6F" if diff >= 0 else "#111827"
    return f"<font color='{color}'>{arrow} {fmt_signed_money(diff)} ({pct1(pct)})</font> <font color='#374151'>vs {label}</font>"

def delta_html_int(current: float, baseline: float, label: str) -> str:
    if baseline == 0:
        return f"<font color='#374151'>vs {label}: n/a</font>"
    diff = current - baseline
    pct = diff / baseline
    arrow = _arrow(diff)
    color = "#00AE6F" if diff >= 0 else "#111827"
    return f"<font color='{color}'>{arrow} {fmt_signed_int(diff)} ({pct1(pct)})</font> <font color='#374151'>vs {label}</font>"

def delta_html_pp(current: float, baseline: float, label: str) -> str:
    if baseline == 0 and current == 0:
        return f"<font color='#374151'>vs {label}: n/a</font>"
    diff = current - baseline
    arrow = _arrow(diff)
    color = "#00AE6F" if diff >= 0 else "#111827"
    return f"<font color='{color}'>{arrow} {pp1(diff)}</font> <font color='#374151'>vs {label}</font>"

def kpi_cell(styles, label: str, value: str, delta_html: str) -> List[Paragraph]:
    return [
        Paragraph(label, styles["KpiLabel"]),
        Paragraph(value, styles["KpiValue"]),
        Paragraph(delta_html, styles["KpiDelta"]),
    ]

def build_kpi_grid(styles, cells: List[List[Paragraph]], cols: int = 4) -> Table:
    while len(cells) % cols != 0:
        cells.append(kpi_cell(styles, "", "", ""))

    rows = [cells[i:i+cols] for i in range(0, len(cells), cols)]
    t = Table(rows, colWidths=[(7.6 * inch) / cols] * cols)

    t.setStyle(TableStyle([
        ("BACKGROUND", (0, 0), (-1, -1), THEME["light_bg"]),
        ("BOX", (0, 0), (-1, -1), 0.6, THEME["border"]),
        ("INNERGRID", (0, 0), (-1, -1), 0.4, THEME["border"]),
        ("LEFTPADDING", (0, 0), (-1, -1), 7),
        ("RIGHTPADDING", (0, 0), (-1, -1), 7),
        ("TOPPADDING", (0, 0), (-1, -1), 6),
        ("BOTTOMPADDING", (0, 0), (-1, -1), 6),
        ("VALIGN", (0, 0), (-1, -1), "TOP"),
    ]))
    return t

def build_table(headers: List[Any], rows: List[List[Any]], col_widths: Optional[List[float]] = None) -> Table:
    data = [headers] + rows
    t = Table(data, colWidths=col_widths, repeatRows=1)

    t.setStyle(TableStyle([
        ("BACKGROUND", (0, 0), (-1, 0), THEME["black"]),
        ("TEXTCOLOR", (0, 0), (-1, 0), THEME["yellow"]),
        ("FONTNAME", (0, 0), (-1, 0), BASE_FONT_BOLD),
        ("FONTNAME", (0, 1), (-1, -1), BASE_FONT),
        ("FONTSIZE", (0, 0), (-1, -1), 8.5),
        ("GRID", (0, 0), (-1, -1), 0.4, THEME["border"]),
        ("ROWBACKGROUNDS", (0, 1), (-1, -1), [colors.white, THEME["row_alt"]]),
        ("LEFTPADDING", (0, 0), (-1, -1), 5),
        ("RIGHTPADDING", (0, 0), (-1, -1), 5),
        ("TOPPADDING", (0, 0), (-1, -1), 3),
        ("BOTTOMPADDING", (0, 0), (-1, -1), 3),
        ("VALIGN", (0, 0), (-1, -1), "MIDDLE"),
    ]))
    return t

def _pdf_text(value: Any, style: ParagraphStyle) -> Paragraph:
    text = str(value if value not in (None, "nan", "NaT") else "").strip()
    text = text.replace("&", "&amp;").replace("<", "&lt;").replace(">", "&gt;")
    return Paragraph(text, style)

def build_customer_counts_table(
    styles,
    today_counts: Dict[str, int],
    mtd_counts: Dict[str, int],
) -> Table:
    rows = [
        ["Today", f"{int(today_counts.get('new', 0)):,}", f"{int(today_counts.get('total', 0)):,}", fmt_new_total(today_counts)],
        ["MTD", f"{int(mtd_counts.get('new', 0)):,}", f"{int(mtd_counts.get('total', 0)):,}", fmt_new_total(mtd_counts)],
    ]
    return build_table(
        ["Customers", "New", "Total", "New / Total"],
        rows,
        [2.30 * inch, 1.25 * inch, 1.25 * inch, 2.50 * inch],
    )

def _customer_count_parts(counts: Dict[str, int]) -> Tuple[int, int, int, float]:
    new = max(int(counts.get("new", 0) or 0), 0)
    total = max(int(counts.get("total", 0) or 0), 0)
    returning = max(total - new, 0)
    share = (new / total) if total else 0.0
    return new, total, returning, share

def _customer_metric_cell(
    styles,
    label: str,
    value: str,
    detail: str,
    *,
    large: bool = False,
) -> List[Paragraph]:
    value_style = styles["CustomerMetricValue"] if large else styles["CustomerMetricValueSmall"]
    return [
        Paragraph(label, styles["CustomerMetricLabel"]),
        Paragraph(value, value_style),
        Paragraph(detail, styles["CustomerMetricDetail"]),
    ]

def build_customer_growth_block(
    styles,
    today_counts: Dict[str, int],
    mtd_counts: Dict[str, int],
    width: float = 7.6 * inch,
) -> Table:
    today_new, today_total, today_returning, today_share = _customer_count_parts(today_counts)
    mtd_new, mtd_total, mtd_returning, mtd_share = _customer_count_parts(mtd_counts)

    rows = [
        [
            Paragraph("CUSTOMER GROWTH", styles["CustomerBlockTitle"]),
            Paragraph("New customer mix", styles["CustomerBlockNote"]),
            "",
            "",
        ],
        [
            _customer_metric_cell(
                styles,
                "NEW CUSTOMERS TODAY",
                f"{today_new:,}",
                f"{pct1(today_share)} of {today_total:,} customers",
                large=True,
            ),
            _customer_metric_cell(
                styles,
                "TODAY CUSTOMER BASE",
                f"{today_total:,}",
                f"{today_returning:,} returning | {today_new:,} new",
            ),
            _customer_metric_cell(
                styles,
                "NEW CUSTOMERS MTD",
                f"{mtd_new:,}",
                f"{pct1(mtd_share)} of {mtd_total:,} customers",
            ),
            _customer_metric_cell(
                styles,
                "MTD CUSTOMER BASE",
                f"{mtd_total:,}",
                f"{mtd_returning:,} returning | {mtd_new:,} new",
            ),
        ],
    ]
    col_widths = [2.10 * inch, 1.83 * inch, 1.83 * inch, width - (5.76 * inch)]
    t = Table(rows, colWidths=col_widths)
    t.setStyle(TableStyle([
        ("SPAN", (1, 0), (-1, 0)),
        ("BACKGROUND", (0, 0), (-1, 0), THEME["black"]),
        ("BACKGROUND", (0, 1), (0, 1), THEME["yellow"]),
        ("BACKGROUND", (1, 1), (-1, 1), colors.white),
        ("BOX", (0, 0), (-1, -1), 0.7, THEME["black"]),
        ("INNERGRID", (0, 1), (-1, -1), 0.45, THEME["border"]),
        ("LINEBELOW", (0, 0), (-1, 0), 0.8, THEME["black"]),
        ("LEFTPADDING", (0, 0), (-1, 0), 7),
        ("RIGHTPADDING", (0, 0), (-1, 0), 7),
        ("TOPPADDING", (0, 0), (-1, 0), 4),
        ("BOTTOMPADDING", (0, 0), (-1, 0), 4),
        ("LEFTPADDING", (0, 1), (-1, 1), 7),
        ("RIGHTPADDING", (0, 1), (-1, 1), 7),
        ("TOPPADDING", (0, 1), (-1, 1), 7),
        ("BOTTOMPADDING", (0, 1), (-1, 1), 7),
        ("VALIGN", (0, 0), (-1, -1), "MIDDLE"),
    ]))
    return t

def _clean_authorization_person(value: Any) -> str:
    if value is None:
        return ""
    try:
        if pd.isna(value):
            return ""
    except Exception:
        pass
    text = str(value).strip()
    if not text:
        return ""
    if text.lower() in {"nan", "none", API_FIELD_UNAVAILABLE.lower(), "api n/a"}:
        return ""
    return text

def loyalty_authorized_by(row: Dict[str, Any]) -> str:
    authorized_by = _first_nonempty(
        _clean_authorization_person(row.get("discount_approved_by")),
        _clean_authorization_person(row.get("approved_by")),
        _clean_authorization_person(row.get("approving_manager")),
        _clean_authorization_person(row.get("adjusted_by")),
    )
    return str(authorized_by).strip() or "API n/a"

def build_loyalty_detail_section(
    styles,
    detail_df: pd.DataFrame,
    title: str,
    include_store: bool = False,
    max_rows: int = LOYALTY_PDF_MAX_ROWS,
    match_text: str = LOYALTY_DISCOUNT_MATCH_TEXT,
) -> List[Any]:
    if detail_df is None or detail_df.empty:
        return []

    d = detail_df.copy()
    d = d.sort_values(["order_time", "order_id", "product"], ascending=[True, True, True])
    total_rows = len(d)
    total_discount = float(pd.to_numeric(d.get("loyalty_adjustment_discount", 0.0), errors="coerce").fillna(0.0).sum())
    shown = d.head(max_rows)

    headers = ["Date", "Order", "Customer", "Product", "Qty", "Disc", "Discount Approved By"]
    widths = [0.92 * inch, 0.86 * inch, 1.28 * inch, 1.95 * inch, 0.34 * inch, 0.58 * inch, 1.67 * inch]
    if include_store:
        headers.insert(0, "Store")
        widths = [0.40 * inch, 0.82 * inch, 0.78 * inch, 0.98 * inch, 1.70 * inch, 0.30 * inch, 0.54 * inch, 2.08 * inch]

    rows: List[List[Any]] = []
    for row in shown.to_dict("records"):
        order_time = pd.to_datetime(row.get("order_time"), errors="coerce")
        date_text = order_time.strftime("%m/%d %I:%M%p") if pd.notna(order_time) else ""
        authorized_by = loyalty_authorized_by(row)
        base = [
            date_text,
            str(row.get("order_id", "")),
            _pdf_text(row.get("customer_name", row.get("customer_id", "")), styles["Tiny"]),
            _pdf_text(row.get("product", ""), styles["Tiny"]),
            f"{as_float(row.get('quantity')):,.1f}",
            money2(as_float(row.get("loyalty_adjustment_discount"))),
            _pdf_text(authorized_by, styles["Tiny"]),
        ]
        if include_store:
            base.insert(0, str(row.get("store", "")))
        rows.append(base)

    section: List[Any] = [
        CondPageBreak(2.5 * inch),
        Paragraph(title, styles["TitleBig"]),
        Paragraph(
            f"{total_rows:,} line item(s) • Total {match_text}: {money2(total_discount)}"
            + (f" • Showing first {len(shown):,}" if total_rows > len(shown) else ""),
            styles["Tiny"],
        ),
        Spacer(1, SPACER["xs"]),
        build_table(headers, rows, widths),
    ]
    return section

def build_register_adjustment_section(
    styles,
    register_df: pd.DataFrame,
    title: str,
    include_store: bool = False,
    max_rows: int = 40,
) -> List[Any]:
    if register_df is None or register_df.empty:
        return []

    d = register_df.copy()
    if "date" in d.columns:
        d = d.sort_values("date", na_position="last")
    total_rows = len(d)
    shown = d.head(max_rows)

    points_total = float(to_number(d["points_delta"]).fillna(0.0).sum()) if "points_delta" in d.columns else 0.0
    headers = ["Date", "Customer", "Points/$", "ApprovingManager", "Reason"]
    widths = [0.95 * inch, 1.55 * inch, 0.65 * inch, 1.45 * inch, 2.70 * inch]
    if include_store:
        headers.insert(0, "Store")
        widths = [0.42 * inch, 0.82 * inch, 1.25 * inch, 0.58 * inch, 1.30 * inch, 3.23 * inch]

    rows: List[List[Any]] = []
    for row in shown.to_dict("records"):
        dt = pd.to_datetime(row.get("date"), errors="coerce")
        date_text = dt.strftime("%m/%d %I:%M%p") if pd.notna(dt) else ""
        points_value = row.get("points_delta", "")
        points_missing = pd.isna(points_value) or str(points_value).strip() == ""
        points_num = as_float(points_value, default=float("nan"))
        if not points_missing and not np.isnan(points_num):
            amount_text = f"{points_num:,.0f} pts"
        elif not (pd.isna(row.get("amount", "")) or str(row.get("amount", "")).strip() == ""):
            amount_text = money2(as_float(row.get("amount")))
        else:
            amount_text = ""
        approving_manager = _first_nonempty(
            row.get("approving_manager"),
            row.get("discount_approved_by"),
            row.get("adjusted_by"),
            row.get("employee"),
            row.get("points_added_by"),
            API_FIELD_UNAVAILABLE,
        )
        customer = _first_nonempty(row.get("customer_name"), row.get("customer_id"), "")
        reason = _first_nonempty(row.get("description"), row.get("type"), row.get("raw_match"), "")
        base = [
            date_text,
            _pdf_text(customer, styles["Tiny"]),
            amount_text,
            _pdf_text(approving_manager, styles["Tiny"]),
            _pdf_text(reason, styles["Tiny"]),
        ]
        if include_store:
            base.insert(0, str(row.get("store", "")))
        rows.append(base)

    return [
        Spacer(1, SPACER["sm"]),
        Paragraph(title, styles["Section"]),
        Paragraph(
            f"{total_rows:,} adjustment row(s)"
            + (f" • Net points: {points_total:,.0f}" if abs(points_total) > 1e-9 else "")
            + (f" • Showing first {len(shown):,}" if total_rows > len(shown) else ""),
            styles["Tiny"],
        ),
        Spacer(1, SPACER["xs"]),
        build_table(headers, rows, widths),
    ]

def build_cart_distribution_strip(dist_df: pd.DataFrame, width: float = 7.3 * inch) -> Optional[Table]:
    if dist_df is None or dist_df.empty:
        return None

    labels = dist_df["bucket"].astype(str).tolist()
    counts = [f"{int(v):,}" for v in dist_df["count"].fillna(0).astype(float).tolist()]
    shares = [pct1(float(v)) for v in dist_df["pct"].fillna(0).astype(float).tolist()]
    if not labels:
        return None

    col_w = width / max(1, len(labels))
    t = Table([labels, counts, shares], colWidths=[col_w] * len(labels))
    t.setStyle(TableStyle([
        ("BACKGROUND", (0, 0), (-1, 0), THEME["black"]),
        ("TEXTCOLOR", (0, 0), (-1, 0), THEME["yellow"]),
        ("BACKGROUND", (0, 1), (-1, 1), THEME["light_bg"]),
        ("BACKGROUND", (0, 2), (-1, 2), colors.white),
        ("TEXTCOLOR", (0, 1), (-1, 1), THEME["black"]),
        ("TEXTCOLOR", (0, 2), (-1, 2), THEME["muted"]),
        ("FONTNAME", (0, 0), (-1, 0), BASE_FONT_BOLD),
        ("FONTNAME", (0, 1), (-1, 1), BASE_FONT_BOLD),
        ("FONTNAME", (0, 2), (-1, 2), BASE_FONT),
        ("FONTSIZE", (0, 0), (-1, 0), 7.2),
        ("FONTSIZE", (0, 1), (-1, 1), 8.8),
        ("FONTSIZE", (0, 2), (-1, 2), 7.2),
        ("GRID", (0, 0), (-1, -1), 0.45, THEME["border"]),
        ("ALIGN", (0, 0), (-1, -1), "CENTER"),
        ("VALIGN", (0, 0), (-1, -1), "MIDDLE"),
        ("LEFTPADDING", (0, 0), (-1, -1), 2),
        ("RIGHTPADDING", (0, 0), (-1, -1), 2),
        ("TOPPADDING", (0, 0), (-1, -1), 3),
        ("BOTTOMPADDING", (0, 0), (-1, -1), 3),
    ]))
    return t

def build_report_day_band(report_day: date, width: float) -> Table:
    p = Paragraph(
        f"<b>REPORT DAY:</b> {report_day.isoformat()} ({report_day.strftime('%A')})",
        ParagraphStyle(
            name="ReportBand",
            fontName=BASE_FONT_BOLD,
            fontSize=10.0,
            textColor=THEME["black"],
            leading=12,
        )
    )
    t = Table([[p]], colWidths=[width])
    t.setStyle(TableStyle([
        ("BACKGROUND", (0, 0), (-1, -1), THEME["yellow"]),
        ("BOX", (0, 0), (-1, -1), 0.8, THEME["black"]),
        ("LEFTPADDING", (0, 0), (-1, -1), 8),
        ("RIGHTPADDING", (0, 0), (-1, -1), 8),
        ("TOPPADDING", (0, 0), (-1, -1), 5),
        ("BOTTOMPADDING", (0, 0), (-1, -1), 5),
    ]))
    return t

def make_footer(left_text: str, report_day: date):
    def _footer(canvas, doc):
        canvas.saveState()
        canvas.setFont(BASE_FONT, 8)
        canvas.setFillColor(THEME["muted"])
        canvas.drawString(doc.leftMargin, 0.30 * inch, f"{left_text} • {report_day.isoformat()} ({report_day.strftime('%A')})")
        canvas.drawRightString(letter[0] - doc.rightMargin, 0.30 * inch, f"Page {canvas.getPageNumber()}")
        canvas.restoreState()
    return _footer


###############################################################################
# Category Summary "BarCell" to mimic the screenshot feel
###############################################################################

class BarCell(Flowable):
    """
    Draws a horizontal bar (ratio * cell width) with a value label on top.
    """
    def __init__(
        self,
        text: str,
        ratio: float,
        bar_hex: str,
        font_name: str,
        font_size: float = 7.9,
        text_color_hex: str = "#111827",
    ):
        super().__init__()
        self.text = str(text)
        self.ratio = max(0.0, min(1.0, float(ratio)))
        self.bar_hex = bar_hex
        self.font_name = font_name
        self.font_size = font_size
        self.text_color_hex = text_color_hex

        self.width = 0
        self.height = 0

    def wrap(self, availWidth, availHeight):
        self.width = availWidth
        self.height = 0.18 * inch
        return self.width, self.height

    def draw(self):
        c = self.canv
        bar_w = self.width * self.ratio
        c.saveState()
        c.setFillColor(colors.HexColor(self.bar_hex))
        c.setStrokeColor(colors.HexColor(self.bar_hex))
        c.rect(0, 0, bar_w, self.height, fill=1, stroke=0)

        c.setFillColor(colors.HexColor(self.text_color_hex))
        c.setFont(self.font_name, self.font_size)
        c.drawRightString(self.width - 2, (self.height / 2) - 3, self.text)
        c.restoreState()

def _safe_max(series: pd.Series) -> float:
    try:
        v = float(series.max())
        return v if v > 0 else 0.0
    except Exception:
        return 0.0

CATEGORY_MAX_ROWS = 8
def build_category_summary_table(
    styles,
    cat_df: pd.DataFrame,
    title: str,
    top_n: int = CATEGORY_MAX_ROWS,
) -> List[Any]:
    if cat_df is None or cat_df.empty:
        return []

    d_all = cat_df.copy()
    d = d_all.head(top_n).copy()

    profit_real_total = float(d_all["profit_real"].sum()) if "profit_real" in d_all.columns else float(d_all["profit"].sum())
    cogs_real_total = float(d_all["cogs_real"].sum()) if "cogs_real" in d_all.columns else float(d_all["cogs"].sum())

    totals = {
        "category": "Totals",
        "net_revenue": float(d_all["net_revenue"].sum()),
        "profit": float(d_all["profit"].sum()),
        "profit_real": profit_real_total,
        "discount": float(d_all["discount"].sum()),
        "cogs": float(d_all["cogs"].sum()),
        "cogs_real": cogs_real_total,
        "items": float(d_all["items"].sum()),
    }

    gross_total = float(d_all["gross_sales"].sum()) if "gross_sales" in d_all.columns else 0.0
    if gross_total:
        totals["discount_rate"] = totals["discount"] / gross_total
    else:
        approx_g = totals["net_revenue"] + totals["discount"]
        totals["discount_rate"] = totals["discount"] / approx_g if approx_g else 0.0

    totals["margin"] = totals["profit"] / totals["net_revenue"] if totals["net_revenue"] else 0.0
    totals["margin_real"] = totals["profit_real"] / totals["net_revenue"] if totals["net_revenue"] else 0.0

    totals["price_per_item"] = totals["net_revenue"] / totals["items"] if totals["items"] else 0.0
    totals["profit_per_item"] = totals["profit"] / totals["items"] if totals["items"] else 0.0
    totals["cogs_pct"] = totals["cogs"] / totals["net_revenue"] if totals["net_revenue"] else 0.0

    total_net = float(d_all["net_revenue"].sum()) or 1.0
    total_profit = float(d_all["profit"].sum()) or 1.0

    d["pct_revenue"] = d["net_revenue"] / total_net
    d["pct_profit"] = d["profit"] / total_profit

    max_rev = _safe_max(d["net_revenue"])
    max_profit = _safe_max(d["profit"].abs())
    max_items = _safe_max(d["items"])
    max_price = _safe_max(d["price_per_item"])
    max_ppi = _safe_max(d["profit_per_item"].abs())
    max_disc = _safe_max(d["discount_rate"])
    max_margin = _safe_max(d["margin"])
    max_cogs = _safe_max(d["cogs_pct"])

    headers = [
        "#", "Major Category", "Revenue", "% Rev", "Profit", "% Profit",
        "Discount %", "Marg(KB)", "Margin",
        "Items", "Price/Item", "Profit/Item", "% COGS",
    ]

    rows: List[List[Any]] = []
    for idx, r in enumerate(d.itertuples(index=False), start=1):
        # margin display: compact + 0 decimals so it fits (KB/REAL)
        mr = float(getattr(r, "margin_real", 0.0))
        margin_text = fmt_margin_display(float(r.margin), mr, compact=True, decimals=0)

        rows.append([
            str(idx),
            str(r.category),
            BarCell(money(r.net_revenue), (r.net_revenue / max_rev) if max_rev else 0.0, HEX_GREEN, BASE_FONT, 6),
            BarCell(pct1(r.pct_revenue), (r.pct_revenue / d["pct_revenue"].max()) if d["pct_revenue"].max() else 0.0, HEX_GREEN, BASE_FONT, 6),
            BarCell(money(r.profit), (abs(r.profit) / max_profit) if max_profit else 0.0, HEX_GREEN, BASE_FONT, 6),
            BarCell(pct1(r.pct_profit), (abs(r.pct_profit) / d["pct_profit"].abs().max()) if d["pct_profit"].abs().max() else 0.0, HEX_GREEN, BASE_FONT, 6),
            BarCell(pct1(r.discount_rate), (r.discount_rate / max_disc) if max_disc else 0.0, HEX_YELLOW, BASE_FONT, 6),

            # ✅ KB/REAL margin label
            BarCell(pct1(r.margin),(r.margin / max_margin) if max_margin else 0.0,HEX_GREEN,BASE_FONT,6),
            BarCell(pct1(getattr(r, "margin_real", 0.0)),(getattr(r, "margin_real", 0.0) / max_margin) if max_margin else 0.0,HEX_YELLOW,BASE_FONT,6),
            BarCell(f"{int(r.items):,}", (r.items / max_items) if max_items else 0.0, HEX_GREEN, BASE_FONT, 6),
            BarCell(money2(r.price_per_item), (r.price_per_item / max_price) if max_price else 0.0, HEX_YELLOW, BASE_FONT, 6),
            BarCell(money2(r.profit_per_item), (abs(r.profit_per_item) / max_ppi) if max_ppi else 0.0, HEX_GREEN, BASE_FONT, 6),
            BarCell(pct1(r.cogs_pct), (r.cogs_pct / max_cogs) if max_cogs else 0.0, HEX_YELLOW, BASE_FONT, 6),
        ])

    rows.append([
        "",
        "Totals",
        money(totals["net_revenue"]),
        "100.0%",
        money(totals["profit"]),
        "100.0%",
        pct1(totals["discount_rate"]),
        pct1(totals["margin"]),
        pct1(totals["margin_real"]),
        f"{int(totals['items']):,}",
        money2(totals["price_per_item"]),
        money2(totals["profit_per_item"]),
        pct1(totals["cogs_pct"]),
    ])

    table = Table(
        [headers] + rows,
        repeatRows=1,
        colWidths=[
            0.18*inch, 1.15*inch, 0.85*inch, 0.55*inch,
            0.80*inch, 0.55*inch, 0.60*inch,
            0.55*inch, 0.55*inch, 
            0.55*inch, 0.70*inch, 0.70*inch, 0.50*inch,
        ],
    )

    table.setStyle(TableStyle([
        ("BACKGROUND", (0, 0), (-1, 0), THEME["black"]),
        ("TEXTCOLOR", (0, 0), (-1, 0), THEME["yellow"]),
        ("FONTNAME", (0, 0), (-1, 0), BASE_FONT_BOLD),
        ("FONTSIZE", (0, 0), (-1, 0), 6.3),
        ("GRID", (0, 0), (-1, -1), 0.3, THEME["border"]),
        ("ROWBACKGROUNDS", (0, 1), (-1, -2), [colors.white, THEME["row_alt"]]),
        ("FONTNAME", (0, 1), (-1, -1), BASE_FONT),
        ("FONTSIZE", (0, 1), (-1, -1), 7.6),
        ("LEFTPADDING", (0, 0), (-1, -1), 3),
        ("RIGHTPADDING", (0, 0), (-1, -1), 3),
        ("TOPPADDING", (0, 0), (-1, -1), 2),
        ("BOTTOMPADDING", (0, 0), (-1, -1), 2),
        ("BACKGROUND", (0, -1), (-1, -1), THEME["soft_gray"]),
        ("FONTNAME", (0, -1), (-1, -1), BASE_FONT_BOLD),
    ]))

    return [KeepTogether([Paragraph(title, styles["Section"]), table])]


###############################################################################
# PDF: Store report
###############################################################################
def build_store_pdf(
    out_pdf: Path,
    store_name: str,
    abbr: str,
    df_raw: pd.DataFrame,
    daily: pd.DataFrame,
    start_day: date,
    end_day: date,
    loyalty_detail: Optional[pd.DataFrame] = None,
    register_loyalty_detail: Optional[pd.DataFrame] = None,
    loyalty_discount_name: str = LOYALTY_DISCOUNT_MATCH_TEXT,
) -> None:
    styles = build_styles()
    label = store_label(store_name)
    generated_at = datetime.now(ZoneInfo(REPORT_TZ)).strftime("%B %d, %Y at %I:%M %p %Z")

    last_week_day = end_day - timedelta(days=7)
    mtd_start = month_start(end_day)
    last_mtd_start, last_mtd_end = prev_month_same_window(end_day)

    today = metrics_for_day(daily, end_day)
    last_week = metrics_for_day(daily, last_week_day)
    mtd = metrics_for_range(daily, mtd_start, end_day)
    last_mtd = metrics_for_range(daily, last_mtd_start, last_mtd_end)
    customer_today = compute_customer_counts(df_raw, end_day, end_day)
    customer_mtd = compute_customer_counts(df_raw, mtd_start, end_day)

    days_elapsed = (end_day - mtd_start).days + 1
    avg_per_day = (mtd["net_revenue"] / days_elapsed) if days_elapsed else 0.0

    trend = daily[daily["date"] <= end_day].copy().tail(max(TREND_DAYS, 1))
    net_trend = chart_trend_bar_with_labels(
        trend,
        "net_revenue",
        f"Net Sales Trend ({len(trend)} Days)",
        days=len(trend),
        kind="money",
        figsize=(7.3, 3.2),
    )

    cart_dist = compute_cart_value_distribution(df_raw, end_day, end_day)
    cart_dist_chart = BytesIO()
    cart_dist_strip = None
    cart_dist_mtd = compute_cart_value_distribution(df_raw, mtd_start, end_day)
    if cart_dist is not None and not cart_dist.empty:
        cart_dist_chart = chart_cart_value_distribution(
            cart_dist,
            f"Cart Value Distribution ({end_day.isoformat()} {dow_short(end_day)})",
            figsize=(7.3, 4.35),
        )
        cart_dist_strip = build_cart_distribution_strip(cart_dist, width=7.3 * inch)

    hourly_today = compute_hourly_metrics(df_raw, end_day)
    hourly_last = compute_hourly_metrics(df_raw, last_week_day)
    if hourly_today is None:
        hourly_today = pd.DataFrame(columns=[
            "hour", "net_revenue", "profit", "profit_real",
            "tickets", "basket", "margin", "margin_real"
        ])

    if hourly_last is None:
        hourly_last = pd.DataFrame(columns=[
            "hour", "net_revenue", "profit", "profit_real",
            "tickets", "basket", "margin", "margin_real"
        ])
    hourly_figsize = (7.3, 2.85)
    ch_rev = chart_hourly_shadow_compare(hourly_today, hourly_last, "net_revenue", "Revenue by Hour", "money", hourly_figsize)
    ch_tix = chart_hourly_shadow_compare(hourly_today, hourly_last, "tickets", "Tickets by Hour", "int", hourly_figsize)
    ch_profit = chart_hourly_shadow_compare(hourly_today, hourly_last, "profit", "Profit by Hour", "money", hourly_figsize)
    ch_basket = chart_hourly_shadow_compare(hourly_today, hourly_last, "basket", "Basket by Hour", "money", hourly_figsize)
    ch_margin_kb = chart_hourly_shadow_compare(hourly_today, hourly_last, "margin", "Kickback Margin by Hour", "pct", hourly_figsize)

    prod_day = compute_breakdown_net(df_raw, COLUMN_CANDIDATES["product"], end_day, end_day, top_n=TOP_N)
    brand_day = compute_brand_summary(df_raw, end_day, end_day, top_n=TOP_N)
    units_day_source = df_raw
    category_col = find_col(df_raw, COLUMN_CANDIDATES["category"])
    if category_col and category_col in df_raw.columns:
        # Exclude accessories only for the daily units sold products table.
        units_day_source = df_raw[
            ~df_raw[category_col].fillna("").astype(str).str.contains(r"accessor", case=False, regex=True)
        ].copy()
    prod_units_day = compute_breakdown_units(
        units_day_source,
        COLUMN_CANDIDATES["product"],
        end_day,
        end_day,
        top_n=DAILY_UNITS_ROWS,
    )

    cat_today = compute_category_summary(df_raw, end_day, end_day)
    cat_mtd = compute_category_summary(df_raw, mtd_start, end_day)

    prod_mtd = compute_breakdown_net(df_raw, COLUMN_CANDIDATES["product"], mtd_start, end_day, top_n=TOP_N)
    prod_chart = BytesIO()
    if prod_mtd is not None and not prod_mtd.empty:
        prod_chart = chart_rank_barh(
            prod_mtd.rename(columns={prod_mtd.columns[0]: "product"}),
            "product", "net_revenue",
            "Top Products (MTD)",
            top_n=TOP_N,
            figsize=(7.3, 4.05),
        )

    brand_mtd = compute_brand_summary(df_raw, mtd_start, end_day, top_n=TOP_N)
    brand_chart = BytesIO()
    if brand_mtd is not None and not brand_mtd.empty:
        brand_chart = chart_rank_barh(
            brand_mtd,
            "brand", "net_revenue",
            "Top Brands (MTD)",
            top_n=TOP_N,
            figsize=(7.3, 3.85),
        )

    bud_today = compute_budtender_summary(df_raw, end_day, end_day)
    bud_mtd = compute_budtender_summary(df_raw, mtd_start, end_day)

    bud_today_chart = BytesIO()
    if bud_today is not None and not bud_today.empty:
        bud_today_chart = chart_rank_barh(
            bud_today, "budtender", "net_revenue",
            "Top Budtenders (Report Day)",
            top_n=min(TOP_N, len(bud_today)),
            figsize=(7.3, 3.0),
        )

    out_pdf.parent.mkdir(parents=True, exist_ok=True)
    doc = SimpleDocTemplate(
        str(out_pdf),
        pagesize=letter,
        leftMargin=PAGE_MARGINS["left"],
        rightMargin=PAGE_MARGINS["right"],
        topMargin=PAGE_MARGINS["top"],
        bottomMargin=PAGE_MARGINS["bottom"],
        pageCompression=1,
        title=f"{abbr} Owner Snapshot - {label}",
        author="Buzz Automation",
    )
    footer = make_footer(f"{abbr} - {label}", end_day)

    story: List[Any] = []

    story.append(Paragraph(f"{abbr} • Owner Snapshot • {label}", styles["TitleBig"]))
    story.append(build_report_day_band(end_day, width=7.6 * inch))
    story.append(Spacer(1, SPACER["xs"]))

    story.append(Paragraph(
        f"<b>Data Window:</b> {start_day.isoformat()} → {end_day.isoformat()} &nbsp;&nbsp; "
        f"<b>MTD Window:</b> {mtd_start.isoformat()} → {end_day.isoformat()} &nbsp;&nbsp; "
        f"<b>Last MTD Ref:</b> {last_mtd_start.isoformat()} → {last_mtd_end.isoformat()}",
        styles["Tiny"],
    ))
    story.append(Paragraph(f"<b>Generated:</b> {generated_at}", styles["Tiny"]))
    story.append(Spacer(1, SPACER["sm"]))

    kpis: List[List[Paragraph]] = []
    kpis.append(kpi_cell(styles, "TODAY • NET SALES", money(today["net_revenue"]),
                         delta_html_currency(today["net_revenue"], last_week["net_revenue"], "last week")))
    kpis.append(kpi_cell(styles, "TODAY • TICKETS", f"{int(today['tickets']):,}",
                         delta_html_int(today["tickets"], last_week["tickets"], "last week")))
    kpis.append(kpi_cell(styles, "TODAY • BASKET", money2(today["basket"]),
                         delta_html_currency(today["basket"], last_week["basket"], "last week")))
    kpis.append(kpi_cell(styles, "TODAY • DISC RATE", pct1(today["discount_rate"]),
                         delta_html_pp(today["discount_rate"], last_week["discount_rate"], "last week")))

    kpis.append(kpi_cell(styles, "MTD • NET SALES", money(mtd["net_revenue"]),
                         delta_html_currency(mtd["net_revenue"], last_mtd["net_revenue"], "last MTD")))
    kpis.append(kpi_cell(styles, "MTD • TICKETS", f"{int(mtd['tickets']):,}",
                         delta_html_int(mtd["tickets"], last_mtd["tickets"], "last MTD")))
    kpis.append(kpi_cell(styles, "MTD • BASKET", money2(mtd["basket"]),
                         delta_html_currency(mtd["basket"], last_mtd["basket"], "last MTD")))
    kpis.append(kpi_cell(
        styles,
        "MTD • MARGIN (KB/REAL)",
        fmt_margin_display(mtd["margin"], mtd.get("margin_real", 0.0), compact=False, decimals=1),
        delta_html_pp_pair(
            mtd["margin"], last_mtd["margin"],
            mtd.get("margin_real", 0.0), last_mtd.get("margin_real", 0.0),"last MTD")))

    story.append(build_kpi_grid(styles, kpis, cols=4))
    story.append(Spacer(1, SPACER["sm"]))
    story.append(build_customer_growth_block(styles, customer_today, customer_mtd))
    story.append(Spacer(1, SPACER["xs"]))

    story.append(Paragraph(
        f"<b>MTD Avg/Day:</b> {money(avg_per_day)}"
        f"&nbsp;&nbsp; <b>MTD Discount:</b> {money(mtd['discount'])}"
        f"&nbsp;&nbsp; <b>MTD Returns:</b> {money(mtd['returns_net'])}",
        styles["Muted"],
    ))
    story.append(Spacer(1, SPACER["sm"]))

    story.append(KeepTogether([
        Paragraph("Trends", styles["Section"]),
        Image(net_trend, width=7.3 * inch, height=3.2 * inch) if net_trend.getbuffer().nbytes > 0 else Spacer(1, 0),
    ]))
    story.append(Spacer(1, SPACER["xs"]))

    has_day_cart_chart = (
        cart_dist is not None
        and not cart_dist.empty
        and cart_dist_chart.getbuffer().nbytes > 0
    )
    has_mtd_cart_table = cart_dist_mtd is not None and not cart_dist_mtd.empty

    if has_day_cart_chart or has_mtd_cart_table:
        story.append(CondPageBreak(5.5 * inch))
        story.append(Paragraph("Cart Value Distribution", styles["TitleBig"]))
        if has_day_cart_chart:
            total_carts = int(cart_dist["count"].sum())
            story.append(Paragraph(
                f"Report Day carts grouped by net cart value • Total carts: {total_carts:,}",
                styles["Tiny"],
            ))
            story.append(Spacer(1, 0.05 * inch))
            if cart_dist_strip is not None:
                story.append(cart_dist_strip)
                story.append(Spacer(1, 0.08 * inch))
            story.append(Image(cart_dist_chart, width=7.3 * inch, height=4.35 * inch))
            story.append(Spacer(1, 0.06 * inch))

        if has_mtd_cart_table:
            mtd_rows = [
                [str(r.bucket), f"{int(r.count):,}", pct1(float(r.pct))]
                for r in cart_dist_mtd.itertuples(index=False)
            ]
            mtd_total_carts = int(cart_dist_mtd["count"].sum())
            story.append(Paragraph(
                f"Cart Value Distribution — MTD ({mtd_start.isoformat()} to {end_day.isoformat()}) • "
                f"Total carts: {mtd_total_carts:,}",
                styles["Section"],
            ))
            story.append(build_table(
                ["Cart Range", "MTD Carts", "MTD Share"],
                mtd_rows,
                [4.10 * inch, 1.60 * inch, 1.60 * inch],
            ))
        story.append(Spacer(1, SPACER["sm"]))

    story.append(Paragraph(
        f"Hourly Snapshot (Report Day vs {last_week_day.isoformat()} {dow_short(last_week_day)})",
        styles["Section"],
    ))
    hourly_img_w = 7.3 * inch
    hourly_img_h = 2.85 * inch
    story.append(Image(ch_rev, width=hourly_img_w, height=hourly_img_h) if ch_rev.getbuffer().nbytes > 0 else Spacer(1, 0))
    story.append(Spacer(1, 0.10 * inch))
    story.append(Image(ch_tix, width=hourly_img_w, height=hourly_img_h) if ch_tix.getbuffer().nbytes > 0 else Spacer(1, 0))

    story.append(CondPageBreak(6.8 * inch))
    story.append(Spacer(1, 0.12 * inch))
    story.append(Paragraph("Hourly Performance", styles["TitleBig"]))
    story.append(Spacer(1, 0.05 * inch))

    story.append(Paragraph(
        "<b>Guide:</b> Yellow = Last Week • Green = Report Day • "
        "Bars are shadow compared (Last Week behind Report Day).",
        styles["Muted"]
    ))

    story.append(Spacer(1, 0.12 * inch))
    story.append(Spacer(1, SPACER["sm"]))

    hourly_perf_charts = [ch_profit, ch_basket, ch_margin_kb]
    for idx, hourly_chart in enumerate(hourly_perf_charts):
        story.append(Image(hourly_chart, width=hourly_img_w, height=hourly_img_h) if hourly_chart.getbuffer().nbytes > 0 else Spacer(1, 0))
        if idx < len(hourly_perf_charts) - 1:
            story.append(Spacer(1, 0.10 * inch))

    story.append(CondPageBreak(6.2 * inch))
    story.append(Paragraph("Drivers", styles["TitleBig"]))
    story.append(Paragraph("Major Categories + Products + Brands (Daily + MTD).", styles["Tiny"]))
    story.append(Spacer(1, SPACER["sm"]))

    if cat_today is not None and not cat_today.empty:
        story += build_category_summary_table(styles, cat_today, "Major Category Summary — Today", top_n=CATEGORY_TOP_N)
        story.append(Spacer(1, SPACER["sm"]))

    if cat_mtd is not None and not cat_mtd.empty:
        story += build_category_summary_table(styles, cat_mtd, "Major Category Summary — MTD", top_n=CATEGORY_TOP_N)
        story.append(Spacer(1, SPACER["sm"]))

    if prod_units_day is not None and not prod_units_day.empty:
        prod_units_day_rows = [[str(r[0]), f"{int(round(float(r.units_sold))):,}"] for r in prod_units_day.itertuples(index=False)]
        story.append(Paragraph(
            f"Top Products by Units — Report Day ({end_day.isoformat()} {dow_short(end_day)})",
            styles["Section"],
        ))
        story.append(build_table(
            ["Product", "Units Sold"],
            prod_units_day_rows,
            [5.85 * inch, 1.4 * inch],
        ))
        story.append(Spacer(1, SPACER["sm"]))

    if prod_day is not None and not prod_day.empty:
        prod_day_rows = [[str(r[0]), money(float(r.net_revenue))] for r in prod_day.itertuples(index=False)]
        story.append(Paragraph(
            f"Top Products — Report Day ({end_day.isoformat()} {dow_short(end_day)})",
            styles["Section"],
        ))
        story.append(build_table(["Product", "Day Net"], prod_day_rows, [5.85 * inch, 1.4 * inch]))
        story.append(Spacer(1, SPACER["sm"]))

    if brand_day is not None and not brand_day.empty:
        brand_day_rows = [[str(r.brand),
        money(float(r.net_revenue)),
        fmt_margin_display(float(r.margin), float(getattr(r, "margin_real", 0.0)), compact=True, decimals=1),] for r in brand_day.itertuples(index=False)]
        story.append(Paragraph(
            f"Top Brands — Report Day ({end_day.isoformat()} {dow_short(end_day)})",
            styles["Section"],
        ))
        story.append(build_table(["Brand", "Day Net", "Avg Margin"], brand_day_rows, [4.65 * inch, 1.4 * inch, 1.55 * inch]))
        story.append(Spacer(1, SPACER["sm"]))

    if prod_mtd is not None and not prod_mtd.empty and prod_chart.getbuffer().nbytes > 0:
        prod_rows = [[str(r[0]), money(float(r.net_revenue))] for r in prod_mtd.itertuples(index=False)]
        story.append(KeepTogether([
            Paragraph("Top Products (MTD)", styles["Section"]),
            Image(prod_chart, width=7.3 * inch, height=4.05 * inch),
        ]))
        story.append(build_table(["Product", "MTD Net"], prod_rows, [5.85*inch, 1.4*inch]))
        story.append(Spacer(1, SPACER["sm"]))

    if brand_mtd is not None and not brand_mtd.empty and brand_chart.getbuffer().nbytes > 0:
        brand_rows = [[str(r.brand), money(float(r.net_revenue)), fmt_margin_display(float(r.margin),float(getattr(r, "margin_real", 0.0)),
        compact=True,decimals=1),] for r in brand_mtd.itertuples(index=False)]
        story.append(KeepTogether([
            Paragraph("Top Brands (MTD)", styles["Section"]),
            Image(brand_chart, width=7.3 * inch, height=3.85 * inch),
        ]))
        story.append(build_table(["Brand", "MTD Net", "Avg Margin"], brand_rows, [4.65 * inch, 1.4 * inch, 1.55 * inch]))

    story.append(CondPageBreak(5.4 * inch))
    story.append(Paragraph("Staff Performance", styles["TitleBig"]))
    story.append(Paragraph("Budtenders — Report Day and MTD (full lists).", styles["Tiny"]))
    story.append(Spacer(1, SPACER["sm"]))

    if bud_today is not None and not bud_today.empty:
        story.append(Paragraph(
            f"Budtenders — Report Day ({end_day.isoformat()} {dow_short(end_day)})",
            styles["Section"],
        ))
        if bud_today_chart.getbuffer().nbytes > 0:
            story.append(Image(bud_today_chart, width=7.3 * inch, height=3.0 * inch))

        bud_today_rows = []
        for r in bud_today.itertuples(index=False):
            bud_today_rows.append([
                str(r.budtender),
                money(float(r.net_revenue)),
                f"{int(r.tickets):,}",
                money2(float(r.basket)),
                pct1(float(r.discount_rate)),
            ])
        story.append(build_table(
            ["Budtender", "Net", "Tickets", "Basket", "Disc Rate"],
            bud_today_rows,
            [2.65*inch, 1.25*inch, 1.0*inch, 1.25*inch, 1.2*inch],
        ))
        story.append(Spacer(1, SPACER["sm"]))

    if bud_mtd is not None and not bud_mtd.empty:
        story.append(Paragraph("Budtenders — MTD", styles["Section"]))

        bud_mtd_rows = []
        for r in bud_mtd.itertuples(index=False):
            bud_mtd_rows.append([
                str(r.budtender),
                money(float(r.net_revenue)),
                f"{int(r.tickets):,}",
                money2(float(r.basket)),
                pct1(float(r.discount_rate)),
            ])
        story.append(build_table(
            ["Budtender", "MTD Net", "MTD Tickets", "MTD Basket", "Disc Rate"],
            bud_mtd_rows,
            [2.65*inch, 1.25*inch, 1.05*inch, 1.25*inch, 1.15*inch],
        ))

    if loyalty_detail is None:
        loyalty_detail = compute_loyalty_points_adjustment_detail(df_raw, mtd_start, end_day, loyalty_discount_name)
    else:
        loyalty_detail = filter_loyalty_detail_df(loyalty_detail, mtd_start, end_day)
    register_loyalty_detail = filter_loyalty_adjustment_df(register_loyalty_detail, mtd_start, end_day)
    story += build_loyalty_detail_section(
        styles,
        loyalty_detail,
        f"{loyalty_discount_name} Detail — MTD ({mtd_start.isoformat()} to {end_day.isoformat()})",
        include_store=False,
        match_text=loyalty_discount_name,
    )
    story += build_register_adjustment_section(
        styles,
        register_loyalty_detail,
        "Loyalty Adjustment Audit",
        include_store=False,
    )

    doc.build(story, onFirstPage=footer, onLaterPages=footer)
    print(f"✅ PDF created: {out_pdf}")


###############################################################################
# PDF: All stores summary (kept simple but consistent)
###############################################################################

def build_all_stores_summary_pdf(
    out_pdf: Path,
    store_daily_map: Dict[str, pd.DataFrame],
    end_day: date,
    start_day: date,
    forecast_bundle: Optional[Dict[str, Any]] = None,
    store_raw_df_map: Optional[Dict[str, pd.DataFrame]] = None,
    loyalty_detail_by_store: Optional[Dict[str, pd.DataFrame]] = None,
    register_loyalty_by_store: Optional[Dict[str, pd.DataFrame]] = None,
    loyalty_discount_name: str = LOYALTY_DISCOUNT_MATCH_TEXT,
) -> None:
    styles = build_styles()
    generated_at = datetime.now(ZoneInfo(REPORT_TZ)).strftime("%B %d, %Y at %I:%M %p %Z")
    mtd_start = month_start(end_day)
    last_week_day = end_day - timedelta(days=7)
    last_mtd_start, last_mtd_end = prev_month_same_window(end_day)

    out_pdf.parent.mkdir(parents=True, exist_ok=True)
    doc = SimpleDocTemplate(
        str(out_pdf),
        pagesize=letter,
        leftMargin=PAGE_MARGINS["left"],
        rightMargin=PAGE_MARGINS["right"],
        topMargin=PAGE_MARGINS["top"],
        bottomMargin=PAGE_MARGINS["bottom"],
        pageCompression=1,
        title=f"All Stores Owner Snapshot - {end_day.isoformat()}",
        author="Buzz Automation",
    )

    footer = make_footer("ALL STORES", end_day)
    story: List[Any] = []

    story.append(Paragraph("All Stores • Owner Snapshot", styles["TitleBig"]))
    story.append(build_report_day_band(end_day, width=7.6 * inch))
    story.append(Spacer(1, SPACER["xs"]))

    story.append(Paragraph(
        f"<b>Data Window:</b> {start_day.isoformat()} → {end_day.isoformat()} &nbsp;&nbsp; "
        f"<b>MTD Window:</b> {mtd_start.isoformat()} → {end_day.isoformat()} &nbsp;&nbsp; "
        f"<b>Last MTD Ref:</b> {last_mtd_start.isoformat()} → {last_mtd_end.isoformat()}",
        styles["Tiny"],
    ))
    story.append(Paragraph(f"<b>Generated:</b> {generated_at}", styles["Tiny"]))
    story.append(Spacer(1, SPACER["sm"]))

    rows = []
    customer_rows = []
    totals_today_net = totals_today_tickets = totals_mtd_net = totals_mtd_tickets = 0.0
    store_rank = []

    for store_name, abbr in store_abbr_map.items():
        daily = store_daily_map.get(abbr)
        if daily is None or daily.empty:
            continue

        today = metrics_for_day(daily, end_day)
        last_week = metrics_for_day(daily, last_week_day)
        mtd = metrics_for_range(daily, mtd_start, end_day)
        last_mtd = metrics_for_range(daily, last_mtd_start, last_mtd_end)

        totals_today_net += today["net_revenue"]
        totals_today_tickets += today["tickets"]
        totals_mtd_net += mtd["net_revenue"]
        totals_mtd_tickets += mtd["tickets"]

        d_today = today["net_revenue"] - last_week["net_revenue"]
        d_mtd = mtd["net_revenue"] - last_mtd["net_revenue"]
        raw_df = (store_raw_df_map or {}).get(abbr)
        if raw_df is not None and not raw_df.empty:
            customer_rows.append([
                f"{abbr} - {store_label(store_name)}",
                fmt_new_total(compute_customer_counts(raw_df, end_day, end_day)),
                fmt_new_total(compute_customer_counts(raw_df, mtd_start, end_day)),
            ])

        rows.append([
            f"{abbr} - {store_label(store_name)}",
            money(today["net_revenue"]),
            fmt_signed_money(d_today),
            money(mtd["net_revenue"]),
            fmt_signed_money(d_mtd),
            fmt_margin_display(mtd["margin"], mtd.get("margin_real", 0.0), compact=True, decimals=1),
            f"{int(mtd['tickets']):,}",
        ])
        store_rank.append([f"{abbr} - {store_label(store_name)}", float(mtd["net_revenue"])])

    story.append(Paragraph(
        f"<b>Totals Today:</b> {money(totals_today_net)} • {int(totals_today_tickets):,} tickets"
        f"&nbsp;&nbsp; <b>Totals MTD:</b> {money(totals_mtd_net)} • {int(totals_mtd_tickets):,} tickets",
        styles["Muted"],
    ))
    story.append(Spacer(1, SPACER["sm"]))

    if customer_rows:
        raw_frames = [
            df.dropna(axis=1, how="all") for df in (store_raw_df_map or {}).values()
            if df is not None and not df.empty
        ]
        if raw_frames:
            all_raw = pd.concat(raw_frames, ignore_index=True)
            all_customer_today = compute_customer_counts(all_raw, end_day, end_day)
            all_customer_mtd = compute_customer_counts(all_raw, mtd_start, end_day)
            story.append(build_customer_growth_block(styles, all_customer_today, all_customer_mtd))
            story.append(Spacer(1, SPACER["sm"]))
            customer_rows.insert(0, [
                "ALL STORES",
                fmt_new_total(all_customer_today),
                fmt_new_total(all_customer_mtd),
            ])
        story.append(Paragraph("Customer Counts", styles["Section"]))
        story.append(build_table(
            ["Store", "Today New / Total", "MTD New / Total"],
            customer_rows,
            [3.40 * inch, 1.90 * inch, 2.00 * inch],
        ))
        story.append(Spacer(1, SPACER["sm"]))

    if store_rank:
        rank_df = pd.DataFrame(store_rank, columns=["store", "net_revenue"]).sort_values("net_revenue", ascending=False)
        rank_chart = chart_rank_barh(rank_df, "store", "net_revenue", "Store Ranking (MTD Net Sales)", top_n=min(10, len(rank_df)), figsize=(7.3, 3.05))
        story.append(KeepTogether([
            Paragraph("MTD Ranking", styles["Section"]),
            Image(rank_chart, width=7.3 * inch, height=3.05 * inch),
        ]))
        story.append(Spacer(1, SPACER["sm"]))

    story.append(Paragraph("Store Table", styles["Section"]))
    story.append(build_table(
        headers=["Store", "Today Net", "Δ vs LW", "MTD Net", "Δ vs Last MTD", "MTD Margin", "MTD Tix"],
        rows=rows,
        col_widths=[2.25*inch, 0.85*inch, 0.78*inch, 0.85*inch, 1.05*inch, 1.00*inch, 0.82*inch],
    ))
    # -------------------------
    # ✅ Month-End Projection Page (ALL STORES)
    # -------------------------
    if forecast_bundle and forecast_bundle.get("stores"):
        stores_fc = forecast_bundle["stores"]
        meta = forecast_bundle.get("meta", {})

        story.append(PageBreak())
        story.append(Paragraph("Month-End Projection", styles["TitleBig"]))
        story.append(Paragraph(
            f"As of {forecast_bundle.get('as_of')} • Model: {meta.get('model_name','baseline')} • "
            f"Training months: {meta.get('n_complete_months',0)} • Samples: {meta.get('n_samples',0)}",
            styles["Tiny"],
        ))
        story.append(Spacer(1, SPACER["sm"]))

        all_fc = stores_fc.get("ALL", {})
        if all_fc:
            # Summary table
            rows = [
                ["Projection Method", str(all_fc.get("model", "adaptive pace"))],
                ["MTD Net Revenue", money(all_fc["mtd_net"])],
                ["Projected Month Net Revenue", money(all_fc["net_pred"])],
                ["Projected Remaining Net", money(all_fc["remaining_net"])],
                ["MTD Net Profit", money(all_fc["mtd_profit"])],
                ["Projected Month Net Profit", money(all_fc["profit_pred"])],
                ["Projected Month Margin", pct1(all_fc["margin_pred"])],
                ["Remaining Days", str(all_fc["remaining_days"])],
                ["Projected Net / Day (remaining)", money(all_fc["req_net_per_day"])],
                ["Projected Profit / Day (remaining)", money(all_fc["req_profit_per_day"])],
            ]

            if all_fc.get("net_p10") is not None and all_fc.get("net_p90") is not None:
                rows.insert(2, ["Net Revenue Band (P10–P90)", f"{money(all_fc['net_p10'])} – {money(all_fc['net_p90'])}"])
            if all_fc.get("profit_p10") is not None and all_fc.get("profit_p90") is not None:
                rows.insert(5, ["Net Profit Band (P10–P90)", f"{money(all_fc['profit_p10'])} – {money(all_fc['profit_p90'])}"])

            story.append(build_table(["Metric", "Projection"], rows, [3.3*inch, 3.9*inch]))
            story.append(Spacer(1, SPACER["sm"]))

        # Store-level projection table
        proj_rows = []
        for store_name, abbr in store_abbr_map.items():
            fc = stores_fc.get(abbr)
            if not fc:
                continue
            proj_rows.append([
                abbr,
                money(fc["mtd_net"]),
                money(fc["net_pred"]),
                money(fc["remaining_net"]),
                money(fc["profit_pred"]),
                pct1(fc["margin_pred"]),
                str(fc["remaining_days"]),
                money(fc["req_net_per_day"]),
            ])

        if proj_rows:
            story.append(Paragraph("Store Projections", styles["Section"]))
            story.append(build_table(
                ["Store", "MTD Net", "Proj Net", "Remaining Net", "Proj Profit", "Proj Margin", "Days Left", "Req Net/Day"],
                proj_rows,
                [0.50*inch, 0.90*inch, 0.90*inch, 1.1*inch, 0.90*inch, 0.90*inch, 0.7*inch, 0.9*inch],
            ))

    loyalty_frames = [
        df for df in (loyalty_detail_by_store or {}).values()
        if df is not None and not df.empty
    ]
    if loyalty_frames:
        all_loyalty = filter_loyalty_detail_df(pd.concat(loyalty_frames, ignore_index=True), mtd_start, end_day)
        story += build_loyalty_detail_section(
            styles,
            all_loyalty,
            f"All Stores {loyalty_discount_name} Detail — MTD ({mtd_start.isoformat()} to {end_day.isoformat()})",
            include_store=True,
            match_text=loyalty_discount_name,
        )

    register_frames = [
        df for df in (register_loyalty_by_store or {}).values()
        if df is not None and not df.empty
    ]
    if register_frames:
        all_register = filter_loyalty_adjustment_df(pd.concat(register_frames, ignore_index=True), mtd_start, end_day)
        story += build_register_adjustment_section(
            styles,
            all_register,
            "All Stores Loyalty Adjustment Audit",
            include_store=True,
        )

    doc.build(story, onFirstPage=footer, onLaterPages=footer)
    print(f"✅ All-stores summary PDF created: {out_pdf}")


###############################################################################
# MAIN
###############################################################################

def main():
    args = parse_cli_args()
    setup_fonts()

    REPORTS_ROOT.mkdir(parents=True, exist_ok=True)
    RAW_ROOT.mkdir(parents=True, exist_ok=True)
    PDF_ROOT.mkdir(parents=True, exist_ok=True)

    abbr_to_file: Dict[str, Path] = {}
    selected_store_codes = _selected_store_codes(args.stores)
    loyalty_detail_by_store: Dict[str, pd.DataFrame] = {}
    register_loyalty_by_store: Dict[str, pd.DataFrame] = {}
    loyalty_detail_workbook: Optional[Path] = None
    run_export = bool(args.run_export)

    if args.backfill_days <= 0:
        raise SystemExit("--backfill-days must be a positive integer.")

    end_override = args.end_date or args.report_day
    if args.report_day and args.end_date and args.report_day != args.end_date:
        raise SystemExit("--report-day and --end-date must match when both are provided.")
    if args.start_date and not end_override:
        raise SystemExit("--start-date requires --end-date (or --report-day).")

    if end_override:
        end_day = end_override
        if args.start_date:
            start_day = args.start_date
        else:
            start_day = end_day - timedelta(days=args.backfill_days - 1)
        if start_day > end_day:
            raise SystemExit("Start date cannot be after end date.")
        forced_range = True
    else:
        start_day, end_day = compute_date_window(args.backfill_days, REPORT_TZ)
        forced_range = False

    print(f"[RANGE] {start_day.isoformat()} -> {end_day.isoformat()} (report day: {end_day.isoformat()})")

    if run_export:
        if args.export_source == "api":
            print("✅ RUN_EXPORT=True → Running Dutchie API export")
            _, abbr_to_file, loyalty_detail_by_store, register_loyalty_by_store, loyalty_detail_workbook = run_api_export_for_range(
                start_day=start_day,
                end_day=end_day,
                selected_store_codes=selected_store_codes,
                env_file=args.api_env_file,
                workers=args.workers,
                loyalty_discount_name=args.loyalty_discount_name,
                loyalty_person=args.loyalty_person,
                write_detail_workbook=False,
            )
        else:
            print("⚠️ RUN_EXPORT=True → Running Selenium export")
            run_export_for_range(start_day, end_day)
            _, abbr_to_file = archive_exports(start_day, end_day)
    else:
        print("✅ RUN_EXPORT=False → Reusing latest raw export folder")
        if forced_range:
            raw_folder = RAW_ROOT / f"{start_day.isoformat()}_to_{end_day.isoformat()}"
            if not raw_folder.exists():
                raise SystemExit(
                    f"Requested range folder not found: {raw_folder}\n"
                    f"Run again with --run-export for this date window."
                )
        else:
            raw_folder = find_latest_raw_folder()
            if raw_folder is None:
                raise SystemExit("No raw export folders found in reports/raw_sales and RUN_EXPORT=False.")

            parsed = parse_range_from_folder_name(raw_folder)
            if parsed:
                start_day, end_day = parsed
                print(f"[RANGE] Using folder window {start_day.isoformat()} -> {end_day.isoformat()}")

        for store_name, abbr in _store_iter(selected_store_codes):
            matches = []
            invalid_matches = []
            for pattern in (f"{abbr}*Sales Export*.xlsx", f"{abbr}*Sales API Export*.xlsx"):
                for candidate in raw_folder.glob(pattern):
                    if is_valid_excel_export(candidate):
                        matches.append(candidate)
                    else:
                        invalid_matches.append(candidate)
            for bad_path in sorted(invalid_matches):
                print(f"[WARN] Skipping invalid cached export for {abbr}: {bad_path.name}")
            if matches:
                abbr_to_file[abbr] = sorted(matches, key=lambda p: p.stat().st_mtime, reverse=True)[0]
            else:
                print(f"[WARN] No valid cached export found for {store_name} ({abbr}) in {raw_folder}")

    if not abbr_to_file:
        raise SystemExit("No store exports found. Check getSalesReport output /files or raw archive.")

    loyalty_start_day = month_start(end_day)
    store_daily_map: Dict[str, pd.DataFrame] = {}
    store_raw_df_map: Dict[str, pd.DataFrame] = {}


    for store_name, abbr in _store_iter(selected_store_codes):
        path = abbr_to_file.get(abbr)
        if not path:
            continue

        print(f"[PARSE] {abbr}: {path.name}")
        df = read_export(path)

        # ✅ APPLY brand-based kickback adjustments BEFORE metrics
        if APPLY_DEAL_KICKBACKS:
            df = enrich_with_deal_kickbacks_by_brand(df, store_code=abbr)

        store_raw_df_map[abbr] = df
        if abbr not in loyalty_detail_by_store:
            loyalty_detail_by_store[abbr] = compute_loyalty_points_adjustment_detail(
                df,
                loyalty_start_day,
                end_day,
                args.loyalty_discount_name,
            )

        daily = compute_daily_metrics(df)
        daily = daily[(daily["date"] >= start_day) & (daily["date"] <= end_day)]
        store_daily_map[abbr] = daily

    if args.loyalty_adjustment_source != "none":
        backoffice_adjustments: Dict[str, pd.DataFrame] = {}
        loyalty_no_data_stores: set[str] = set()
        if run_export:
            required = args.loyalty_adjustment_source == "browser"
            export_store_codes = selected_store_codes
            if args.loyalty_adjustment_source == "auto":
                cached_adjustments = load_cached_backoffice_loyalty_adjustments(
                    loyalty_start_day,
                    end_day,
                    selected_store_codes,
                    data_start_day=start_day,
                )
                if cached_adjustments:
                    print(f"[LOYALTY] Reused archived Backoffice adjustment report rows for: {', '.join(sorted(cached_adjustments))}")
                    backoffice_adjustments = merge_loyalty_adjustment_maps(backoffice_adjustments, cached_adjustments)
                loyalty_no_data_stores = existing_loyalty_adjustment_no_data_store_codes(
                    loyalty_start_day,
                    end_day,
                    selected_store_codes,
                    data_start_day=start_day,
                )
                loyalty_no_data_stores -= set(backoffice_adjustments)
                if loyalty_no_data_stores:
                    print(f"[LOYALTY] Reused archived no-data markers for: {', '.join(sorted(loyalty_no_data_stores))}")
                export_store_codes = [
                    abbr for abbr in selected_store_codes
                    if abbr not in backoffice_adjustments and abbr not in loyalty_no_data_stores
                ]
            if export_store_codes:
                print(f"[LOYALTY] Pulling Backoffice loyalty adjustment report from {LOYALTY_ADJUSTMENT_REPORT_URL}")
                backoffice_adjustments = merge_loyalty_adjustment_maps(
                    backoffice_adjustments,
                    run_backoffice_loyalty_adjustment_export_for_range(
                        loyalty_start_day,
                        end_day,
                        export_store_codes,
                        required=required,
                        workers=args.loyalty_browser_workers,
                    ),
                )
                updated_no_data_stores = existing_loyalty_adjustment_no_data_store_codes(
                    loyalty_start_day,
                    end_day,
                    selected_store_codes,
                    data_start_day=start_day,
                )
                loyalty_no_data_stores |= (updated_no_data_stores - set(backoffice_adjustments))
            elif backoffice_adjustments or loyalty_no_data_stores:
                print("[LOYALTY] All Backoffice adjustment rows reused from archive.")
        else:
            backoffice_adjustments = load_cached_backoffice_loyalty_adjustments(
                loyalty_start_day,
                end_day,
                selected_store_codes,
                data_start_day=start_day,
            )
            loyalty_no_data_stores = existing_loyalty_adjustment_no_data_store_codes(
                loyalty_start_day,
                end_day,
                selected_store_codes,
                data_start_day=start_day,
            )
            loyalty_no_data_stores -= set(backoffice_adjustments)
            if backoffice_adjustments:
                print(f"[LOYALTY] Reused archived Backoffice adjustment report rows for: {', '.join(sorted(backoffice_adjustments))}")
            if loyalty_no_data_stores:
                print(f"[LOYALTY] Reused archived no-data markers for: {', '.join(sorted(loyalty_no_data_stores))}")
        if backoffice_adjustments:
            counts = {
                abbr: int(len(df))
                for abbr, df in backoffice_adjustments.items()
                if df is not None and not df.empty
            }
            if counts:
                print("[LOYALTY] Backoffice adjustment row counts: " + ", ".join(f"{abbr}={count}" for abbr, count in sorted(counts.items())))
            if loyalty_no_data_stores:
                print("[LOYALTY] Confirmed no Backoffice point adjustment rows for: " + ", ".join(sorted(loyalty_no_data_stores)))
            missing_adjustment_stores = [
                abbr for abbr in selected_store_codes
                if abbr not in counts and abbr not in loyalty_no_data_stores
            ]
            if missing_adjustment_stores:
                print(
                    "[LOYALTY] No Backoffice point adjustment rows found for: "
                    + ", ".join(missing_adjustment_stores)
                    + f" ({loyalty_start_day.isoformat()} to {end_day.isoformat()})."
                )
        elif loyalty_no_data_stores:
            print("[LOYALTY] Confirmed no Backoffice point adjustment rows for: " + ", ".join(sorted(loyalty_no_data_stores)))
        else:
            print(
                "[LOYALTY] WARN: No Backoffice point adjustment rows were loaded. "
                "Use --loyalty-adjustment-source browser to require the browser export instead of silently continuing."
            )
        register_loyalty_by_store = merge_loyalty_adjustment_maps(register_loyalty_by_store, backoffice_adjustments)
        if any(df is not None and not df.empty for df in backoffice_adjustments.values()):
            loyalty_detail_workbook = None

    if args.discount_detail_source != "none":
        discount_approvals: Dict[str, pd.DataFrame] = {}
        discount_no_data_stores: set[str] = set()
        if run_export:
            required = args.discount_detail_source == "browser"
            export_store_codes = selected_store_codes
            if args.discount_detail_source == "auto":
                cached_file_stores = existing_discount_detail_export_store_codes(
                    loyalty_start_day,
                    end_day,
                    selected_store_codes,
                )
                cached_approvals = load_discount_detail_approvals_for_range(
                    loyalty_start_day,
                    end_day,
                    selected_store_codes,
                    match_text=args.loyalty_discount_name,
                )
                if cached_file_stores:
                    print(f"[DISCOUNT DETAIL] Reused archived Discount Detail files for: {', '.join(sorted(cached_file_stores))}")
                if cached_approvals:
                    discount_approvals = merge_discount_approval_maps(discount_approvals, cached_approvals)
                discount_no_data_stores = existing_discount_detail_no_data_store_codes(
                    loyalty_start_day,
                    end_day,
                    selected_store_codes,
                )
                discount_no_data_stores -= cached_file_stores
                if discount_no_data_stores:
                    print(f"[DISCOUNT DETAIL] Reused archived no-data markers for: {', '.join(sorted(discount_no_data_stores))}")
                export_store_codes = [
                    abbr for abbr in selected_store_codes
                    if abbr not in cached_file_stores and abbr not in discount_no_data_stores
                ]
            if export_store_codes:
                print(f"[DISCOUNT DETAIL] Pulling Backoffice Discount Detail report from {DISCOUNT_DETAIL_REPORT_URL}")
                discount_approvals = merge_discount_approval_maps(
                    discount_approvals,
                    run_discount_detail_export_for_range(
                        loyalty_start_day,
                        end_day,
                        export_store_codes,
                        match_text=args.loyalty_discount_name,
                        required=required,
                        workers=args.discount_detail_browser_workers,
                    ),
                )
                updated_no_data_stores = existing_discount_detail_no_data_store_codes(
                    loyalty_start_day,
                    end_day,
                    selected_store_codes,
                )
                discount_no_data_stores |= (updated_no_data_stores - existing_discount_detail_export_store_codes(
                    loyalty_start_day,
                    end_day,
                    selected_store_codes,
                ))
            elif discount_approvals or discount_no_data_stores:
                print("[DISCOUNT DETAIL] All available Discount Detail files reused from archive.")
        else:
            discount_approvals = load_discount_detail_approvals_for_range(
                loyalty_start_day,
                end_day,
                selected_store_codes,
                match_text=args.loyalty_discount_name,
            )
            if not discount_approvals and loyalty_start_day != start_day:
                discount_approvals = load_discount_detail_approvals_for_range(
                    start_day,
                    end_day,
                    selected_store_codes,
                    match_text=args.loyalty_discount_name,
                )
            if discount_approvals:
                print(f"[DISCOUNT DETAIL] Reused archived approval rows for: {', '.join(sorted(discount_approvals))}")
            discount_no_data_stores = existing_discount_detail_no_data_store_codes(
                loyalty_start_day,
                end_day,
                selected_store_codes,
            )
            if discount_no_data_stores:
                print(f"[DISCOUNT DETAIL] Reused archived no-data markers for: {', '.join(sorted(discount_no_data_stores))}")

        if discount_approvals:
            counts = {
                abbr: int(len(df))
                for abbr, df in discount_approvals.items()
                if df is not None and not df.empty
            }
            if counts:
                print("[DISCOUNT DETAIL] Approval row counts: " + ", ".join(f"{abbr}={count}" for abbr, count in sorted(counts.items())))
                loyalty_detail_by_store = enrich_loyalty_detail_maps_with_discount_approvals(
                    loyalty_detail_by_store,
                    discount_approvals,
                )
                loyalty_detail_workbook = None
            if discount_no_data_stores:
                print("[DISCOUNT DETAIL] Confirmed no Discount Detail rows for: " + ", ".join(sorted(discount_no_data_stores)))
        elif discount_no_data_stores:
            print("[DISCOUNT DETAIL] Confirmed no Discount Detail rows for: " + ", ".join(sorted(discount_no_data_stores)))
        else:
            print(
                "[DISCOUNT DETAIL] WARN: No Discount Detail approval rows were loaded. "
                "Use --discount-detail-source browser to require the browser export instead of silently continuing."
            )

    loyalty_detail_by_store, register_loyalty_by_store = filter_loyalty_maps_to_range(
        loyalty_detail_by_store,
        register_loyalty_by_store,
        loyalty_start_day,
        end_day,
    )
    if loyalty_start_day != start_day:
        loyalty_detail_workbook = None

    forecast_bundle = None
    if FORECAST_ENABLED and not args.no_forecast:
        try:
            forecast_bundle = run_month_end_forecast_pipeline(store_daily_map, as_of=end_day)
            print_forecast_bundle(forecast_bundle)
        except Exception as e:
            print(f"[FORECAST] WARN: Forecast pipeline failed: {e}")
            forecast_bundle = None
    elif args.no_forecast:
        print("[FORECAST] Skipped (--no-forecast).")

    if loyalty_detail_workbook is None:
        loyalty_detail_workbook = write_loyalty_detail_workbook(
            LOYALTY_DETAIL_ROOT / f"{start_day.isoformat()}_to_{end_day.isoformat()}",
            loyalty_start_day,
            end_day,
            loyalty_detail_by_store,
            register_loyalty_by_store,
            match_text=args.loyalty_discount_name,
        )
        if loyalty_detail_workbook:
            print(f"[LOYALTY] Detailed discount report: {loyalty_detail_workbook}")

    pdf_run_dir = PDF_ROOT / f"{start_day.isoformat()}_to_{end_day.isoformat()}"
    pdf_run_dir.mkdir(parents=True, exist_ok=True)

    for store_name, abbr in _store_iter(selected_store_codes):
        daily = store_daily_map.get(abbr)
        df_raw = store_raw_df_map.get(abbr)
        if daily is None or daily.empty or df_raw is None:
            print(f"[SKIP] {abbr} missing data")
            continue

        out_pdf = pdf_run_dir / safe_filename(
            f"{abbr} - Owner Snapshot - {store_label(store_name)} - {end_day.isoformat()}.pdf"
        )
        build_store_pdf(
            out_pdf,
            store_name,
            abbr,
            df_raw,
            daily,
            start_day,
            end_day,
            loyalty_detail=loyalty_detail_by_store.get(abbr),
            register_loyalty_detail=register_loyalty_by_store.get(abbr),
            loyalty_discount_name=args.loyalty_discount_name,
        )

    if GENERATE_ALL_STORES_SUMMARY_PDF:
        out_pdf = pdf_run_dir / safe_filename(f"ALL STORES - Owner Snapshot - {end_day.isoformat()}.pdf")
        build_all_stores_summary_pdf(
            out_pdf,
            store_daily_map,
            end_day=end_day,
            start_day=start_day,
            forecast_bundle=forecast_bundle,
            store_raw_df_map=store_raw_df_map,
            loyalty_detail_by_store=loyalty_detail_by_store,
            register_loyalty_by_store=register_loyalty_by_store,
            loyalty_discount_name=args.loyalty_discount_name,
        )


    pdfs = sorted(str(p) for p in pdf_run_dir.glob("*.pdf"))
    executive_summary: Dict[str, Any] = {}
    store_summaries: List[Dict[str, Any]] = []

    try:
        mtd_start = month_start(end_day)
        fc_stores = (forecast_bundle or {}).get("stores", {})

        for store_name, abbr in _store_iter(selected_store_codes):
            daily = store_daily_map.get(abbr)
            if daily is None or daily.empty:
                continue
            s_today = metrics_for_day(daily, end_day)
            s_mtd = metrics_for_range(daily, mtd_start, end_day)
            s_fc = fc_stores.get(abbr, {}) if isinstance(fc_stores, dict) else {}
            store_summaries.append({
                "abbr": abbr,
                "store_label": store_label(store_name),
                "today_net": float(s_today.get("net_revenue", 0.0)),
                "today_tickets": float(s_today.get("tickets", 0.0)),
                "today_basket": float(s_today.get("basket", 0.0)),
                "today_discount_rate": float(s_today.get("discount_rate", 0.0)),
                "mtd_net": float(s_mtd.get("net_revenue", 0.0)),
                "mtd_tickets": float(s_mtd.get("tickets", 0.0)),
                "mtd_basket": float(s_mtd.get("basket", 0.0)),
                "mtd_margin": float(s_mtd.get("margin", 0.0)),
                "mtd_margin_real": float(s_mtd.get("margin_real", 0.0)),
                "proj_month_net": float(s_fc.get("net_pred", 0.0)) if s_fc else 0.0,
                "proj_month_profit": float(s_fc.get("profit_pred", 0.0)) if s_fc else 0.0,
                "proj_margin": float(s_fc.get("margin_pred", 0.0)) if s_fc else 0.0,
            })

        all_daily = _aggregate_all_stores_daily(store_daily_map)
        if all_daily is not None and not all_daily.empty:
            all_today = metrics_for_day(all_daily, end_day)
            all_mtd = metrics_for_range(all_daily, mtd_start, end_day)

            executive_summary.update({
                "today_net": float(all_today.get("net_revenue", 0.0)),
                "today_tickets": float(all_today.get("tickets", 0.0)),
                "today_basket": float(all_today.get("basket", 0.0)),
                "today_discount_rate": float(all_today.get("discount_rate", 0.0)),
                "mtd_net": float(all_mtd.get("net_revenue", 0.0)),
                "mtd_tickets": float(all_mtd.get("tickets", 0.0)),
                "mtd_basket": float(all_mtd.get("basket", 0.0)),
                "mtd_margin": float(all_mtd.get("margin", 0.0)),
            })

        all_fc = (forecast_bundle or {}).get("stores", {}).get("ALL", {})
        if all_fc:
            executive_summary.update({
                "proj_month_net": float(all_fc.get("net_pred", 0.0)),
                "proj_month_profit": float(all_fc.get("profit_pred", 0.0)),
                "proj_margin": float(all_fc.get("margin_pred", 0.0)),
                "remaining_days": int(all_fc.get("remaining_days", 0)),
            })
    except Exception as e:
        print(f"[EMAIL] WARN: Could not build executive summary: {e}")
        executive_summary = {}

    if args.no_email:
        print("[EMAIL] Skipped (--no-email).")
    else:
        email_recipients = parse_email_recipients(args.email) or [
            "anthony@buzzcannabis.com",
            "ray@buzzcannabis.com",
            "kevin@buzzcannabis.com",
            # "joseph@buzzcannabis.com",
            "stevei@buzzcannabis.com",
            "andyhirmez@yahoo.com",
            "stevegabbo@hotmail.com"
        ]
        send_owner_snapshot_email(
            pdf_paths=pdfs,
            report_day=end_day,
            data_start=start_day,
            data_end=end_day,
            executive_summary=executive_summary,
            store_summaries=store_summaries,
            to_email=email_recipients,
        )
    print("\nDone ✅")
#python owner_snapshot.py --report-day 2026-01-31 --run-export

if __name__ == "__main__":
    main()
