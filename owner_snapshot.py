import re
import shutil
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
from owner_emailer import send_owner_snapshot_email

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

# --- IMPORTANT: uses your existing exporter ---
from getSalesReport import run_sales_report, store_abbr_map  # store_name -> "MV"

###############################################################################
# CONFIG (easy to change)
###############################################################################

REPORT_TZ = "America/Los_Angeles"

# Backfill window used only when RUN_EXPORT=True
BACKFILL_DAYS = 61

REPORTS_ROOT = Path("reports").resolve()
RAW_ROOT = REPORTS_ROOT / "raw_sales"
PDF_ROOT = REPORTS_ROOT / "pdf"

# If True: run Selenium export and archive fresh files
# If False: reuse latest RAW folder, do NOT run Selenium
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

# --- Dutchie export header row ---
FORCE_HEADER_ROW = True
EXPORT_HEADER_ROW_INDEX = 4  # Excel row 5

# Discover getSalesReport /files directory
import getSalesReport as gsr
FILES_DIR = Path(gsr.__file__).resolve().parent / "files"


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

def find_latest_raw_folder() -> Optional[Path]:
    if not RAW_ROOT.exists():
        return None
    folders = [p for p in RAW_ROOT.iterdir() if p.is_dir()]
    if not folders:
        return None
    return sorted(folders, key=lambda p: p.stat().st_mtime, reverse=True)[0]


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
    return daily[(daily["date"] >= start) & (daily["date"] <= end)].copy()

def metrics_for_day(daily: pd.DataFrame, day: date) -> Dict[str, float]:
    row = daily[daily["date"] == day]
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

    doc.build(story, onFirstPage=footer, onLaterPages=footer)
    print(f"✅ PDF created: {out_pdf}")


###############################################################################
# PDF: All stores summary (kept simple but consistent)
###############################################################################

def build_all_stores_summary_pdf(out_pdf: Path, store_daily_map: Dict[str, pd.DataFrame], end_day: date, start_day: date, forecast_bundle: Optional[Dict[str, Any]] = None) -> None:
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
        headers=["Store", "Today Net", "Δ vs LW", "MTD Net", "Δ vs Last MTD", "MTD Margin", "MTD Tickets"],
        rows=rows,
        col_widths=[2.45*inch, 0.85*inch, 0.80*inch, 0.85*inch, 0.95*inch, 1.05*inch, 0.65*inch],
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

    doc.build(story, onFirstPage=footer, onLaterPages=footer)
    print(f"✅ All-stores summary PDF created: {out_pdf}")


###############################################################################
# MAIN
###############################################################################

def main():
    setup_fonts()

    REPORTS_ROOT.mkdir(parents=True, exist_ok=True)
    RAW_ROOT.mkdir(parents=True, exist_ok=True)
    PDF_ROOT.mkdir(parents=True, exist_ok=True)

    abbr_to_file: Dict[str, Path] = {}

    if RUN_EXPORT:
        print("⚠️ RUN_EXPORT=True → Running Selenium export")
        start_day, end_day = compute_date_window(BACKFILL_DAYS, REPORT_TZ)
        run_export_for_range(start_day, end_day)
        _, abbr_to_file = archive_exports(start_day, end_day)
    else:
        print("✅ RUN_EXPORT=False → Reusing latest raw export folder")
        raw_folder = find_latest_raw_folder()
        if raw_folder is None:
            raise SystemExit("No raw export folders found in reports/raw_sales and RUN_EXPORT=False.")

        parsed = parse_range_from_folder_name(raw_folder)
        if parsed:
            start_day, end_day = parsed
        else:
            start_day, end_day = compute_date_window(BACKFILL_DAYS, REPORT_TZ)

        for store_name, abbr in store_abbr_map.items():
            matches = list(raw_folder.glob(f"{abbr}*Sales Export*.xlsx"))
            if matches:
                abbr_to_file[abbr] = matches[0]

    if not abbr_to_file:
        raise SystemExit("No store exports found. Check getSalesReport output /files or raw archive.")

    store_daily_map: Dict[str, pd.DataFrame] = {}
    store_raw_df_map: Dict[str, pd.DataFrame] = {}


    for store_name, abbr in store_abbr_map.items():
        path = abbr_to_file.get(abbr)
        if not path:
            continue

        print(f"[PARSE] {abbr}: {path.name}")
        df = read_export(path)

        # ✅ APPLY brand-based kickback adjustments BEFORE metrics
        if APPLY_DEAL_KICKBACKS:
            df = enrich_with_deal_kickbacks_by_brand(df, store_code=abbr)

        store_raw_df_map[abbr] = df

        daily = compute_daily_metrics(df)
        daily = daily[(daily["date"] >= start_day) & (daily["date"] <= end_day)]
        store_daily_map[abbr] = daily
    forecast_bundle = None
    if FORECAST_ENABLED:
        try:
            forecast_bundle = run_month_end_forecast_pipeline(store_daily_map, as_of=end_day)
            print_forecast_bundle(forecast_bundle)
        except Exception as e:
            print(f"[FORECAST] WARN: Forecast pipeline failed: {e}")
            forecast_bundle = None

    pdf_run_dir = PDF_ROOT / f"{start_day.isoformat()}_to_{end_day.isoformat()}"
    pdf_run_dir.mkdir(parents=True, exist_ok=True)

    for store_name, abbr in store_abbr_map.items():
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
        )

    if GENERATE_ALL_STORES_SUMMARY_PDF:
        out_pdf = pdf_run_dir / safe_filename(f"ALL STORES - Owner Snapshot - {end_day.isoformat()}.pdf")
        build_all_stores_summary_pdf(out_pdf, store_daily_map, end_day=end_day, start_day=start_day, forecast_bundle=forecast_bundle)


    pdfs = sorted(str(p) for p in pdf_run_dir.glob("*.pdf"))
    executive_summary: Dict[str, Any] = {}
    store_summaries: List[Dict[str, Any]] = []

    try:
        mtd_start = month_start(end_day)
        fc_stores = (forecast_bundle or {}).get("stores", {})

        for store_name, abbr in store_abbr_map.items():
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

    send_owner_snapshot_email(
        pdf_paths=pdfs,
        report_day=end_day,
        data_start=start_day,
        data_end=end_day,
        executive_summary=executive_summary,
        store_summaries=store_summaries,
        to_email=[
        "anthony@buzzcannabis.com",
        # "ray@buzzcannabis.com",
        # "kevin@buzzcannabis.com",
        # "joseph@buzzcannabis.com",
    ],
    )
    print("\nDone ✅")


if __name__ == "__main__":
    main()
