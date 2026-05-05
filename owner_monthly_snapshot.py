import argparse
import calendar
import json
import math
import re
import shutil
import time
from concurrent.futures import ThreadPoolExecutor, as_completed
from dataclasses import dataclass
from datetime import date, datetime, timedelta
from decimal import Decimal, ROUND_CEILING, ROUND_DOWN, ROUND_HALF_UP
from io import BytesIO
from pathlib import Path
from typing import Any, Dict, List, Optional, Tuple
from zoneinfo import ZoneInfo

import numpy as np
import pandas as pd

import matplotlib

matplotlib.use("Agg")
import matplotlib.pyplot as plt
from matplotlib.ticker import FuncFormatter

from reportlab.lib import colors
from reportlab.lib.pagesizes import letter
from reportlab.lib.styles import ParagraphStyle
from reportlab.lib.units import inch
from reportlab.platypus import (
    CondPageBreak,
    KeepTogether,
    Image,
    LongTable,
    PageBreak,
    Paragraph,
    SimpleDocTemplate,
    Spacer,
    Table,
    TableStyle,
)

import getSalesReport as gsr
from store_discount_roundup_sheet import STORE_TAXES as ROUNDUP_STORE_TAXES
from dutchie_api_reports import (
    DEFAULT_ENV_FILE as DUTCHIE_DEFAULT_ENV_FILE,
    STORE_CODES as DUTCHIE_STORE_CODES,
    canonical_env_map,
    create_session,
    isoformat_utc,
    local_date_range_to_utc_strings,
    request_json,
    resolve_integrator_key,
    resolve_store_keys,
)
import owner_snapshot as _owner_snapshot
from owner_emailer import send_owner_snapshot_email
from owner_snapshot import (
    setup_fonts,
    money,
    money2,
    pct1,
    pctN,
    pp1,
    fmt_signed_money,
    fmt_signed_int,
    safe_filename,
    store_label,
    to_number,
    find_col,
    parse_brand_from_product,
    read_export,
    enrich_with_deal_kickbacks_by_brand,
    compute_daily_metrics,
    metrics_for_day,
    metrics_for_range,
    compute_category_summary,
    compute_brand_summary,
    compute_budtender_summary,
    compute_cart_value_distribution,
    chart_rank_barh,
    chart_cart_value_distribution,
    build_styles,
    build_table,
    build_kpi_grid,
    make_footer,
    run_export_for_range,
    archive_exports,
    store_abbr_map,
)


REPORT_TZ = "America/Los_Angeles"
REPORTS_ROOT = Path("reports").resolve()
MONTHLY_ROOT = REPORTS_ROOT / "monthly"
MONTHLY_RAW_ROOT = MONTHLY_ROOT / "raw_sales"
MONTHLY_PDF_ROOT = MONTHLY_ROOT / "pdf"
MONTHLY_DATA_ROOT = MONTHLY_ROOT / "data"
MONTHLY_CLOSING_ROOT = MONTHLY_ROOT / "closing"
DAILY_RAW_ROOT = REPORTS_ROOT / "raw_sales"
FILES_DIR = Path(gsr.__file__).resolve().parent / "files"

BUZZ = {
    "yellow": colors.HexColor("#FFF200"),
    "green": colors.HexColor("#00AE6F"),
    "black": colors.HexColor("#000000"),
    "white": colors.white,
    "muted": colors.HexColor("#374151"),
    "muted2": colors.HexColor("#6B7280"),
    "soft": colors.HexColor("#F7F7F7"),
    "soft_gray": colors.HexColor("#F3F4F6"),
    "border": colors.HexColor("#E5E7EB"),
    "row_alt": colors.HexColor("#FAFAFA"),
    "danger": colors.HexColor("#FEE2E2"),
    "warn": colors.HexColor("#FEF3C7"),
}

HEX_GREEN = "#00AE6F"
HEX_YELLOW = "#FFF200"
HEX_BLACK = "#000000"
HEX_MUTED = "#374151"
HEX_BORDER = "#E5E7EB"
HEX_GOOD = "#047857"
HEX_BAD = "#B91C1C"
HEX_NEUTRAL = "#6B7280"

PAGE_MARGINS = {
    "left": 0.45 * inch,
    "right": 0.45 * inch,
    "top": 0.45 * inch,
    "bottom": 0.45 * inch,
}

STORE_NAME_BY_ABBR = {abbr: store_name for store_name, abbr in store_abbr_map.items()}
STORE_LABEL_BY_ABBR = {abbr: store_label(store_name) for store_name, abbr in store_abbr_map.items()}
STORE_ORDER = list(STORE_NAME_BY_ABBR.keys())
STORE_ORDER_INDEX = {abbr: i for i, abbr in enumerate(STORE_ORDER)}
WEEKDAY_NAMES = ["Monday", "Tuesday", "Wednesday", "Thursday", "Friday", "Saturday", "Sunday"]
WEEKDAY_SHORT = ["Mon", "Tue", "Wed", "Thu", "Fri", "Sat", "Sun"]

COLUMN_CANDIDATES = getattr(_owner_snapshot, "COLUMN_CANDIDATES", {
    "date": ["Order Time", "Transaction Date", "Transaction Date (Local)", "Date", "Sold At", "Created At", "Order Date"],
    "transaction_id": ["Order ID", "Transaction ID", "Order Number", "Receipt ID", "Ticket", "Ticket Number", "Sale ID", "Cart ID"],
    "employee": ["Budtender Name", "Budtender", "Employee", "Employee Name", "Cashier"],
    "product": ["Product Name", "Product", "Item Name", "Item"],
    "category": ["Major Category", "Category", "Product Category", "Product Category Name"],
    "quantity": ["Total Inventory Sold", "Quantity", "Qty", "Items", "Item Quantity"],
    "gross_sales": ["Gross Sales", "Gross Revenue", "Subtotal", "Total", "Gross"],
    "net_sales": ["Net Sales", "Net Revenue", "Net Total", "Net", "Net Amount", "Total (Net)"],
    "discount_main": ["Discounted Amount", "Discount Amount", "Discount", "Total Discount"],
    "discount_loyalty": ["Loyalty as Discount"],
    "cogs": ["Inventory Cost", "COGS", "Cost of Goods Sold", "Cost"],
    "profit": ["Order Profit", "Profit", "Gross Profit", "Net Profit"],
    "return_date": ["Return Date"],
    "total_weight_sold": ["Total Weight Sold", "Total Weight", "Weight Sold"],
})

METRIC_SUM_FIELDS = [
    "net_revenue",
    "gross_sales",
    "tickets",
    "items",
    "discount",
    "discount_main",
    "loyalty_discount",
    "cogs",
    "profit",
    "cogs_real",
    "profit_real",
    "returns_net",
    "returns_tickets",
    "weight_sold",
]


@dataclass
class StoreBundle:
    abbr: str
    store_name: str
    label: str
    path: Path
    raw_df: pd.DataFrame
    daily_df: pd.DataFrame
    metrics: Dict[str, float]
    detected_columns: Dict[str, Optional[str]]


def parse_iso_date(value: str) -> date:
    try:
        return datetime.strptime(value, "%Y-%m-%d").date()
    except ValueError as exc:
        raise argparse.ArgumentTypeError(f"Invalid date '{value}'. Use YYYY-MM-DD.") from exc


def parse_month(value: str) -> Tuple[date, date]:
    if not re.match(r"^\d{4}-\d{2}$", value or ""):
        raise argparse.ArgumentTypeError("Month must use YYYY-MM, for example 2026-04.")
    year, month = [int(part) for part in value.split("-")]
    if month < 1 or month > 12:
        raise argparse.ArgumentTypeError("Month must be between 01 and 12.")
    last_day = calendar.monthrange(year, month)[1]
    return date(year, month, 1), date(year, month, last_day)


def parse_cli_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="Generate monthly owner review PDFs and data exports.")
    parser.add_argument("--month", help="Full month to report, in YYYY-MM format.")
    parser.add_argument("--start-date", type=parse_iso_date, help="Custom report start date, YYYY-MM-DD.")
    parser.add_argument("--end-date", type=parse_iso_date, help="Custom report end date, YYYY-MM-DD.")
    parser.add_argument("--run-export", dest="run_export", action="store_true", help="Run Dutchie export for this monthly window. Enabled by default.")
    parser.add_argument(
        "--run-export-api",
        action="store_true",
        help="Shortcut for --run-export --export-source api. Pulls monthly sales through the Dutchie POS API.",
    )
    parser.add_argument("--reuse-latest", dest="run_export", action="store_false", help="Reuse archived monthly raw files.")
    parser.add_argument(
        "--export-source",
        choices=["backoffice", "api"],
        default="api",
        help="Sales export source used by --run-export. Default: api.",
    )
    parser.add_argument("--no-email", action="store_true", help="Build files but do not send email.")
    parser.add_argument("--dry-run-email", action="store_true", help="Print monthly email details without sending.")
    parser.add_argument(
        "--to-email",
        action="append",
        default=None,
        help="Override monthly email recipients. May be repeated or comma-separated.",
    )
    parser.add_argument(
        "--detail-level",
        choices=["executive", "standard", "deep"],
        default="deep",
        help="Report detail level. Default: deep.",
    )
    parser.add_argument(
        "--pdf-style",
        choices=["executive-clean", "legacy"],
        default="executive-clean",
        help="Monthly PDF presentation style. Default: executive-clean.",
    )
    parser.add_argument(
        "--max-main-table-rows",
        type=int,
        default=None,
        help="Maximum table rows shown on main PDF pages. Defaults by detail level.",
    )
    parser.add_argument(
        "--main-top-n",
        type=int,
        default=None,
        help="Top-N row limit for executive/main PDF pages. Defaults by detail level.",
    )
    parser.add_argument(
        "--appendix-top-n",
        type=int,
        default=None,
        help="Top-N row limit for capped appendix tables. Full-detail daily/new-customer/kickback sections are never capped.",
    )
    full_detail_group = parser.add_mutually_exclusive_group()
    full_detail_group.add_argument(
        "--full-detail-pdf",
        dest="full_detail_pdf",
        action="store_true",
        default=None,
        help="Include full-detail PDF appendix sections for daily, new-customer, and kickback detail.",
    )
    full_detail_group.add_argument(
        "--no-full-detail-pdf",
        dest="full_detail_pdf",
        action="store_false",
        help="Skip full-detail PDF appendix sections. CSV/XLSX exports are still written.",
    )
    parser.add_argument(
        "--no-store-pdfs",
        action="store_true",
        help="Generate only the all-stores monthly PDF.",
    )
    parser.add_argument(
        "--summary-only",
        action="store_true",
        help="Generate a short all-stores executive report only.",
    )
    parser.add_argument("--workers", type=int, default=6, help="Worker count for parsing XLSX/API work. Default: 6.")
    parser.add_argument(
        "--closing-report-dir",
        type=Path,
        default=None,
        help="Folder containing closing report CSV/XLSX files with new customer counts. Default: reports/monthly/closing/YYYY-MM/.",
    )
    parser.add_argument(
        "--fetch-closing-api",
        action="store_true",
        help="Fetch daily new-customer counts from Dutchie /reporting/closing-report into the monthly closing folder.",
    )
    parser.add_argument(
        "--fetch-closing-summary-api",
        dest="fetch_closing_summary_api",
        action="store_true",
        help="Fetch one full-window Dutchie /reporting/closing-report per store for Backoffice closing report totals. Enabled by default.",
    )
    parser.add_argument(
        "--no-fetch-closing-summary-api",
        dest="fetch_closing_summary_api",
        action="store_false",
        help="Do not fetch monthly closing report summary totals.",
    )
    parser.add_argument(
        "--closing-summary-only",
        action="store_true",
        help="Only fetch/write the monthly closing report summary, then exit without building PDFs.",
    )
    parser.add_argument(
        "--fetch-new-customers-api",
        dest="fetch_new_customers_api",
        action="store_true",
        help="Fetch customer profile creation dates as fallback if daily closing-report counts are unavailable. Enabled by default.",
    )
    parser.add_argument(
        "--no-fetch-new-customers-api",
        dest="fetch_new_customers_api",
        action="store_false",
        help="Do not fetch customer profile creation fallback data.",
    )
    parser.add_argument(
        "--fetch-inventory-api",
        dest="fetch_inventory_api",
        action="store_true",
        help="Fetch Dutchie /inventory/snapshot for first-of-month and end-of-month inventory gain/loss reporting. Enabled by default.",
    )
    parser.add_argument(
        "--no-fetch-inventory-api",
        dest="fetch_inventory_api",
        action="store_false",
        help="Do not fetch inventory snapshots.",
    )
    parser.add_argument(
        "--api-env-file",
        default=str(DUTCHIE_DEFAULT_ENV_FILE),
        help=f"Env file containing Dutchie location keys for --fetch-closing-api. Default: {DUTCHIE_DEFAULT_ENV_FILE}.",
    )
    data_book_group = parser.add_mutually_exclusive_group()
    data_book_group.add_argument("--include-data-book", dest="include_data_book", action="store_true", help="Write the Excel data book. Enabled by default.")
    data_book_group.add_argument("--no-data-book", dest="include_data_book", action="store_false", help="Skip the Excel data book.")
    parser.set_defaults(
        run_export=True,
        include_data_book=True,
        fetch_closing_summary_api=True,
        fetch_new_customers_api=True,
        fetch_inventory_api=True,
    )
    return parser.parse_args()


def month_start_end(year: int, month: int) -> Tuple[date, date]:
    return date(year, month, 1), date(year, month, calendar.monthrange(year, month)[1])


def previous_completed_month(today: Optional[date] = None) -> Tuple[date, date]:
    if today is None:
        today = datetime.now(ZoneInfo(REPORT_TZ)).date()
    first_this_month = date(today.year, today.month, 1)
    last_prev = first_this_month - timedelta(days=1)
    return month_start_end(last_prev.year, last_prev.month)


def is_full_month(start_day: date, end_day: date) -> bool:
    full_start, full_end = month_start_end(start_day.year, start_day.month)
    return start_day == full_start and end_day == full_end


def month_key_for_range(start_day: date, end_day: date) -> str:
    if is_full_month(start_day, end_day):
        return start_day.strftime("%Y-%m")
    return f"{start_day.isoformat()}_to_{end_day.isoformat()}"


def infer_range_from_key(key: str) -> Optional[Tuple[date, date]]:
    if re.match(r"^\d{4}-\d{2}$", key):
        year, month = [int(part) for part in key.split("-")]
        return month_start_end(year, month)
    m = re.match(r"^(\d{4}-\d{2}-\d{2})_to_(\d{4}-\d{2}-\d{2})$", key)
    if not m:
        return None
    return parse_iso_date(m.group(1)), parse_iso_date(m.group(2))


def resolve_date_window(args: argparse.Namespace) -> Tuple[date, date, bool]:
    explicit = bool(args.month or args.start_date or args.end_date)
    if args.month and (args.start_date or args.end_date):
        raise SystemExit("Use either --month or --start-date/--end-date, not both.")
    if args.month:
        return (*parse_month(args.month), True)
    if args.start_date or args.end_date:
        if not args.start_date or not args.end_date:
            raise SystemExit("--start-date and --end-date must be passed together.")
        if args.start_date > args.end_date:
            raise SystemExit("Start date cannot be after end date.")
        return args.start_date, args.end_date, explicit
    start_day, end_day = previous_completed_month()
    return start_day, end_day, False


def previous_month_range(start_day: date) -> Tuple[date, date]:
    prior_last = start_day - timedelta(days=1)
    return month_start_end(prior_last.year, prior_last.month)


def same_month_prior_year_range(start_day: date, end_day: date) -> Tuple[date, date]:
    year = start_day.year - 1
    month = start_day.month
    start = date(year, month, min(start_day.day, calendar.monthrange(year, month)[1]))
    end = date(year, month, min(end_day.day, calendar.monthrange(year, month)[1]))
    return start, end


def ensure_monthly_dirs(month_key: str) -> Tuple[Path, Path, Path]:
    raw_dir = MONTHLY_RAW_ROOT / month_key
    pdf_dir = MONTHLY_PDF_ROOT / month_key
    data_dir = MONTHLY_DATA_ROOT / month_key
    for path in [raw_dir, pdf_dir, data_dir]:
        path.mkdir(parents=True, exist_ok=True)
    return raw_dir, pdf_dir, data_dir


def store_sort_key(abbr: str) -> Tuple[int, str]:
    key = str(abbr or "").upper()
    return STORE_ORDER_INDEX.get(key, 999), key


def list_sales_export_paths(folder: Path) -> Dict[str, Path]:
    out: Dict[str, Path] = {}
    if not folder.exists():
        return out
    for abbr in STORE_ORDER:
        matches = sorted(folder.glob(f"{abbr}*Sales Export*.xlsx"))
        if matches:
            out[abbr] = matches[0]
    return out


def parse_daily_range_folder(folder: Path) -> Optional[Tuple[date, date]]:
    m = re.match(r"^(\d{4}-\d{2}-\d{2})_to_(\d{4}-\d{2}-\d{2})$", folder.name)
    if not m:
        return None
    try:
        return parse_iso_date(m.group(1)), parse_iso_date(m.group(2))
    except Exception:
        return None


def find_daily_folder_covering(start_day: date, end_day: date) -> Optional[Path]:
    if not DAILY_RAW_ROOT.exists():
        return None
    candidates: List[Tuple[int, float, Path]] = []
    for folder in DAILY_RAW_ROOT.iterdir():
        if not folder.is_dir():
            continue
        parsed = parse_daily_range_folder(folder)
        if not parsed:
            continue
        folder_start, folder_end = parsed
        if folder_start <= start_day and folder_end >= end_day and list_sales_export_paths(folder):
            span_days = (folder_end - folder_start).days
            candidates.append((span_days, -folder.stat().st_mtime, folder))
    if not candidates:
        return None
    return sorted(candidates)[0][2]


def find_latest_monthly_raw_folder() -> Optional[Path]:
    if not MONTHLY_RAW_ROOT.exists():
        return None
    folders = [p for p in MONTHLY_RAW_ROOT.iterdir() if p.is_dir() and list_sales_export_paths(p)]
    if not folders:
        return None
    return sorted(folders, key=lambda p: p.stat().st_mtime, reverse=True)[0]


def copy_exports_to_monthly_raw(source_folder: Path, raw_dir: Path, warnings: List[Dict[str, Any]]) -> Dict[str, Path]:
    raw_dir.mkdir(parents=True, exist_ok=True)
    copied: Dict[str, Path] = {}
    source_paths = list_sales_export_paths(source_folder)
    for abbr, src in source_paths.items():
        dst = raw_dir / src.name
        if not dst.exists():
            shutil.copy2(src, dst)
        copied[abbr] = dst
    warnings.append({
        "severity": "Low",
        "message": f"Copied reusable raw exports from {source_folder} into monthly folder {raw_dir}.",
    })
    return list_sales_export_paths(raw_dir)


def monthly_archive_exports(start_day: date, end_day: date, raw_dir: Path, warnings: List[Dict[str, Any]]) -> Dict[str, Path]:
    raw_dir.mkdir(parents=True, exist_ok=True)
    abbr_to_path: Dict[str, Path] = {}
    for store_name, abbr in store_abbr_map.items():
        src = FILES_DIR / f"sales{abbr}.xlsx"
        if not src.exists():
            warnings.append({
                "severity": "Medium",
                "message": f"Missing export for {abbr} after --run-export: {src}",
            })
            continue
        dst_name = f"{abbr} - Sales Export - {store_label(store_name)} - {start_day.isoformat()}_to_{end_day.isoformat()}.xlsx"
        dst = raw_dir / safe_filename(dst_name)
        shutil.move(str(src), str(dst))
        abbr_to_path[abbr] = dst
        print(f"[ARCHIVE monthly] {abbr}: {dst}")
    return abbr_to_path


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
        for key in ("data", "results", "items", "transactions", "products"):
            rows = payload.get(key)
            if isinstance(rows, list):
                return [row for row in rows if isinstance(row, dict)]
        return [payload]
    return []


def normalize_api_product_label(product_name: Any, brand_name: Any) -> str:
    product = str(product_name or "").strip() or "Unknown Product"
    brand = str(brand_name or "").strip()
    if not brand:
        return product
    parsed = parse_brand_from_product(product)
    if parsed.strip().lower() == brand.lower():
        return product
    return f"{brand} | {product}"


def build_catalog_lookup(catalog_rows: List[Dict[str, Any]]) -> Dict[str, Dict[str, Any]]:
    lookup: Dict[str, Dict[str, Any]] = {}
    for row in catalog_rows:
        product_id = row.get("productId") or row.get("id") or row.get("globalProductId")
        if product_id in (None, ""):
            continue
        lookup[str(product_id)] = row
    return lookup


def normalize_api_sales_rows(
    store_code: str,
    transactions: List[Dict[str, Any]],
    catalog_lookup: Dict[str, Dict[str, Any]],
) -> pd.DataFrame:
    rows: List[Dict[str, Any]] = []
    for tx in transactions:
        if bool(tx.get("isVoid")):
            continue
        items = parse_api_nested(tx.get("items"))
        if not isinstance(items, list):
            items = []
        transaction_id = tx.get("transactionId") or tx.get("globalId") or tx.get("invoiceNumber")
        tx_date = tx.get("transactionDateLocalTime") or tx.get("transactionDate")
        employee = tx.get("completedByUser") or tx.get("employeeName") or tx.get("employeeId") or "Unknown"
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
            rows.append({
                "Order ID": transaction_id,
                "Order Time": tx_date,
                "Budtender Name": employee,
                "Product Name": "Unknown Product",
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
            })
            continue

        for item in items:
            if not isinstance(item, dict) or bool(item.get("isCoupon")):
                continue
            product_id = item.get("productId")
            catalog = catalog_lookup.get(str(product_id), {})
            product_name = catalog.get("productName") or item.get("productName") or item.get("name") or f"Product {product_id or 'Unknown'}"
            brand_name = catalog.get("brandName") or item.get("brandName")
            master_category = catalog.get("masterCategory") or catalog.get("category") or item.get("masterCategory") or item.get("category") or "Unknown"
            product_category = catalog.get("category") or master_category

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
            rows.append({
                "Order ID": transaction_id,
                "Order Time": tx_date,
                "Transaction Date": tx_date,
                "Budtender Name": employee,
                "Product Name": normalize_api_product_label(product_name, brand_name),
                "Brand": str(brand_name or parse_brand_from_product(product_name) or "Unknown").strip(),
                "Major Category": master_category,
                "Category": master_category,
                "Product Category": product_category,
                "Total Inventory Sold": qty,
                "Gross Sales": gross,
                "Discounted Amount": discount,
                "Loyalty as Discount": 0.0,
                "Net Sales": net,
                "Inventory Cost": cogs,
                "Order Profit": net - cogs,
                "Return Date": return_date if row_is_return else None,
                "Total Weight Sold": as_float(item.get("unitWeight")) * abs(qty),
                "Package ID": item.get("packageId"),
                "Inventory ID": item.get("inventoryId"),
                "Product ID": product_id,
                "Store": store_code,
            })

    if not rows:
        return pd.DataFrame(columns=[
            "Order ID",
            "Order Time",
            "Budtender Name",
            "Product Name",
            "Major Category",
            "Category",
            "Total Inventory Sold",
            "Gross Sales",
            "Discounted Amount",
            "Loyalty as Discount",
            "Net Sales",
            "Inventory Cost",
            "Order Profit",
            "Return Date",
            "Total Weight Sold",
            "Store",
        ])
    out = pd.DataFrame(rows)
    if "Order Time" in out.columns:
        out["Order Time"] = pd.to_datetime(out["Order Time"], errors="coerce")
    return out


def fetch_store_sales_export_api(
    store_code: str,
    location_key: str,
    integrator_key: str,
    start_day: date,
    end_day: date,
    raw_dir: Path,
) -> Tuple[str, Optional[Path], int, List[Dict[str, Any]]]:
    session = create_session(location_key, integrator_key)
    store_warnings: List[Dict[str, Any]] = []
    try:
        from_utc, to_utc = local_date_range_to_utc_strings(start_day.isoformat(), end_day.isoformat(), REPORT_TZ)
        sales_payload = request_json(
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
            max_attempts=4,
        )
        catalog_payload = request_json(
            session,
            "/reporting/products",
            params={},
            timeout=240,
            max_attempts=4,
        )
    finally:
        session.close()

    sales_rows = api_payload_records(sales_payload)
    catalog_lookup = build_catalog_lookup(api_payload_records(catalog_payload))
    export_df = normalize_api_sales_rows(store_code, sales_rows, catalog_lookup)
    if export_df.empty:
        store_warnings.append({
            "severity": "Medium",
            "message": f"Dutchie sales API returned no line-item rows for {store_code}.",
        })
        return store_code, None, 0, store_warnings

    raw_dir.mkdir(parents=True, exist_ok=True)
    store_name = STORE_NAME_BY_ABBR.get(store_code, DUTCHIE_STORE_CODES.get(store_code, store_code))
    dst_name = f"{store_code} - Sales Export - {store_label(store_name)} - {start_day.isoformat()}_to_{end_day.isoformat()}.xlsx"
    dst = raw_dir / safe_filename(dst_name)
    with pd.ExcelWriter(dst, engine="openpyxl") as writer:
        export_df.to_excel(writer, index=False, sheet_name="Sales")
    return store_code, dst, len(export_df), store_warnings


def run_monthly_sales_export_api(
    start_day: date,
    end_day: date,
    raw_dir: Path,
    env_file: str,
    workers: Optional[int],
    warnings: List[Dict[str, Any]],
) -> Dict[str, Path]:
    env_map = canonical_env_map(env_file)
    selected_store_codes = [abbr for abbr in STORE_ORDER if abbr in DUTCHIE_STORE_CODES]
    store_keys = resolve_store_keys(env_map, selected_store_codes)
    integrator_key = resolve_integrator_key(env_map)
    missing = [abbr for abbr in selected_store_codes if abbr not in store_keys]
    if missing:
        warnings.append({
            "severity": "Medium",
            "message": "Missing Dutchie API location key(s) for monthly sales export: " + ", ".join(missing),
        })

    jobs = [(abbr, store_keys[abbr]) for abbr in selected_store_codes if abbr in store_keys]
    if not jobs:
        raise SystemExit(
            "No Dutchie API location keys were available for monthly sales export. "
            f"Add keys to {env_file} using names like DUTCHIE_API_KEY_MV or MV."
        )

    worker_count = max(1, min(int(workers or 4), len(jobs)))
    print(f"[EXPORT API] Fetching monthly sales exports with {worker_count} worker(s)")
    abbr_to_path: Dict[str, Path] = {}
    manifest: List[Dict[str, Any]] = []
    if worker_count == 1:
        results = [
            fetch_store_sales_export_api(abbr, key, integrator_key, start_day, end_day, raw_dir)
            for abbr, key in jobs
        ]
    else:
        results = []
        with ThreadPoolExecutor(max_workers=worker_count) as executor:
            futures = {
                executor.submit(fetch_store_sales_export_api, abbr, key, integrator_key, start_day, end_day, raw_dir): abbr
                for abbr, key in jobs
            }
            for future in as_completed(futures):
                abbr = futures[future]
                try:
                    results.append(future.result())
                except Exception as exc:
                    warnings.append({
                        "severity": "High",
                        "message": f"Dutchie sales API export failed for {abbr}: {exc}",
                    })

    for store_code, path, row_count, store_warnings in sorted(results, key=lambda item: STORE_ORDER_INDEX.get(item[0], 999)):
        warnings.extend(store_warnings)
        if path is None:
            print(f"[EXPORT API] {store_code}: no rows")
            manifest.append({"store": store_code, "rows": row_count, "path": None})
            continue
        abbr_to_path[store_code] = path
        manifest.append({"store": store_code, "rows": row_count, "path": str(path)})
        print(f"[EXPORT API] {store_code}: {row_count:,} row(s) -> {path}")

    (raw_dir / "api_sales_export_manifest.json").write_text(
        json.dumps(manifest, indent=2, ensure_ascii=False, default=str),
        encoding="utf-8",
    )
    if not abbr_to_path:
        raise SystemExit("Dutchie sales API export completed, but no usable monthly XLSX exports were created.")
    return abbr_to_path


def resolve_raw_exports(
    args: argparse.Namespace,
    start_day: date,
    end_day: date,
    month_key: str,
    explicit_dates: bool,
    warnings: List[Dict[str, Any]],
) -> Tuple[date, date, str, Path, Dict[str, Path]]:
    raw_dir, _, _ = ensure_monthly_dirs(month_key)

    if args.run_export is True:
        if args.export_source == "api":
            print(f"[EXPORT API] Running monthly sales API export for {start_day.isoformat()} -> {end_day.isoformat()}")
            return (
                start_day,
                end_day,
                month_key,
                raw_dir,
                run_monthly_sales_export_api(start_day, end_day, raw_dir, args.api_env_file, args.workers, warnings),
            )
        print(f"[EXPORT] Running monthly Backoffice export for {start_day.isoformat()} -> {end_day.isoformat()}")
        run_export_for_range(start_day, end_day)
        return start_day, end_day, month_key, raw_dir, monthly_archive_exports(start_day, end_day, raw_dir, warnings)

    existing = list_sales_export_paths(raw_dir)
    if existing:
        print(f"[RAW] Reusing monthly raw folder: {raw_dir}")
        return start_day, end_day, month_key, raw_dir, existing

    if args.run_export is False and not explicit_dates:
        latest = find_latest_monthly_raw_folder()
        if latest is not None:
            inferred = infer_range_from_key(latest.name)
            if inferred:
                start_day, end_day = inferred
                month_key = latest.name
                warnings.append({
                    "severity": "Low",
                    "message": f"No explicit date was provided; using latest monthly raw folder {latest.name}.",
                })
                print(f"[RAW] Using latest monthly folder {latest}")
                return start_day, end_day, month_key, latest, list_sales_export_paths(latest)

    daily_source = find_daily_folder_covering(start_day, end_day)
    if daily_source is not None:
        print(f"[RAW] Monthly raw not found; using daily raw source: {daily_source}")
        return start_day, end_day, month_key, raw_dir, copy_exports_to_monthly_raw(daily_source, raw_dir, warnings)

    if args.run_export is False:
        raise SystemExit(
            f"No monthly raw exports found for {month_key}: {raw_dir}\n"
            "Run again with --run-export, or add matching XLSX files to the monthly raw folder."
        )

    if args.export_source == "api":
        print(f"[EXPORT API] No monthly raw exports found for {month_key}; running sales API export.")
        return (
            start_day,
            end_day,
            month_key,
            raw_dir,
            run_monthly_sales_export_api(start_day, end_day, raw_dir, args.api_env_file, args.workers, warnings),
        )

    print(f"[EXPORT] No monthly raw exports found for {month_key}; running Backoffice export.")
    run_export_for_range(start_day, end_day)
    return start_day, end_day, month_key, raw_dir, monthly_archive_exports(start_day, end_day, raw_dir, warnings)


def filter_df_date_range(df: pd.DataFrame, start_day: date, end_day: date) -> pd.DataFrame:
    date_col = find_col(df, COLUMN_CANDIDATES["date"])
    if not date_col:
        return df.iloc[0:0].copy()
    tmp = df.copy()
    tmp[date_col] = pd.to_datetime(tmp[date_col], errors="coerce")
    tmp = tmp[tmp[date_col].notna()].copy()
    tmp["_date"] = tmp[date_col].dt.date
    return tmp[(tmp["_date"] >= start_day) & (tmp["_date"] <= end_day)].copy()


def detected_columns(df: pd.DataFrame) -> Dict[str, Optional[str]]:
    return {key: find_col(df, candidates) for key, candidates in COLUMN_CANDIDATES.items()}


def parse_store_export(abbr: str, path: Path, start_day: date, end_day: date) -> StoreBundle:
    print(f"[PARSE] {abbr}: {path.name}")
    df = read_export(path)
    cols = detected_columns(df)
    if not cols.get("date"):
        raise RuntimeError(f"{abbr}: required date column not found.")
    if not cols.get("net_sales"):
        raise RuntimeError(f"{abbr}: required net sales column not found.")

    df = enrich_with_deal_kickbacks_by_brand(df, store_code=abbr)
    df["_store"] = abbr
    df["_store_label"] = STORE_LABEL_BY_ABBR.get(abbr, abbr)

    daily = compute_daily_metrics(df)
    daily = daily[(daily["date"] >= start_day) & (daily["date"] <= end_day)].copy()
    metrics = metrics_for_range(daily, start_day, end_day)
    return StoreBundle(
        abbr=abbr,
        store_name=STORE_NAME_BY_ABBR.get(abbr, abbr),
        label=STORE_LABEL_BY_ABBR.get(abbr, abbr),
        path=path,
        raw_df=df,
        daily_df=daily,
        metrics=metrics,
        detected_columns=cols,
    )


def load_store_bundles(
    abbr_to_path: Dict[str, Path],
    start_day: date,
    end_day: date,
    workers: Optional[int],
    warnings: List[Dict[str, Any]],
) -> Dict[str, StoreBundle]:
    if not abbr_to_path:
        raise SystemExit("No store exports found for the monthly report.")
    worker_count = workers if workers is not None else min(4, max(1, len(abbr_to_path)))
    worker_count = max(1, min(worker_count, max(1, len(abbr_to_path))))
    print(f"[WORKERS] Parsing with {worker_count} worker(s)")

    bundles: Dict[str, StoreBundle] = {}
    with ThreadPoolExecutor(max_workers=worker_count) as pool:
        futures = {
            pool.submit(parse_store_export, abbr, path, start_day, end_day): abbr
            for abbr, path in sorted(abbr_to_path.items(), key=lambda item: store_sort_key(item[0]))
        }
        for future in as_completed(futures):
            abbr = futures[future]
            try:
                bundle = future.result()
                if bundle.daily_df.empty or float(bundle.metrics.get("net_revenue", 0.0)) == 0.0:
                    warnings.append({
                        "severity": "Medium",
                        "message": f"{abbr} has no net revenue in the selected date range.",
                    })
                bundles[abbr] = bundle
            except Exception as exc:
                warnings.append({
                    "severity": "High",
                    "message": f"Failed to parse {abbr}: {exc}",
                })
                print(f"[WARN] Failed to parse {abbr}: {exc}")

    if not bundles:
        raise SystemExit("No store exports could be parsed. See warnings for details.")
    return dict(sorted(bundles.items(), key=lambda item: store_sort_key(item[0])))


def as_float(value: Any, default: float = 0.0) -> float:
    try:
        out = float(value)
        if math.isfinite(out):
            return out
    except Exception:
        pass
    return default


def optional_money(value: Any) -> str:
    try:
        parsed = float(value)
    except Exception:
        return "N/A"
    if not math.isfinite(parsed):
        return "N/A"
    return money(parsed)


def money1(value: Any) -> str:
    return f"${as_float(value):,.1f}"


def optional_pct(value: Any) -> str:
    try:
        parsed = float(value)
    except Exception:
        return "N/A"
    if not math.isfinite(parsed):
        return "N/A"
    return pct1(parsed)


def optional_signed_money(value: Any) -> str:
    try:
        parsed = float(value)
    except Exception:
        return "N/A"
    if not math.isfinite(parsed):
        return "N/A"
    return fmt_signed_money(parsed)


def aggregate_daily(store_daily_map: Dict[str, pd.DataFrame], start_day: date, end_day: date) -> pd.DataFrame:
    frames = [df for df in store_daily_map.values() if df is not None and not df.empty]
    full_dates = pd.DataFrame({"date": [start_day + timedelta(days=i) for i in range((end_day - start_day).days + 1)]})
    if not frames:
        return full_dates.assign(**{field: 0.0 for field in METRIC_SUM_FIELDS})

    combined = pd.concat(frames, ignore_index=True)
    grouped = combined.groupby("date", as_index=False).agg({
        field: "sum" for field in METRIC_SUM_FIELDS if field in combined.columns
    })
    grouped = full_dates.merge(grouped, on="date", how="left").fillna(0.0)

    for field in METRIC_SUM_FIELDS:
        if field not in grouped.columns:
            grouped[field] = 0.0

    grouped["basket"] = grouped["net_revenue"] / grouped["tickets"].replace({0: np.nan})
    grouped["items_per_ticket"] = grouped["items"] / grouped["tickets"].replace({0: np.nan})
    grouped["net_price_per_item"] = grouped["net_revenue"] / grouped["items"].replace({0: np.nan})
    grouped["margin"] = grouped["profit"] / grouped["net_revenue"].replace({0: np.nan})
    grouped["margin_real"] = grouped["profit_real"] / grouped["net_revenue"].replace({0: np.nan})
    grouped["discount_rate"] = grouped.apply(
        lambda row: row["discount"] / row["gross_sales"]
        if row["gross_sales"]
        else (row["discount"] / (row["net_revenue"] + row["discount"]) if (row["net_revenue"] + row["discount"]) else 0.0),
        axis=1,
    )
    return grouped.fillna(0.0).sort_values("date")


def combine_raw_dfs(bundles: Dict[str, StoreBundle], start_day: date, end_day: date) -> pd.DataFrame:
    frames = []
    for bundle in bundles.values():
        tmp = filter_df_date_range(bundle.raw_df, start_day, end_day)
        if not tmp.empty:
            tmp["_store"] = bundle.abbr
            tmp["_store_label"] = bundle.label
            frames.append(tmp)
    if not frames:
        return pd.DataFrame()
    return pd.concat(frames, ignore_index=True)


def add_share_columns(df: pd.DataFrame, value_col: str, share_col: str) -> pd.DataFrame:
    out = df.copy()
    total = float(out[value_col].sum()) if value_col in out.columns and not out.empty else 0.0
    out[share_col] = out[value_col] / total if total else 0.0
    return out


def compute_adjusted_cart_metrics(df: pd.DataFrame, start_day: date, end_day: date) -> pd.DataFrame:
    columns = [
        "store",
        "store_label",
        "adjusted_cart_net",
        "adjusted_cart_count",
        "adjusted_basket",
        "low_value_cart_count",
        "low_value_cart_net",
    ]
    if df is None or df.empty:
        return pd.DataFrame(columns=columns)
    date_col = find_col(df, COLUMN_CANDIDATES["date"])
    net_col = find_col(df, COLUMN_CANDIDATES["net_sales"])
    tx_col = find_col(df, COLUMN_CANDIDATES["transaction_id"])
    if not date_col or not net_col:
        return pd.DataFrame(columns=columns)

    tmp = filter_df_date_range(df, start_day, end_day)
    if tmp.empty:
        return pd.DataFrame(columns=columns)
    tmp["_store"] = tmp["_store"].astype(str) if "_store" in tmp.columns else "ALL"
    tmp["_store_label"] = tmp["_store_label"].astype(str) if "_store_label" in tmp.columns else tmp["_store"]
    tmp["_net"] = to_number(tmp[net_col]).fillna(0.0).astype(float)
    if tx_col:
        tmp["_cart_key"] = tmp["_store"].astype(str) + "|" + tmp[tx_col].fillna("").astype(str)
    else:
        tmp["_cart_key"] = tmp["_store"].astype(str) + "|" + tmp.index.astype(str)

    carts = tmp.groupby(["_store", "_store_label", "_cart_key"], as_index=False).agg(cart_net=("_net", "sum"))
    carts = carts[carts["cart_net"] >= 0.0].copy()
    if carts.empty:
        return pd.DataFrame(columns=columns)
    carts["valid_for_avg_cart"] = carts["cart_net"] > 1.0
    carts["low_value_cart"] = (carts["cart_net"] >= 0.0) & (carts["cart_net"] <= 1.0)
    carts["valid_cart_net"] = np.where(carts["valid_for_avg_cart"], carts["cart_net"], 0.0)
    carts["low_value_net"] = np.where(carts["low_value_cart"], carts["cart_net"], 0.0)

    out = carts.groupby(["_store", "_store_label"], as_index=False).agg(
        adjusted_cart_net=("valid_cart_net", "sum"),
        adjusted_cart_count=("valid_for_avg_cart", "sum"),
        low_value_cart_count=("low_value_cart", "sum"),
        low_value_cart_net=("low_value_net", "sum"),
    ).rename(columns={"_store": "store", "_store_label": "store_label"})
    out["adjusted_basket"] = out["adjusted_cart_net"] / out["adjusted_cart_count"].replace({0: np.nan})
    return out[columns].fillna(0.0).sort_values("store")


def apply_adjusted_cart_metrics(
    all_metrics: Dict[str, Any],
    bundles: Dict[str, StoreBundle],
    adjusted_cart_metrics: pd.DataFrame,
) -> None:
    if adjusted_cart_metrics is None or adjusted_cart_metrics.empty:
        return
    total_net = as_float(adjusted_cart_metrics["adjusted_cart_net"].sum())
    total_count = as_float(adjusted_cart_metrics["adjusted_cart_count"].sum())
    all_metrics["basket_raw_including_low_value_carts"] = all_metrics.get("basket")
    all_metrics["basket"] = total_net / total_count if total_count else as_float(all_metrics.get("basket"))
    all_metrics["adjusted_cart_count"] = total_count
    all_metrics["low_value_cart_count_excluded_from_basket"] = as_float(adjusted_cart_metrics["low_value_cart_count"].sum())
    all_metrics["low_value_cart_net_excluded_from_basket"] = as_float(adjusted_cart_metrics["low_value_cart_net"].sum())

    by_store = {str(row["store"]).upper(): row for _, row in adjusted_cart_metrics.iterrows()}
    for abbr, bundle in bundles.items():
        row = by_store.get(str(abbr).upper())
        if row is None:
            continue
        bundle.metrics["basket_raw_including_low_value_carts"] = bundle.metrics.get("basket")
        bundle.metrics["basket"] = as_float(row.get("adjusted_basket"), as_float(bundle.metrics.get("basket")))
        bundle.metrics["adjusted_cart_count"] = as_float(row.get("adjusted_cart_count"))
        bundle.metrics["low_value_cart_count_excluded_from_basket"] = as_float(row.get("low_value_cart_count"))
        bundle.metrics["low_value_cart_net_excluded_from_basket"] = as_float(row.get("low_value_cart_net"))


def apply_adjusted_basket_to_budtenders(
    raw_df: pd.DataFrame,
    budtender_summary: pd.DataFrame,
    start_day: date,
    end_day: date,
) -> pd.DataFrame:
    if raw_df is None or raw_df.empty or budtender_summary is None or budtender_summary.empty:
        return budtender_summary
    employee_col = find_col(raw_df, COLUMN_CANDIDATES["employee"])
    tx_col = find_col(raw_df, COLUMN_CANDIDATES["transaction_id"])
    net_col = find_col(raw_df, COLUMN_CANDIDATES["net_sales"])
    date_col = find_col(raw_df, COLUMN_CANDIDATES["date"])
    if not employee_col or not net_col or not date_col:
        return budtender_summary
    tmp = filter_df_date_range(raw_df, start_day, end_day)
    if tmp.empty:
        return budtender_summary
    tmp["_budtender"] = tmp[employee_col].fillna("Unknown").astype(str)
    tmp["_net"] = to_number(tmp[net_col]).fillna(0.0).astype(float)
    if tx_col:
        store_part = tmp["_store"].astype(str) if "_store" in tmp.columns else pd.Series("STORE", index=tmp.index)
        tmp["_cart_key"] = store_part + "|" + tmp[tx_col].fillna("").astype(str)
    else:
        tmp["_cart_key"] = tmp.index.astype(str)
    carts = tmp.groupby(["_budtender", "_cart_key"], as_index=False).agg(cart_net=("_net", "sum"))
    carts = carts[carts["cart_net"] >= 0.0].copy()
    if carts.empty:
        return budtender_summary
    carts["valid_for_avg_cart"] = carts["cart_net"] > 1.0
    carts["low_value_cart"] = (carts["cart_net"] >= 0.0) & (carts["cart_net"] <= 1.0)
    carts["valid_cart_net"] = np.where(carts["valid_for_avg_cart"], carts["cart_net"], 0.0)
    adjusted = carts.groupby("_budtender", as_index=False).agg(
        adjusted_cart_net=("valid_cart_net", "sum"),
        adjusted_cart_count=("valid_for_avg_cart", "sum"),
        low_value_cart_count=("low_value_cart", "sum"),
    ).rename(columns={"_budtender": "budtender"})
    adjusted["adjusted_basket"] = adjusted["adjusted_cart_net"] / adjusted["adjusted_cart_count"].replace({0: np.nan})
    out = budtender_summary.copy()
    out = out.merge(adjusted, on="budtender", how="left")
    out["basket_raw_including_low_value_carts"] = out.get("basket")
    out["basket"] = pd.to_numeric(out["adjusted_basket"], errors="coerce").fillna(pd.to_numeric(out.get("basket"), errors="coerce").fillna(0.0))
    for col in ["adjusted_cart_net", "adjusted_cart_count", "low_value_cart_count"]:
        out[col] = pd.to_numeric(out[col], errors="coerce").fillna(0.0)
    return out


def generate_store_status(row: Dict[str, Any], company_metrics: Dict[str, float]) -> str:
    if as_float(row.get("tickets")) <= 0 or as_float(row.get("net_revenue")) <= 0:
        return "Needs Attention"

    flags = 0
    company_margin_real = as_float(company_metrics.get("margin_real"))
    company_basket = as_float(company_metrics.get("basket"))
    store_margin_real = as_float(row.get("margin_real"))
    store_basket = as_float(row.get("basket"))
    discount_rate = as_float(row.get("discount_rate"))

    if company_margin_real and store_margin_real < company_margin_real - 0.05:
        flags += 1
    if company_basket and store_basket < company_basket * 0.90:
        flags += 1
    if discount_rate > 0.20:
        flags += 1
    if as_float(row.get("profit_real")) < 0:
        flags += 2

    if flags >= 2:
        return "Needs Attention"
    if flags == 1:
        return "Watch"
    return "Strong"


def build_store_scorecards(bundles: Dict[str, StoreBundle], all_metrics: Dict[str, float]) -> pd.DataFrame:
    rows: List[Dict[str, Any]] = []
    total_net = sum(as_float(bundle.metrics.get("net_revenue")) for bundle in bundles.values())
    total_profit = sum(as_float(bundle.metrics.get("profit")) for bundle in bundles.values())
    store_count = max(1, len(bundles))
    avg_daily_denominator = 1.0

    for bundle in bundles.values():
        metrics = dict(bundle.metrics)
        row = {
            "store": bundle.abbr,
            "store_label": bundle.label,
            **metrics,
            "revenue_share": as_float(metrics.get("net_revenue")) / total_net if total_net else 0.0,
            "profit_share": as_float(metrics.get("profit")) / total_profit if total_profit else 0.0,
        }
        row["status"] = generate_store_status(row, all_metrics)
        rows.append(row)

    out = pd.DataFrame(rows)
    if not out.empty:
        out["_sort"] = out["store"].map(lambda x: store_sort_key(str(x))[0])
        out = out.sort_values(["net_revenue", "_sort"], ascending=[False, True]).drop(columns=["_sort"])
    return out


def compute_store_kickback_summary(df: pd.DataFrame, store_scorecards: pd.DataFrame) -> pd.DataFrame:
    columns = [
        "store",
        "store_label",
        "net_revenue",
        "profit_real",
        "kickback",
        "profit",
        "margin_real",
        "margin",
        "kickback_per_ticket",
        "top_deal_brand",
    ]
    if df is None or df.empty or "_deal_kickback_amt" not in df.columns:
        return pd.DataFrame(columns=columns)

    tmp = df.copy()
    net_col = find_col(tmp, COLUMN_CANDIDATES["net_sales"])
    profit_col = find_col(tmp, COLUMN_CANDIDATES["profit"])
    cogs_col = find_col(tmp, COLUMN_CANDIDATES["cogs"])
    tx_col = find_col(tmp, COLUMN_CANDIDATES["transaction_id"])
    tmp["_store"] = tmp.get("_store", "Unknown").astype(str)
    tmp["_store_label"] = tmp.get("_store_label", tmp["_store"]).astype(str)
    tmp["_kickback"] = to_number(tmp["_deal_kickback_amt"]).fillna(0).astype(float)
    tmp["_net"] = to_number(tmp[net_col]).fillna(0).astype(float) if net_col else 0.0
    tmp["_profit_real"] = to_number(tmp[profit_col]).fillna(0).astype(float) if profit_col else (
        tmp["_net"] - (to_number(tmp[cogs_col]).fillna(0).astype(float) if cogs_col else 0.0)
    )
    tmp["_profit"] = tmp["_profit_real"] + tmp["_kickback"]
    tmp["_ticket_key"] = tmp["_store"] + "|" + tmp[tx_col].astype(str) if tx_col else tmp.index.astype(str)

    out = tmp.groupby(["_store", "_store_label"], as_index=False).agg(
        net_revenue=("_net", "sum"),
        profit_real=("_profit_real", "sum"),
        kickback=("_kickback", "sum"),
        profit=("_profit", "sum"),
        tickets=("_ticket_key", "nunique"),
    ).rename(columns={"_store": "store", "_store_label": "store_label"})
    out["margin_real"] = out["profit_real"] / out["net_revenue"].replace({0: np.nan})
    out["margin"] = out["profit"] / out["net_revenue"].replace({0: np.nan})
    out["kickback_per_ticket"] = out["kickback"] / out["tickets"].replace({0: np.nan})

    deal_rows = tmp[tmp["_kickback"] > 0].copy()
    if not deal_rows.empty:
        brand_col = "_deal_brand" if "_deal_brand" in deal_rows.columns else None
        if brand_col:
            top_deals = (
                deal_rows.groupby(["_store", brand_col], as_index=False)["_kickback"]
                .sum()
                .sort_values(["_store", "_kickback"], ascending=[True, False])
                .groupby("_store", as_index=False)
                .head(1)
                .rename(columns={"_store": "store", brand_col: "top_deal_brand"})
            )
            out = out.merge(top_deals[["store", "top_deal_brand"]], on="store", how="left")
    if "top_deal_brand" not in out.columns:
        out["top_deal_brand"] = "N/A"

    if store_scorecards is not None and not store_scorecards.empty:
        score = store_scorecards[["store", "net_revenue", "profit_real", "profit", "margin_real", "margin"]].copy()
        out = score.merge(
            out[["store", "kickback", "kickback_per_ticket", "top_deal_brand"]],
            on="store",
            how="left",
        )
        out["store_label"] = out["store"].map(lambda value: STORE_LABEL_BY_ABBR.get(str(value), str(value)))

    for col in ["kickback", "kickback_per_ticket"]:
        out[col] = out[col].fillna(0.0)
    out["profit_before_kickback"] = out["profit_real"]
    out["profit_after_kickback"] = out["profit"]
    out["margin_lift_pp"] = out["margin"] - out["margin_real"]
    return out.fillna({"top_deal_brand": "N/A"}).sort_values("kickback", ascending=False)


def tax_multiplier_for_store(store_code: Any) -> float:
    config = ROUNDUP_STORE_TAXES.get(str(store_code or "").upper())
    if not config:
        return 1.0
    return 1.0 + as_float(config.city_tax) + as_float(config.excise_tax) + as_float(config.state_tax)


def decimal_money(value: Any) -> Decimal:
    try:
        return Decimal(str(as_float(value)))
    except Exception:
        return Decimal("0")


def spreadsheet_round(value: Decimal, places: str = "0.01") -> Decimal:
    return value.quantize(Decimal(places), rounding=ROUND_HALF_UP)


def spreadsheet_rounddown(value: Decimal, places: str = "0.01") -> Decimal:
    return value.quantize(Decimal(places), rounding=ROUND_DOWN)


def spreadsheet_roundup(value: Decimal, places: str = "0.01") -> Decimal:
    return value.quantize(Decimal(places), rounding=ROUND_CEILING)


def spreadsheet_round_array(values: Any, decimals: int = 2) -> np.ndarray:
    factor = float(10 ** decimals)
    arr = np.asarray(values, dtype=float)
    return np.floor((arr * factor) + 0.5 + 1e-9) / factor


def spreadsheet_rounddown_array(values: Any, decimals: int = 2) -> np.ndarray:
    factor = float(10 ** decimals)
    arr = np.asarray(values, dtype=float)
    return np.floor((arr * factor) + 1e-9) / factor


def spreadsheet_roundup_array(values: Any, decimals: int = 2) -> np.ndarray:
    factor = float(10 ** decimals)
    arr = np.asarray(values, dtype=float)
    return np.ceil((arr * factor) - 1e-9) / factor


def tax_rounding_loss_for_net(net_revenue: Any, tax_multiplier: Any) -> Tuple[float, float, float, float, float, float, float]:
    """Mirror the roundup sheet's OTD tax math at the transaction level."""
    net = decimal_money(net_revenue)
    multiplier = decimal_money(tax_multiplier)
    if net <= 0 or multiplier <= 0:
        return 0.0, 0.0, 0.0, 0.0, 0.0, 0.0, 0.0
    current_otd = spreadsheet_round(net * multiplier)
    rounded_otd_target = spreadsheet_roundup(current_otd, "1")
    backed_out_revenue = spreadsheet_rounddown(current_otd / multiplier)
    rounded_target_revenue = spreadsheet_roundup(rounded_otd_target / multiplier)
    tax_backout_rounddown_loss = max(net - backed_out_revenue, Decimal("0"))
    tax_included_roundup_opportunity = max(rounded_otd_target - current_otd, Decimal("0"))
    tax_rounding_loss = max(rounded_target_revenue - backed_out_revenue, Decimal("0"))
    return (
        float(tax_rounding_loss),
        float(tax_included_roundup_opportunity),
        float(tax_backout_rounddown_loss),
        float(current_otd),
        float(rounded_otd_target),
        float(backed_out_revenue),
        float(rounded_target_revenue),
    )


def compute_tax_rounding_loss(df: pd.DataFrame, start_day: date, end_day: date) -> Tuple[pd.DataFrame, pd.DataFrame]:
    summary_columns = [
        "store",
        "store_label",
        "tax_multiplier",
        "transactions_analyzed",
        "net_revenue_analyzed",
        "tax_rounding_loss",
        "tax_included_roundup_opportunity",
        "tax_backout_rounddown_loss",
        "current_otd_total",
        "rounded_otd_target_total",
        "tax_backed_revenue_at_current_otd",
        "tax_backed_revenue_at_rounded_otd",
        "avg_loss_per_transaction",
        "loss_rate",
    ]
    daily_columns = ["date", *summary_columns]
    if df is None or df.empty:
        return pd.DataFrame(columns=summary_columns), pd.DataFrame(columns=daily_columns)

    date_col = find_col(df, COLUMN_CANDIDATES["date"])
    net_col = find_col(df, COLUMN_CANDIDATES["net_sales"])
    tx_col = find_col(df, COLUMN_CANDIDATES["transaction_id"])
    if not date_col or not net_col:
        return pd.DataFrame(columns=summary_columns), pd.DataFrame(columns=daily_columns)

    tmp = filter_df_date_range(df, start_day, end_day)
    if tmp.empty:
        return pd.DataFrame(columns=summary_columns), pd.DataFrame(columns=daily_columns)
    tmp[date_col] = pd.to_datetime(tmp[date_col], errors="coerce")
    tmp = tmp[tmp[date_col].notna()].copy()
    tmp["_date"] = tmp[date_col].dt.date
    tmp["_store"] = tmp["_store"].astype(str) if "_store" in tmp.columns else "Unknown"
    tmp["_store_label"] = tmp["_store_label"].astype(str) if "_store_label" in tmp.columns else tmp["_store"]
    tmp["_net"] = to_number(tmp[net_col]).fillna(0.0).astype(float)
    tmp = tmp[tmp["_net"] > 0].copy()
    if tmp.empty:
        return pd.DataFrame(columns=summary_columns), pd.DataFrame(columns=daily_columns)
    if tx_col:
        tmp["_transaction_id"] = tmp[tx_col].astype(str)
    else:
        tmp["_transaction_id"] = tmp.index.astype(str)

    tx = tmp.groupby(["_store", "_store_label", "_date", "_transaction_id"], as_index=False).agg(
        net_revenue=("_net", "sum"),
    )
    tx["tax_multiplier"] = tx["_store"].apply(tax_multiplier_for_store)
    tx = tx[tx["tax_multiplier"] > 1.0].copy()
    if tx.empty:
        return pd.DataFrame(columns=summary_columns), pd.DataFrame(columns=daily_columns)
    net_values = pd.to_numeric(tx["net_revenue"], errors="coerce").fillna(0.0).to_numpy(dtype=float)
    multiplier_values = pd.to_numeric(tx["tax_multiplier"], errors="coerce").fillna(0.0).to_numpy(dtype=float)
    current_otd = spreadsheet_round_array(net_values * multiplier_values, 2)
    rounded_otd_target = spreadsheet_roundup_array(current_otd, 0)
    backed_out_revenue = spreadsheet_rounddown_array(current_otd / multiplier_values, 2)
    rounded_target_revenue = spreadsheet_roundup_array(rounded_otd_target / multiplier_values, 2)
    tx["current_otd"] = current_otd
    tx["rounded_otd_target"] = rounded_otd_target
    tx["tax_backed_revenue_at_current_otd"] = backed_out_revenue
    tx["tax_backed_revenue_at_rounded_otd"] = rounded_target_revenue
    tx["tax_backout_rounddown_loss"] = np.maximum(net_values - backed_out_revenue, 0.0)
    tx["tax_included_roundup_opportunity"] = np.maximum(rounded_otd_target - current_otd, 0.0)
    tx["tax_rounding_loss"] = np.maximum(rounded_target_revenue - backed_out_revenue, 0.0)

    daily = tx.groupby(["_date", "_store", "_store_label"], as_index=False).agg(
        tax_multiplier=("tax_multiplier", "first"),
        transactions_analyzed=("_transaction_id", "nunique"),
        net_revenue_analyzed=("net_revenue", "sum"),
        tax_rounding_loss=("tax_rounding_loss", "sum"),
        tax_included_roundup_opportunity=("tax_included_roundup_opportunity", "sum"),
        tax_backout_rounddown_loss=("tax_backout_rounddown_loss", "sum"),
        current_otd_total=("current_otd", "sum"),
        rounded_otd_target_total=("rounded_otd_target", "sum"),
        tax_backed_revenue_at_current_otd=("tax_backed_revenue_at_current_otd", "sum"),
        tax_backed_revenue_at_rounded_otd=("tax_backed_revenue_at_rounded_otd", "sum"),
    ).rename(columns={"_date": "date", "_store": "store", "_store_label": "store_label"})
    daily["avg_loss_per_transaction"] = daily["tax_rounding_loss"] / daily["transactions_analyzed"].replace({0: np.nan})
    daily["loss_rate"] = daily["tax_rounding_loss"] / daily["net_revenue_analyzed"].replace({0: np.nan})
    daily = daily.fillna(0.0).sort_values(["date", "store"])

    summary = daily.groupby(["store", "store_label"], as_index=False).agg(
        tax_multiplier=("tax_multiplier", "first"),
        transactions_analyzed=("transactions_analyzed", "sum"),
        net_revenue_analyzed=("net_revenue_analyzed", "sum"),
        tax_rounding_loss=("tax_rounding_loss", "sum"),
        tax_included_roundup_opportunity=("tax_included_roundup_opportunity", "sum"),
        tax_backout_rounddown_loss=("tax_backout_rounddown_loss", "sum"),
        current_otd_total=("current_otd_total", "sum"),
        rounded_otd_target_total=("rounded_otd_target_total", "sum"),
        tax_backed_revenue_at_current_otd=("tax_backed_revenue_at_current_otd", "sum"),
        tax_backed_revenue_at_rounded_otd=("tax_backed_revenue_at_rounded_otd", "sum"),
    )
    summary["avg_loss_per_transaction"] = summary["tax_rounding_loss"] / summary["transactions_analyzed"].replace({0: np.nan})
    summary["loss_rate"] = summary["tax_rounding_loss"] / summary["net_revenue_analyzed"].replace({0: np.nan})
    summary = summary.fillna(0.0).sort_values("tax_rounding_loss", ascending=False)
    return summary[summary_columns], daily[daily_columns]


def apply_tax_rounding_metrics(
    all_metrics: Dict[str, Any],
    store_scorecards: pd.DataFrame,
    tax_rounding_summary: pd.DataFrame,
) -> pd.DataFrame:
    total_loss = as_float(tax_rounding_summary["tax_rounding_loss"].sum()) if tax_rounding_summary is not None and not tax_rounding_summary.empty else 0.0
    total_otd_gap = as_float(tax_rounding_summary["tax_included_roundup_opportunity"].sum()) if tax_rounding_summary is not None and not tax_rounding_summary.empty and "tax_included_roundup_opportunity" in tax_rounding_summary.columns else 0.0
    total_backout_loss = as_float(tax_rounding_summary["tax_backout_rounddown_loss"].sum()) if tax_rounding_summary is not None and not tax_rounding_summary.empty and "tax_backout_rounddown_loss" in tax_rounding_summary.columns else 0.0
    total_net = as_float(tax_rounding_summary["net_revenue_analyzed"].sum()) if tax_rounding_summary is not None and not tax_rounding_summary.empty else 0.0
    total_transactions = as_float(tax_rounding_summary["transactions_analyzed"].sum()) if tax_rounding_summary is not None and not tax_rounding_summary.empty else 0.0
    all_metrics["tax_rounding_loss"] = total_loss
    all_metrics["tax_included_roundup_opportunity"] = total_otd_gap
    all_metrics["tax_backout_rounddown_loss"] = total_backout_loss
    all_metrics["tax_rounding_net_revenue_analyzed"] = total_net
    all_metrics["tax_rounding_transactions"] = total_transactions
    all_metrics["tax_rounding_avg_loss_per_transaction"] = total_loss / total_transactions if total_transactions else 0.0
    all_metrics["tax_rounding_loss_rate"] = total_loss / total_net if total_net else 0.0
    all_metrics["profit_real_before_tax_rounding_loss"] = all_metrics.get("profit_real")
    all_metrics["profit_before_tax_rounding_loss"] = all_metrics.get("profit")
    if total_loss:
        all_metrics["profit_real"] = as_float(all_metrics.get("profit_real")) - total_loss
        all_metrics["profit"] = as_float(all_metrics.get("profit")) - total_loss
        net_revenue = as_float(all_metrics.get("net_revenue"))
        all_metrics["margin_real"] = all_metrics["profit_real"] / net_revenue if net_revenue else 0.0
        all_metrics["margin"] = all_metrics["profit"] / net_revenue if net_revenue else 0.0

    if store_scorecards is None or store_scorecards.empty:
        return store_scorecards
    out = store_scorecards.copy()
    if tax_rounding_summary is None or tax_rounding_summary.empty:
        for col in [
            "tax_rounding_loss",
            "tax_included_roundup_opportunity",
            "tax_backout_rounddown_loss",
            "tax_rounding_loss_rate",
            "tax_rounding_avg_loss_per_transaction",
            "tax_rounding_transactions",
        ]:
            out[col] = 0.0
        return out
    cols = [
        "store",
        "tax_rounding_loss",
        "tax_included_roundup_opportunity",
        "tax_backout_rounddown_loss",
        "loss_rate",
        "avg_loss_per_transaction",
        "transactions_analyzed",
        "net_revenue_analyzed",
    ]
    out = out.merge(tax_rounding_summary[cols], on="store", how="left")
    out = out.rename(columns={
        "loss_rate": "tax_rounding_loss_rate",
        "avg_loss_per_transaction": "tax_rounding_avg_loss_per_transaction",
        "transactions_analyzed": "tax_rounding_transactions",
        "net_revenue_analyzed": "tax_rounding_net_revenue_analyzed",
    })
    for col in [
        "tax_rounding_loss",
        "tax_included_roundup_opportunity",
        "tax_backout_rounddown_loss",
        "tax_rounding_loss_rate",
        "tax_rounding_avg_loss_per_transaction",
        "tax_rounding_transactions",
        "tax_rounding_net_revenue_analyzed",
    ]:
        out[col] = pd.to_numeric(out[col], errors="coerce").fillna(0.0)
    out["profit_real_before_tax_rounding_loss"] = out.get("profit_real")
    out["profit_before_tax_rounding_loss"] = out.get("profit")
    if "profit_real" in out.columns:
        out["profit_real"] = pd.to_numeric(out["profit_real"], errors="coerce").fillna(0.0) - out["tax_rounding_loss"]
    if "profit" in out.columns:
        out["profit"] = pd.to_numeric(out["profit"], errors="coerce").fillna(0.0) - out["tax_rounding_loss"]
    if "net_revenue" in out.columns:
        net = pd.to_numeric(out["net_revenue"], errors="coerce").replace({0: np.nan})
        out["margin_real"] = out["profit_real"] / net if "profit_real" in out.columns else out.get("margin_real", 0.0)
        out["margin"] = out["profit"] / net if "profit" in out.columns else out.get("margin", 0.0)
        out[["margin_real", "margin"]] = out[["margin_real", "margin"]].fillna(0.0)
    total_profit = as_float(all_metrics.get("profit"))
    if total_profit and "profit" in out.columns:
        out["profit_share"] = out["profit"] / total_profit
    out["status"] = out.apply(lambda row: generate_store_status(row.to_dict(), all_metrics), axis=1)
    return out


def sync_bundle_metrics_from_scorecards(bundles: Dict[str, StoreBundle], store_scorecards: pd.DataFrame) -> None:
    if store_scorecards is None or store_scorecards.empty:
        return
    for _, row in store_scorecards.iterrows():
        abbr = str(row.get("store", "")).upper()
        if abbr not in bundles:
            continue
        for key, value in row.to_dict().items():
            if key in {"store", "store_label"}:
                continue
            bundles[abbr].metrics[key] = value


def apply_tax_rounding_to_daily_detail(
    all_daily: pd.DataFrame,
    bundles: Dict[str, StoreBundle],
    tax_rounding_daily: pd.DataFrame,
) -> pd.DataFrame:
    if tax_rounding_daily is None or tax_rounding_daily.empty or "tax_rounding_loss" not in tax_rounding_daily.columns:
        return all_daily

    def adjust_daily(daily: pd.DataFrame, losses: pd.DataFrame) -> pd.DataFrame:
        if daily is None or daily.empty:
            return daily
        out = daily.copy()
        out["date"] = pd.to_datetime(out["date"], errors="coerce").dt.date
        loss_by_day = losses.copy()
        loss_by_day["date"] = pd.to_datetime(loss_by_day["date"], errors="coerce").dt.date
        loss_by_day = loss_by_day.groupby("date", as_index=False).agg(tax_rounding_loss=("tax_rounding_loss", "sum"))
        out = out.merge(loss_by_day, on="date", how="left")
        out["tax_rounding_loss"] = pd.to_numeric(out["tax_rounding_loss"], errors="coerce").fillna(0.0)
        out["profit_real_before_tax_rounding_loss"] = out.get("profit_real", 0.0)
        out["profit_before_tax_rounding_loss"] = out.get("profit", 0.0)
        if "profit_real" in out.columns:
            out["profit_real"] = pd.to_numeric(out["profit_real"], errors="coerce").fillna(0.0) - out["tax_rounding_loss"]
        if "profit" in out.columns:
            out["profit"] = pd.to_numeric(out["profit"], errors="coerce").fillna(0.0) - out["tax_rounding_loss"]
        net = pd.to_numeric(out.get("net_revenue", 0.0), errors="coerce").replace({0: np.nan})
        if "profit_real" in out.columns:
            out["margin_real"] = (out["profit_real"] / net).fillna(0.0)
        if "profit" in out.columns:
            out["margin"] = (out["profit"] / net).fillna(0.0)
        return out

    adjusted_all = adjust_daily(all_daily, tax_rounding_daily)
    for abbr, bundle in bundles.items():
        store_losses = tax_rounding_daily[tax_rounding_daily["store"].astype(str).str.upper() == str(abbr).upper()].copy()
        if store_losses.empty:
            continue
        bundle.daily_df = adjust_daily(bundle.daily_df, store_losses)
    return adjusted_all


def is_excluded_product_name(value: Any) -> bool:
    text = str(value or "").lower()
    return any(token in text for token in ["promo", "sample", "420"])


def compute_monthly_product_summary(df: pd.DataFrame, start_day: date, end_day: date) -> pd.DataFrame:
    prod_col = find_col(df, COLUMN_CANDIDATES["product"])
    cat_col = find_col(df, COLUMN_CANDIDATES["category"])
    tx_col = find_col(df, COLUMN_CANDIDATES["transaction_id"])
    net_col = find_col(df, COLUMN_CANDIDATES["net_sales"])
    gross_col = find_col(df, COLUMN_CANDIDATES["gross_sales"])
    qty_col = find_col(df, COLUMN_CANDIDATES["quantity"])
    disc_main_col = find_col(df, COLUMN_CANDIDATES["discount_main"])
    disc_loyal_col = find_col(df, COLUMN_CANDIDATES["discount_loyalty"])
    cogs_col = find_col(df, COLUMN_CANDIDATES["cogs"])
    profit_col = find_col(df, COLUMN_CANDIDATES["profit"])

    if not prod_col or not net_col:
        return pd.DataFrame()

    tmp = filter_df_date_range(df, start_day, end_day)
    if tmp.empty:
        return pd.DataFrame()

    tmp["_product"] = tmp[prod_col].fillna("Unknown").astype(str)
    tmp = tmp[~tmp["_product"].apply(is_excluded_product_name)].copy()
    if tmp.empty:
        return pd.DataFrame()
    tmp["_brand"] = tmp["_product"].apply(parse_brand_from_product)
    tmp["_category"] = tmp[cat_col].fillna("Unknown").astype(str) if cat_col else "Unknown"
    tmp["_net"] = to_number(tmp[net_col]).fillna(0).astype(float)
    tmp["_gross"] = to_number(tmp[gross_col]).fillna(0).astype(float) if gross_col else 0.0
    tmp["_items"] = to_number(tmp[qty_col]).fillna(0).astype(float) if qty_col else 1.0
    tmp["_disc_main"] = to_number(tmp[disc_main_col]).fillna(0).astype(float) if disc_main_col else 0.0
    tmp["_disc_loyal"] = to_number(tmp[disc_loyal_col]).fillna(0).astype(float) if disc_loyal_col else 0.0
    tmp["_discount"] = tmp["_disc_main"] + tmp["_disc_loyal"]
    tmp["_kickback"] = to_number(tmp["_deal_kickback_amt"]).fillna(0).astype(float) if "_deal_kickback_amt" in tmp.columns else 0.0
    tmp["_cogs_real"] = to_number(tmp["_cogs_raw"]).fillna(0).astype(float) if "_cogs_raw" in tmp.columns else (to_number(tmp[cogs_col]).fillna(0).astype(float) if cogs_col else 0.0)

    if profit_col:
        tmp["_profit_real"] = to_number(tmp[profit_col]).fillna(0).astype(float)
    else:
        tmp["_profit_real"] = tmp["_net"] - tmp["_cogs_real"]
    tmp["_profit"] = tmp["_profit_real"] + tmp["_kickback"]
    tmp["_ticket_key"] = tmp["_store"].astype(str) + "|" + tmp[tx_col].astype(str) if tx_col and "_store" in tmp.columns else tmp.index.astype(str)

    out = tmp.groupby(["_product", "_brand", "_category"], as_index=False).agg(
        net_revenue=("_net", "sum"),
        gross_sales=("_gross", "sum"),
        profit=("_profit", "sum"),
        profit_real=("_profit_real", "sum"),
        discount=("_discount", "sum"),
        items=("_items", "sum"),
        tickets=("_ticket_key", "nunique"),
        kickback=("_kickback", "sum"),
    )
    out = out.rename(columns={"_product": "product", "_brand": "brand", "_category": "category"})
    out["margin"] = out["profit"] / out["net_revenue"].replace({0: np.nan})
    out["margin_real"] = out["profit_real"] / out["net_revenue"].replace({0: np.nan})
    out["discount_rate"] = out.apply(
        lambda row: row["discount"] / row["gross_sales"]
        if row["gross_sales"]
        else (row["discount"] / (row["net_revenue"] + row["discount"]) if (row["net_revenue"] + row["discount"]) else 0.0),
        axis=1,
    )
    out["revenue_share"] = out["net_revenue"] / out["net_revenue"].sum() if float(out["net_revenue"].sum()) else 0.0
    return out.fillna(0.0).sort_values("net_revenue", ascending=False)


def compute_monthly_brand_summary(df: pd.DataFrame, start_day: date, end_day: date) -> pd.DataFrame:
    base = compute_brand_summary(df, start_day, end_day, top_n=10_000)
    if base is None or base.empty:
        return pd.DataFrame()
    prod = compute_monthly_product_summary(df, start_day, end_day)
    if prod.empty:
        return base
    extra = prod.groupby("brand", as_index=False).agg(
        items=("items", "sum"),
        tickets=("tickets", "sum"),
        discount=("discount", "sum"),
        gross_sales=("gross_sales", "sum"),
        kickback=("kickback", "sum"),
    )
    out = base.merge(extra, on="brand", how="left")
    out["discount_rate"] = out.apply(
        lambda row: row["discount"] / row["gross_sales"]
        if row.get("gross_sales", 0)
        else (row["discount"] / (row["net_revenue"] + row["discount"]) if (row["net_revenue"] + row["discount"]) else 0.0),
        axis=1,
    )
    out["revenue_share"] = out["net_revenue"] / out["net_revenue"].sum() if float(out["net_revenue"].sum()) else 0.0
    return out.fillna(0.0).sort_values("net_revenue", ascending=False)


def compute_monthly_hourly_summary(df: pd.DataFrame, start_day: date, end_day: date) -> pd.DataFrame:
    date_col = find_col(df, COLUMN_CANDIDATES["date"])
    tx_col = find_col(df, COLUMN_CANDIDATES["transaction_id"])
    net_col = find_col(df, COLUMN_CANDIDATES["net_sales"])
    gross_col = find_col(df, COLUMN_CANDIDATES["gross_sales"])
    disc_main_col = find_col(df, COLUMN_CANDIDATES["discount_main"])
    disc_loyal_col = find_col(df, COLUMN_CANDIDATES["discount_loyalty"])
    profit_col = find_col(df, COLUMN_CANDIDATES["profit"])
    cogs_col = find_col(df, COLUMN_CANDIDATES["cogs"])
    if not date_col or not net_col:
        return pd.DataFrame()

    tmp = filter_df_date_range(df, start_day, end_day)
    if tmp.empty:
        return pd.DataFrame()
    tmp[date_col] = pd.to_datetime(tmp[date_col], errors="coerce")
    tmp["_hour"] = tmp[date_col].dt.hour
    tmp["_net"] = to_number(tmp[net_col]).fillna(0).astype(float)
    tmp["_gross"] = to_number(tmp[gross_col]).fillna(0).astype(float) if gross_col else 0.0
    tmp["_disc_main"] = to_number(tmp[disc_main_col]).fillna(0).astype(float) if disc_main_col else 0.0
    tmp["_disc_loyal"] = to_number(tmp[disc_loyal_col]).fillna(0).astype(float) if disc_loyal_col else 0.0
    tmp["_discount"] = tmp["_disc_main"] + tmp["_disc_loyal"]
    tmp["_kickback"] = to_number(tmp["_deal_kickback_amt"]).fillna(0).astype(float) if "_deal_kickback_amt" in tmp.columns else 0.0
    tmp["_cogs_real"] = to_number(tmp["_cogs_raw"]).fillna(0).astype(float) if "_cogs_raw" in tmp.columns else (to_number(tmp[cogs_col]).fillna(0).astype(float) if cogs_col else 0.0)
    tmp["_profit_real"] = to_number(tmp[profit_col]).fillna(0).astype(float) if profit_col else (tmp["_net"] - tmp["_cogs_real"])
    tmp["_profit"] = tmp["_profit_real"] + tmp["_kickback"]
    tmp["_ticket_key"] = tmp["_store"].astype(str) + "|" + tmp[tx_col].astype(str) if tx_col and "_store" in tmp.columns else tmp.index.astype(str)

    out = tmp.groupby("_hour", as_index=False).agg(
        net_revenue=("_net", "sum"),
        gross_sales=("_gross", "sum"),
        discount=("_discount", "sum"),
        profit=("_profit", "sum"),
        profit_real=("_profit_real", "sum"),
        tickets=("_ticket_key", "nunique"),
    ).rename(columns={"_hour": "hour"})

    out["basket"] = out["net_revenue"] / out["tickets"].replace({0: np.nan})
    out["margin"] = out["profit"] / out["net_revenue"].replace({0: np.nan})
    out["margin_real"] = out["profit_real"] / out["net_revenue"].replace({0: np.nan})
    out["discount_rate"] = out.apply(
        lambda row: row["discount"] / row["gross_sales"]
        if row["gross_sales"]
        else (row["discount"] / (row["net_revenue"] + row["discount"]) if (row["net_revenue"] + row["discount"]) else 0.0),
        axis=1,
    )
    out = out.fillna(0.0)
    return out[(out["net_revenue"] > 0) | (out["tickets"] > 0)].sort_values("hour")


def weekday_count_frame(start_day: date, end_day: date) -> pd.DataFrame:
    counts = {i: 0 for i in range(7)}
    current = start_day
    while current <= end_day:
        counts[current.weekday()] += 1
        current += timedelta(days=1)
    return pd.DataFrame([
        {
            "weekday_num": i,
            "weekday": WEEKDAY_NAMES[i],
            "weekday_short": WEEKDAY_SHORT[i],
            "day_count": counts[i],
        }
        for i in range(7)
    ])


def compute_weekday_summary(daily: pd.DataFrame, start_day: date, end_day: date) -> pd.DataFrame:
    base = weekday_count_frame(start_day, end_day)
    if daily is None or daily.empty:
        out = base.copy()
        for field in METRIC_SUM_FIELDS:
            out[field] = 0.0
    else:
        tmp = daily.copy()
        tmp["date"] = pd.to_datetime(tmp["date"]).dt.date
        tmp = tmp[(tmp["date"] >= start_day) & (tmp["date"] <= end_day)]
        tmp["weekday_num"] = pd.to_datetime(tmp["date"]).dt.weekday
        agg = tmp.groupby("weekday_num", as_index=False).agg({
            field: "sum" for field in METRIC_SUM_FIELDS if field in tmp.columns
        })
        out = base.merge(agg, on="weekday_num", how="left").fillna(0.0)
        for field in METRIC_SUM_FIELDS:
            if field not in out.columns:
                out[field] = 0.0

    day_count = out["day_count"].replace({0: np.nan})
    out["avg_net_revenue"] = out["net_revenue"] / day_count
    out["avg_tickets"] = out["tickets"] / day_count
    out["avg_items"] = out["items"] / day_count
    out["avg_discount"] = out["discount"] / day_count
    out["basket"] = out["net_revenue"] / out["tickets"].replace({0: np.nan})
    out["items_per_ticket"] = out["items"] / out["tickets"].replace({0: np.nan})
    out["margin"] = out["profit"] / out["net_revenue"].replace({0: np.nan})
    out["margin_real"] = out["profit_real"] / out["net_revenue"].replace({0: np.nan})
    out["discount_rate"] = out.apply(
        lambda row: row["discount"] / row["gross_sales"]
        if row["gross_sales"]
        else (row["discount"] / (row["net_revenue"] + row["discount"]) if (row["net_revenue"] + row["discount"]) else 0.0),
        axis=1,
    )
    return out.fillna(0.0).sort_values("weekday_num")


def compute_weekday_hour_summary(df: pd.DataFrame, start_day: date, end_day: date) -> pd.DataFrame:
    date_col = find_col(df, COLUMN_CANDIDATES["date"])
    tx_col = find_col(df, COLUMN_CANDIDATES["transaction_id"])
    net_col = find_col(df, COLUMN_CANDIDATES["net_sales"])
    gross_col = find_col(df, COLUMN_CANDIDATES["gross_sales"])
    disc_main_col = find_col(df, COLUMN_CANDIDATES["discount_main"])
    disc_loyal_col = find_col(df, COLUMN_CANDIDATES["discount_loyalty"])
    profit_col = find_col(df, COLUMN_CANDIDATES["profit"])
    cogs_col = find_col(df, COLUMN_CANDIDATES["cogs"])
    if not date_col or not net_col:
        return pd.DataFrame()

    tmp = filter_df_date_range(df, start_day, end_day)
    if tmp.empty:
        return pd.DataFrame()

    tmp[date_col] = pd.to_datetime(tmp[date_col], errors="coerce")
    tmp = tmp[tmp[date_col].notna()].copy()
    tmp["_weekday_num"] = tmp[date_col].dt.weekday
    tmp["_weekday"] = tmp["_weekday_num"].map(lambda i: WEEKDAY_NAMES[int(i)])
    tmp["_hour"] = tmp[date_col].dt.hour
    tmp["_net"] = to_number(tmp[net_col]).fillna(0).astype(float)
    tmp["_gross"] = to_number(tmp[gross_col]).fillna(0).astype(float) if gross_col else 0.0
    tmp["_disc_main"] = to_number(tmp[disc_main_col]).fillna(0).astype(float) if disc_main_col else 0.0
    tmp["_disc_loyal"] = to_number(tmp[disc_loyal_col]).fillna(0).astype(float) if disc_loyal_col else 0.0
    tmp["_discount"] = tmp["_disc_main"] + tmp["_disc_loyal"]
    tmp["_kickback"] = to_number(tmp["_deal_kickback_amt"]).fillna(0).astype(float) if "_deal_kickback_amt" in tmp.columns else 0.0
    tmp["_cogs_real"] = to_number(tmp["_cogs_raw"]).fillna(0).astype(float) if "_cogs_raw" in tmp.columns else (to_number(tmp[cogs_col]).fillna(0).astype(float) if cogs_col else 0.0)
    tmp["_profit_real"] = to_number(tmp[profit_col]).fillna(0).astype(float) if profit_col else (tmp["_net"] - tmp["_cogs_real"])
    tmp["_profit"] = tmp["_profit_real"] + tmp["_kickback"]
    if tx_col:
        store_part = tmp["_store"].astype(str) if "_store" in tmp.columns else pd.Series("STORE", index=tmp.index)
        tmp["_ticket_key"] = store_part + "|" + tmp[tx_col].astype(str)
    else:
        tmp["_ticket_key"] = tmp.index.astype(str)

    out = tmp.groupby(["_weekday_num", "_weekday", "_hour"], as_index=False).agg(
        net_revenue=("_net", "sum"),
        gross_sales=("_gross", "sum"),
        profit=("_profit", "sum"),
        profit_real=("_profit_real", "sum"),
        discount=("_discount", "sum"),
        tickets=("_ticket_key", "nunique"),
    ).rename(columns={"_weekday_num": "weekday_num", "_weekday": "weekday", "_hour": "hour"})

    counts = weekday_count_frame(start_day, end_day)[["weekday_num", "day_count"]]
    out = out.merge(counts, on="weekday_num", how="left")
    out["avg_net_revenue"] = out["net_revenue"] / out["day_count"].replace({0: np.nan})
    out["avg_tickets"] = out["tickets"] / out["day_count"].replace({0: np.nan})
    out["basket"] = out["net_revenue"] / out["tickets"].replace({0: np.nan})
    out["margin_real"] = out["profit_real"] / out["net_revenue"].replace({0: np.nan})
    out["discount_rate"] = out.apply(
        lambda row: row["discount"] / row["gross_sales"]
        if row["gross_sales"]
        else (row["discount"] / (row["net_revenue"] + row["discount"]) if (row["net_revenue"] + row["discount"]) else 0.0),
        axis=1,
    )
    out = out.fillna(0.0)
    return out[(out["tickets"] > 0) | (out["net_revenue"] > 0)].sort_values(["weekday_num", "hour"])


def top_weekday_hours(weekday_hour_summary: pd.DataFrame, per_day: int = 5) -> pd.DataFrame:
    if weekday_hour_summary is None or weekday_hour_summary.empty:
        return pd.DataFrame()
    return (
        weekday_hour_summary
        .sort_values(["weekday_num", "avg_tickets", "avg_net_revenue"], ascending=[True, False, False])
        .groupby("weekday_num", group_keys=False)
        .head(per_day)
        .sort_values(["weekday_num", "avg_tickets"], ascending=[True, False])
    )


def compute_store_group_matrix(
    df: pd.DataFrame,
    start_day: date,
    end_day: date,
    group_name: str,
    top_n: int = 12,
) -> pd.DataFrame:
    net_col = find_col(df, COLUMN_CANDIDATES["net_sales"])
    if not net_col or df.empty or "_store" not in df.columns:
        return pd.DataFrame()

    tmp = filter_df_date_range(df, start_day, end_day)
    if tmp.empty:
        return pd.DataFrame()

    if group_name == "brand":
        prod_col = find_col(tmp, COLUMN_CANDIDATES["product"])
        if not prod_col:
            return pd.DataFrame()
        tmp["_group"] = tmp[prod_col].apply(parse_brand_from_product)
        label_col = "brand"
    elif group_name == "category":
        cat_col = find_col(tmp, COLUMN_CANDIDATES["category"])
        if not cat_col:
            return pd.DataFrame()
        tmp["_group"] = tmp[cat_col].fillna("Unknown").astype(str)
        label_col = "category"
    else:
        return pd.DataFrame()

    tmp["_net"] = to_number(tmp[net_col]).fillna(0).astype(float)
    totals = tmp.groupby("_group", as_index=False)["_net"].sum().sort_values("_net", ascending=False).head(top_n)
    top_groups = totals["_group"].tolist()
    tmp = tmp[tmp["_group"].isin(top_groups)]
    pivot = tmp.pivot_table(index="_group", columns="_store", values="_net", aggfunc="sum", fill_value=0.0)
    for abbr in STORE_ORDER:
        if abbr not in pivot.columns:
            pivot[abbr] = 0.0
    pivot["total"] = pivot[STORE_ORDER].sum(axis=1)
    pivot = pivot.reset_index().rename(columns={"_group": label_col})
    pivot = pivot.sort_values("total", ascending=False)
    return pivot[[label_col, "total"] + STORE_ORDER]


CLOSING_COLUMN_CANDIDATES = {
    "date": ["Date", "Report Date", "Business Date", "Closing Date", "Day"],
    "store": ["Store", "Location", "Dispensary", "Retail Location"],
    "new_customers": [
        "New Customers",
        "New Customer",
        "First Time Customers",
        "First-Time Customers",
        "First Time Patients",
        "First-Time Patients",
        "New Patients",
        "New Customer Count",
        "New Customers Count",
    ],
    "total_customers": [
        "Total Customers",
        "Customers",
        "Unique Customers",
        "Customer Count",
        "Total Patients",
        "Patients",
        "Visitors",
    ],
}


def find_loose_col(df: pd.DataFrame, candidates: List[str]) -> Optional[str]:
    exact = find_col(df, candidates)
    if exact:
        return exact
    normalized_cols = {
        re.sub(r"[^a-z0-9]+", "", str(col).lower()): col
        for col in df.columns
    }
    for candidate in candidates:
        key = re.sub(r"[^a-z0-9]+", "", str(candidate).lower())
        if key in normalized_cols:
            return normalized_cols[key]
    for candidate in candidates:
        key = re.sub(r"[^a-z0-9]+", "", str(candidate).lower())
        for col_key, col in normalized_cols.items():
            if key and (key in col_key or col_key in key):
                return col
    return None


def parse_store_from_filename(path: Path) -> Optional[str]:
    stem = path.stem.upper()
    for abbr in STORE_ORDER:
        if re.search(rf"(^|[^A-Z0-9]){re.escape(abbr)}([^A-Z0-9]|$)", stem):
            return abbr
    compact_stem = re.sub(r"[^A-Z0-9]+", "", stem)
    for abbr, label in STORE_LABEL_BY_ABBR.items():
        if re.sub(r"[^A-Z0-9]+", "", label.upper()) in compact_stem:
            return abbr
    return None


def parse_date_from_filename(path: Path) -> Optional[date]:
    stem = path.stem
    patterns = [
        (r"(\d{4})[-_](\d{2})[-_](\d{2})", "%Y-%m-%d"),
        (r"(\d{2})[-_](\d{2})[-_](\d{4})", "%m-%d-%Y"),
    ]
    for pattern, fmt in patterns:
        match = re.search(pattern, stem)
        if not match:
            continue
        raw = "-".join(match.groups())
        try:
            return datetime.strptime(raw, fmt).date()
        except Exception:
            continue
    return None


def normalize_store_value(value: Any, fallback: Optional[str] = None) -> str:
    text = str(value or "").strip()
    if not text:
        return fallback or "Unknown"
    upper = text.upper()
    for abbr in STORE_ORDER:
        if upper == abbr or re.search(rf"(^|[^A-Z0-9]){re.escape(abbr)}([^A-Z0-9]|$)", upper):
            return abbr
    compact = re.sub(r"[^A-Z0-9]+", "", upper)
    for abbr, label in STORE_LABEL_BY_ABBR.items():
        if re.sub(r"[^A-Z0-9]+", "", label.upper()) in compact:
            return abbr
    return fallback or text


def read_closing_report_file(path: Path, start_day: date, end_day: date, warnings: List[Dict[str, Any]]) -> pd.DataFrame:
    try:
        if path.suffix.lower() in [".xlsx", ".xls"]:
            df = pd.read_excel(path, engine="openpyxl")
        else:
            df = pd.read_csv(path)
    except Exception as exc:
        warnings.append({"severity": "Low", "message": f"Could not read closing report file {path}: {exc}"})
        return pd.DataFrame()

    if df.empty:
        return pd.DataFrame()
    df.columns = [str(col).strip() for col in df.columns]

    date_col = find_loose_col(df, CLOSING_COLUMN_CANDIDATES["date"])
    store_col = find_loose_col(df, CLOSING_COLUMN_CANDIDATES["store"])
    new_col = find_loose_col(df, CLOSING_COLUMN_CANDIDATES["new_customers"])
    total_col = find_loose_col(df, CLOSING_COLUMN_CANDIDATES["total_customers"])

    if not new_col:
        warnings.append({
            "severity": "Low",
            "message": f"Closing report file skipped because no new-customer column was found: {path.name}",
        })
        return pd.DataFrame()

    fallback_date = parse_date_from_filename(path)
    fallback_store = parse_store_from_filename(path)
    out = pd.DataFrame()

    if date_col:
        out["date"] = pd.to_datetime(df[date_col], errors="coerce").dt.date
    elif fallback_date:
        out["date"] = fallback_date
    else:
        warnings.append({
            "severity": "Low",
            "message": f"Closing report file skipped because no date column or filename date was found: {path.name}",
        })
        return pd.DataFrame()

    if store_col:
        out["store"] = df[store_col].apply(lambda value: normalize_store_value(value, fallback_store))
    elif fallback_store:
        out["store"] = fallback_store
    else:
        out["store"] = "ALL"

    out["new_customers"] = to_number(df[new_col]).fillna(0).astype(float)
    out["total_customers"] = to_number(df[total_col]).fillna(0).astype(float) if total_col else 0.0
    out["source_file"] = path.name
    out = out[out["date"].notna()].copy()
    out = out[(out["date"] >= start_day) & (out["date"] <= end_day)].copy()
    return out


def fetch_store_daily_closing_api(
    store_code: str,
    location_key: str,
    integrator_key: str,
    start_day: date,
    end_day: date,
    delay_seconds: float = 6.5,
) -> Tuple[List[Dict[str, Any]], List[Dict[str, Any]]]:
    session = create_session(location_key, integrator_key)
    rows: List[Dict[str, Any]] = []
    fetch_warnings: List[Dict[str, Any]] = []
    day = start_day
    while day <= end_day:
        try:
            from_utc, to_utc = local_date_range_to_utc_strings(day.isoformat(), day.isoformat(), REPORT_TZ)
            payload = request_json(
                session,
                "/reporting/closing-report",
                params={"fromDateUTC": from_utc, "toDateUTC": to_utc},
                timeout=120,
                max_attempts=3,
            )
            if not isinstance(payload, dict):
                fetch_warnings.append({
                    "severity": "Low",
                    "message": f"Dutchie closing report returned unexpected payload for {store_code} on {day.isoformat()}.",
                })
                day += timedelta(days=1)
                continue
            rows.append({
                "date": day,
                "store": store_code,
                "new_customers": as_float(payload.get("newCustomerCount")),
                "total_customers": as_float(payload.get("customerCount")),
                "transaction_count": as_float(payload.get("transactionCount")),
                "net_sales": as_float(payload.get("netSales")),
                "gross_sales": as_float(payload.get("grossSales")),
                "discount": as_float(payload.get("discount")),
                "item_count": as_float(payload.get("itemCount")),
                "average_cart_net_sales": as_float(payload.get("averageCartNetSales")),
                "source_file": "Dutchie API /reporting/closing-report",
            })
        except Exception as exc:
            fetch_warnings.append({
                "severity": "Low",
                "message": f"Dutchie closing report API failed for {store_code} on {day.isoformat()}: {exc}",
            })
        if day < end_day:
            time.sleep(delay_seconds)
        day += timedelta(days=1)
    return rows, fetch_warnings


def fetch_monthly_closing_reports_from_api(
    start_day: date,
    end_day: date,
    closing_dir: Path,
    env_file: str,
    workers: Optional[int],
    warnings: List[Dict[str, Any]],
) -> pd.DataFrame:
    env_map = canonical_env_map(env_file)
    selected_store_codes = [abbr for abbr in STORE_ORDER if abbr in DUTCHIE_STORE_CODES]
    store_keys = resolve_store_keys(env_map, selected_store_codes)
    integrator_key = resolve_integrator_key(env_map)

    missing = [abbr for abbr in selected_store_codes if abbr not in store_keys]
    if missing:
        warnings.append({
            "severity": "Low",
            "message": "Missing Dutchie API location key(s) for closing reports: " + ", ".join(missing),
        })

    jobs = [(abbr, store_keys[abbr]) for abbr in selected_store_codes if abbr in store_keys]
    if not jobs:
        warnings.append({
            "severity": "Low",
            "message": "No Dutchie API location keys were available for closing-report new customer data.",
        })
        return pd.DataFrame(columns=["date", "store", "new_customers", "total_customers", "source_file"])

    worker_count = max(1, min(int(workers or 3), len(jobs)))
    delay_seconds = 6.5
    print(f"[CLOSING API] Fetching daily Dutchie closing reports with {worker_count} store worker(s), throttled to avoid Dutchie rate limits")
    rows: List[Dict[str, Any]] = []
    api_warnings: List[Dict[str, Any]] = []
    with ThreadPoolExecutor(max_workers=worker_count) as executor:
        futures = {
            executor.submit(
                fetch_store_daily_closing_api,
                abbr,
                key,
                integrator_key,
                start_day,
                end_day,
                delay_seconds,
            ): abbr
            for abbr, key in jobs
        }
        for future in as_completed(futures):
            abbr = futures[future]
            try:
                store_rows, store_warnings = future.result()
                rows.extend(store_rows)
                api_warnings.extend(store_warnings)
                print(f"[CLOSING API] {abbr}: {len(store_rows)} day(s)")
            except Exception as exc:
                api_warnings.append({
                    "severity": "Low",
                    "message": f"Dutchie closing report API failed for {abbr}: {exc}",
                })

    warnings.extend(api_warnings)
    if not rows:
        return pd.DataFrame(columns=["date", "store", "new_customers", "total_customers", "source_file"])

    closing_dir.mkdir(parents=True, exist_ok=True)
    api_df = pd.DataFrame(rows).sort_values(["date", "store"])
    api_path = closing_dir / f"monthly_closing_new_customers_api_{start_day.isoformat()}_to_{end_day.isoformat()}.csv"
    api_df.to_csv(api_path, index=False)
    print(f"[CLOSING API] Saved: {api_path}")
    return api_df


def fetch_monthly_closing_summary_from_api(
    start_day: date,
    end_day: date,
    data_dir: Path,
    env_file: str,
    workers: Optional[int],
    warnings: List[Dict[str, Any]],
) -> pd.DataFrame:
    env_map = canonical_env_map(env_file)
    selected_store_codes = [abbr for abbr in STORE_ORDER if abbr in DUTCHIE_STORE_CODES]
    store_keys = resolve_store_keys(env_map, selected_store_codes)
    integrator_key = resolve_integrator_key(env_map)
    missing = [abbr for abbr in selected_store_codes if abbr not in store_keys]
    if missing:
        warnings.append({
            "severity": "Low",
            "message": "Missing Dutchie API location key(s) for closing summary: " + ", ".join(missing),
        })

    jobs = [(abbr, store_keys[abbr]) for abbr in selected_store_codes if abbr in store_keys]
    if not jobs:
        warnings.append({
            "severity": "Low",
            "message": "No Dutchie API location keys were available for closing report summary.",
        })
        return pd.DataFrame()

    from_utc, to_utc = local_date_range_to_utc_strings(start_day.isoformat(), end_day.isoformat(), REPORT_TZ)
    worker_count = max(1, min(int(workers or 4), len(jobs)))
    print(f"[CLOSING SUMMARY API] Fetching full-window closing reports with {worker_count} worker(s)")

    def fetch_one(store_code: str, location_key: str) -> Dict[str, Any]:
        session = create_session(location_key, integrator_key)
        payload = request_json(
            session,
            "/reporting/closing-report",
            params={"fromDateUTC": from_utc, "toDateUTC": to_utc},
            timeout=180,
            max_attempts=3,
        )
        if not isinstance(payload, dict):
            raise RuntimeError("Unexpected closing report payload")
        net_sales = as_float(payload.get("netSales"))
        cost = as_float(payload.get("cost"))
        gross_profit = net_sales - cost
        return {
            "store": store_code,
            "store_label": STORE_LABEL_BY_ABBR.get(store_code, store_code),
            "from_utc": from_utc,
            "to_utc": to_utc,
            "gross_sales": as_float(payload.get("grossSales")),
            "discount": as_float(payload.get("discount")),
            "returns": as_float(payload.get("returnTotal")),
            "net_sales": net_sales,
            "cost_of_goods": cost,
            "gross_profit": gross_profit,
            "gross_margin": gross_profit / net_sales if net_sales else 0.0,
            "tax": as_float(payload.get("totalTax")),
            "transaction_count": as_float(payload.get("transactionCount")),
            "single_item_transactions": as_float(payload.get("singleItemTransactionCount")),
            "multi_item_transactions": as_float(payload.get("multiItemTransactionCount")),
            "customer_count": as_float(payload.get("customerCount")),
            "new_customer_count": as_float(payload.get("newCustomerCount")),
            "item_count": as_float(payload.get("itemCount")),
            "average_cart_net_sales": as_float(payload.get("averageCartNetSales")),
            "void_count": as_float(payload.get("voidCount")),
            "void_total": as_float(payload.get("voidTotal")),
            "cash": as_float(payload.get("cashPaid")),
            "debit": as_float(payload.get("debitPaid")),
            "credit": as_float(payload.get("creditPaid")),
            "gift": as_float(payload.get("giftPaid")),
            "total_payments": as_float(payload.get("totalPayments")),
            "source": "Dutchie API /reporting/closing-report",
        }

    rows: List[Dict[str, Any]] = []
    with ThreadPoolExecutor(max_workers=worker_count) as executor:
        futures = {executor.submit(fetch_one, abbr, key): abbr for abbr, key in jobs}
        for future in as_completed(futures):
            abbr = futures[future]
            try:
                row = future.result()
                rows.append(row)
                print(
                    f"[CLOSING SUMMARY API] {abbr}: "
                    f"new={int(row['new_customer_count']):,}, tx={int(row['transaction_count']):,}, net={money(row['net_sales'])}"
                )
            except Exception as exc:
                warnings.append({
                    "severity": "Low",
                    "message": f"Dutchie closing report summary API failed for {abbr}: {exc}",
                })

    if not rows:
        return pd.DataFrame()
    out = pd.DataFrame(rows).sort_values("store")
    data_dir.mkdir(parents=True, exist_ok=True)
    out.to_csv(data_dir / "monthly_closing_report_summary.csv", index=False)
    write_json(data_dir / "monthly_closing_report_summary.json", rows)
    return out


def load_monthly_closing_summary(data_dir: Path) -> pd.DataFrame:
    path = data_dir / "monthly_closing_report_summary.csv"
    if path.exists():
        try:
            return pd.read_csv(path)
        except pd.errors.EmptyDataError:
            return pd.DataFrame()
    return pd.DataFrame()


def add_closing_summary_metrics(all_metrics: Dict[str, Any], closing_summary: pd.DataFrame) -> None:
    if closing_summary is None or closing_summary.empty:
        all_metrics["closing_summary_has_data"] = False
        return
    all_metrics["closing_summary_has_data"] = True
    for source_col, target_key in [
        ("new_customer_count", "closing_new_customers"),
        ("customer_count", "closing_customers"),
        ("transaction_count", "closing_transactions"),
        ("net_sales", "closing_net_sales"),
        ("gross_sales", "closing_gross_sales"),
        ("discount", "closing_discount"),
        ("cost_of_goods", "closing_cost_of_goods"),
        ("gross_profit", "closing_gross_profit"),
        ("item_count", "closing_items_sold"),
    ]:
        all_metrics[target_key] = as_float(closing_summary[source_col].sum()) if source_col in closing_summary.columns else 0.0
    all_metrics["closing_gross_margin"] = (
        all_metrics["closing_gross_profit"] / all_metrics["closing_net_sales"]
        if all_metrics.get("closing_net_sales") else 0.0
    )
    all_metrics["closing_new_customer_rate"] = (
        all_metrics["closing_new_customers"] / all_metrics["closing_customers"]
        if all_metrics.get("closing_customers") else 0.0
    )
    all_metrics["new_customer_rate"] = all_metrics["closing_new_customer_rate"]


def apply_closing_summary_to_scorecards(store_scorecards: pd.DataFrame, closing_summary: pd.DataFrame) -> pd.DataFrame:
    if store_scorecards is None or store_scorecards.empty or closing_summary is None or closing_summary.empty:
        return store_scorecards
    needed = ["store", "customer_count", "new_customer_count", "transaction_count", "average_cart_net_sales"]
    available = [col for col in needed if col in closing_summary.columns]
    if "store" not in available:
        return store_scorecards
    closing = closing_summary[available].copy()
    rename_map = {
        "customer_count": "closing_customers",
        "new_customer_count": "closing_new_customers",
        "transaction_count": "closing_transactions",
        "average_cart_net_sales": "closing_average_cart_net_sales",
    }
    closing = closing.rename(columns=rename_map)
    out = store_scorecards.copy()
    out = out.merge(closing, on="store", how="left")
    for col in ["closing_customers", "closing_new_customers", "closing_transactions", "closing_average_cart_net_sales"]:
        if col in out.columns:
            out[col] = pd.to_numeric(out[col], errors="coerce").fillna(0.0)
    if {"closing_new_customers", "closing_customers"}.issubset(out.columns):
        out["closing_new_customer_rate"] = out["closing_new_customers"] / out["closing_customers"].replace({0: np.nan})
        out["closing_new_customer_rate"] = out["closing_new_customer_rate"].fillna(0.0)
    return out


def fetch_new_customer_profiles_from_api(
    start_day: date,
    end_day: date,
    closing_dir: Path,
    env_file: str,
    warnings: List[Dict[str, Any]],
) -> pd.DataFrame:
    env_map = canonical_env_map(env_file)
    selected_store_codes = [abbr for abbr in STORE_ORDER if abbr in DUTCHIE_STORE_CODES]
    store_keys = resolve_store_keys(env_map, selected_store_codes)
    integrator_key = resolve_integrator_key(env_map)
    if not store_keys:
        warnings.append({
            "severity": "Low",
            "message": "No Dutchie API location keys were available for customer profile new-customer data.",
        })
        return pd.DataFrame(columns=["date", "store", "new_customers", "total_customers", "source_file"])

    last_error = None
    rows: List[Dict[str, Any]] = []
    for store_code in selected_store_codes:
        location_key = store_keys.get(store_code)
        if not location_key:
            continue
        try:
            session = create_session(location_key, integrator_key)
            page_number = 0
            page_size = 10000
            print(f"[NEW CUSTOMER API] Fetching customer profiles with {store_code} key")
            while True:
                payload = request_json(
                    session,
                    "/reporting/customers-paginated",
                    params={
                        "PageNumber": page_number,
                        "PageSize": page_size,
                        "includeAnonymous": False,
                    },
                    timeout=180,
                    max_attempts=3,
                )
                if not isinstance(payload, list) or not payload:
                    break
                for customer in payload:
                    if not isinstance(customer, dict):
                        continue
                    created_at = pd.to_datetime(customer.get("creationDate"), errors="coerce")
                    if pd.isna(created_at):
                        continue
                    created_day = created_at.date()
                    if not (start_day <= created_day <= end_day):
                        continue
                    rows.append({
                        "date": created_day,
                        "store": normalize_store_value(customer.get("createdAtLocation")),
                        "new_customers": 1.0,
                        "total_customers": 0.0,
                        "source_file": "Dutchie API /reporting/customers-paginated",
                    })
                print(f"[NEW CUSTOMER API] Page {page_number}: {len(payload)} customer(s)")
                if len(payload) < page_size:
                    break
                page_number += 1
                if page_number > 200:
                    warnings.append({
                        "severity": "Low",
                        "message": "Stopped Dutchie customer profile pagination after 200 pages.",
                    })
                    break
            last_error = None
            break
        except Exception as exc:
            last_error = exc
            warnings.append({
                "severity": "Low",
                "message": f"Dutchie customer profile API failed with {store_code} key: {exc}",
            })

    if last_error is not None and not rows:
        return pd.DataFrame(columns=["date", "store", "new_customers", "total_customers", "source_file"])

    if not rows:
        warnings.append({
            "severity": "Low",
            "message": f"No Dutchie customer profiles were created from {start_day.isoformat()} to {end_day.isoformat()}.",
        })
        return pd.DataFrame(columns=["date", "store", "new_customers", "total_customers", "source_file"])

    closing_dir.mkdir(parents=True, exist_ok=True)
    profile_df = pd.DataFrame(rows)
    profile_summary = (
        profile_df.groupby(["date", "store"], as_index=False)
        .agg(
            new_customers=("new_customers", "sum"),
            total_customers=("total_customers", "sum"),
            source_file=("source_file", "first"),
        )
        .sort_values(["date", "store"])
    )
    api_path = closing_dir / f"monthly_new_customer_profiles_api_{start_day.isoformat()}_to_{end_day.isoformat()}.csv"
    profile_summary.to_csv(api_path, index=False)
    print(f"[NEW CUSTOMER API] Saved: {api_path}")
    return profile_summary


def inventory_snapshot_utc(day: date, end_of_day: bool) -> str:
    local_time = datetime.max.time().replace(microsecond=0) if end_of_day else datetime.min.time()
    local_dt = datetime.combine(day, local_time, tzinfo=ZoneInfo(REPORT_TZ))
    return isoformat_utc(local_dt)


def normalize_inventory_snapshot_payload(payload: Any, store_code: str, snapshot_label: str, snapshot_day: date) -> pd.DataFrame:
    rows: List[Dict[str, Any]] = []
    for item in payload or []:
        if not isinstance(item, dict):
            continue
        product = str(item.get("product") or item.get("productName") or item.get("name") or "Unknown").strip()
        quantity = as_float(item.get("quantity"), 0.0)
        total_cost = as_float(item.get("totalCost"), 0.0)
        unit_cost = total_cost / quantity if quantity else 0.0
        rows.append({
            "snapshot": snapshot_label,
            "snapshot_date": snapshot_day.isoformat(),
            "store": store_code,
            "store_label": STORE_LABEL_BY_ABBR.get(store_code, store_code),
            "product_id": item.get("productId"),
            "product": product,
            "brand": parse_brand_from_product(product),
            "sku": item.get("sku"),
            "vendor": item.get("vendor"),
            "room": item.get("room"),
            "status": item.get("status"),
            "quantity": quantity,
            "total_cost": total_cost,
            "unit_cost": unit_cost,
            "inventory_id": item.get("inventoryId"),
            "package_id": item.get("packageId"),
            "batch_id": item.get("batchId"),
            "batch_name": item.get("batchName"),
        })
    return pd.DataFrame(rows)


def fetch_store_inventory_snapshots(
    store_code: str,
    location_key: str,
    integrator_key: str,
    start_day: date,
    end_day: date,
) -> Tuple[pd.DataFrame, pd.DataFrame, List[Dict[str, Any]]]:
    session = create_session(location_key, integrator_key)
    warnings: List[Dict[str, Any]] = []
    frames: Dict[str, pd.DataFrame] = {}
    for label, day, end_of_day in [
        ("start", start_day, False),
        ("end", end_day, True),
    ]:
        try:
            payload = request_json(
                session,
                "/inventory/snapshot",
                params={"fromDate": inventory_snapshot_utc(day, end_of_day=end_of_day)},
                timeout=180,
                max_attempts=3,
            )
            frames[label] = normalize_inventory_snapshot_payload(payload, store_code, label, day)
        except Exception as exc:
            warnings.append({
                "severity": "Low",
                "message": f"Dutchie inventory snapshot API failed for {store_code} {label} snapshot: {exc}",
            })
            frames[label] = pd.DataFrame()
    return frames["start"], frames["end"], warnings


def fetch_monthly_inventory_snapshots_from_api(
    start_day: date,
    end_day: date,
    data_dir: Path,
    env_file: str,
    workers: Optional[int],
    warnings: List[Dict[str, Any]],
) -> Tuple[pd.DataFrame, pd.DataFrame]:
    env_map = canonical_env_map(env_file)
    selected_store_codes = [abbr for abbr in STORE_ORDER if abbr in DUTCHIE_STORE_CODES]
    store_keys = resolve_store_keys(env_map, selected_store_codes)
    integrator_key = resolve_integrator_key(env_map)
    missing = [abbr for abbr in selected_store_codes if abbr not in store_keys]
    if missing:
        warnings.append({
            "severity": "Low",
            "message": "Missing Dutchie API location key(s) for inventory snapshots: " + ", ".join(missing),
        })
    jobs = [(abbr, store_keys[abbr]) for abbr in selected_store_codes if abbr in store_keys]
    if not jobs:
        warnings.append({
            "severity": "Low",
            "message": "No Dutchie API location keys were available for inventory snapshots.",
        })
        return pd.DataFrame(), pd.DataFrame()

    worker_count = max(1, min(int(workers or 4), len(jobs)))
    print(f"[INVENTORY API] Fetching first/end inventory snapshots with {worker_count} worker(s)")
    start_frames: List[pd.DataFrame] = []
    end_frames: List[pd.DataFrame] = []
    with ThreadPoolExecutor(max_workers=worker_count) as executor:
        futures = {
            executor.submit(fetch_store_inventory_snapshots, abbr, key, integrator_key, start_day, end_day): abbr
            for abbr, key in jobs
        }
        for future in as_completed(futures):
            abbr = futures[future]
            try:
                start_df, end_df, store_warnings = future.result()
                warnings.extend(store_warnings)
                if not start_df.empty:
                    start_frames.append(start_df)
                if not end_df.empty:
                    end_frames.append(end_df)
                print(f"[INVENTORY API] {abbr}: start {len(start_df):,} row(s), end {len(end_df):,} row(s)")
            except Exception as exc:
                warnings.append({
                    "severity": "Low",
                    "message": f"Dutchie inventory snapshot API failed for {abbr}: {exc}",
                })

    start_all = pd.concat(start_frames, ignore_index=True) if start_frames else pd.DataFrame()
    end_all = pd.concat(end_frames, ignore_index=True) if end_frames else pd.DataFrame()
    if not start_all.empty:
        start_all.to_csv(data_dir / "monthly_inventory_snapshot_start.csv", index=False)
    if not end_all.empty:
        end_all.to_csv(data_dir / "monthly_inventory_snapshot_end.csv", index=False)
    return start_all, end_all


def load_monthly_inventory_snapshots(data_dir: Path) -> Tuple[pd.DataFrame, pd.DataFrame]:
    start_path = data_dir / "monthly_inventory_snapshot_start.csv"
    end_path = data_dir / "monthly_inventory_snapshot_end.csv"
    start_df = pd.read_csv(start_path) if start_path.exists() else pd.DataFrame()
    end_df = pd.read_csv(end_path) if end_path.exists() else pd.DataFrame()
    return start_df, end_df


def compute_inventory_snapshot_totals(df: pd.DataFrame, prefix: str) -> pd.DataFrame:
    columns = [
        "store",
        f"{prefix}_inventory_value",
        f"{prefix}_inventory_units",
        f"{prefix}_sku_count",
        f"{prefix}_package_count",
    ]
    if df is None or df.empty:
        return pd.DataFrame(columns=columns)
    tmp = df.copy()
    tmp["quantity"] = pd.to_numeric(tmp.get("quantity"), errors="coerce").fillna(0.0)
    tmp["total_cost"] = pd.to_numeric(tmp.get("total_cost"), errors="coerce").fillna(0.0)
    tmp = tmp[tmp["quantity"] > 0].copy()
    if tmp.empty:
        return pd.DataFrame(columns=columns)
    tmp["sku_key"] = tmp.get("sku", "").fillna("").astype(str)
    tmp.loc[tmp["sku_key"].str.strip() == "", "sku_key"] = tmp.get("product", "").fillna("").astype(str)
    return tmp.groupby("store", as_index=False).agg(
        **{
            f"{prefix}_inventory_value": ("total_cost", "sum"),
            f"{prefix}_inventory_units": ("quantity", "sum"),
            f"{prefix}_sku_count": ("sku_key", "nunique"),
            f"{prefix}_package_count": ("inventory_id", "nunique"),
        }
    )


def build_inventory_summary(
    start_inventory: pd.DataFrame,
    end_inventory: pd.DataFrame,
    store_scorecards: pd.DataFrame,
) -> pd.DataFrame:
    start_totals = compute_inventory_snapshot_totals(start_inventory, "opening")
    end_totals = compute_inventory_snapshot_totals(end_inventory, "ending")
    stores = pd.DataFrame({"store": STORE_ORDER})
    out = stores.merge(start_totals, on="store", how="left").merge(end_totals, on="store", how="left")
    out["store_label"] = out["store"].map(lambda value: STORE_LABEL_BY_ABBR.get(str(value), str(value)))
    numeric_cols = [col for col in out.columns if col not in {"store", "store_label"}]
    out[numeric_cols] = out[numeric_cols].fillna(0.0)
    out["inventory_value_change"] = out["ending_inventory_value"] - out["opening_inventory_value"]
    out["inventory_value_change_pct"] = out["inventory_value_change"] / out["opening_inventory_value"].replace({0: np.nan})
    out["inventory_unit_change"] = out["ending_inventory_units"] - out["opening_inventory_units"]
    out["sku_change"] = out["ending_sku_count"] - out["opening_sku_count"]
    out["avg_inventory_value"] = (out["opening_inventory_value"] + out["ending_inventory_value"]) / 2.0
    if store_scorecards is not None and not store_scorecards.empty:
        score = store_scorecards[["store", "cogs_real", "net_revenue"]].copy()
        out = out.merge(score, on="store", how="left")
        out["inventory_turns_est"] = out["cogs_real"] / out["avg_inventory_value"].replace({0: np.nan})
        out["inventory_to_revenue"] = out["ending_inventory_value"] / out["net_revenue"].replace({0: np.nan})
    else:
        out["inventory_turns_est"] = np.nan
        out["inventory_to_revenue"] = np.nan
    return out.sort_values("ending_inventory_value", ascending=False)


def add_inventory_metrics(all_metrics: Dict[str, Any], inventory_summary: pd.DataFrame) -> None:
    if inventory_summary is None or inventory_summary.empty:
        all_metrics["inventory_has_data"] = False
        return
    all_metrics["inventory_has_data"] = bool(as_float(inventory_summary["opening_inventory_value"].sum()) or as_float(inventory_summary["ending_inventory_value"].sum()))
    all_metrics["inventory_start_value"] = as_float(inventory_summary["opening_inventory_value"].sum())
    all_metrics["inventory_end_value"] = as_float(inventory_summary["ending_inventory_value"].sum())
    all_metrics["inventory_value_change"] = all_metrics["inventory_end_value"] - all_metrics["inventory_start_value"]
    all_metrics["inventory_value_change_pct"] = (
        all_metrics["inventory_value_change"] / all_metrics["inventory_start_value"]
        if all_metrics["inventory_start_value"] else np.nan
    )
    all_metrics["inventory_start_units"] = as_float(inventory_summary["opening_inventory_units"].sum())
    all_metrics["inventory_end_units"] = as_float(inventory_summary["ending_inventory_units"].sum())
    all_metrics["inventory_unit_change"] = all_metrics["inventory_end_units"] - all_metrics["inventory_start_units"]
    all_metrics["inventory_store_count"] = int((inventory_summary["ending_inventory_value"] > 0).sum())


def load_monthly_closing_reports(
    closing_dir: Path,
    start_day: date,
    end_day: date,
    warnings: List[Dict[str, Any]],
) -> pd.DataFrame:
    if not closing_dir.exists():
        warnings.append({
            "severity": "Low",
            "message": f"No closing report folder found for new customers: {closing_dir}",
        })
        return pd.DataFrame(columns=["date", "store", "new_customers", "total_customers", "source_file"])

    files = [
        path for path in closing_dir.rglob("*")
        if path.is_file() and path.suffix.lower() in [".csv", ".xlsx", ".xls"]
    ]
    profile_api_files = [path for path in files if path.name.startswith("monthly_new_customer_profiles_api_")]
    closing_api_files = [path for path in files if path.name.startswith("monthly_closing_new_customers_api_")]
    if closing_api_files:
        files = closing_api_files
    elif profile_api_files:
        files = profile_api_files
        warnings.append({
            "severity": "Low",
            "message": "Using customer profile creation dates for new-customer daily detail because no daily closing-report file was found. These counts may not match the Backoffice closing report.",
        })
    if not files:
        warnings.append({
            "severity": "Low",
            "message": f"No closing report CSV/XLSX files found for new customers in {closing_dir}",
        })
        return pd.DataFrame(columns=["date", "store", "new_customers", "total_customers", "source_file"])

    frames = [read_closing_report_file(path, start_day, end_day, warnings) for path in sorted(files)]
    frames = [frame for frame in frames if frame is not None and not frame.empty]
    if not frames:
        return pd.DataFrame(columns=["date", "store", "new_customers", "total_customers", "source_file"])
    combined = pd.concat(frames, ignore_index=True)
    return combined.groupby(["date", "store"], as_index=False).agg(
        new_customers=("new_customers", "sum"),
        total_customers=("total_customers", "sum"),
        source_file=("source_file", lambda values: ", ".join(sorted(set(str(v) for v in values if str(v))))),
    ).sort_values(["date", "store"])


def build_new_customer_summaries(closing_df: pd.DataFrame, store_scorecards: pd.DataFrame) -> Tuple[pd.DataFrame, pd.DataFrame]:
    if closing_df is None or closing_df.empty:
        return pd.DataFrame(), pd.DataFrame()

    daily = closing_df.groupby("date", as_index=False).agg(
        new_customers=("new_customers", "sum"),
        total_customers=("total_customers", "sum"),
    ).sort_values("date")
    daily["new_customer_rate"] = daily["new_customers"] / daily["total_customers"].replace({0: np.nan})
    daily[["new_customers", "total_customers"]] = daily[["new_customers", "total_customers"]].fillna(0.0)

    summary = closing_df.groupby("store", as_index=False).agg(
        new_customers=("new_customers", "sum"),
        total_customers=("total_customers", "sum"),
        days_reported=("date", "nunique"),
    )
    summary["new_customer_rate"] = summary["new_customers"] / summary["total_customers"].replace({0: np.nan})
    summary["avg_new_customers_per_day"] = summary["new_customers"] / summary["days_reported"].replace({0: np.nan})

    best_rows = (
        closing_df.sort_values(["store", "new_customers"], ascending=[True, False])
        .groupby("store", as_index=False)
        .head(1)[["store", "date", "new_customers"]]
        .rename(columns={"date": "best_new_customer_day", "new_customers": "best_day_new_customers"})
    )
    summary = summary.merge(best_rows, on="store", how="left")

    if store_scorecards is not None and not store_scorecards.empty:
        summary = summary.merge(
            store_scorecards[["store", "net_revenue", "tickets"]],
            on="store",
            how="left",
        )
        summary["new_customers_per_100_tickets"] = summary["new_customers"] / summary["tickets"].replace({0: np.nan}) * 100.0
        summary["net_revenue_per_new_customer"] = summary["net_revenue"] / summary["new_customers"].replace({0: np.nan})
    else:
        summary["new_customers_per_100_tickets"] = 0.0
        summary["net_revenue_per_new_customer"] = 0.0

    for col in ["new_customers", "total_customers", "days_reported", "avg_new_customers_per_day", "best_day_new_customers", "net_revenue", "tickets", "new_customers_per_100_tickets", "net_revenue_per_new_customer"]:
        if col in summary.columns:
            summary[col] = summary[col].fillna(0.0)
    if "best_new_customer_day" in summary.columns:
        summary["best_new_customer_day"] = summary["best_new_customer_day"].apply(
            lambda value: value.isoformat() if isinstance(value, date) else str(value)
        )
    return summary.sort_values("new_customers", ascending=False), daily


def combine_unique_text(values: pd.Series, max_items: int = 4) -> str:
    seen: List[str] = []
    for value in values:
        text = str(value or "").strip()
        if not text or text.lower() in {"nan", "none"}:
            continue
        if text not in seen:
            seen.append(text)
    if not seen:
        return "N/A"
    if len(seen) <= max_items:
        return ", ".join(seen)
    return ", ".join(seen[:max_items]) + f" +{len(seen) - max_items} more"


def combine_unique_money2(values: pd.Series, max_items: int = 4) -> str:
    cleaned = pd.to_numeric(values, errors="coerce").dropna().round(4).unique().tolist()
    cleaned = sorted(float(value) for value in cleaned)
    if not cleaned:
        return "N/A"
    labels = [money2(value) for value in cleaned[:max_items]]
    if len(cleaned) > max_items:
        labels.append(f"+{len(cleaned) - max_items} more")
    return ", ".join(labels)


def add_deal_performance_signal(out: pd.DataFrame) -> pd.DataFrame:
    if out is None or out.empty:
        return out
    result = out.copy()
    company_real_margin = (
        as_float(result["profit_real"].sum()) / as_float(result["net_revenue"].sum())
        if "profit_real" in result.columns and "net_revenue" in result.columns and as_float(result["net_revenue"].sum())
        else 0.0
    )

    def signal(row: pd.Series) -> str:
        margin_real = as_float(row.get("margin_real"))
        margin = as_float(row.get("margin"))
        revenue = as_float(row.get("net_revenue"))
        if revenue <= 0:
            return "No revenue"
        if margin_real < 0:
            return "Negative real margin"
        if company_real_margin and margin_real < company_real_margin - 0.05:
            return "Below deal avg"
        if margin - margin_real >= 0.10 and margin_real < company_real_margin:
            return "Kickback-dependent"
        return "Healthy"

    result["deal_signal"] = result.apply(signal, axis=1)
    return result


def compute_kickback_summary(df: pd.DataFrame) -> pd.DataFrame:
    if df.empty or "_deal_kickback_amt" not in df.columns:
        return pd.DataFrame(columns=["brand", "rule", "discount_rule", "discount_rule_display", "stores", "store_count", "rule_count", "net_revenue", "generated_profit", "profit_real", "kickback", "profit", "margin_real", "margin", "margin_lift", "deal_signal"])
    tmp = df.copy()
    tmp["_kickback"] = to_number(tmp["_deal_kickback_amt"]).fillna(0).astype(float)
    tmp = tmp[tmp["_kickback"] > 0].copy()
    if tmp.empty:
        return pd.DataFrame(columns=["brand", "rule", "discount_rule", "discount_rule_display", "stores", "store_count", "rule_count", "net_revenue", "generated_profit", "profit_real", "kickback", "profit", "margin_real", "margin", "margin_lift", "deal_signal"])
    net_col = find_col(tmp, COLUMN_CANDIDATES["net_sales"])
    profit_col = find_col(tmp, COLUMN_CANDIDATES["profit"])
    cogs_col = find_col(tmp, COLUMN_CANDIDATES["cogs"])
    tmp["_net"] = to_number(tmp[net_col]).fillna(0).astype(float) if net_col else 0.0
    tmp["_profit_real"] = to_number(tmp[profit_col]).fillna(0).astype(float) if profit_col else (tmp["_net"] - (to_number(tmp[cogs_col]).fillna(0).astype(float) if cogs_col else 0.0))
    tmp["_profit"] = tmp["_profit_real"] + tmp["_kickback"]
    tmp["_deal_brand"] = tmp.get("_deal_brand", "Unknown")
    tmp["_deal_rule"] = tmp.get("_deal_rule", "Unknown")
    tmp["_deal_discount"] = tmp.get("_deal_discount", 0.0)
    tmp["_store"] = tmp["_store"].astype(str) if "_store" in tmp.columns else "ALL"

    out = tmp.groupby("_deal_brand", as_index=False).agg(
        net_revenue=("_net", "sum"),
        profit_real=("_profit_real", "sum"),
        kickback=("_kickback", "sum"),
        profit=("_profit", "sum"),
        rule=("_deal_rule", combine_unique_text),
        rule_count=("_deal_rule", lambda s: len({str(v).strip() for v in s if str(v).strip() and str(v).lower() != "nan"})),
        discount_rule=("_deal_discount", "mean"),
        discount_rule_display=("_deal_discount", combine_unique_money2),
        stores=("_store", combine_unique_text),
        store_count=("_store", lambda s: len({str(v).strip() for v in s if str(v).strip() and str(v).lower() != "nan"})),
    ).rename(columns={"_deal_brand": "brand"})
    out["margin_real"] = out["profit_real"] / out["net_revenue"].replace({0: np.nan})
    out["margin"] = out["profit"] / out["net_revenue"].replace({0: np.nan})
    out["margin_lift"] = out["margin"] - out["margin_real"]
    out["generated_profit"] = out["profit"]
    out = add_deal_performance_signal(out.fillna(0.0))
    return out.sort_values("generated_profit", ascending=False)


def best_worst_day(daily: pd.DataFrame) -> Tuple[Optional[Dict[str, Any]], Optional[Dict[str, Any]]]:
    if daily is None or daily.empty:
        return None, None
    active = daily[daily["net_revenue"] > 0].copy()
    if active.empty:
        return None, None
    best = active.sort_values("net_revenue", ascending=False).iloc[0].to_dict()
    worst = active.sort_values("net_revenue", ascending=True).iloc[0].to_dict()
    return best, worst


def enrich_monthly_metrics(metrics: Dict[str, float], daily: pd.DataFrame, category: pd.DataFrame, brand: pd.DataFrame, product: pd.DataFrame) -> Dict[str, Any]:
    out: Dict[str, Any] = {k: as_float(v) for k, v in metrics.items()}
    active_daily = daily[daily["net_revenue"] > 0] if daily is not None and not daily.empty else pd.DataFrame()
    out["average_daily_revenue"] = as_float(active_daily["net_revenue"].mean()) if not active_daily.empty else 0.0
    out["median_daily_revenue"] = as_float(active_daily["net_revenue"].median()) if not active_daily.empty else 0.0
    best, worst = best_worst_day(daily)
    out["best_day"] = {
        "date": best["date"].isoformat(),
        "net_revenue": as_float(best.get("net_revenue")),
    } if best else None
    out["worst_day"] = {
        "date": worst["date"].isoformat(),
        "net_revenue": as_float(worst.get("net_revenue")),
    } if worst else None

    if daily is not None and not daily.empty:
        tmp = daily.copy()
        tmp["weekday"] = pd.to_datetime(tmp["date"]).dt.weekday
        weekend = tmp[tmp["weekday"].isin([5, 6])]["net_revenue"].sum()
        weekday = tmp[~tmp["weekday"].isin([5, 6])]["net_revenue"].sum()
        out["weekend_revenue"] = as_float(weekend)
        out["weekday_revenue"] = as_float(weekday)
        out["weekend_revenue_share"] = weekend / (weekend + weekday) if (weekend + weekday) else 0.0

    out["top_category"] = str(category.iloc[0]["category"]) if category is not None and not category.empty else "N/A"
    out["top_brand"] = str(brand.iloc[0]["brand"]) if brand is not None and not brand.empty else "N/A"
    out["top_product"] = str(product.iloc[0]["product"]) if product is not None and not product.empty else "N/A"
    out["top_5_brand_share"] = as_float(brand.head(5)["net_revenue"].sum()) / as_float(brand["net_revenue"].sum()) if brand is not None and not brand.empty else 0.0
    out["top_10_product_share"] = as_float(product.head(10)["net_revenue"].sum()) / as_float(product["net_revenue"].sum()) if product is not None and not product.empty else 0.0
    return out


def percentage_change(current: float, baseline: float) -> Optional[float]:
    if not baseline:
        return None
    return (current - baseline) / baseline


def comparison_label(current: Any, baseline: Any, delta: Any) -> str:
    if baseline == "N/A" or delta == "N/A" or baseline is None or delta is None:
        return "N/A"
    return f"{money(as_float(baseline))} ({pct1(as_float(delta))})"


def generate_owner_action_items(
    all_metrics: Dict[str, Any],
    store_scorecards: pd.DataFrame,
    category_summary: pd.DataFrame,
    brand_summary: pd.DataFrame,
    product_summary: pd.DataFrame,
    cart_distribution: pd.DataFrame,
    comparison_metrics: Dict[str, Optional[Dict[str, Any]]],
) -> List[Dict[str, str]]:
    items: List[Dict[str, str]] = []

    prev = comparison_metrics.get("previous_month")
    if prev:
        net_delta = percentage_change(as_float(all_metrics.get("net_revenue")), as_float(prev.get("net_revenue")))
        if net_delta is not None and net_delta < -0.10:
            items.append({
                "severity": "High",
                "issue": "Revenue declined vs previous month",
                "metric_value": pct1(net_delta),
                "recommended_action": "Review traffic, staffing coverage, and category mix by store.",
            })

    if as_float(all_metrics.get("discount_rate")) > 0.20:
        items.append({
            "severity": "High",
            "issue": "Discount rate is elevated",
            "metric_value": pct1(as_float(all_metrics.get("discount_rate"))),
            "recommended_action": "Review promo effectiveness and discount guardrails.",
        })

    if as_float(all_metrics.get("margin_real")) < 0.35 and as_float(all_metrics.get("net_revenue")) > 0:
        items.append({
            "severity": "Medium",
            "issue": "Real margin is below target",
            "metric_value": pct1(as_float(all_metrics.get("margin_real"))),
            "recommended_action": "Review COGS, product mix, and high-discount categories.",
        })

    if store_scorecards is not None and not store_scorecards.empty:
        needs = store_scorecards[store_scorecards["status"] == "Needs Attention"]
        if not needs.empty:
            names = ", ".join(needs["store"].astype(str).head(4).tolist())
            items.append({
                "severity": "High",
                "issue": "Stores need attention",
                "metric_value": names,
                "recommended_action": "Use the store scorecards to focus on margin, basket, and discount coaching.",
            })

        company_margin = as_float(all_metrics.get("margin_real"))
        low_margin = store_scorecards[store_scorecards["margin_real"] < company_margin - 0.05] if company_margin else pd.DataFrame()
        if not low_margin.empty:
            row = low_margin.sort_values("margin_real").iloc[0]
            items.append({
                "severity": "Medium",
                "issue": f"{row['store']} real margin trails company average",
                "metric_value": f"{pct1(row['margin_real'])} vs {pct1(company_margin)}",
                "recommended_action": "Review discounting, COGS, and mix for that store.",
            })

        company_basket = as_float(all_metrics.get("basket"))
        low_basket = store_scorecards[store_scorecards["basket"] < company_basket * 0.90] if company_basket else pd.DataFrame()
        if not low_basket.empty:
            row = low_basket.sort_values("basket").iloc[0]
            items.append({
                "severity": "Medium",
                "issue": f"{row['store']} basket is below company average",
                "metric_value": f"{money1(row['basket'])} vs {money1(company_basket)}",
                "recommended_action": "Consider add-on prompts, bundles, and basket coaching.",
            })

    top5_brand_share = as_float(all_metrics.get("top_5_brand_share"))
    if top5_brand_share > 0.60:
        items.append({
            "severity": "Medium",
            "issue": "Revenue is concentrated in top brands",
            "metric_value": pct1(top5_brand_share),
            "recommended_action": "Protect inventory depth on key movers and monitor replenishment risk.",
        })

    if cart_distribution is not None and not cart_distribution.empty:
        low_cart_share = as_float(cart_distribution[cart_distribution["bucket"].isin(["$0-$1", "$1-$10", "$10-$20"])]["pct"].sum())
        if low_cart_share > 0.25:
            items.append({
                "severity": "Medium",
                "issue": "Low-value carts are high",
                "metric_value": pct1(low_cart_share),
                "recommended_action": "Use upsell prompts and bundles for carts under $20.",
            })

    if category_summary is not None and not category_summary.empty:
        company_margin = as_float(all_metrics.get("margin_real"))
        low_margin_categories = category_summary[
            (category_summary["pct_revenue"] >= 0.10) &
            (category_summary["margin_real"] < company_margin - 0.05)
        ] if company_margin else pd.DataFrame()
        if not low_margin_categories.empty:
            row = low_margin_categories.sort_values("margin_real").iloc[0]
            items.append({
                "severity": "Medium",
                "issue": f"{row['category']} is high-volume and low-margin",
                "metric_value": f"{pct1(row['margin_real'])} real margin",
                "recommended_action": "Review pricing, promo cadence, and cost on this category.",
            })

    if not items:
        items.append({
            "severity": "Low",
            "issue": "No major threshold exceptions detected",
            "metric_value": "N/A",
            "recommended_action": "Maintain current operating cadence and keep watching store-level mix.",
        })

    severity_order = {"High": 0, "Medium": 1, "Low": 2}
    return sorted(items, key=lambda item: severity_order.get(item["severity"], 9))[:8]


def generate_category_insights(category_summary: pd.DataFrame, all_metrics: Dict[str, Any]) -> List[str]:
    if category_summary is None or category_summary.empty:
        return ["Category data was not available in the export."]
    insights = []
    top = category_summary.iloc[0]
    insights.append(f"{top['category']} led revenue at {money(top['net_revenue'])} ({pct1(top['pct_revenue'])} share).")
    company_margin = as_float(all_metrics.get("margin_real"))
    low = category_summary[(category_summary["pct_revenue"] > 0.10) & (category_summary["margin_real"] < company_margin - 0.05)] if company_margin else pd.DataFrame()
    if not low.empty:
        row = low.sort_values("margin_real").iloc[0]
        insights.append(f"{row['category']} carried volume but real margin was {pct1(row['margin_real'])}.")
    high_disc = category_summary[category_summary["discount_rate"] > 0.20]
    if not high_disc.empty:
        row = high_disc.sort_values("discount_rate", ascending=False).iloc[0]
        insights.append(f"{row['category']} had the highest discount rate at {pct1(row['discount_rate'])}.")
    return insights[:4]


def generate_brand_insights(brand_summary: pd.DataFrame) -> List[str]:
    if brand_summary is None or brand_summary.empty:
        return ["Brand data was not available in the export."]
    insights = []
    top = brand_summary.iloc[0]
    insights.append(f"{top['brand']} led brands at {money(top['net_revenue'])}.")
    total = as_float(brand_summary["net_revenue"].sum())
    top5 = as_float(brand_summary.head(5)["net_revenue"].sum()) / total if total else 0.0
    if top5 > 0.60:
        insights.append(f"Top 5 brands represented {pct1(top5)} of revenue.")
    if "discount_rate" in brand_summary.columns:
        high_disc = brand_summary[(brand_summary["net_revenue"] > total * 0.02) & (brand_summary["discount_rate"] > 0.20)]
        if not high_disc.empty:
            row = high_disc.sort_values("discount_rate", ascending=False).iloc[0]
            insights.append(f"{row['brand']} discount rate was elevated at {pct1(row['discount_rate'])}.")
    low_margin = brand_summary[(brand_summary["net_revenue"] > total * 0.02) & (brand_summary["margin_real"] < 0.30)]
    if not low_margin.empty:
        row = low_margin.sort_values("margin_real").iloc[0]
        insights.append(f"{row['brand']} was low-margin at {pct1(row['margin_real'])} real margin.")
    return insights[:4]


def generate_discount_insights(all_metrics: Dict[str, Any], store_scorecards: pd.DataFrame, category_summary: pd.DataFrame) -> List[str]:
    insights = [f"Total discounts were {money(all_metrics.get('discount'))}, a {pct1(all_metrics.get('discount_rate'))} discount rate."]
    if store_scorecards is not None and not store_scorecards.empty:
        row = store_scorecards.sort_values("discount_rate", ascending=False).iloc[0]
        insights.append(f"{row['store']} had the highest store discount rate at {pct1(row['discount_rate'])}.")
    if category_summary is not None and not category_summary.empty and "discount_rate" in category_summary.columns:
        row = category_summary.sort_values("discount_rate", ascending=False).iloc[0]
        insights.append(f"{row['category']} had the highest category discount rate at {pct1(row['discount_rate'])}.")
    return insights[:4]


def generate_budtender_insights(budtender_summary: pd.DataFrame) -> List[str]:
    if budtender_summary is None or budtender_summary.empty:
        return ["Budtender data was not available in the export."]
    insights = []
    rev = budtender_summary.sort_values("net_revenue", ascending=False).iloc[0]
    insights.append(f"Top budtender revenue was {money(rev['net_revenue'])}.")
    tix = budtender_summary.sort_values("tickets", ascending=False).iloc[0]
    insights.append(f"Highest ticket count was {int(tix['tickets']):,}.")
    if "discount_rate" in budtender_summary.columns:
        disc = budtender_summary.sort_values("discount_rate", ascending=False).iloc[0]
        insights.append(f"Highest budtender discount rate was {pct1(disc['discount_rate'])}.")
    return insights[:4]


def generate_cart_value_insights(cart_distribution: pd.DataFrame) -> List[str]:
    if cart_distribution is None or cart_distribution.empty:
        return ["Cart value distribution was not available in the export."]
    top = cart_distribution.sort_values("count", ascending=False).iloc[0]
    low_share = as_float(cart_distribution[cart_distribution["bucket"].isin(["$0-$1", "$1-$10", "$10-$20"])]["pct"].sum())
    insights = [f"Most common cart bucket was {top['bucket']} at {int(top['count']):,} carts ({pct1(top['pct'])})."]
    if low_share > 0.25:
        insights.append(f"Carts under $20 were {pct1(low_share)} of carts.")
    return insights[:3]


def _mpl_setup() -> None:
    plt.rcParams.update({
        "font.size": 8.3,
        "axes.titlesize": 11.0,
        "axes.labelsize": 8.1,
        "axes.edgecolor": HEX_BORDER,
        "axes.linewidth": 0.7,
        "axes.titleweight": "bold",
        "axes.facecolor": "#F9FAFB",
        "figure.facecolor": "#FFFFFF",
        "grid.color": HEX_BORDER,
        "grid.linewidth": 0.7,
        "xtick.color": HEX_MUTED,
        "ytick.color": HEX_MUTED,
    })


def save_chart(fig) -> BytesIO:
    buf = BytesIO()
    fig.savefig(buf, format="png", dpi=160, bbox_inches="tight", pad_inches=0.14)
    plt.close(fig)
    buf.seek(0)
    return buf


def chart_daily_revenue_profit(daily: pd.DataFrame, title: str) -> BytesIO:
    _mpl_setup()
    if daily is None or daily.empty:
        return BytesIO()
    d = daily.copy()
    labels = [f"{pd.to_datetime(x).month}/{pd.to_datetime(x).day}" for x in d["date"]]
    x = np.arange(len(d))
    fig, ax = plt.subplots(figsize=(7.35, 3.0))
    ax.bar(x, d["net_revenue"], color=HEX_GREEN, edgecolor="#047857", linewidth=0.4, label="Net Revenue")
    ax.plot(x, d["profit"], color="#111827", linewidth=1.4, marker="o", markersize=2.8, label="Profit")
    ax.set_title(title)
    step = max(1, int(math.ceil(len(labels) / 12)))
    ax.set_xticks(x[::step])
    ax.set_xticklabels(labels[::step], rotation=35, ha="right")
    ax.grid(True, axis="y")
    ax.spines["top"].set_visible(False)
    ax.spines["right"].set_visible(False)
    ax.legend(loc="upper left", fontsize=7.5, frameon=False)
    max_val = as_float(d["net_revenue"].max()) if not d.empty else 0.0
    if max_val:
        label_step = max(1, int(math.ceil(len(d) / 10)))
        for i, value in enumerate(d["net_revenue"].fillna(0).astype(float).tolist()):
            if i % label_step == 0 or value == max_val:
                ax.text(i, value + (max_val * 0.015), money(value), ha="center", va="bottom", fontsize=6.2, fontweight="bold")
    return save_chart(fig)


def chart_barh_value(df: pd.DataFrame, label_col: str, value_col: str, title: str, formatter, top_n: int = 10) -> BytesIO:
    _mpl_setup()
    if df is None or df.empty or label_col not in df.columns or value_col not in df.columns:
        return BytesIO()
    d = df.head(top_n).copy().iloc[::-1]
    labels = [str(value)[:34] + ("..." if len(str(value)) > 34 else "") for value in d[label_col].astype(str).tolist()]
    values = d[value_col].fillna(0).astype(float).tolist()
    fig, ax = plt.subplots(figsize=(7.35, 3.15))
    y = np.arange(len(labels))
    bars = ax.barh(y, values, color=HEX_GREEN, edgecolor="#047857", linewidth=0.4)
    ax.set_title(title)
    ax.set_yticks(y)
    ax.set_yticklabels(labels, fontsize=7.3)
    ax.grid(True, axis="x")
    ax.spines["top"].set_visible(False)
    ax.spines["right"].set_visible(False)
    ax.spines["left"].set_visible(False)
    max_val = max(values) if values else 0.0
    pad = max_val * 0.012 if max_val else 1.0
    for bar, val in zip(bars, values):
        ax.text(val + pad, bar.get_y() + bar.get_height() / 2, formatter(val), va="center", fontsize=7.2, fontweight="bold")
    if max_val:
        ax.set_xlim(0, max_val * 1.34)
    fig.subplots_adjust(left=0.28, right=0.88)
    return save_chart(fig)


def chart_kpi_comparison_strip(all_metrics: Dict[str, Any]) -> BytesIO:
    _mpl_setup()
    metrics = [
        ("Revenue", "net_revenue", "money"),
        ("Profit", "profit", "money"),
        ("Real Margin", "margin_real", "pct"),
        ("Tickets", "tickets", "int"),
        ("Avg Cart", "basket", "money1"),
        ("Discount", "discount_rate", "pct"),
    ]
    labels: List[str] = []
    current: List[float] = []
    previous: List[float] = []
    for label, key, _kind in metrics:
        prev = all_metrics.get(f"previous_month_{key}", "N/A")
        if prev == "N/A":
            continue
        labels.append(label)
        current.append(as_float(all_metrics.get(key)))
        previous.append(as_float(prev))
    if not labels:
        return BytesIO()
    x = np.arange(len(labels))
    fig, ax = plt.subplots(figsize=(7.35, 2.85))
    width = 0.36
    curr_norm = []
    prev_norm = []
    for cur, prev in zip(current, previous):
        max_val = max(abs(cur), abs(prev), 1.0)
        curr_norm.append(cur / max_val)
        prev_norm.append(prev / max_val)
    ax.bar(x - width / 2, prev_norm, width, label="Previous", color="#CBD5E1", edgecolor="#94A3B8", linewidth=0.4)
    ax.bar(x + width / 2, curr_norm, width, label="Current", color=HEX_GREEN, edgecolor="#047857", linewidth=0.4)
    ax.set_title("Current Month vs Previous Month")
    ax.set_xticks(x)
    ax.set_xticklabels(labels, rotation=0)
    ax.set_ylim(0, max(max(curr_norm), max(prev_norm)) * 1.3)
    ax.set_yticks([])
    ax.legend(loc="upper left", fontsize=7.5, frameon=False)
    for idx, (cur, prev) in enumerate(zip(current, previous)):
        delta = percentage_change(cur, prev)
        label = "N/A" if delta is None else pct1(delta)
        ax.text(idx, max(curr_norm[idx], prev_norm[idx]) + 0.04, label, ha="center", va="bottom", fontsize=7.0, fontweight="bold")
    ax.spines["top"].set_visible(False)
    ax.spines["right"].set_visible(False)
    ax.spines["left"].set_visible(False)
    ax.spines["bottom"].set_color(HEX_BORDER)
    return save_chart(fig)


def chart_store_quadrant(store_scorecards: pd.DataFrame, title: str = "Store Revenue + Margin Quadrant") -> BytesIO:
    _mpl_setup()
    if store_scorecards is None or store_scorecards.empty:
        return BytesIO()
    d = store_scorecards.copy()
    x = d["net_revenue"].fillna(0).astype(float)
    y = d["margin_real"].fillna(0).astype(float)
    sizes = np.clip(d["tickets"].fillna(0).astype(float), 1, None)
    sizes = 90 + (sizes / sizes.max() * 520 if sizes.max() else sizes)
    fig, ax = plt.subplots(figsize=(7.35, 3.25))
    colors_by_status = d["status"].map({"Strong": HEX_GREEN, "Watch": "#F59E0B", "Needs Attention": "#DC2626"}).fillna(HEX_NEUTRAL)
    ax.scatter(x, y, s=sizes, c=colors_by_status, alpha=0.74, edgecolors="#111827", linewidths=0.45)
    ax.axvline(x.mean(), color="#94A3B8", linestyle="--", linewidth=0.8)
    ax.axhline(y.mean(), color="#94A3B8", linestyle="--", linewidth=0.8)
    for _, row in d.iterrows():
        ax.text(row["net_revenue"], row["margin_real"], str(row["store"]), fontsize=7.2, fontweight="bold", ha="center", va="center")
    ax.set_title(title)
    ax.set_xlabel("Net Revenue")
    ax.set_ylabel("Real Margin")
    ax.xaxis.set_major_formatter(FuncFormatter(lambda value, _pos: money(value)))
    ax.yaxis.set_major_formatter(FuncFormatter(lambda value, _pos: pct1(value)))
    ax.grid(True)
    ax.spines["top"].set_visible(False)
    ax.spines["right"].set_visible(False)
    return save_chart(fig)


def chart_margin_discount_risk(df: pd.DataFrame, label_col: str, title: str, top_n: int = 10) -> BytesIO:
    _mpl_setup()
    if df is None or df.empty or label_col not in df.columns or "discount_rate" not in df.columns or "margin_real" not in df.columns:
        return BytesIO()
    d = df.sort_values("net_revenue", ascending=False).head(top_n).copy() if "net_revenue" in df.columns else df.head(top_n).copy()
    fig, ax = plt.subplots(figsize=(7.35, 3.05))
    revenue = d["net_revenue"].fillna(0).astype(float) if "net_revenue" in d.columns else pd.Series([1] * len(d))
    sizes = 80 + (revenue / revenue.max() * 460 if revenue.max() else revenue)
    ax.scatter(d["discount_rate"], d["margin_real"], s=sizes, c=HEX_GREEN, alpha=0.70, edgecolors="#111827", linewidths=0.4)
    ax.axvline(d["discount_rate"].mean(), color="#F59E0B", linestyle="--", linewidth=0.8)
    ax.axhline(d["margin_real"].mean(), color="#94A3B8", linestyle="--", linewidth=0.8)
    for _, row in d.iterrows():
        label = str(row[label_col])[:18]
        ax.text(row["discount_rate"], row["margin_real"], label, fontsize=6.6, ha="center", va="center")
    ax.set_title(title)
    ax.set_xlabel("Discount Rate")
    ax.set_ylabel("Real Margin")
    ax.xaxis.set_major_formatter(FuncFormatter(lambda value, _pos: pct1(value)))
    ax.yaxis.set_major_formatter(FuncFormatter(lambda value, _pos: pct1(value)))
    ax.grid(True)
    ax.spines["top"].set_visible(False)
    ax.spines["right"].set_visible(False)
    return save_chart(fig)


def chart_inventory_movement(inventory_summary: pd.DataFrame, title: str = "Inventory Opening vs Ending by Store") -> BytesIO:
    _mpl_setup()
    if inventory_summary is None or inventory_summary.empty:
        return BytesIO()
    required = {"store", "opening_inventory_value", "ending_inventory_value"}
    if not required.issubset(set(inventory_summary.columns)):
        return BytesIO()
    d = inventory_summary.copy().sort_values("ending_inventory_value", ascending=False)
    x = np.arange(len(d))
    fig, ax = plt.subplots(figsize=(7.35, 3.0))
    width = 0.36
    ax.bar(x - width / 2, d["opening_inventory_value"], width, label="Opening", color="#CBD5E1", edgecolor="#94A3B8", linewidth=0.4)
    ax.bar(x + width / 2, d["ending_inventory_value"], width, label="Ending", color=HEX_GREEN, edgecolor="#047857", linewidth=0.4)
    ax.set_title(title)
    ax.set_xticks(x)
    ax.set_xticklabels(d["store"].astype(str).tolist())
    ax.yaxis.set_major_formatter(FuncFormatter(lambda value, _pos: money(value)))
    ax.grid(True, axis="y")
    ax.legend(loc="upper left", frameon=False, fontsize=7.5)
    max_val = as_float(max(d["opening_inventory_value"].max(), d["ending_inventory_value"].max()))
    for idx, row in d.iterrows():
        pos = list(d.index).index(idx)
        val = as_float(row.get("ending_inventory_value"))
        ax.text(pos + width / 2, val + max_val * 0.02, money(val), ha="center", va="bottom", fontsize=6.4, fontweight="bold")
    ax.spines["top"].set_visible(False)
    ax.spines["right"].set_visible(False)
    return save_chart(fig)


def chart_new_customer_trend(new_customer_daily: pd.DataFrame, title: str = "New Customer Trend") -> BytesIO:
    _mpl_setup()
    if new_customer_daily is None or new_customer_daily.empty:
        return BytesIO()
    d = new_customer_daily.copy()
    d["date"] = pd.to_datetime(d["date"])
    labels = [f"{dt.month}/{dt.day}" for dt in d["date"]]
    x = np.arange(len(d))
    fig, ax = plt.subplots(figsize=(7.35, 2.8))
    max_new = as_float(d["new_customers"].max())
    colors_ = [HEX_GREEN if as_float(v) == max_new and max_new else "#22C55E" for v in d["new_customers"]]
    ax.bar(x, d["new_customers"], color=colors_, edgecolor="#047857", linewidth=0.4)
    ax.set_title(title)
    step = max(1, int(math.ceil(len(labels) / 12)))
    ax.set_xticks(x[::step])
    ax.set_xticklabels(labels[::step], rotation=35, ha="right")
    ax.grid(True, axis="y")
    if max_new:
        for i, value in enumerate(d["new_customers"].fillna(0).astype(float).tolist()):
            if value == max_new or i % max(1, int(math.ceil(len(d) / 8))) == 0:
                ax.text(i, value + max_new * 0.02, f"{int(value):,}", ha="center", va="bottom", fontsize=6.5, fontweight="bold")
    ax.spines["top"].set_visible(False)
    ax.spines["right"].set_visible(False)
    return save_chart(fig)


def chart_category_mix(category_summary: pd.DataFrame, title: str = "Category Revenue Share", top_n: int = 8) -> BytesIO:
    _mpl_setup()
    if category_summary is None or category_summary.empty:
        return BytesIO()
    d = category_summary.sort_values("net_revenue", ascending=False).head(top_n).copy().iloc[::-1]
    labels = d["category"].astype(str).tolist()
    values = d["pct_revenue"].fillna(0).astype(float).tolist()
    fig, ax = plt.subplots(figsize=(7.35, 2.8))
    y = np.arange(len(labels))
    bars = ax.barh(y, values, color=HEX_GREEN, edgecolor="#047857", linewidth=0.4)
    ax.set_title(title)
    ax.set_yticks(y)
    ax.set_yticklabels(labels, fontsize=7.4)
    ax.xaxis.set_major_formatter(FuncFormatter(lambda value, _pos: pct1(value)))
    ax.grid(True, axis="x")
    for bar, value in zip(bars, values):
        ax.text(value + 0.005, bar.get_y() + bar.get_height() / 2, pct1(value), va="center", fontsize=7.0, fontweight="bold")
    max_val = max(values) if values else 0.0
    if max_val:
        ax.set_xlim(0, max_val * 1.22)
    ax.spines["top"].set_visible(False)
    ax.spines["right"].set_visible(False)
    ax.spines["left"].set_visible(False)
    return save_chart(fig)


def chart_product_pareto(product_summary: pd.DataFrame, top_n: int = 20) -> BytesIO:
    _mpl_setup()
    if product_summary is None or product_summary.empty:
        return BytesIO()
    d = product_summary.head(top_n).copy()
    total = float(product_summary["net_revenue"].sum())
    d["cum_share"] = d["net_revenue"].cumsum() / total if total else 0.0
    labels = d["product"].astype(str).str.slice(0, 26).tolist()
    x = np.arange(len(d))
    fig, ax = plt.subplots(figsize=(7.35, 3.25))
    ax.bar(x, d["net_revenue"], color=HEX_GREEN, edgecolor="#047857", linewidth=0.4)
    ax.set_title("Product Pareto - Revenue and Cumulative Share")
    ax.set_xticks(x)
    ax.set_xticklabels(labels, rotation=55, ha="right", fontsize=6.3)
    ax.grid(True, axis="y")
    ax2 = ax.twinx()
    ax2.plot(x, d["cum_share"], color="#111827", marker="o", linewidth=1.3, markersize=2.8)
    ax2.set_ylim(0, 1.05)
    ax2.yaxis.set_major_formatter(FuncFormatter(lambda value, _pos: pct1(value)))
    ax.spines["top"].set_visible(False)
    ax2.spines["top"].set_visible(False)
    return save_chart(fig)


def chart_discount_waterfall(all_metrics: Dict[str, Any]) -> BytesIO:
    _mpl_setup()
    gross = as_float(all_metrics.get("gross_sales"))
    discount_main = as_float(all_metrics.get("discount_main"))
    loyalty = as_float(all_metrics.get("loyalty_discount"))
    net = as_float(all_metrics.get("net_revenue"))
    fig, ax = plt.subplots(figsize=(7.35, 2.7))
    labels = ["Gross Sales", "Main Discounts", "Loyalty", "Net Revenue"]
    values = [gross, -abs(discount_main), -abs(loyalty), net]
    colors_ = [HEX_GREEN, "#F59E0B", "#FBBF24", "#111827"]
    ax.bar(labels, values, color=colors_, edgecolor="#374151", linewidth=0.4)
    ax.axhline(0, color="#111827", linewidth=0.8)
    ax.set_title("Discount Waterfall")
    ax.grid(True, axis="y")
    for i, value in enumerate(values):
        ax.text(i, value, money(value), ha="center", va="bottom" if value >= 0 else "top", fontsize=7.5, fontweight="bold")
    ax.spines["top"].set_visible(False)
    ax.spines["right"].set_visible(False)
    return save_chart(fig)


def chart_hourly_revenue(hourly: pd.DataFrame, title: str) -> BytesIO:
    _mpl_setup()
    if hourly is None or hourly.empty:
        return BytesIO()
    d = hourly.copy()
    labels = [fmt_hour(hour) for hour in d["hour"].astype(int)]
    fig, ax = plt.subplots(figsize=(7.35, 2.8))
    ax.bar(labels, d["net_revenue"], color=HEX_GREEN, edgecolor="#047857", linewidth=0.4)
    ax.set_title(title)
    ax.tick_params(axis="x", labelrotation=45, labelsize=7)
    ax.grid(True, axis="y")
    ax.spines["top"].set_visible(False)
    ax.spines["right"].set_visible(False)
    max_val = as_float(d["net_revenue"].max()) if not d.empty else 0.0
    if max_val:
        for i, value in enumerate(d["net_revenue"].fillna(0).astype(float).tolist()):
            ax.text(i, value + (max_val * 0.02), money(value), ha="center", va="bottom", fontsize=6.4, fontweight="bold")
    return save_chart(fig)


def chart_weekday_avg_revenue(weekday_summary: pd.DataFrame, title: str) -> BytesIO:
    _mpl_setup()
    if weekday_summary is None or weekday_summary.empty:
        return BytesIO()
    d = weekday_summary.sort_values("weekday_num").copy()
    labels = d["weekday_short"].astype(str).tolist()
    values = d["avg_net_revenue"].fillna(0).astype(float).tolist()
    fig, ax = plt.subplots(figsize=(7.35, 2.65))
    bars = ax.bar(labels, values, color=HEX_GREEN, edgecolor="#047857", linewidth=0.4)
    ax.set_title(title)
    ax.grid(True, axis="y")
    ax.spines["top"].set_visible(False)
    ax.spines["right"].set_visible(False)
    max_val = max(values) if values else 0.0
    if max_val:
        ax.set_ylim(0, max_val * 1.24)
    for bar, value in zip(bars, values):
        ax.text(
            bar.get_x() + bar.get_width() / 2,
            value + (max_val * 0.025 if max_val else 1),
            money(value),
            ha="center",
            va="bottom",
            fontsize=7.1,
            fontweight="bold",
        )
    return save_chart(fig)


def chart_weekday_hour_heatmap(weekday_hour_summary: pd.DataFrame, title: str) -> BytesIO:
    _mpl_setup()
    if weekday_hour_summary is None or weekday_hour_summary.empty:
        return BytesIO()
    d = weekday_hour_summary.copy()
    active_hours = sorted(d.loc[d["avg_tickets"] > 0, "hour"].astype(int).unique().tolist())
    if not active_hours:
        return BytesIO()
    pivot = (
        d.pivot_table(index="weekday_num", columns="hour", values="avg_tickets", aggfunc="sum", fill_value=0.0)
        .reindex(index=list(range(7)), columns=active_hours, fill_value=0.0)
    )
    values = pivot.values.astype(float)
    fig_width = max(7.35, min(10.0, 3.2 + 0.34 * len(active_hours)))
    fig, ax = plt.subplots(figsize=(fig_width, 3.25))
    im = ax.imshow(values, aspect="auto", cmap="YlGn")
    ax.set_title(title)
    ax.set_yticks(np.arange(7))
    ax.set_yticklabels(WEEKDAY_SHORT)
    ax.set_xticks(np.arange(len(active_hours)))
    ax.set_xticklabels([fmt_hour(h) for h in active_hours], rotation=45, ha="right", fontsize=7)
    max_val = float(np.nanmax(values)) if values.size else 0.0
    for row_idx in range(values.shape[0]):
        for col_idx in range(values.shape[1]):
            value = values[row_idx, col_idx]
            if value <= 0:
                continue
            txt_color = "white" if max_val and value > max_val * 0.58 else "#111827"
            ax.text(col_idx, row_idx, f"{value:.0f}", ha="center", va="center", fontsize=6.6, fontweight="bold", color=txt_color)
    cbar = fig.colorbar(im, ax=ax, fraction=0.025, pad=0.02)
    cbar.ax.set_ylabel("Avg Tickets", rotation=270, labelpad=10)
    return save_chart(fig)


def fmt_hour(hour: int) -> str:
    h = int(hour)
    if h == 0:
        return "12a"
    if h < 12:
        return f"{h}a"
    if h == 12:
        return "12p"
    return f"{h - 12}p"


def chart_to_image(buf: BytesIO, width: float = 7.25 * inch, height: float = 2.8 * inch) -> Optional[Image]:
    try:
        if not buf or len(buf.getvalue()) == 0:
            return None
        return Image(BytesIO(buf.getvalue()), width=width, height=height)
    except Exception:
        return None


def chart_or_spacer(buf: BytesIO, width: float = 7.25 * inch, height: float = 2.8 * inch) -> Any:
    return chart_to_image(buf, width=width, height=height) or Spacer(1, height)


def monthly_footer(month_key: str, date_range_text: str, generated_at: str):
    def _footer(canvas, doc):
        canvas.saveState()
        canvas.setFont("Helvetica", 7)
        canvas.setFillColor(colors.HexColor("#6B7280"))
        left = f"Buzz Monthly Owner Review | {month_key} | {date_range_text}"
        canvas.drawString(PAGE_MARGINS["left"], 0.25 * inch, left)
        canvas.drawRightString(letter[0] - PAGE_MARGINS["right"], 0.25 * inch, f"Generated {generated_at} | Page {doc.page}")
        canvas.restoreState()

    return _footer


def add_section(story: List[Any], styles: Dict[str, Any], title: str) -> None:
    story.append(Spacer(1, 0.08 * inch))
    story.append(Paragraph(title, styles["Section"]))


def monthly_kpi_cell(styles: Dict[str, Any], label: str, value: str, detail: str = "") -> List[Paragraph]:
    return [
        Paragraph(label, styles["KpiLabel"]),
        Paragraph(value, styles["KpiValue"]),
        Paragraph(detail or "&nbsp;", styles["KpiDelta"]),
    ]


def build_table(headers: List[Any], rows: List[List[Any]], col_widths: Optional[List[float]] = None) -> Table:
    data = [headers] + rows
    table = Table(data, colWidths=col_widths, repeatRows=1)
    table.setStyle(TableStyle([
        ("BACKGROUND", (0, 0), (-1, 0), BUZZ["black"]),
        ("TEXTCOLOR", (0, 0), (-1, 0), BUZZ["yellow"]),
        ("FONTNAME", (0, 0), (-1, 0), "Helvetica-Bold"),
        ("FONTNAME", (0, 1), (-1, -1), "Helvetica"),
        ("FONTSIZE", (0, 0), (-1, 0), 7.6),
        ("FONTSIZE", (0, 1), (-1, -1), 7.4),
        ("LEADING", (0, 0), (-1, -1), 8.8),
        ("LINEBELOW", (0, 0), (-1, 0), 0.7, BUZZ["green"]),
        ("GRID", (0, 0), (-1, -1), 0.25, BUZZ["border"]),
        ("ROWBACKGROUNDS", (0, 1), (-1, -1), [colors.white, BUZZ["row_alt"]]),
        ("LEFTPADDING", (0, 0), (-1, -1), 4.5),
        ("RIGHTPADDING", (0, 0), (-1, -1), 4.5),
        ("TOPPADDING", (0, 0), (-1, -1), 3.2),
        ("BOTTOMPADDING", (0, 0), (-1, -1), 3.2),
        ("VALIGN", (0, 0), (-1, -1), "MIDDLE"),
    ]))
    return table


def p_text(styles: Dict[str, Any], text: Any, style_name: str = "Small") -> Paragraph:
    safe = str(text if text is not None else "")
    return Paragraph(safe.replace("&", "&amp;").replace("<", "&lt;").replace(">", "&gt;"), styles[style_name])


def df_rows(
    df: pd.DataFrame,
    columns: List[Tuple[str, str, str]],
    max_rows: int,
    styles: Dict[str, Any],
) -> Tuple[List[Any], List[List[Any]]]:
    headers = [label for _, label, _ in columns]
    rows: List[List[Any]] = []
    if df is None or df.empty:
        return headers, [["No data available"] + [""] * (len(headers) - 1)]
    for _, row in df.head(max_rows).iterrows():
        rendered: List[Any] = []
        for col, _, kind in columns:
            value = row.get(col, "")
            if kind == "money":
                rendered.append(money(as_float(value)))
            elif kind == "money_optional":
                rendered.append(optional_money(value))
            elif kind == "signed_money":
                rendered.append(optional_signed_money(value))
            elif kind == "money2":
                rendered.append(money2(as_float(value)))
            elif kind == "money1":
                rendered.append(money1(value))
            elif kind == "pct":
                rendered.append("N/A" if pd.isna(value) else pct1(as_float(value)))
            elif kind == "pct_optional":
                rendered.append(optional_pct(value))
            elif kind == "pp":
                rendered.append(pp1(as_float(value)))
            elif kind == "int":
                rendered.append(f"{int(round(as_float(value))):,}")
            elif kind == "float1":
                rendered.append(f"{as_float(value):,.1f}")
            elif kind == "float2":
                rendered.append(f"{as_float(value):,.2f}")
            elif kind == "date":
                try:
                    rendered.append(pd.to_datetime(value).date().isoformat())
                except Exception:
                    rendered.append(str(value))
            elif kind == "textwrap":
                rendered.append(p_text(styles, value, "Tiny"))
            else:
                rendered.append(str(value))
        rows.append(rendered)
    return headers, rows


def add_df_table(
    story: List[Any],
    styles: Dict[str, Any],
    df: pd.DataFrame,
    columns: List[Tuple[str, str, str]],
    max_rows: int,
    col_widths: Optional[List[float]] = None,
) -> None:
    headers, rows = df_rows(df, columns, max_rows, styles)
    story.append(build_table(headers, rows, col_widths))


def build_long_table(
    title: str,
    df: pd.DataFrame,
    columns: List[Tuple[str, str, str]],
    col_widths: List[float],
    styles: Dict[str, Any],
    row_limit: Optional[int] = None,
    repeat_header: bool = True,
    continuation_label: bool = True,
) -> Tuple[List[Any], int, int]:
    source = df.copy() if df is not None else pd.DataFrame()
    expected_rows = len(source)
    rendered_df = source.head(row_limit).copy() if row_limit is not None else source
    rendered_rows = len(rendered_df)
    headers, rows = df_rows(rendered_df, columns, max(1, rendered_rows), styles)
    if source.empty:
        rendered_rows = 0
    flowables: List[Any] = [
        CondPageBreak(1.45 * inch),
        Paragraph(escape_html(title), styles["Section"]),
    ]
    if row_limit is None:
        note = f"Rows shown: {rendered_rows} of {expected_rows}"
    else:
        note = f"Rows shown: {rendered_rows} of {expected_rows}. Full detail in CSV/XLSX data book."
    if continuation_label and rendered_rows > 18:
        note += " Headers repeat if the table continues on the next page."
    flowables.append(Paragraph(escape_html(note), styles["Tiny"]))
    flowables.append(Spacer(1, 0.04 * inch))

    table = LongTable([headers] + rows, colWidths=col_widths, repeatRows=1 if repeat_header else 0, splitByRow=1)
    table.setStyle(TableStyle([
        ("BACKGROUND", (0, 0), (-1, 0), BUZZ["black"]),
        ("TEXTCOLOR", (0, 0), (-1, 0), BUZZ["yellow"]),
        ("FONTNAME", (0, 0), (-1, 0), "Helvetica-Bold"),
        ("FONTNAME", (0, 1), (-1, -1), "Helvetica"),
        ("FONTSIZE", (0, 0), (-1, 0), 6.4),
        ("FONTSIZE", (0, 1), (-1, -1), 6.7),
        ("LEADING", (0, 0), (-1, -1), 7.7),
        ("GRID", (0, 0), (-1, -1), 0.25, BUZZ["border"]),
        ("ROWBACKGROUNDS", (0, 1), (-1, -1), [colors.white, BUZZ["row_alt"]]),
        ("LEFTPADDING", (0, 0), (-1, -1), 3.2),
        ("RIGHTPADDING", (0, 0), (-1, -1), 3.2),
        ("TOPPADDING", (0, 0), (-1, -1), 2.2),
        ("BOTTOMPADDING", (0, 0), (-1, -1), 2.2),
        ("VALIGN", (0, 0), (-1, -1), "TOP"),
    ]))
    flowables.append(table)
    return flowables, expected_rows, rendered_rows


def append_long_table(
    story: List[Any],
    title: str,
    df: pd.DataFrame,
    columns: List[Tuple[str, str, str]],
    col_widths: List[float],
    styles: Dict[str, Any],
    row_limit: Optional[int] = None,
) -> Tuple[int, int]:
    flowables, expected_rows, rendered_rows = build_long_table(
        title=title,
        df=df,
        columns=columns,
        col_widths=col_widths,
        styles=styles,
        row_limit=row_limit,
    )
    story.extend(flowables)
    story.append(Spacer(1, 0.08 * inch))
    return expected_rows, rendered_rows


def date_range_days(start_day: date, end_day: date) -> List[date]:
    return [start_day + timedelta(days=i) for i in range((end_day - start_day).days + 1)]


def complete_daily_detail_df(daily: pd.DataFrame, start_day: date, end_day: date, store: Optional[str] = None) -> pd.DataFrame:
    full = pd.DataFrame({"date": date_range_days(start_day, end_day)})
    if daily is None or daily.empty:
        out = full
    else:
        tmp = daily.copy()
        tmp["date"] = pd.to_datetime(tmp["date"], errors="coerce").dt.date
        tmp = tmp[tmp["date"].notna()].copy()
        out = full.merge(tmp, on="date", how="left")
    for col in [
        "net_revenue",
        "profit",
        "profit_real",
        "tickets",
        "basket",
        "discount",
        "discount_rate",
        "margin_real",
        "margin",
    ]:
        if col not in out.columns:
            out[col] = 0.0
        out[col] = pd.to_numeric(out[col], errors="coerce").fillna(0.0)
    out["date_label"] = out["date"].apply(lambda value: value.isoformat() if isinstance(value, date) else str(value))
    if store:
        out.insert(0, "store", store)
    return out


def complete_new_customer_daily_df(daily: pd.DataFrame, start_day: date, end_day: date, store: Optional[str] = None) -> pd.DataFrame:
    full = pd.DataFrame({"date": date_range_days(start_day, end_day)})
    if daily is None or daily.empty:
        return pd.DataFrame()
    tmp = daily.copy()
    tmp["date"] = pd.to_datetime(tmp["date"], errors="coerce").dt.date
    tmp = tmp[tmp["date"].notna()].copy()
    if tmp.empty:
        return pd.DataFrame()
    out = full.merge(tmp, on="date", how="left")
    for col in ["new_customers", "total_customers", "new_customer_rate"]:
        if col not in out.columns:
            out[col] = 0.0
        out[col] = pd.to_numeric(out[col], errors="coerce").fillna(0.0)
    if "source_file" in out.columns:
        out["source_file"] = out["source_file"].fillna("")
    out["date_label"] = out["date"].apply(lambda value: value.isoformat() if isinstance(value, date) else str(value))
    if store:
        out.insert(0, "store", store)
    return out


def build_store_daily_detail_export(bundles: Dict[str, StoreBundle], start_day: date, end_day: date) -> pd.DataFrame:
    frames = [
        complete_daily_detail_df(bundle.daily_df, start_day, end_day, store=abbr)
        for abbr, bundle in bundles.items()
    ]
    return pd.concat(frames, ignore_index=True) if frames else pd.DataFrame()


def compute_kickback_detail_by_store(df: pd.DataFrame) -> pd.DataFrame:
    columns = [
        "store",
        "store_label",
        "brand",
        "rule",
        "discount_rule",
        "discount_rule_display",
        "rule_count",
        "net_revenue",
        "generated_profit",
        "profit_real",
        "kickback",
        "profit",
        "margin_real",
        "margin",
        "margin_lift",
        "deal_signal",
    ]
    if df is None or df.empty or "_deal_kickback_amt" not in df.columns:
        return pd.DataFrame(columns=columns)
    tmp = df.copy()
    tmp["_kickback"] = to_number(tmp["_deal_kickback_amt"]).fillna(0).astype(float)
    tmp = tmp[tmp["_kickback"] > 0].copy()
    if tmp.empty:
        return pd.DataFrame(columns=columns)
    net_col = find_col(tmp, COLUMN_CANDIDATES["net_sales"])
    profit_col = find_col(tmp, COLUMN_CANDIDATES["profit"])
    cogs_col = find_col(tmp, COLUMN_CANDIDATES["cogs"])
    tmp["_net"] = to_number(tmp[net_col]).fillna(0).astype(float) if net_col else 0.0
    tmp["_profit_real"] = to_number(tmp[profit_col]).fillna(0).astype(float) if profit_col else (
        tmp["_net"] - (to_number(tmp[cogs_col]).fillna(0).astype(float) if cogs_col else 0.0)
    )
    tmp["_profit"] = tmp["_profit_real"] + tmp["_kickback"]
    tmp["_store"] = tmp.get("_store", "ALL").astype(str)
    tmp["_store_label"] = tmp.get("_store_label", tmp["_store"]).astype(str)
    tmp["_deal_brand"] = tmp.get("_deal_brand", "Unknown")
    tmp["_deal_rule"] = tmp.get("_deal_rule", "Unknown")
    tmp["_deal_discount"] = tmp.get("_deal_discount", 0.0)
    out = tmp.groupby(["_store", "_store_label", "_deal_brand"], as_index=False).agg(
        net_revenue=("_net", "sum"),
        profit_real=("_profit_real", "sum"),
        kickback=("_kickback", "sum"),
        profit=("_profit", "sum"),
        rule=("_deal_rule", combine_unique_text),
        rule_count=("_deal_rule", lambda s: len({str(v).strip() for v in s if str(v).strip() and str(v).lower() != "nan"})),
        discount_rule=("_deal_discount", "mean"),
        discount_rule_display=("_deal_discount", combine_unique_money2),
    ).rename(columns={
        "_store": "store",
        "_store_label": "store_label",
        "_deal_brand": "brand",
    })
    out["generated_profit"] = out["profit"]
    out["margin_real"] = out["profit_real"] / out["net_revenue"].replace({0: np.nan})
    out["margin"] = out["profit"] / out["net_revenue"].replace({0: np.nan})
    out["margin_lift"] = out["margin"] - out["margin_real"]
    out = add_deal_performance_signal(out.fillna(0.0))
    return out.sort_values(["store", "generated_profit"], ascending=[True, False])


def add_full_daily_detail_section(
    story: List[Any],
    styles: Dict[str, Any],
    daily: pd.DataFrame,
    start_day: date,
    end_day: date,
    title: str,
    store: Optional[str] = None,
) -> Dict[str, int]:
    detail = complete_daily_detail_df(daily, start_day, end_day, store=store)
    expected_days = len(date_range_days(start_day, end_day))
    story.append(PageBreak())
    columns = [
        ("date_label", "Date", "date"),
        ("net_revenue", "Net Revenue", "money"),
        ("profit", "Profit", "money"),
        ("profit_real", "Real Profit", "money"),
        ("tickets", "Tickets", "int"),
        ("basket", "Avg Cart", "money1"),
        ("discount", "Discounts", "money"),
        ("discount_rate", "Disc Rate", "pct"),
        ("margin_real", "Real Margin", "pct"),
        ("margin", "KB Margin", "pct"),
    ]
    widths = [0.72 * inch, 0.78 * inch, 0.72 * inch, 0.72 * inch, 0.50 * inch, 0.55 * inch, 0.72 * inch, 0.60 * inch, 0.62 * inch, 0.62 * inch]
    append_long_table(story, title, detail, columns, widths, styles, row_limit=None)
    story.append(Paragraph(f"Days shown: {len(detail)} of {expected_days}", styles["Tiny"]))
    return {"expected": expected_days, "rendered": len(detail)}


def add_full_new_customer_daily_section(
    story: List[Any],
    styles: Dict[str, Any],
    daily: pd.DataFrame,
    start_day: date,
    end_day: date,
    title: str,
    store: Optional[str] = None,
    authoritative_total: Optional[float] = None,
    authoritative_customers: Optional[float] = None,
) -> Dict[str, int]:
    detail = complete_new_customer_daily_df(daily, start_day, end_day, store=store)
    expected_days = len(date_range_days(start_day, end_day)) if detail is not None and not detail.empty else 0
    story.append(PageBreak())
    if detail is None or detail.empty:
        add_section_title(story, styles, title, "New customer daily detail was not available for this period.")
        return {"expected": 0, "rendered": 0}
    columns = [("date_label", "Date", "date"), ("new_customers", "New Customers", "int")]
    widths = [1.0 * inch, 1.15 * inch]
    if "total_customers" in detail.columns and as_float(detail["total_customers"].sum()) > 0:
        columns.extend([("total_customers", "Total Customers", "int"), ("new_customer_rate", "New Cust Rate", "pct")])
        widths.extend([1.15 * inch, 1.0 * inch])
    else:
        story.append(Paragraph("Total customer counts were unavailable or zero in the source data.", styles["Tiny"]))
    if "source_file" in detail.columns and detail["source_file"].astype(str).str.strip().any():
        columns.append(("source_file", "Source", "textwrap"))
        widths.append(2.7 * inch)
    append_long_table(story, title, detail, columns, widths, styles, row_limit=None)
    total_new_customers = int(round(as_float(detail["new_customers"].sum()))) if "new_customers" in detail.columns else 0
    total_customers = int(round(as_float(detail["total_customers"].sum()))) if "total_customers" in detail.columns else 0
    story.append(Paragraph(f"Days shown: {len(detail)} of {expected_days}", styles["Tiny"]))
    if authoritative_total is not None and not is_missing_display(authoritative_total):
        official_total = int(round(as_float(authoritative_total)))
        story.append(Paragraph(f"<b>Total new customers this month: {official_total:,}</b>", styles["Small"]))
        if authoritative_customers is not None and not is_missing_display(authoritative_customers):
            official_customers = int(round(as_float(authoritative_customers)))
            official_rate = official_total / official_customers if official_customers else 0.0
            story.append(Paragraph(f"<b>Total customers this month: {official_customers:,} | New customer rate: {pct1(official_rate)}</b>", styles["Small"]))
        if abs(official_total - total_new_customers) >= 1:
            diff = official_total - total_new_customers
            sign = "+" if diff > 0 else ""
            story.append(Paragraph(
                f"Daily row sum: {total_new_customers:,} new customers / {total_customers:,} total customers | Full-window closing report difference: {sign}{diff:,}",
                styles["Tiny"],
            ))
    else:
        story.append(Paragraph(f"<b>Total new customers this month: {total_new_customers:,}</b>", styles["Small"]))
        if total_customers:
            story.append(Paragraph(f"<b>Total customers this month: {total_customers:,} | New customer rate: {pct1(total_new_customers / total_customers)}</b>", styles["Small"]))
    return {"expected": expected_days, "rendered": len(detail)}


def kickback_detail_with_lift(df: pd.DataFrame, include_store: bool = False) -> pd.DataFrame:
    if df is None or df.empty:
        return pd.DataFrame()
    out = df.copy()
    if "margin_lift" not in out.columns:
        out["margin_lift"] = pd.to_numeric(out.get("margin"), errors="coerce").fillna(0.0) - pd.to_numeric(out.get("margin_real"), errors="coerce").fillna(0.0)
    if include_store and "store" not in out.columns:
        out["store"] = "ALL"
    return out


def add_full_kickback_detail_section(
    story: List[Any],
    styles: Dict[str, Any],
    kickback_detail: pd.DataFrame,
    title: str,
    include_store: bool = False,
) -> Dict[str, int]:
    detail = kickback_detail_with_lift(kickback_detail, include_store=include_store)
    story.append(PageBreak())
    total_kickback = as_float(detail["kickback"].sum()) if detail is not None and not detail.empty and "kickback" in detail.columns else 0.0
    profit_before = as_float(detail["profit_real"].sum()) if detail is not None and not detail.empty and "profit_real" in detail.columns else 0.0
    profit_after = as_float(detail["profit"].sum()) if detail is not None and not detail.empty and "profit" in detail.columns else 0.0
    margin_lift = (profit_after / as_float(detail["net_revenue"].sum()) if detail is not None and not detail.empty and as_float(detail["net_revenue"].sum()) else 0.0) - (
        profit_before / as_float(detail["net_revenue"].sum()) if detail is not None and not detail.empty and as_float(detail["net_revenue"].sum()) else 0.0
    )
    summary = [
        monthly_kpi_cell(styles, "Total Kickback", money(total_kickback), ""),
        monthly_kpi_cell(styles, "Profit Before KB", money(profit_before), ""),
        monthly_kpi_cell(styles, "Profit After KB", money(profit_after), ""),
        monthly_kpi_cell(styles, "Margin Lift", pp1(margin_lift), ""),
        monthly_kpi_cell(styles, "Kickback Rows", f"{len(detail):,}" if detail is not None else "0", ""),
    ]
    add_section_title(story, styles, title, "Full Detail")
    story.append(build_kpi_grid(styles, summary, cols=5))
    story.append(Spacer(1, 0.05 * inch))
    columns: List[Tuple[str, str, str]] = []
    widths: List[float] = []
    if include_store:
        columns.append(("store", "Store", "text"))
        widths.append(0.42 * inch)
    elif "stores" in detail.columns:
        columns.append(("stores", "Stores", "textwrap"))
        widths.append(0.58 * inch)
    columns.extend([
        ("brand", "Brand", "textwrap"),
        ("rule", "Rules Combined", "textwrap"),
        ("net_revenue", "Revenue", "money"),
        ("generated_profit", "Gen Profit", "money"),
        ("profit_real", "Real Profit", "money"),
        ("kickback", "Kickback", "money"),
        ("margin_real", "Real M", "pct"),
        ("margin", "KB M", "pct"),
        ("deal_signal", "Signal", "textwrap"),
    ])
    widths.extend([1.0 * inch, 1.1 * inch, 0.78 * inch, 0.78 * inch, 0.78 * inch, 0.65 * inch, 0.48 * inch, 0.48 * inch, 0.8 * inch])
    append_long_table(story, f"{title} Rows", detail, columns, widths, styles, row_limit=None)
    story.append(Paragraph(f"Kickback rows shown: {len(detail)} of {len(detail)}", styles["Tiny"]))
    return {"expected": len(detail), "rendered": len(detail)}


def add_detail_appendix_index(story: List[Any], styles: Dict[str, Any], include_new_customer: bool = True) -> None:
    rows = [
        ["Appendix A", "Full Daily Detail"],
        ["Appendix B", "Full New Customer Detail" if include_new_customer else "New Customer Detail: unavailable"],
        ["Appendix C", "Combined Kickback / Deal Brand Detail"],
        ["Appendix D", "Category Detail"],
        ["Appendix E", "Brand Detail"],
        ["Appendix F", "Budtender Detail"],
        ["Appendix G", "Product Detail"],
    ]
    story.append(PageBreak())
    add_section_title(story, styles, "Detail Appendix Index", "Complete operational detail follows the visual executive pages.")
    story.append(build_table(["Section", "Contents"], rows, [1.2 * inch, 4.8 * inch]))


def add_report_sections_card(story: List[Any], styles: Dict[str, Any], is_store: bool = True) -> None:
    sections = [
        "1. Scorecard",
        "2. Store Comparisons" if is_store else "2. What Changed This Month",
        "3. Revenue Pattern",
        "4. Store Mix" if is_store else "4. Category / Mix",
        "5. Store Health" if is_store else "5. Discount / Inventory / Staff Health",
        "6. Appendix A: Daily Detail",
        "7. Appendix B: New Customers",
        "8. Appendix C: Combined Deal Brands",
        "9. Appendix D-G: Detail Tables",
    ]
    rows = [[Paragraph(escape_html(item), styles["Small"])] for item in sections]
    table = Table([[Paragraph("<b>Report Sections</b>", styles["InsightTitle"])]] + rows, colWidths=[7.25 * inch])
    table.setStyle(TableStyle([
        ("BACKGROUND", (0, 0), (-1, -1), colors.HexColor("#FAFAFA")),
        ("BOX", (0, 0), (-1, -1), 0.45, BUZZ["border"]),
        ("LINEBEFORE", (0, 0), (-1, -1), 2.2, BUZZ["green"]),
        ("LEFTPADDING", (0, 0), (-1, -1), 7),
        ("RIGHTPADDING", (0, 0), (-1, -1), 7),
        ("TOPPADDING", (0, 0), (-1, -1), 3.2),
        ("BOTTOMPADDING", (0, 0), (-1, -1), 3.0),
    ]))
    story.append(table)
    story.append(Spacer(1, 0.06 * inch))


def add_appendix_table_section(
    story: List[Any],
    styles: Dict[str, Any],
    title: str,
    df: pd.DataFrame,
    columns: List[Tuple[str, str, str]],
    col_widths: List[float],
    appendix_top_n: int,
    full_if_rows_leq: Optional[int] = None,
) -> Dict[str, int]:
    expected = len(df) if df is not None else 0
    if full_if_rows_leq is not None and expected <= full_if_rows_leq:
        row_limit = None
        display_title = f"{title} - Full Detail"
    else:
        row_limit = appendix_top_n if appendix_top_n > 0 else 0
        display_title = f"{title} - Top {row_limit} (Full Detail in Data Book)"
    story.append(PageBreak())
    _, rendered = append_long_table(story, display_title, df, columns, col_widths, styles, row_limit=row_limit)
    return {"expected": expected, "rendered": rendered}


def build_calendar_heatmap_table(daily: pd.DataFrame, start_day: date, end_day: date, styles: Dict[str, Any]) -> Table:
    cal = calendar.Calendar(firstweekday=6)
    days = list(cal.itermonthdates(start_day.year, start_day.month))
    if end_day.month != start_day.month or end_day.year != start_day.year:
        days = [start_day + timedelta(days=i) for i in range((end_day - start_day).days + 1)]
        while len(days) % 7:
            days.append(days[-1] + timedelta(days=1))

    revenue_by_day = {}
    if daily is not None and not daily.empty:
        revenue_by_day = {
            pd.to_datetime(row["date"]).date(): as_float(row.get("net_revenue"))
            for _, row in daily.iterrows()
        }
    values = [v for d, v in revenue_by_day.items() if start_day <= d <= end_day and v > 0]
    q1, q2, q3 = (np.quantile(values, [0.25, 0.50, 0.75]) if values else [0, 0, 0])
    palette = [
        colors.HexColor("#FFFFFF"),
        colors.HexColor("#D1FAE5"),
        colors.HexColor("#A7F3D0"),
        colors.HexColor("#34D399"),
        colors.HexColor("#047857"),
    ]

    headers = ["Sun", "Mon", "Tue", "Wed", "Thu", "Fri", "Sat"]
    cells: List[List[Any]] = [headers]
    style_cmds = [
        ("BACKGROUND", (0, 0), (-1, 0), BUZZ["black"]),
        ("TEXTCOLOR", (0, 0), (-1, 0), BUZZ["yellow"]),
        ("FONTNAME", (0, 0), (-1, 0), "Helvetica-Bold"),
        ("GRID", (0, 0), (-1, -1), 0.35, BUZZ["border"]),
        ("VALIGN", (0, 0), (-1, -1), "TOP"),
        ("ALIGN", (0, 0), (-1, 0), "CENTER"),
        ("FONTSIZE", (0, 0), (-1, -1), 7.6),
        ("LEFTPADDING", (0, 0), (-1, -1), 3),
        ("RIGHTPADDING", (0, 0), (-1, -1), 3),
        ("TOPPADDING", (0, 0), (-1, -1), 4),
        ("BOTTOMPADDING", (0, 0), (-1, -1), 4),
    ]

    for week_idx in range(0, len(days), 7):
        row = []
        for day_idx, day in enumerate(days[week_idx:week_idx + 7]):
            in_range = start_day <= day <= end_day
            value = revenue_by_day.get(day, 0.0) if in_range else 0.0
            if not in_range:
                text = ""
                bg = BUZZ["soft_gray"]
            else:
                text = f"<b>{day.day}</b><br/>{money(value)}"
                if value <= 0:
                    bg = palette[0]
                elif value <= q1:
                    bg = palette[1]
                elif value <= q2:
                    bg = palette[2]
                elif value <= q3:
                    bg = palette[3]
                else:
                    bg = palette[4]
            col = day_idx
            pdf_row = 1 + (week_idx // 7)
            style_cmds.append(("BACKGROUND", (col, pdf_row), (col, pdf_row), bg))
            if bg == palette[4]:
                style_cmds.append(("TEXTCOLOR", (col, pdf_row), (col, pdf_row), colors.white))
            row.append(Paragraph(text or "&nbsp;", styles["Tiny"]))
        cells.append(row)

    table = Table(cells, colWidths=[7.25 * inch / 7] * 7, rowHeights=[0.28 * inch] + [0.55 * inch] * (len(cells) - 1))
    table.setStyle(TableStyle(style_cmds))
    return table


def build_new_customer_calendar_table(daily: pd.DataFrame, start_day: date, end_day: date, styles: Dict[str, Any]) -> Table:
    cal = calendar.Calendar(firstweekday=6)
    days = list(cal.itermonthdates(start_day.year, start_day.month))
    if end_day.month != start_day.month or end_day.year != start_day.year:
        days = [start_day + timedelta(days=i) for i in range((end_day - start_day).days + 1)]
        while len(days) % 7:
            days.append(days[-1] + timedelta(days=1))

    gained_by_day: Dict[date, float] = {}
    total_by_day: Dict[date, float] = {}
    if daily is not None and not daily.empty:
        for _, row in daily.iterrows():
            try:
                day = pd.to_datetime(row["date"]).date()
            except Exception:
                continue
            gained_by_day[day] = gained_by_day.get(day, 0.0) + as_float(row.get("new_customers"))
            total_by_day[day] = total_by_day.get(day, 0.0) + as_float(row.get("total_customers"))

    values = [v for d, v in gained_by_day.items() if start_day <= d <= end_day and v > 0]
    q1, q2, q3 = (np.quantile(values, [0.25, 0.50, 0.75]) if values else [0, 0, 0])
    palette = [
        colors.HexColor("#FFFFFF"),
        colors.HexColor("#E0F2FE"),
        colors.HexColor("#BAE6FD"),
        colors.HexColor("#7DD3FC"),
        colors.HexColor("#0284C7"),
    ]

    headers = ["Sun", "Mon", "Tue", "Wed", "Thu", "Fri", "Sat"]
    cells: List[List[Any]] = [headers]
    style_cmds = [
        ("BACKGROUND", (0, 0), (-1, 0), BUZZ["black"]),
        ("TEXTCOLOR", (0, 0), (-1, 0), BUZZ["yellow"]),
        ("FONTNAME", (0, 0), (-1, 0), "Helvetica-Bold"),
        ("GRID", (0, 0), (-1, -1), 0.35, BUZZ["border"]),
        ("VALIGN", (0, 0), (-1, -1), "TOP"),
        ("ALIGN", (0, 0), (-1, 0), "CENTER"),
        ("FONTSIZE", (0, 0), (-1, -1), 7.6),
        ("LEFTPADDING", (0, 0), (-1, -1), 3),
        ("RIGHTPADDING", (0, 0), (-1, -1), 3),
        ("TOPPADDING", (0, 0), (-1, -1), 4),
        ("BOTTOMPADDING", (0, 0), (-1, -1), 4),
    ]

    for week_idx in range(0, len(days), 7):
        row = []
        for day_idx, day in enumerate(days[week_idx:week_idx + 7]):
            in_range = start_day <= day <= end_day
            gained = gained_by_day.get(day, 0.0) if in_range else 0.0
            total = total_by_day.get(day, 0.0) if in_range else 0.0
            if not in_range:
                text = ""
                bg = BUZZ["soft_gray"]
            else:
                rate = gained / total if total else 0.0
                if total:
                    text = f"<b>{day.day}</b><br/>{int(round(gained)):,} new<br/><font size='6'>{pct1(rate)}</font>"
                else:
                    text = f"<b>{day.day}</b><br/>{int(round(gained)):,} new"
                if gained <= 0:
                    bg = palette[0]
                elif gained <= q1:
                    bg = palette[1]
                elif gained <= q2:
                    bg = palette[2]
                elif gained <= q3:
                    bg = palette[3]
                else:
                    bg = palette[4]
            col = day_idx
            pdf_row = 1 + (week_idx // 7)
            style_cmds.append(("BACKGROUND", (col, pdf_row), (col, pdf_row), bg))
            if bg == palette[4]:
                style_cmds.append(("TEXTCOLOR", (col, pdf_row), (col, pdf_row), colors.white))
            row.append(Paragraph(text or "&nbsp;", styles["Tiny"]))
        cells.append(row)

    table = Table(cells, colWidths=[7.25 * inch / 7] * 7, rowHeights=[0.28 * inch] + [0.64 * inch] * (len(cells) - 1))
    table.setStyle(TableStyle(style_cmds))
    return table


def build_cover_page(
    story: List[Any],
    styles: Dict[str, Any],
    scope_title: str,
    month_key: str,
    start_day: date,
    end_day: date,
    generated_at: str,
    stores_included: List[str],
) -> None:
    story.append(Spacer(1, 0.65 * inch))
    story.append(Paragraph("BUZZ CANNABIS", styles["TitleBig"]))
    story.append(Spacer(1, 0.05 * inch))
    story.append(Paragraph("Monthly Owner Review", ParagraphStyle(
        "MonthlyCoverTitle",
        parent=styles["TitleBig"],
        fontSize=24,
        leading=27,
        textColor=BUZZ["black"],
        spaceAfter=8,
    )))
    story.append(Paragraph(scope_title, ParagraphStyle(
        "MonthlyCoverScope",
        parent=styles["TitleBig"],
        fontSize=17,
        leading=20,
        textColor=BUZZ["green"],
        spaceAfter=15,
    )))
    rows = [
        ["Month", month_key],
        ["Date Range", f"{start_day.isoformat()} to {end_day.isoformat()}"],
        ["Generated", generated_at],
        ["Stores Included", ", ".join(stores_included)],
    ]
    story.append(build_table(["Field", "Value"], rows, [1.45 * inch, 5.8 * inch]))
    story.append(Spacer(1, 0.2 * inch))
    story.append(Table([[""]], colWidths=[7.25 * inch], rowHeights=[0.08 * inch], style=TableStyle([
        ("BACKGROUND", (0, 0), (-1, -1), BUZZ["yellow"]),
    ])))
    story.append(Spacer(1, 0.04 * inch))
    story.append(Table([[""]], colWidths=[7.25 * inch], rowHeights=[0.08 * inch], style=TableStyle([
        ("BACKGROUND", (0, 0), (-1, -1), BUZZ["green"]),
    ])))
    story.append(PageBreak())


def action_items_table(action_items: List[Dict[str, str]]) -> Table:
    rows = [[item["severity"], item["issue"], item["metric_value"], item["recommended_action"]] for item in action_items]
    return build_table(["Severity", "Issue", "Metric", "Recommended Action"], rows, [0.75 * inch, 1.8 * inch, 1.15 * inch, 3.55 * inch])


def add_insights(story: List[Any], styles: Dict[str, Any], insights: List[str]) -> None:
    if not insights:
        return
    rows = [[p_text(styles, text, "Small")] for text in insights]
    table = Table(rows, colWidths=[7.25 * inch])
    table.setStyle(TableStyle([
        ("BACKGROUND", (0, 0), (-1, -1), BUZZ["soft"]),
        ("BOX", (0, 0), (-1, -1), 0.45, BUZZ["border"]),
        ("LEFTPADDING", (0, 0), (-1, -1), 6),
        ("RIGHTPADDING", (0, 0), (-1, -1), 6),
        ("TOPPADDING", (0, 0), (-1, -1), 4),
        ("BOTTOMPADDING", (0, 0), (-1, -1), 4),
    ]))
    story.append(table)
    story.append(Spacer(1, 0.06 * inch))


def add_chart(story: List[Any], buf: BytesIO, height: float = 2.8 * inch) -> None:
    img = chart_to_image(buf, height=height)
    if img:
        story.append(img)
        story.append(Spacer(1, 0.06 * inch))


def escape_html(value: Any) -> str:
    safe = str(value if value is not None else "")
    return safe.replace("&", "&amp;").replace("<", "&lt;").replace(">", "&gt;")


def is_missing_display(value: Any) -> bool:
    if value is None:
        return True
    try:
        if pd.isna(value):
            return True
    except (TypeError, ValueError):
        pass
    text = str(value).strip()
    return text == "" or text.upper() in {"N/A", "NA", "NAN", "NONE", "NULL"}


def display_or_dash(value: Any, dash: str = "-") -> str:
    return dash if is_missing_display(value) else str(value)


def prepare_monthly_styles(styles: Dict[str, Any]) -> None:
    additions = [
        ParagraphStyle(
            name="DashboardTitle",
            parent=styles["TitleBig"],
            fontSize=18,
            leading=21,
            textColor=BUZZ["black"],
            spaceAfter=2,
        ),
        ParagraphStyle(
            name="DashboardSubtitle",
            parent=styles["Tiny"],
            fontSize=8.2,
            leading=9.6,
            textColor=BUZZ["muted2"],
            spaceAfter=4,
        ),
        ParagraphStyle(
            name="CardLabel",
            parent=styles["KpiLabel"],
            fontSize=7.9,
            leading=9,
            textColor=BUZZ["muted"],
        ),
        ParagraphStyle(
            name="CardValue",
            parent=styles["KpiValue"],
            fontSize=13.2,
            leading=14.4,
            textColor=BUZZ["black"],
        ),
        ParagraphStyle(
            name="CardDelta",
            parent=styles["KpiDelta"],
            fontSize=7.5,
            leading=8.6,
            textColor=BUZZ["muted2"],
        ),
        ParagraphStyle(
            name="InsightTitle",
            parent=styles["KpiLabel"],
            fontSize=8.5,
            leading=9.8,
            textColor=BUZZ["black"],
        ),
    ]
    for style in additions:
        if style.name not in styles:
            styles.add(style)


def first_available_metric(metrics: Dict[str, Any], keys: List[str]) -> Any:
    for key in keys:
        value = metrics.get(key)
        if value is None or value == "N/A":
            continue
        if isinstance(value, float) and (pd.isna(value) or not math.isfinite(value)):
            continue
        return value
    return "N/A"


def format_metric_value(value: Any, kind: str) -> str:
    if value == "N/A" or value is None:
        return "N/A"
    try:
        if kind == "money":
            return money(value)
        if kind == "signed_money":
            return optional_signed_money(value)
        if kind == "money2":
            return money2(value)
        if kind == "money1":
            return money1(value)
        if kind == "int":
            return f"{int(round(as_float(value))):,}"
        if kind == "pct":
            return pct1(value)
        if kind == "pp":
            return pp1(value)
        if kind == "float1":
            return f"{as_float(value):,.1f}"
    except Exception:
        return "N/A"
    return str(value)


def metric_direction(metric_name: str) -> str:
    down_good = {"discount_rate", "returns_net", "returns_tickets", "low_value_cart_share"}
    neutral = {"inventory_value_change", "inventory_unit_change", "inventory_value_change_pct"}
    if metric_name in neutral:
        return "neutral"
    return "down_good" if metric_name in down_good else "up_good"


def delta_color_class(delta: Optional[float], direction: str) -> Tuple[str, str]:
    if delta is None or abs(delta) < 0.005:
        return HEX_NEUTRAL, "Flat"
    if direction == "neutral":
        return HEX_NEUTRAL, "Flat"
    good = delta > 0 if direction == "up_good" else delta < 0
    return (HEX_GOOD, "Up" if delta > 0 else "Down") if good else (HEX_BAD, "Up" if delta > 0 else "Down")


def build_delta_value(current: Any, comparison: Any, kind: str = "money", direction: str = "up_good") -> Dict[str, str]:
    if current == "N/A" or comparison == "N/A" or current is None or comparison is None:
        return {"text": "N/A", "color": HEX_NEUTRAL, "status": "N/A"}
    cur = as_float(current)
    base = as_float(comparison)
    if not math.isfinite(cur) or not math.isfinite(base):
        return {"text": "N/A", "color": HEX_NEUTRAL, "status": "N/A"}
    if kind == "pct":
        delta = cur - base
        text = pp1(delta)
    else:
        delta_pct = percentage_change(cur, base)
        if delta_pct is None:
            text = f"{format_metric_value(cur - base, 'signed_money' if kind == 'money' else kind)}"
        else:
            text = pct1(delta_pct)
        delta = cur - base
    color, status = delta_color_class(delta, direction)
    if not text.startswith("-") and text != "N/A" and not text.startswith("+"):
        text = f"+{text}"
    return {"text": text, "color": color, "status": status}


def build_metric_status(metric_name: str, current: Any, comparison: Any, direction: Optional[str] = None) -> str:
    return build_delta_value(current, comparison, "pct" if "margin" in metric_name or "rate" in metric_name else "money", direction or metric_direction(metric_name))["status"]


def comparison_line(label: str, current: Any, comparison: Any, kind: str, metric_name: str) -> str:
    delta = build_delta_value(current, comparison, kind, metric_direction(metric_name))
    return f"{escape_html(label)} <font color=\"{delta['color']}\">{escape_html(delta['text'])}</font>"


def format_store_delta(row: Dict[str, Any], prefix: str, metric: str, kind: str) -> str:
    comparison = row.get(f"{prefix}_{metric}", "N/A")
    if comparison == "N/A" or comparison is None:
        return "N/A"
    return build_delta_value(row.get(metric), comparison, kind, metric_direction(metric))["text"]


def build_metric_card(
    styles: Dict[str, Any],
    label: str,
    value: str,
    lines: List[str],
    status: str = "N/A",
    width: float = 1.74 * inch,
) -> Table:
    status_color = {
        "Up": HEX_GOOD,
        "Down": HEX_BAD,
        "Flat": HEX_NEUTRAL,
        "N/A": HEX_NEUTRAL,
    }.get(status, HEX_NEUTRAL)
    data = [
        [Paragraph(escape_html(label), styles["CardLabel"])],
        [Paragraph(escape_html(value), styles["CardValue"])],
        [Paragraph("<br/>".join(lines or ["Comparison unavailable"]), styles["CardDelta"])],
    ]
    if not is_missing_display(status):
        data.append([Paragraph(f"<font color=\"{status_color}\">{escape_html(status)}</font>", styles["CardDelta"])])
    table = Table(data, colWidths=[width])
    table.setStyle(TableStyle([
        ("BACKGROUND", (0, 0), (-1, -1), colors.HexColor("#F9FAFB")),
        ("BOX", (0, 0), (-1, -1), 0.45, BUZZ["border"]),
        ("LINEABOVE", (0, 0), (-1, 0), 2.0, BUZZ["green"]),
        ("LEFTPADDING", (0, 0), (-1, -1), 6),
        ("RIGHTPADDING", (0, 0), (-1, -1), 6),
        ("TOPPADDING", (0, 0), (-1, -1), 3.2),
        ("BOTTOMPADDING", (0, 0), (-1, -1), 2.8),
        ("VALIGN", (0, 0), (-1, -1), "TOP"),
    ]))
    return table


def build_scorecard_grid(styles: Dict[str, Any], cards: List[Table], cols: int = 4) -> Table:
    width = 7.25 * inch / cols
    rows = []
    for idx in range(0, len(cards), cols):
        row = cards[idx:idx + cols]
        while len(row) < cols:
            row.append(Spacer(1, 0.01 * inch))
        rows.append(row)
    table = Table(rows, colWidths=[width] * cols, hAlign="LEFT")
    table.setStyle(TableStyle([
        ("LEFTPADDING", (0, 0), (-1, -1), 3),
        ("RIGHTPADDING", (0, 0), (-1, -1), 3),
        ("TOPPADDING", (0, 0), (-1, -1), 3),
        ("BOTTOMPADDING", (0, 0), (-1, -1), 3),
        ("VALIGN", (0, 0), (-1, -1), "TOP"),
    ]))
    return table


def build_status_badge(styles: Dict[str, Any], status: str) -> Table:
    palette = {
        "Strong": (colors.HexColor("#D1FAE5"), "#065F46"),
        "Watch": (colors.HexColor("#FEF3C7"), "#92400E"),
        "Needs Attention": (colors.HexColor("#FEE2E2"), "#991B1B"),
    }
    bg, fg = palette.get(status, (BUZZ["soft_gray"], HEX_MUTED))
    table = Table([[Paragraph(f"<font color=\"{fg}\"><b>{escape_html(status)}</b></font>", styles["Tiny"])]])
    table.setStyle(TableStyle([
        ("BACKGROUND", (0, 0), (-1, -1), bg),
        ("BOX", (0, 0), (-1, -1), 0.35, bg),
        ("LEFTPADDING", (0, 0), (-1, -1), 5),
        ("RIGHTPADDING", (0, 0), (-1, -1), 5),
        ("TOPPADDING", (0, 0), (-1, -1), 2),
        ("BOTTOMPADDING", (0, 0), (-1, -1), 2),
    ]))
    return table


def build_insight_cards(
    story: List[Any],
    styles: Dict[str, Any],
    title: str,
    insights: List[str],
    max_items: int = 3,
    color: colors.Color = BUZZ["green"],
) -> None:
    if not insights:
        insights = ["No threshold exceptions detected."]
    rows = [[Paragraph(f"<b>{escape_html(title)}</b>", styles["InsightTitle"])]]
    for text in insights[:max_items]:
        rows.append([Paragraph(escape_html(text), styles["Small"])])
    table = Table(rows, colWidths=[7.25 * inch])
    table.setStyle(TableStyle([
        ("BACKGROUND", (0, 0), (-1, -1), colors.HexColor("#FAFAFA")),
        ("BOX", (0, 0), (-1, -1), 0.45, BUZZ["border"]),
        ("LINEBEFORE", (0, 0), (-1, -1), 2.4, color),
        ("LEFTPADDING", (0, 0), (-1, -1), 8),
        ("RIGHTPADDING", (0, 0), (-1, -1), 7),
        ("TOPPADDING", (0, 0), (-1, -1), 4),
        ("BOTTOMPADDING", (0, 0), (-1, -1), 3.5),
    ]))
    story.append(table)
    story.append(Spacer(1, 0.05 * inch))


def build_two_column_layout(left: List[Any], right: List[Any], widths: Optional[List[float]] = None) -> Table:
    widths = widths or [3.58 * inch, 3.58 * inch]
    table = Table([[left, right]], colWidths=widths)
    table.setStyle(TableStyle([
        ("VALIGN", (0, 0), (-1, -1), "TOP"),
        ("LEFTPADDING", (0, 0), (-1, -1), 0),
        ("RIGHTPADDING", (0, 0), (-1, -1), 5),
        ("TOPPADDING", (0, 0), (-1, -1), 0),
        ("BOTTOMPADDING", (0, 0), (-1, -1), 0),
    ]))
    return table


def add_compact_header(
    story: List[Any],
    styles: Dict[str, Any],
    scope_title: str,
    month_key: str,
    start_day: date,
    end_day: date,
    generated_at: str,
) -> None:
    month_label = datetime.strptime(month_key, "%Y-%m").strftime("%B %Y") if re.match(r"^\d{4}-\d{2}$", month_key) else month_key
    header = Table(
        [[
            Paragraph("<b>Buzz Cannabis Monthly Review</b>", styles["Tiny"]),
            Paragraph(f"<b>{escape_html(scope_title)}</b>", styles["Tiny"]),
            Paragraph(f"{escape_html(month_label)}<br/>{start_day.strftime('%b %-d')} - {end_day.strftime('%b %-d')}", styles["Tiny"]),
        ]],
        colWidths=[2.55 * inch, 2.45 * inch, 2.25 * inch],
    )
    header.setStyle(TableStyle([
        ("BACKGROUND", (0, 0), (-1, -1), colors.white),
        ("LINEBELOW", (0, 0), (-1, -1), 1.0, BUZZ["black"]),
        ("LINEABOVE", (0, 0), (-1, -1), 2.0, BUZZ["green"]),
        ("ALIGN", (1, 0), (1, 0), "CENTER"),
        ("ALIGN", (2, 0), (2, 0), "RIGHT"),
        ("LEFTPADDING", (0, 0), (-1, -1), 0),
        ("RIGHTPADDING", (0, 0), (-1, -1), 0),
        ("TOPPADDING", (0, 0), (-1, -1), 4),
        ("BOTTOMPADDING", (0, 0), (-1, -1), 5),
        ("VALIGN", (0, 0), (-1, -1), "MIDDLE"),
    ]))
    story.append(header)
    story.append(Spacer(1, 0.08 * inch))


def add_section_title(story: List[Any], styles: Dict[str, Any], title: str, subtitle: str = "") -> None:
    story.append(Paragraph(escape_html(title), styles["DashboardTitle"]))
    if subtitle:
        story.append(Paragraph(escape_html(subtitle), styles["DashboardSubtitle"]))


def add_page_break_if_needed(story: List[Any], height: float = 1.4 * inch) -> None:
    story.append(CondPageBreak(height))


def build_executive_metric_cards(all_metrics: Dict[str, Any], styles: Dict[str, Any], store_context: Optional[Dict[str, Any]] = None) -> List[Table]:
    current_new_customers = first_available_metric(all_metrics, ["closing_new_customers", "new_customers"])
    definitions = [
        ("Net Revenue", "net_revenue", "money"),
        ("Profit", "profit", "money"),
        ("Real Profit", "profit_real", "money"),
        ("Real Margin", "margin_real", "pct"),
        ("Tickets", "tickets", "int"),
        ("Avg Cart >$1", "basket", "money1"),
        ("Customers", "closing_customers", "int"),
        ("New Customers", "closing_new_customers", "int"),
        ("New Cust %", "closing_new_customer_rate", "pct"),
        ("Ending Inventory", "inventory_end_value", "money"),
        ("Discount Rate", "discount_rate", "pct"),
        ("OTD Round-Up Loss", "tax_rounding_loss", "money"),
        ("Kickback Amount", "kickback", "money"),
        ("Inventory Gain/Loss", "inventory_value_change", "signed_money"),
    ]
    cards: List[Table] = []
    for label, key, kind in definitions:
        current = current_new_customers if key == "closing_new_customers" else all_metrics.get(key, "N/A")
        value = format_metric_value(current, kind)
        prev = all_metrics.get(f"previous_month_{key}", "N/A")
        yoy = all_metrics.get(f"same_month_previous_year_{key}", "N/A")
        trailing = all_metrics.get(f"trailing_3mo_avg_{key}", "N/A")
        lines = []
        for label_text, comparison in [("MoM", prev), ("YoY", yoy), ("3-mo avg", trailing)]:
            if comparison != "N/A" and comparison is not None:
                lines.append(comparison_line(label_text, current, comparison, kind if kind != "signed_money" else "money", key))
            if len(lines) >= 1:
                break
        if store_context and key in store_context:
            lines.append(escape_html(store_context[key]))
        if not lines:
            lines = ["Comparison unavailable"]
        status = build_metric_status(key, current, prev, metric_direction(key)) if prev != "N/A" else "N/A"
        cards.append(build_metric_card(styles, label, value, lines, status=status))
    return cards


def build_comparison_panel(styles: Dict[str, Any], rows: List[Tuple[str, Any, Any, str, str]], comparison_header: str = "Previous") -> Table:
    data = [[
        Paragraph(f"<font color=\"{HEX_YELLOW}\"><b>Metric</b></font>", styles["Tiny"]),
        Paragraph(f"<font color=\"{HEX_YELLOW}\"><b>Current</b></font>", styles["Tiny"]),
        Paragraph(f"<font color=\"{HEX_YELLOW}\"><b>{escape_html(comparison_header)}</b></font>", styles["Tiny"]),
        Paragraph(f"<font color=\"{HEX_YELLOW}\"><b>Change</b></font>", styles["Tiny"]),
    ]]
    for label, current, previous, kind, key in rows:
        delta = build_delta_value(current, previous, kind, metric_direction(key))
        data.append([
            Paragraph(escape_html(label), styles["Small"]),
            Paragraph(escape_html(format_metric_value(current, kind)), styles["Small"]),
            Paragraph(escape_html(format_metric_value(previous, kind)), styles["Small"]),
            Paragraph(f"<font color=\"{delta['color']}\"><b>{escape_html(delta['text'])}</b></font>", styles["Small"]),
        ])
    table = Table(data, colWidths=[1.85 * inch, 1.55 * inch, 1.55 * inch, 1.35 * inch])
    table.setStyle(TableStyle([
        ("BACKGROUND", (0, 0), (-1, 0), BUZZ["black"]),
        ("TEXTCOLOR", (0, 0), (-1, 0), BUZZ["yellow"]),
        ("GRID", (0, 0), (-1, -1), 0.25, BUZZ["border"]),
        ("ROWBACKGROUNDS", (0, 1), (-1, -1), [colors.white, BUZZ["row_alt"]]),
        ("LEFTPADDING", (0, 0), (-1, -1), 5),
        ("RIGHTPADDING", (0, 0), (-1, -1), 5),
        ("TOPPADDING", (0, 0), (-1, -1), 3.5),
        ("BOTTOMPADDING", (0, 0), (-1, -1), 3.5),
    ]))
    return table


def build_store_company_comparison_panel(
    styles: Dict[str, Any],
    rows: List[Dict[str, Any]],
) -> Table:
    data = [[
        Paragraph(f"<font color=\"{HEX_YELLOW}\"><b>Metric</b></font>", styles["Tiny"]),
        Paragraph(f"<font color=\"{HEX_YELLOW}\"><b>Store</b></font>", styles["Tiny"]),
        Paragraph(f"<font color=\"{HEX_YELLOW}\"><b>Company Avg</b></font>", styles["Tiny"]),
        Paragraph(f"<font color=\"{HEX_YELLOW}\"><b>Rank</b></font>", styles["Tiny"]),
        Paragraph(f"<font color=\"{HEX_YELLOW}\"><b>Signal</b></font>", styles["Tiny"]),
    ]]
    for row in rows:
        data.append([
            Paragraph(escape_html(display_or_dash(row.get("metric", ""))), styles["Small"]),
            Paragraph(escape_html(display_or_dash(row.get("store"))), styles["Small"]),
            Paragraph(escape_html(display_or_dash(row.get("company"))), styles["Small"]),
            Paragraph(escape_html(display_or_dash(row.get("rank"))), styles["Tiny"]),
            Paragraph(escape_html(display_or_dash(row.get("status"))), styles["Tiny"]),
        ])
    table = Table(data, colWidths=[1.35 * inch, 1.25 * inch, 1.35 * inch, 1.25 * inch, 2.05 * inch])
    table.setStyle(TableStyle([
        ("BACKGROUND", (0, 0), (-1, 0), BUZZ["black"]),
        ("TEXTCOLOR", (0, 0), (-1, 0), BUZZ["yellow"]),
        ("GRID", (0, 0), (-1, -1), 0.25, BUZZ["border"]),
        ("ROWBACKGROUNDS", (0, 1), (-1, -1), [colors.white, BUZZ["row_alt"]]),
        ("LEFTPADDING", (0, 0), (-1, -1), 5),
        ("RIGHTPADDING", (0, 0), (-1, -1), 5),
        ("TOPPADDING", (0, 0), (-1, -1), 3.5),
        ("BOTTOMPADDING", (0, 0), (-1, -1), 3.5),
        ("VALIGN", (0, 0), (-1, -1), "MIDDLE"),
    ]))
    return table


def company_average_signal(metric: str, current: Any, company_average: Any) -> str:
    if is_missing_display(current) or is_missing_display(company_average):
        return ""
    current_value = as_float(current)
    average_value = as_float(company_average)
    if average_value == 0 and current_value == 0:
        return "Near avg"
    if average_value == 0:
        return "Above avg" if current_value else ""

    diff = current_value - average_value
    if metric in {"margin_real", "margin", "discount_rate"}:
        if abs(diff) < 0.01:
            return "Near avg"
        if metric == "discount_rate":
            return "Below avg (good)" if diff < 0 else "Above avg - review"
        return "Above avg" if diff > 0 else "Below avg - review"

    pct_diff = diff / abs(average_value)
    if abs(pct_diff) < 0.05:
        return "Near avg"
    return "Above avg" if pct_diff > 0 else "Below avg - review"


def rank_stores(store_scorecards: pd.DataFrame, metric: str, ascending: bool = False) -> Dict[str, int]:
    if store_scorecards is None or store_scorecards.empty or metric not in store_scorecards.columns:
        return {}
    ranked = store_scorecards.sort_values(metric, ascending=ascending).reset_index(drop=True)
    return {str(row["store"]): idx + 1 for idx, row in ranked.iterrows()}


def build_store_rankings(store_scorecards: pd.DataFrame, inventory_summary: pd.DataFrame) -> pd.DataFrame:
    if store_scorecards is None or store_scorecards.empty:
        return pd.DataFrame()
    out = store_scorecards.copy()
    store_count = len(out)
    out["revenue_rank"] = out["store"].map(rank_stores(out, "net_revenue", ascending=False))
    out["profit_rank"] = out["store"].map(rank_stores(out, "profit_real", ascending=False))
    out["margin_rank"] = out["store"].map(rank_stores(out, "margin_real", ascending=False))
    out["discount_rank"] = out["store"].map(rank_stores(out, "discount_rate", ascending=True))
    out["tickets_rank"] = out["store"].map(rank_stores(out, "tickets", ascending=False))
    out["basket_rank"] = out["store"].map(rank_stores(out, "basket", ascending=False))
    out["store_count"] = store_count
    if inventory_summary is not None and not inventory_summary.empty:
        inv_cols = [
            col for col in [
                "store",
                "opening_inventory_value",
                "ending_inventory_value",
                "inventory_value_change",
                "inventory_value_change_pct",
                "inventory_turns_est",
                "inventory_to_revenue",
                "sku_change",
            ]
            if col in inventory_summary.columns
        ]
        out = out.merge(inventory_summary[inv_cols], on="store", how="left")
    return out


def store_rank_context(store_rankings: pd.DataFrame, store_abbr: str, all_metrics: Dict[str, Any]) -> Dict[str, Any]:
    if store_rankings is None or store_rankings.empty:
        return {}
    rows = store_rankings[store_rankings["store"].astype(str).str.upper() == store_abbr.upper()]
    if rows.empty:
        return {}
    row = rows.iloc[0].to_dict()
    count = int(as_float(row.get("store_count"), len(store_rankings)))
    def _rank(value: Any) -> str:
        rank = int(as_float(value))
        return f"#{rank} of {count}" if rank else ""
    return {
        "net_revenue": _rank(row.get("revenue_rank")),
        "profit": _rank(row.get("profit_rank")),
        "profit_real": _rank(row.get("profit_rank")),
        "margin_real": _rank(row.get("margin_rank")),
        "discount_rate": _rank(row.get("discount_rank")),
        "basket": _rank(row.get("basket_rank")),
        "tickets": _rank(row.get("tickets_rank")),
    }


def generate_monthly_wins(
    all_metrics: Dict[str, Any],
    store_scorecards: pd.DataFrame,
    category_summary: pd.DataFrame,
    brand_summary: pd.DataFrame,
    new_customer_daily: pd.DataFrame,
    inventory_summary: pd.DataFrame,
) -> List[str]:
    wins: List[str] = []
    prev_net = all_metrics.get("previous_month_net_revenue")
    if prev_net != "N/A":
        delta = percentage_change(as_float(all_metrics.get("net_revenue")), as_float(prev_net))
        if delta is not None and delta > 0:
            wins.append(f"Revenue increased {pct1(delta)} vs previous month to {money(all_metrics.get('net_revenue'))}.")
    if store_scorecards is not None and not store_scorecards.empty:
        strong = store_scorecards[store_scorecards["status"] == "Strong"]
        if not strong.empty:
            wins.append(f"{len(strong)} stores finished in Strong status, led by {strong.sort_values('net_revenue', ascending=False).iloc[0]['store']}.")
    if category_summary is not None and not category_summary.empty:
        top = category_summary.iloc[0]
        wins.append(f"{top['category']} led category revenue at {money(top['net_revenue'])} with {pct1(top.get('margin_real'))} real margin.")
    if new_customer_daily is not None and not new_customer_daily.empty:
        best = new_customer_daily.sort_values("new_customers", ascending=False).iloc[0]
        wins.append(f"Best new-customer day was {pd.to_datetime(best['date']).date().isoformat()} with {int(as_float(best['new_customers'])):,} new customers.")
    if inventory_summary is not None and not inventory_summary.empty and "inventory_turns_est" in inventory_summary.columns:
        active_turns = inventory_summary.replace([np.inf, -np.inf], np.nan).dropna(subset=["inventory_turns_est"])
        if not active_turns.empty:
            row = active_turns.sort_values("inventory_turns_est", ascending=False).iloc[0]
            wins.append(f"{row['store']} had the strongest estimated inventory turns at {as_float(row['inventory_turns_est']):.2f}.")
    return wins[:5]


def generate_monthly_concerns(
    all_metrics: Dict[str, Any],
    store_scorecards: pd.DataFrame,
    category_summary: pd.DataFrame,
    brand_summary: pd.DataFrame,
    cart_distribution: pd.DataFrame,
    inventory_summary: pd.DataFrame,
) -> List[str]:
    concerns: List[str] = []
    prev_discount = all_metrics.get("previous_month_discount_rate")
    if prev_discount != "N/A":
        delta = as_float(all_metrics.get("discount_rate")) - as_float(prev_discount)
        if delta > 0.01:
            concerns.append(f"Discount rate rose {pp1(delta)} vs previous month to {pct1(all_metrics.get('discount_rate'))}.")
    if as_float(all_metrics.get("discount_rate")) > 0.20:
        concerns.append(f"Discount rate is elevated at {pct1(all_metrics.get('discount_rate'))}, with total discounts of {money(all_metrics.get('discount'))}.")
    if store_scorecards is not None and not store_scorecards.empty:
        needs = store_scorecards[store_scorecards["status"] == "Needs Attention"]
        if not needs.empty:
            concerns.append(f"{len(needs)} stores need attention: {', '.join(needs['store'].astype(str).head(4))}.")
    if cart_distribution is not None and not cart_distribution.empty:
        low_share = as_float(cart_distribution[cart_distribution["bucket"].isin(["$0-$1", "$1-$10", "$10-$20"])]["pct"].sum())
        if low_share > 0.25:
            concerns.append(f"Carts under $20 were {pct1(low_share)} of carts.")
    if inventory_summary is not None and not inventory_summary.empty and "inventory_to_revenue" in inventory_summary.columns:
        tmp = inventory_summary.replace([np.inf, -np.inf], np.nan).dropna(subset=["inventory_to_revenue"])
        if not tmp.empty:
            row = tmp.sort_values("inventory_to_revenue", ascending=False).iloc[0]
            if as_float(row.get("inventory_to_revenue")) > 0.75:
                concerns.append(f"{row['store']} ended with inventory at {pct1(row['inventory_to_revenue'])} of monthly revenue.")
    return concerns[:5]


def generate_monthly_action_items(action_items: List[Dict[str, str]]) -> List[str]:
    return [
        f"{item.get('issue')}: {item.get('recommended_action')}"
        for item in action_items[:5]
    ]


def generate_store_comparison_insights(
    row: Dict[str, Any],
    all_metrics: Dict[str, Any],
    rank_context: Dict[str, Any],
) -> List[str]:
    insights = []
    if row:
        rank_text = rank_context.get("net_revenue") or "not ranked"
        insights.append(f"Revenue share was {pct1(row.get('revenue_share'))}; revenue rank {rank_text}.")
        margin_gap = as_float(row.get("margin_real")) - as_float(all_metrics.get("margin_real"))
        insights.append(f"Real margin was {pp1(margin_gap)} vs company average.")
        disc_gap = as_float(row.get("discount_rate")) - as_float(all_metrics.get("discount_rate"))
        insights.append(f"Discount rate was {pp1(disc_gap)} vs company average.")
    return insights[:4]


def generate_inventory_health_insights(inventory_summary: pd.DataFrame, all_metrics: Dict[str, Any]) -> List[str]:
    if inventory_summary is None or inventory_summary.empty:
        return ["Inventory snapshots were not available; run with --fetch-inventory-api."]
    insights = [
        f"Ending inventory was {optional_money(all_metrics.get('inventory_end_value'))}, a change of {optional_signed_money(all_metrics.get('inventory_value_change'))} for the month."
    ]
    if "inventory_value_change" in inventory_summary.columns:
        row = inventory_summary.reindex(inventory_summary["inventory_value_change"].abs().sort_values(ascending=False).index).iloc[0]
        insights.append(f"{row['store']} had the largest inventory movement at {optional_signed_money(row.get('inventory_value_change'))}.")
    if "inventory_to_revenue" in inventory_summary.columns:
        tmp = inventory_summary.replace([np.inf, -np.inf], np.nan).dropna(subset=["inventory_to_revenue"])
        if not tmp.empty:
            high = tmp.sort_values("inventory_to_revenue", ascending=False).iloc[0]
            insights.append(f"{high['store']} had the highest inventory-to-revenue ratio at {pct1(high['inventory_to_revenue'])}.")
    return insights[:4]


def generate_discount_margin_insights(all_metrics: Dict[str, Any], store_scorecards: pd.DataFrame, category_summary: pd.DataFrame, brand_summary: pd.DataFrame) -> List[str]:
    insights = [
        f"Total discounts were {money(all_metrics.get('discount'))}, equal to {pct1(all_metrics.get('discount_rate'))} of gross sales."
    ]
    if as_float(all_metrics.get("tax_rounding_loss")) > 0:
        insights.append(
            f"Whole-dollar OTD rounding opportunity is an estimated {money(all_metrics.get('tax_rounding_loss'))} in tax-backed revenue across {int(as_float(all_metrics.get('tax_rounding_transactions'))):,} transactions."
        )
    if store_scorecards is not None and not store_scorecards.empty:
        row = store_scorecards.sort_values("discount_rate", ascending=False).iloc[0]
        insights.append(f"{row['store']} had the highest discount rate at {pct1(row['discount_rate'])}.")
    if category_summary is not None and not category_summary.empty:
        row = category_summary.sort_values("discount_rate", ascending=False).iloc[0]
        insights.append(f"{row['category']} had the highest category discount rate at {pct1(row['discount_rate'])}.")
    if brand_summary is not None and not brand_summary.empty:
        risky = brand_summary[(brand_summary["net_revenue"] > as_float(brand_summary["net_revenue"].sum()) * 0.02)].sort_values(["discount_rate", "margin_real"], ascending=[False, True])
        if not risky.empty:
            row = risky.iloc[0]
            insights.append(f"{row['brand']} is the top brand discount/margin risk at {pct1(row['discount_rate'])} discount and {pct1(row['margin_real'])} real margin.")
    return insights[:4]


def generate_customer_growth_insights(new_customer_summary: pd.DataFrame, new_customer_daily: pd.DataFrame, all_metrics: Dict[str, Any]) -> List[str]:
    if new_customer_summary is None or new_customer_summary.empty:
        if all_metrics.get("closing_summary_has_data"):
            return [
                f"Closing reports show {int(as_float(all_metrics.get('closing_customers'))):,} customers and {int(as_float(all_metrics.get('closing_new_customers'))):,} new customers ({pct1(all_metrics.get('closing_new_customer_rate'))})."
            ]
        return ["New-customer data was not available; run with --fetch-closing-summary-api or --fetch-new-customers-api."]
    insights = []
    if all_metrics.get("closing_summary_has_data"):
        insights.append(
            f"Closing reports show {int(as_float(all_metrics.get('closing_customers'))):,} customers and {int(as_float(all_metrics.get('closing_new_customers'))):,} new customers ({pct1(all_metrics.get('closing_new_customer_rate'))})."
        )
    total_new = as_float(new_customer_summary["new_customers"].sum())
    insights.append(f"Daily customer-growth rows total {int(total_new):,} new customers.")
    top = new_customer_summary.sort_values("new_customers", ascending=False).iloc[0]
    insights.append(f"{top['store']} led new-customer growth with {int(as_float(top['new_customers'])):,}.")
    if new_customer_daily is not None and not new_customer_daily.empty:
        best = new_customer_daily.sort_values("new_customers", ascending=False).iloc[0]
        insights.append(f"Best day was {pd.to_datetime(best['date']).date().isoformat()} with {int(as_float(best['new_customers'])):,} new customers.")
    return insights[:4]


def generate_mix_shift_insights(category_summary: pd.DataFrame, brand_summary: pd.DataFrame, product_summary: pd.DataFrame, all_metrics: Dict[str, Any]) -> List[str]:
    insights = []
    if category_summary is not None and not category_summary.empty:
        row = category_summary.iloc[0]
        insights.append(f"{row['category']} was the top category at {pct1(row.get('pct_revenue'))} of revenue.")
    if brand_summary is not None and not brand_summary.empty:
        top5_share = as_float(brand_summary.head(5)["net_revenue"].sum()) / as_float(brand_summary["net_revenue"].sum()) if as_float(brand_summary["net_revenue"].sum()) else 0.0
        insights.append(f"Top 5 brands represented {pct1(top5_share)} of revenue.")
    if product_summary is not None and not product_summary.empty:
        top10_share = as_float(product_summary.head(10)["net_revenue"].sum()) / as_float(product_summary["net_revenue"].sum()) if as_float(product_summary["net_revenue"].sum()) else 0.0
        insights.append(f"Top 10 products represented {pct1(top10_share)} of revenue.")
    return insights[:4]


def generate_staff_coaching_insights(budtender_summary: pd.DataFrame) -> List[str]:
    if budtender_summary is None or budtender_summary.empty:
        return ["Budtender data was not available in the export."]
    insights = []
    revenue = budtender_summary.sort_values("net_revenue", ascending=False).iloc[0]
    insights.append(f"Strong revenue performance: top budtender revenue was {money(revenue['net_revenue'])}.")
    basket = budtender_summary[budtender_summary["tickets"] >= 25].sort_values("basket", ascending=False) if "tickets" in budtender_summary.columns else pd.DataFrame()
    if not basket.empty:
        row = basket.iloc[0]
        insights.append(f"Strong basket performance: {money1(row['basket'])} average basket across {int(as_float(row['tickets'])):,} tickets.")
    discount = budtender_summary[budtender_summary["tickets"] >= 25].sort_values("discount_rate", ascending=False) if "discount_rate" in budtender_summary.columns else pd.DataFrame()
    if not discount.empty:
        row = discount.iloc[0]
        insights.append(f"Discount review: highest high-volume discount rate was {pct1(row['discount_rate'])}.")
    return insights[:4]


def build_all_stores_pdf(
    out_pdf: Path,
    month_key: str,
    start_day: date,
    end_day: date,
    generated_at: str,
    bundles: Dict[str, StoreBundle],
    all_metrics: Dict[str, Any],
    all_daily: pd.DataFrame,
    store_scorecards: pd.DataFrame,
    category_summary: pd.DataFrame,
    brand_summary: pd.DataFrame,
    product_summary: pd.DataFrame,
    budtender_summary: pd.DataFrame,
    cart_distribution: pd.DataFrame,
    hourly_summary: pd.DataFrame,
    weekday_summary: pd.DataFrame,
    weekday_hour_summary: pd.DataFrame,
    brand_store_matrix: pd.DataFrame,
    category_store_matrix: pd.DataFrame,
    new_customer_summary: pd.DataFrame,
    new_customer_daily: pd.DataFrame,
    closing_summary: pd.DataFrame,
    inventory_summary: pd.DataFrame,
    kickback_summary: pd.DataFrame,
    store_kickback_summary: pd.DataFrame,
    action_items: List[Dict[str, str]],
    warnings: List[Dict[str, Any]],
    detail_level: str,
    top_n: int,
    appendix_rows: int,
    full_detail_pdf: bool,
    all_kickback_detail: pd.DataFrame,
    summary_only: bool,
    files_used: List[str],
) -> Dict[str, Any]:
    print(f"[PDF] All stores start: {out_pdf}")
    styles = build_styles()
    prepare_monthly_styles(styles)
    footer = monthly_footer(month_key, f"{start_day.isoformat()} to {end_day.isoformat()}", generated_at)
    doc = SimpleDocTemplate(
        str(out_pdf),
        pagesize=letter,
        leftMargin=PAGE_MARGINS["left"],
        rightMargin=PAGE_MARGINS["right"],
        topMargin=PAGE_MARGINS["top"],
        bottomMargin=PAGE_MARGINS["bottom"],
        title=f"Monthly Owner Review - {month_key}",
    )
    story: List[Any] = []
    store_rankings = build_store_rankings(store_scorecards, inventory_summary)
    wins = generate_monthly_wins(all_metrics, store_scorecards, category_summary, brand_summary, new_customer_daily, inventory_summary)
    concerns = generate_monthly_concerns(all_metrics, store_scorecards, category_summary, brand_summary, cart_distribution, inventory_summary)
    actions = generate_monthly_action_items(action_items)

    add_compact_header(story, styles, "All Stores", month_key, start_day, end_day, generated_at)
    add_section_title(
        story,
        styles,
        "Monthly Owner Review - All Stores",
        f"{month_key} | {start_day.isoformat()} to {end_day.isoformat()} | Generated {generated_at}",
    )
    story.append(build_scorecard_grid(styles, build_executive_metric_cards(all_metrics, styles), cols=4))
    story.append(Spacer(1, 0.08 * inch))
    story.append(build_two_column_layout(
        [
            Paragraph("<b>3 Biggest Wins</b>", styles["InsightTitle"]),
            *[Paragraph(escape_html(text), styles["Small"]) for text in wins[:3]],
        ],
        [
            Paragraph("<b>3 Biggest Concerns</b>", styles["InsightTitle"]),
            *[Paragraph(escape_html(text), styles["Small"]) for text in concerns[:3]],
        ],
    ))
    story.append(Spacer(1, 0.07 * inch))
    build_insight_cards(story, styles, "3 Action Items", actions, max_items=3, color=BUZZ["yellow"])
    if not summary_only:
        add_report_sections_card(story, styles, is_store=False)

    if summary_only:
        doc.build(story, onFirstPage=footer, onLaterPages=footer)
        print(f"[PDF] All stores complete: {out_pdf}")
        days = len(date_range_days(start_day, end_day))
        return {
            "pdf": str(out_pdf),
            "store": "ALL",
            "date_range_days": days,
            "daily_rows_expected": days,
            "daily_rows_rendered": 0,
            "kickback_rows_expected": len(kickback_summary) if kickback_summary is not None else 0,
            "kickback_rows_rendered": 0,
            "new_customer_days_expected": 0,
            "new_customer_days_rendered": 0,
            "full_detail_pdf": False,
        }

    story.append(PageBreak())

    add_compact_header(story, styles, "All Stores", month_key, start_day, end_day, generated_at)
    add_section_title(story, styles, "What Changed This Month", "Current month compared with the previous archived month where data exists.")
    add_chart(story, chart_kpi_comparison_strip(all_metrics), height=2.65 * inch)
    comparison_rows = [
        ("Net Revenue", all_metrics.get("net_revenue"), all_metrics.get("previous_month_net_revenue", "N/A"), "money", "net_revenue"),
        ("Profit", all_metrics.get("profit"), all_metrics.get("previous_month_profit", "N/A"), "money", "profit"),
        ("Real Margin", all_metrics.get("margin_real"), all_metrics.get("previous_month_margin_real", "N/A"), "pct", "margin_real"),
        ("Tickets", all_metrics.get("tickets"), all_metrics.get("previous_month_tickets", "N/A"), "int", "tickets"),
        ("Avg Cart >$1", all_metrics.get("basket"), all_metrics.get("previous_month_basket", "N/A"), "money1", "basket"),
        ("Discount Rate", all_metrics.get("discount_rate"), all_metrics.get("previous_month_discount_rate", "N/A"), "pct", "discount_rate"),
        ("New Customers", first_available_metric(all_metrics, ["closing_new_customers", "new_customers"]), all_metrics.get("previous_month_closing_new_customers", "N/A"), "int", "closing_new_customers"),
        ("New Customer %", all_metrics.get("closing_new_customer_rate"), all_metrics.get("previous_month_closing_new_customer_rate", "N/A"), "pct", "closing_new_customer_rate"),
        ("Ending Inventory", all_metrics.get("inventory_end_value"), all_metrics.get("previous_month_inventory_end_value", "N/A"), "money", "inventory_end_value"),
    ]
    story.append(build_comparison_panel(styles, comparison_rows))
    story.append(Spacer(1, 0.08 * inch))
    summary_rows = []
    if store_scorecards is not None and not store_scorecards.empty:
        best_store = store_scorecards.sort_values("net_revenue", ascending=False).iloc[0]
        high_margin = store_scorecards.sort_values("margin_real", ascending=False).iloc[0]
        high_discount = store_scorecards.sort_values("discount_rate", ascending=False).iloc[0]
        summary_rows.extend([
            ["Best Store", f"{best_store['store']} - {money(best_store['net_revenue'])}"],
            ["Highest Margin Store", f"{high_margin['store']} - {pct1(high_margin['margin_real'])}"],
            ["Highest Discount-Risk Store", f"{high_discount['store']} - {pct1(high_discount['discount_rate'])}"],
        ])
    if inventory_summary is not None and not inventory_summary.empty and "inventory_value_change" in inventory_summary.columns:
        inv = inventory_summary.reindex(inventory_summary["inventory_value_change"].abs().sort_values(ascending=False).index).iloc[0]
        summary_rows.append(["Largest Inventory Change", f"{inv['store']} - {optional_signed_money(inv['inventory_value_change'])}"])
    summary_rows.append(["Top Category", str(all_metrics.get("top_category", "N/A"))])
    story.append(build_table(["Signal", "Value"], summary_rows, [2.4 * inch, 3.8 * inch]))

    story.append(PageBreak())

    add_compact_header(story, styles, "All Stores", month_key, start_day, end_day, generated_at)
    add_section_title(story, styles, "Store Scoreboard", "Stores ranked by revenue, real profit, margin, and discount risk.")
    add_chart(story, chart_store_quadrant(store_scorecards), height=2.95 * inch)
    story.append(build_two_column_layout(
        [chart_or_spacer(chart_barh_value(store_scorecards.sort_values("net_revenue", ascending=False), "store", "net_revenue", "Revenue Ranking", money, top_n=min(top_n, 10)), width=3.45 * inch, height=2.35 * inch)],
        [chart_or_spacer(chart_barh_value(store_scorecards.sort_values("margin_real", ascending=False), "store", "margin_real", "Real Margin Ranking", pct1, top_n=min(top_n, 10)), width=3.45 * inch, height=2.35 * inch)],
    ))
    scoreboard = store_rankings.copy() if store_rankings is not None and not store_rankings.empty else store_scorecards.copy()
    if not scoreboard.empty:
        scoreboard["mom"] = scoreboard.apply(lambda row: format_store_delta(row.to_dict(), "previous_month", "net_revenue", "money"), axis=1)
        scoreboard["yoy"] = scoreboard.apply(lambda row: format_store_delta(row.to_dict(), "same_month_previous_year", "net_revenue", "money"), axis=1)
        if "inventory_value_change" not in scoreboard.columns:
            scoreboard["inventory_value_change"] = np.nan
    add_df_table(
        story,
        styles,
        scoreboard.sort_values("net_revenue", ascending=False),
        [
            ("store", "Store", "text"),
            ("net_revenue", "Revenue", "money"),
            ("mom", "MoM", "text"),
            ("yoy", "YoY", "text"),
            ("margin_real", "Real Margin", "pct"),
            ("discount_rate", "Disc Rate", "pct"),
            ("inventory_value_change", "Inventory Chg", "signed_money"),
            ("status", "Status", "text"),
        ],
        max_rows=min(top_n, 10),
        col_widths=[0.55 * inch, 0.95 * inch, 0.55 * inch, 0.55 * inch, 0.82 * inch, 0.78 * inch, 0.98 * inch, 1.0 * inch],
    )

    story.append(PageBreak())

    add_compact_header(story, styles, "All Stores", month_key, start_day, end_day, generated_at)
    add_section_title(story, styles, "Revenue + Traffic Pattern", "Daily revenue, weekday averages, and busyness by hour.")
    story.append(build_calendar_heatmap_table(all_daily, start_day, end_day, styles))
    story.append(Spacer(1, 0.06 * inch))
    add_chart(story, chart_daily_revenue_profit(all_daily, "Daily Net Revenue and Profit"), height=2.35 * inch)
    story.append(build_two_column_layout(
        [chart_or_spacer(chart_weekday_avg_revenue(weekday_summary, "Average Net Revenue by Weekday"), width=3.45 * inch, height=2.15 * inch)],
        [chart_or_spacer(chart_weekday_hour_heatmap(weekday_hour_summary, "Avg Tickets by Weekday and Hour"), width=3.45 * inch, height=2.15 * inch)],
    ))
    pattern_insights = []
    if all_metrics.get("best_day"):
        pattern_insights.append(f"Best day was {all_metrics['best_day']['date']} at {money(all_metrics['best_day']['net_revenue'])}.")
    if all_metrics.get("worst_day"):
        pattern_insights.append(f"Slowest revenue day was {all_metrics['worst_day']['date']} at {money(all_metrics['worst_day']['net_revenue'])}.")
    if weekday_summary is not None and not weekday_summary.empty:
        row = weekday_summary.sort_values("avg_net_revenue", ascending=False).iloc[0]
        pattern_insights.append(f"Best weekday average was {row['weekday']} at {money(row['avg_net_revenue'])}.")
    if weekday_hour_summary is not None and not weekday_hour_summary.empty:
        busy = weekday_hour_summary.sort_values("avg_tickets", ascending=False).iloc[0]
        pattern_insights.append(f"Busiest recurring hour was {busy['weekday']} {fmt_hour(int(busy['hour']))} with {as_float(busy['avg_tickets']):.1f} avg tickets.")
    build_insight_cards(story, styles, "Pattern Signals", pattern_insights, max_items=4)

    story.append(PageBreak())

    add_compact_header(story, styles, "All Stores", month_key, start_day, end_day, generated_at)
    add_section_title(story, styles, "Category / Mix Performance", "Mix, margin, and discount pressure by category.")
    story.append(build_two_column_layout(
        [chart_or_spacer(chart_category_mix(category_summary, "Category Revenue Share", top_n=min(top_n, 8)), width=3.45 * inch, height=2.35 * inch)],
        [chart_or_spacer(chart_margin_discount_risk(category_summary, "category", "Category Margin vs Discount", top_n=min(top_n, 8)), width=3.45 * inch, height=2.35 * inch)],
    ))
    build_insight_cards(story, styles, "Category Insights", generate_category_insights(category_summary, all_metrics), max_items=4)
    add_df_table(
        story,
        styles,
        category_summary,
        [
            ("category", "Category", "textwrap"),
            ("net_revenue", "Revenue", "money"),
            ("profit", "Profit", "money"),
            ("margin_real", "Real Margin", "pct"),
            ("discount_rate", "Disc Rate", "pct"),
            ("pct_revenue", "Rev Share", "pct"),
        ],
        max_rows=min(top_n, 10),
        col_widths=[1.8 * inch, 0.95 * inch, 0.86 * inch, 0.82 * inch, 0.78 * inch, 0.75 * inch],
    )

    story.append(PageBreak())

    add_compact_header(story, styles, "All Stores", month_key, start_day, end_day, generated_at)
    add_section_title(story, styles, "Brands + Products", "Top movers, product concentration, and low-margin high-volume risk.")
    story.append(build_two_column_layout(
        [chart_or_spacer(chart_barh_value(brand_summary.sort_values("net_revenue", ascending=False), "brand", "net_revenue", "Top Brands by Revenue", money, top_n=min(top_n, 10)), width=3.45 * inch, height=2.45 * inch)],
        [chart_or_spacer(chart_barh_value(brand_summary.sort_values("profit", ascending=False), "brand", "profit", "Top Brands by Profit", money, top_n=min(top_n, 10)), width=3.45 * inch, height=2.45 * inch)],
    ))
    add_chart(story, chart_product_pareto(product_summary, top_n=min(top_n, 12)), height=2.55 * inch)
    build_insight_cards(story, styles, "Mix Insights", generate_mix_shift_insights(category_summary, brand_summary, product_summary, all_metrics), max_items=3)
    top_units = product_summary.sort_values("items", ascending=False) if product_summary is not None and not product_summary.empty else pd.DataFrame()
    low_margin_products = product_summary[
        (product_summary["items"] >= product_summary["items"].quantile(0.75)) &
        (product_summary["margin_real"] < all_metrics.get("margin_real", 0))
    ].sort_values(["margin_real", "items"], ascending=[True, False]) if product_summary is not None and not product_summary.empty and "items" in product_summary.columns else pd.DataFrame()
    story.append(build_two_column_layout(
        [
            Paragraph("<b>Top Products by Revenue</b>", styles["InsightTitle"]),
            build_table(*df_rows(product_summary, [("product", "Product", "textwrap"), ("net_revenue", "Revenue", "money"), ("items", "Units", "int")], min(top_n, 10), styles), col_widths=[1.85 * inch, 0.85 * inch, 0.55 * inch]),
        ],
        [
            Paragraph("<b>Low-Margin High-Volume Products</b>", styles["InsightTitle"]),
            build_table(*df_rows(low_margin_products, [("product", "Product", "textwrap"), ("margin_real", "Margin", "pct"), ("items", "Units", "int")], min(top_n, 10), styles), col_widths=[1.9 * inch, 0.65 * inch, 0.55 * inch]),
        ],
    ))
    story.append(Spacer(1, 0.05 * inch))
    add_df_table(
        story,
        styles,
        top_units,
        [("product", "Top Products by Units", "textwrap"), ("brand", "Brand", "textwrap"), ("items", "Units", "int"), ("net_revenue", "Revenue", "money")],
        max_rows=min(top_n, 10),
        col_widths=[2.35 * inch, 1.35 * inch, 0.65 * inch, 0.95 * inch],
    )

    story.append(PageBreak())

    add_compact_header(story, styles, "All Stores", month_key, start_day, end_day, generated_at)
    add_section_title(story, styles, "Customer Growth + Cart Behavior", "New customers, cart mix, and low-value cart risk.")
    customer_rows = [
        ["Total Customers", f"{int(as_float(all_metrics.get('closing_customers'))):,}" if all_metrics.get("closing_summary_has_data") else "N/A"],
        ["New Customers", f"{int(as_float(all_metrics.get('closing_new_customers'))):,}" if all_metrics.get("closing_summary_has_data") else "N/A"],
        ["New Customer %", pct1(all_metrics.get("closing_new_customer_rate")) if all_metrics.get("closing_summary_has_data") else "N/A"],
        ["Avg Cart > $1", money1(all_metrics.get("basket"))],
        ["$0-$1 Carts Excluded", f"{int(as_float(all_metrics.get('low_value_cart_count_excluded_from_basket'))):,}"],
    ]
    story.append(build_table(["Customer / Cart Metric", "Value"], customer_rows, [2.45 * inch, 2.0 * inch]))
    story.append(Spacer(1, 0.06 * inch))
    build_insight_cards(story, styles, "Customer Signals", generate_customer_growth_insights(new_customer_summary, new_customer_daily, all_metrics), max_items=3)
    if new_customer_daily is not None and not new_customer_daily.empty:
        add_chart(story, chart_new_customer_trend(new_customer_daily), height=2.25 * inch)
        story.append(build_new_customer_calendar_table(new_customer_daily, start_day, end_day, styles))
        story.append(Spacer(1, 0.06 * inch))
    story.append(build_two_column_layout(
        [chart_or_spacer(chart_cart_value_distribution(cart_distribution, "Cart Value Distribution"), width=3.45 * inch, height=2.45 * inch)],
        [
            Paragraph("<b>Cart Mix</b>", styles["InsightTitle"]),
            build_table(*df_rows(cart_distribution, [("bucket", "Bucket", "text"), ("count", "Carts", "int"), ("pct", "Share", "pct")], min(top_n, 10), styles), col_widths=[1.15 * inch, 0.8 * inch, 0.7 * inch]),
        ],
    ))

    story.append(PageBreak())

    add_compact_header(story, styles, "All Stores", month_key, start_day, end_day, generated_at)
    add_section_title(story, styles, "Discount + Margin Health", "Discount pressure, margin health, and deal kickback impact.")
    build_insight_cards(story, styles, "Discount / Margin Signals", generate_discount_margin_insights(all_metrics, store_scorecards, category_summary, brand_summary), max_items=4, color=colors.HexColor("#F59E0B"))
    add_chart(story, chart_discount_waterfall(all_metrics), height=2.45 * inch)
    story.append(build_two_column_layout(
        [chart_or_spacer(chart_barh_value(store_scorecards.sort_values("discount_rate", ascending=False), "store", "discount_rate", "Discount Rate by Store", pct1, top_n=min(top_n, 10)), width=3.45 * inch, height=2.25 * inch)],
        [chart_or_spacer(chart_margin_discount_risk(brand_summary, "brand", "Brand Margin vs Discount", top_n=min(top_n, 10)), width=3.45 * inch, height=2.25 * inch)],
    ))
    kickback_total = as_float(kickback_summary["kickback"].sum()) if kickback_summary is not None and not kickback_summary.empty else 0.0
    impact_rows = [
        ["Total Discounts", money(all_metrics.get("discount"))],
        ["OTD Round-Up Revenue Opportunity", money(all_metrics.get("tax_rounding_loss"))],
        ["Tax-Included OTD Gap", money(all_metrics.get("tax_included_roundup_opportunity"))],
        ["Tax Backout Rounddown Loss", money(all_metrics.get("tax_backout_rounddown_loss"))],
        ["Rounding Loss / Revenue", pct1(all_metrics.get("tax_rounding_loss_rate"))],
        ["Total Kickback", money(kickback_total)],
        ["Profit Before Kickback", money(all_metrics.get("profit_real"))],
        ["Profit After Kickback", money(all_metrics.get("profit"))],
        ["Margin Real / KB", f"{pct1(all_metrics.get('margin_real'))} / {pct1(all_metrics.get('margin'))}"],
    ]
    story.append(build_table(["Metric", "Value"], impact_rows, [2.25 * inch, 2.2 * inch]))
    story.append(Spacer(1, 0.05 * inch))
    add_df_table(
        story,
        styles,
        kickback_summary.sort_values("generated_profit", ascending=False) if kickback_summary is not None and not kickback_summary.empty and "generated_profit" in kickback_summary.columns else kickback_summary,
        [
            ("brand", "Deal Brand", "textwrap"),
            ("stores", "Stores", "textwrap"),
            ("rule", "Rules", "textwrap"),
            ("generated_profit", "Gen Profit", "money"),
            ("margin_real", "Real M", "pct"),
            ("margin", "KB M", "pct"),
            ("kickback", "Kickback", "money"),
            ("deal_signal", "Signal", "textwrap"),
        ],
        max_rows=min(top_n, 10),
        col_widths=[1.2 * inch, 0.62 * inch, 1.2 * inch, 0.82 * inch, 0.5 * inch, 0.5 * inch, 0.72 * inch, 0.95 * inch],
    )
    if kickback_summary is not None and not kickback_summary.empty and "margin_real" in kickback_summary.columns:
        review_deals = kickback_summary.sort_values(["margin_real", "net_revenue"], ascending=[True, False])
        add_df_table(
            story,
            styles,
            review_deals,
            [
                ("brand", "Lowest-Margin Deal Brands", "textwrap"),
                ("net_revenue", "Revenue", "money"),
                ("profit_real", "Real Profit", "money"),
                ("kickback", "Kickback", "money"),
                ("margin_real", "Real M", "pct"),
                ("deal_signal", "Signal", "textwrap"),
            ],
            max_rows=min(top_n, 8),
            col_widths=[1.55 * inch, 0.9 * inch, 0.9 * inch, 0.78 * inch, 0.56 * inch, 1.15 * inch],
        )
    if store_scorecards is not None and not store_scorecards.empty and "tax_rounding_loss" in store_scorecards.columns:
        add_df_table(
            story,
            styles,
            store_scorecards.sort_values("tax_rounding_loss", ascending=False),
            [
                ("store", "Store", "text"),
                ("tax_rounding_loss", "OTD Round-Up Loss", "money"),
                ("tax_rounding_transactions", "Transactions", "int"),
                ("tax_rounding_avg_loss_per_transaction", "Avg / Txn", "money2"),
                ("tax_included_roundup_opportunity", "OTD Gap", "money"),
            ],
            max_rows=min(top_n, 10),
            col_widths=[0.55 * inch, 1.15 * inch, 0.95 * inch, 0.85 * inch, 0.75 * inch],
        )

    story.append(PageBreak())

    add_compact_header(story, styles, "All Stores", month_key, start_day, end_day, generated_at)
    add_section_title(story, styles, "Inventory Health", "Opening inventory, ending inventory, gain/loss, turns, and inventory-to-revenue.")
    build_insight_cards(story, styles, "Inventory Signals", generate_inventory_health_insights(inventory_summary, all_metrics), max_items=4)
    add_chart(story, chart_inventory_movement(inventory_summary), height=2.5 * inch)
    if inventory_summary is None or inventory_summary.empty or not all_metrics.get("inventory_has_data"):
        add_insights(story, styles, ["Inventory snapshots were not found. Run with --fetch-inventory-api to populate this page."])
    else:
        add_df_table(
            story,
            styles,
            inventory_summary,
            [
                ("store", "Store", "text"),
                ("opening_inventory_value", "Opening Inv", "money"),
                ("ending_inventory_value", "Ending Inv", "money"),
                ("inventory_value_change", "Gain/Loss", "signed_money"),
                ("inventory_turns_est", "Turns Est", "float2"),
                ("inventory_to_revenue", "Inv/Revenue", "pct_optional"),
                ("sku_change", "SKU Chg", "float1"),
            ],
            max_rows=min(top_n, 10),
            col_widths=[0.55 * inch, 0.95 * inch, 0.95 * inch, 0.95 * inch, 0.75 * inch, 0.82 * inch, 0.65 * inch],
        )

    story.append(PageBreak())

    add_compact_header(story, styles, "All Stores", month_key, start_day, end_day, generated_at)
    add_section_title(story, styles, "Staff / Budtender Summary", "Neutral coaching-focused view of revenue, tickets, basket, and discounting.")
    build_insight_cards(story, styles, "Staff Coaching Signals", generate_staff_coaching_insights(budtender_summary), max_items=3)
    top_basket = budtender_summary[budtender_summary["tickets"] >= 25].sort_values("basket", ascending=False) if budtender_summary is not None and not budtender_summary.empty and "tickets" in budtender_summary.columns else pd.DataFrame()
    top_discount = budtender_summary[budtender_summary["tickets"] >= 25].sort_values("discount_rate", ascending=False) if budtender_summary is not None and not budtender_summary.empty and "discount_rate" in budtender_summary.columns else pd.DataFrame()
    story.append(build_two_column_layout(
        [
            Paragraph("<b>Top Revenue Performers</b>", styles["InsightTitle"]),
            build_table(*df_rows(budtender_summary, [("budtender", "Budtender", "textwrap"), ("net_revenue", "Revenue", "money"), ("tickets", "Tickets", "int")], min(top_n, 10), styles), col_widths=[1.75 * inch, 0.85 * inch, 0.62 * inch]),
        ],
        [
            Paragraph("<b>Strong Basket Performance</b>", styles["InsightTitle"]),
            build_table(*df_rows(top_basket, [("budtender", "Budtender", "textwrap"), ("basket", "Avg Cart", "money1"), ("tickets", "Tickets", "int")], min(top_n, 10), styles), col_widths=[1.75 * inch, 0.75 * inch, 0.62 * inch]),
        ],
    ))
    story.append(Spacer(1, 0.05 * inch))
    add_df_table(
        story,
        styles,
        top_discount,
        [("budtender", "Discount Review", "textwrap"), ("discount_rate", "Disc Rate", "pct"), ("tickets", "Tickets", "int"), ("net_revenue", "Revenue", "money")],
        max_rows=min(top_n, 10),
        col_widths=[2.2 * inch, 0.78 * inch, 0.7 * inch, 0.95 * inch],
    )

    days = len(date_range_days(start_day, end_day))
    daily_counts = {"expected": days, "rendered": 0}
    new_counts = {"expected": 0, "rendered": 0}
    all_store_kickback_pdf_detail = kickback_summary if kickback_summary is not None else pd.DataFrame()
    kick_counts = {"expected": len(all_store_kickback_pdf_detail), "rendered": 0}
    if full_detail_pdf:
        add_detail_appendix_index(story, styles, include_new_customer=new_customer_daily is not None and not new_customer_daily.empty)
        daily_counts = add_full_daily_detail_section(
            story,
            styles,
            all_daily,
            start_day,
            end_day,
            "Appendix A - Full Daily Detail",
        )
        new_counts = add_full_new_customer_daily_section(
            story,
            styles,
            new_customer_daily,
            start_day,
            end_day,
            "Appendix B - Full New Customer Detail",
            authoritative_total=all_metrics.get("closing_new_customers") if all_metrics.get("closing_summary_has_data") else None,
            authoritative_customers=all_metrics.get("closing_customers") if all_metrics.get("closing_summary_has_data") else None,
        )
        kick_counts = add_full_kickback_detail_section(
            story,
            styles,
            all_store_kickback_pdf_detail,
            "Appendix C - Combined All-Store Kickback / Deal Brand Detail",
            include_store=False,
        )
        add_appendix_table_section(
            story,
            styles,
            "Appendix D - Category Detail",
            category_summary,
            [("category", "Category", "textwrap"), ("net_revenue", "Revenue", "money"), ("profit", "Profit", "money"), ("margin_real", "Real Margin", "pct"), ("discount_rate", "Disc Rate", "pct"), ("pct_revenue", "Rev Share", "pct")],
            [1.85 * inch, 0.9 * inch, 0.82 * inch, 0.76 * inch, 0.76 * inch, 0.72 * inch],
            appendix_rows,
            full_if_rows_leq=max(30, appendix_rows),
        )
        add_appendix_table_section(
            story,
            styles,
            "Appendix E - Brand Detail",
            brand_summary,
            [("brand", "Brand", "textwrap"), ("net_revenue", "Revenue", "money"), ("profit", "Profit", "money"), ("margin_real", "Real Margin", "pct"), ("discount_rate", "Disc Rate", "pct"), ("items", "Items", "int")],
            [1.85 * inch, 0.9 * inch, 0.82 * inch, 0.76 * inch, 0.76 * inch, 0.62 * inch],
            appendix_rows,
        )
        add_appendix_table_section(
            story,
            styles,
            "Appendix F - Budtender Detail",
            budtender_summary,
            [("budtender", "Budtender", "textwrap"), ("net_revenue", "Revenue", "money"), ("tickets", "Tickets", "int"), ("basket", "Avg Cart", "money1"), ("discount_rate", "Disc Rate", "pct")],
            [2.25 * inch, 0.92 * inch, 0.68 * inch, 0.78 * inch, 0.78 * inch],
            appendix_rows,
        )
        add_appendix_table_section(
            story,
            styles,
            "Appendix G - Product Detail",
            product_summary,
            [("product", "Product", "textwrap"), ("brand", "Brand", "textwrap"), ("net_revenue", "Revenue", "money"), ("items", "Units", "int"), ("margin_real", "Real Margin", "pct"), ("discount_rate", "Disc Rate", "pct")],
            [2.0 * inch, 1.1 * inch, 0.82 * inch, 0.55 * inch, 0.72 * inch, 0.72 * inch],
            appendix_rows,
        )
        if warnings:
            story.append(PageBreak())
            append_long_table(
                story,
                "Warnings",
                pd.DataFrame(warnings),
                [("severity", "Severity", "text"), ("message", "Warning", "textwrap")],
                [0.85 * inch, 5.9 * inch],
                styles,
                row_limit=appendix_rows if appendix_rows else None,
            )
        if files_used and detail_level == "deep":
            story.append(PageBreak())
            append_long_table(story, "Files Used", pd.DataFrame({"file": files_used}), [("file", "Files Used", "textwrap")], [6.6 * inch], styles, row_limit=None)

    doc.build(story, onFirstPage=footer, onLaterPages=footer)
    print(f"[PDF] All stores complete: {out_pdf}")
    return {
        "pdf": str(out_pdf),
        "store": "ALL",
        "date_range_days": days,
        "daily_rows_expected": daily_counts["expected"],
        "daily_rows_rendered": daily_counts["rendered"],
        "kickback_rows_expected": kick_counts["expected"],
        "kickback_rows_rendered": kick_counts["rendered"],
        "new_customer_days_expected": new_counts["expected"],
        "new_customer_days_rendered": new_counts["rendered"],
        "full_detail_pdf": bool(full_detail_pdf),
    }


def build_store_pdf(
    out_pdf: Path,
    month_key: str,
    start_day: date,
    end_day: date,
    generated_at: str,
    bundle: StoreBundle,
    all_metrics: Dict[str, Any],
    category_summary: pd.DataFrame,
    brand_summary: pd.DataFrame,
    product_summary: pd.DataFrame,
    budtender_summary: pd.DataFrame,
    cart_distribution: pd.DataFrame,
    hourly_summary: pd.DataFrame,
    weekday_summary: pd.DataFrame,
    weekday_hour_summary: pd.DataFrame,
    new_customer_summary: pd.DataFrame,
    new_customer_daily: pd.DataFrame,
    closing_summary_row: Dict[str, Any],
    inventory_row: Dict[str, Any],
    kickback_summary: pd.DataFrame,
    action_items: List[Dict[str, str]],
    top_n: int,
    appendix_rows: int,
    full_detail_pdf: bool,
    detail_level: str,
    store_rankings: pd.DataFrame,
) -> Dict[str, Any]:
    print(f"[PDF] Store start: {out_pdf}")
    styles = build_styles()
    prepare_monthly_styles(styles)
    footer = monthly_footer(month_key, f"{start_day.isoformat()} to {end_day.isoformat()}", generated_at)
    doc = SimpleDocTemplate(
        str(out_pdf),
        pagesize=letter,
        leftMargin=PAGE_MARGINS["left"],
        rightMargin=PAGE_MARGINS["right"],
        topMargin=PAGE_MARGINS["top"],
        bottomMargin=PAGE_MARGINS["bottom"],
        title=f"{bundle.abbr} Monthly Owner Review - {month_key}",
    )
    story: List[Any] = []
    m = bundle.metrics
    store_metrics = dict(m)
    store_metrics["kickback"] = as_float(kickback_summary["kickback"].sum()) if kickback_summary is not None and not kickback_summary.empty and "kickback" in kickback_summary.columns else 0.0
    if store_rankings is not None and not store_rankings.empty:
        ranking_rows = store_rankings[store_rankings["store"].astype(str).str.upper() == bundle.abbr.upper()]
        if not ranking_rows.empty:
            store_metrics.update(ranking_rows.iloc[0].to_dict())
    if closing_summary_row:
        store_metrics["closing_new_customers"] = as_float(closing_summary_row.get("new_customer_count"))
        store_metrics["closing_customers"] = as_float(closing_summary_row.get("customer_count"))
        store_metrics["closing_new_customer_rate"] = (
            store_metrics["closing_new_customers"] / store_metrics["closing_customers"]
            if store_metrics.get("closing_customers") else 0.0
        )
    if inventory_row:
        for key in [
            "opening_inventory_value",
            "ending_inventory_value",
            "inventory_value_change",
            "inventory_value_change_pct",
            "inventory_turns_est",
            "inventory_to_revenue",
        ]:
            store_metrics[key if key != "ending_inventory_value" else "inventory_end_value"] = inventory_row.get(key)
    rank_context = store_rank_context(store_rankings, bundle.abbr, all_metrics)
    status = generate_store_status(m, all_metrics)

    add_compact_header(story, styles, bundle.abbr, month_key, start_day, end_day, generated_at)
    add_section_title(
        story,
        styles,
        f"Monthly Owner Review - {bundle.abbr} / {bundle.label}",
        f"Status: {status} | {month_key} | Generated {generated_at}",
    )
    story.append(build_scorecard_grid(styles, build_executive_metric_cards(store_metrics, styles, rank_context), cols=4))
    story.append(Spacer(1, 0.08 * inch))
    build_insight_cards(story, styles, "Top Store Action Items", generate_monthly_action_items(action_items), max_items=3, color=BUZZ["yellow"])
    add_report_sections_card(story, styles, is_store=True)
    story.append(build_table(
        ["Store Rank", "Value"],
        [
            ["Revenue Rank", rank_context.get("net_revenue") or "-"],
            ["Real Margin Rank", rank_context.get("margin_real") or "-"],
            ["Discount Rate Rank", rank_context.get("discount_rate") or "-"],
            ["Revenue Share", pct1(as_float(m.get("net_revenue")) / as_float(all_metrics.get("net_revenue")) if all_metrics.get("net_revenue") else 0.0)],
        ],
        [1.6 * inch, 3.6 * inch],
    ))

    story.append(PageBreak())

    add_compact_header(story, styles, bundle.abbr, month_key, start_day, end_day, generated_at)
    add_section_title(story, styles, "Store Comparisons", "Store performance vs company average and current store rank.")
    store_count = max(1, int(as_float((store_rankings["store_count"].iloc[0] if store_rankings is not None and not store_rankings.empty and "store_count" in store_rankings.columns else 1))))
    company_revenue = as_float(all_metrics.get("net_revenue")) / store_count
    company_profit = as_float(all_metrics.get("profit")) / store_count
    company_tickets = as_float(all_metrics.get("tickets")) / store_count
    company_new_customers = as_float(all_metrics.get("closing_new_customers")) / store_count if all_metrics.get("closing_summary_has_data") else None
    company_inventory_change = as_float(all_metrics.get("inventory_value_change")) / store_count if all_metrics.get("inventory_has_data") else None
    company_kickback = as_float(all_metrics.get("kickback")) / store_count
    company_tax_rounding_loss = as_float(all_metrics.get("tax_rounding_loss")) / store_count
    comparison_rows = [
        {"metric": "Revenue", "store": money(m.get("net_revenue")), "company": money(company_revenue), "rank": rank_context.get("net_revenue", ""), "status": company_average_signal("net_revenue", m.get("net_revenue"), company_revenue)},
        {"metric": "Profit", "store": money(m.get("profit")), "company": money(company_profit), "rank": rank_context.get("profit", ""), "status": company_average_signal("profit", m.get("profit"), company_profit)},
        {"metric": "Real Margin", "store": pct1(m.get("margin_real")), "company": pct1(all_metrics.get("margin_real")), "rank": rank_context.get("margin_real", ""), "status": company_average_signal("margin_real", m.get("margin_real"), all_metrics.get("margin_real"))},
        {"metric": "Tickets", "store": f"{int(as_float(m.get('tickets'))):,}", "company": f"{int(company_tickets):,}", "rank": rank_context.get("tickets", ""), "status": company_average_signal("tickets", m.get("tickets"), company_tickets)},
        {"metric": "Avg Cart >$1", "store": money1(m.get("basket")), "company": money1(all_metrics.get("basket")), "rank": rank_context.get("basket", ""), "status": company_average_signal("basket", m.get("basket"), all_metrics.get("basket"))},
        {"metric": "Discount Rate", "store": pct1(m.get("discount_rate")), "company": pct1(all_metrics.get("discount_rate")), "rank": rank_context.get("discount_rate", ""), "status": company_average_signal("discount_rate", m.get("discount_rate"), all_metrics.get("discount_rate"))},
        {
            "metric": "New Customers",
            "store": f"{int(as_float(store_metrics.get('closing_new_customers'))):,}" if "closing_new_customers" in store_metrics else "Unavailable",
            "company": f"{int(company_new_customers):,}" if company_new_customers is not None else "Unavailable",
            "rank": "Not ranked",
            "status": company_average_signal("closing_new_customers", store_metrics.get("closing_new_customers"), company_new_customers),
        },
        {
            "metric": "New Customer %",
            "store": pct1(store_metrics.get("closing_new_customer_rate")) if "closing_new_customer_rate" in store_metrics else "Unavailable",
            "company": pct1(all_metrics.get("closing_new_customer_rate")) if all_metrics.get("closing_summary_has_data") else "Unavailable",
            "rank": "Not ranked",
            "status": "Closing report mix",
        },
        {
            "metric": "Inventory Change",
            "store": optional_signed_money(store_metrics.get("inventory_value_change")) if all_metrics.get("inventory_has_data") else "Unavailable",
            "company": optional_signed_money(company_inventory_change) if company_inventory_change is not None else "Unavailable",
            "rank": "Not ranked",
            "status": "Watch growth vs sales" if company_inventory_change is not None else "",
        },
        {
            "metric": "Kickback Amount",
            "store": money(store_metrics.get("kickback")),
            "company": money(company_kickback),
            "rank": "Not ranked",
            "status": "Deal impact tracked",
        },
        {
            "metric": "OTD Round-Up Loss",
            "store": money(store_metrics.get("tax_rounding_loss")),
            "company": money(company_tax_rounding_loss),
            "rank": "Not ranked",
            "status": "Rounding-down impact",
        },
    ]
    story.append(build_store_company_comparison_panel(styles, comparison_rows))
    story.append(Spacer(1, 0.08 * inch))
    build_insight_cards(story, styles, "Comparison Signals", generate_store_comparison_insights({**m, "revenue_share": as_float(m.get("net_revenue")) / as_float(all_metrics.get("net_revenue")) if all_metrics.get("net_revenue") else 0.0}, all_metrics, rank_context), max_items=4)

    story.append(PageBreak())

    add_compact_header(story, styles, bundle.abbr, month_key, start_day, end_day, generated_at)
    add_section_title(story, styles, "Store Revenue Pattern", "Calendar revenue, daily trend, weekday average, and busy hours.")
    story.append(build_calendar_heatmap_table(bundle.daily_df, start_day, end_day, styles))
    story.append(Spacer(1, 0.06 * inch))
    add_chart(story, chart_daily_revenue_profit(bundle.daily_df, f"{bundle.abbr} Daily Net Revenue and Profit"), height=2.35 * inch)
    story.append(build_two_column_layout(
        [chart_or_spacer(chart_weekday_avg_revenue(weekday_summary, f"{bundle.abbr} Average Net Revenue by Weekday"), width=3.45 * inch, height=2.15 * inch)],
        [chart_or_spacer(chart_weekday_hour_heatmap(weekday_hour_summary, f"{bundle.abbr} Avg Tickets by Weekday and Hour"), width=3.45 * inch, height=2.15 * inch)],
    ))
    pattern_insights = []
    best, worst = best_worst_day(bundle.daily_df)
    if best:
        pattern_insights.append(f"Best store day was {best['date'].isoformat()} at {money(best.get('net_revenue'))}.")
    if worst:
        pattern_insights.append(f"Slowest revenue day was {worst['date'].isoformat()} at {money(worst.get('net_revenue'))}.")
    if weekday_summary is not None and not weekday_summary.empty:
        row = weekday_summary.sort_values("avg_net_revenue", ascending=False).iloc[0]
        pattern_insights.append(f"Best weekday average was {row['weekday']} at {money(row['avg_net_revenue'])}.")
    build_insight_cards(story, styles, "Pattern Signals", pattern_insights, max_items=3)

    story.append(PageBreak())

    add_compact_header(story, styles, bundle.abbr, month_key, start_day, end_day, generated_at)
    add_section_title(story, styles, "Store Mix", "Top categories, brands, and products for this store.")
    story.append(build_two_column_layout(
        [chart_or_spacer(chart_category_mix(category_summary, f"{bundle.abbr} Category Revenue Share", top_n=min(top_n, 8)), width=3.45 * inch, height=2.35 * inch)],
        [chart_or_spacer(chart_barh_value(brand_summary.sort_values("net_revenue", ascending=False) if brand_summary is not None and not brand_summary.empty and "net_revenue" in brand_summary.columns else pd.DataFrame(), "brand", "net_revenue", f"{bundle.abbr} Top Brands", money, top_n=min(top_n, 10)), width=3.45 * inch, height=2.35 * inch)],
    ))
    add_chart(story, chart_product_pareto(product_summary, top_n=min(top_n, 12)), height=2.45 * inch)
    build_insight_cards(story, styles, "Store Mix Insights", generate_mix_shift_insights(category_summary, brand_summary, product_summary, m), max_items=3)
    add_df_table(
        story,
        styles,
        product_summary,
        [("product", "Top Products", "textwrap"), ("brand", "Brand", "textwrap"), ("net_revenue", "Revenue", "money"), ("items", "Units", "int"), ("margin_real", "Margin", "pct")],
        max_rows=min(top_n, 10),
        col_widths=[2.25 * inch, 1.2 * inch, 0.9 * inch, 0.58 * inch, 0.76 * inch],
    )

    story.append(PageBreak())

    add_compact_header(story, styles, bundle.abbr, month_key, start_day, end_day, generated_at)
    add_section_title(story, styles, "Store Health", "Discounting, inventory movement, customer growth, cart behavior, and staff signals.")
    story.append(build_two_column_layout(
        [chart_or_spacer(chart_discount_waterfall(m), width=3.45 * inch, height=2.25 * inch)],
        [chart_or_spacer(chart_cart_value_distribution(cart_distribution, f"{bundle.abbr} Cart Value Distribution"), width=3.45 * inch, height=2.25 * inch)],
    ))
    store_health_rows = [
        ["Total Discounts", money(m.get("discount"))],
        ["Discount Rate", pct1(m.get("discount_rate"))],
        ["OTD Round-Up Revenue Opportunity", money(store_metrics.get("tax_rounding_loss"))],
        ["Tax-Included OTD Gap", money(store_metrics.get("tax_included_roundup_opportunity"))],
        ["Total Customers", f"{int(as_float(store_metrics.get('closing_customers'))):,}" if closing_summary_row else "N/A"],
        ["New Customers", f"{int(as_float(store_metrics.get('closing_new_customers'))):,}" if closing_summary_row else "N/A"],
        ["New Customer %", pct1(store_metrics.get("closing_new_customer_rate")) if closing_summary_row else "N/A"],
        ["Avg Cart > $1", money1(store_metrics.get("basket"))],
        ["$0-$1 Carts Excluded", f"{int(as_float(store_metrics.get('low_value_cart_count_excluded_from_basket'))):,}"],
        ["Ending Inventory", optional_money(store_metrics.get("inventory_end_value"))],
        ["Inventory Gain/Loss", optional_signed_money(store_metrics.get("inventory_value_change"))],
    ]
    story.append(build_table(["Metric", "Value"], store_health_rows, [2.1 * inch, 2.3 * inch]))
    story.append(Spacer(1, 0.06 * inch))
    if new_customer_daily is not None and not new_customer_daily.empty:
        add_chart(story, chart_new_customer_trend(new_customer_daily, f"{bundle.abbr} New Customer Trend"), height=2.1 * inch)
    build_insight_cards(story, styles, "Staff Coaching Signals", generate_staff_coaching_insights(budtender_summary), max_items=3)
    add_df_table(
        story,
        styles,
        budtender_summary,
        [("budtender", "Top Budtenders", "textwrap"), ("net_revenue", "Revenue", "money"), ("tickets", "Tickets", "int"), ("basket", "Avg Cart", "money1"), ("discount_rate", "Disc Rate", "pct")],
        max_rows=min(top_n, 10),
        col_widths=[2.25 * inch, 0.92 * inch, 0.68 * inch, 0.78 * inch, 0.78 * inch],
    )
    story.append(Spacer(1, 0.05 * inch))
    kickback_total = as_float(kickback_summary["kickback"].sum()) if kickback_summary is not None and not kickback_summary.empty else 0.0
    story.append(build_table(
        ["Kickback Metric", "Value"],
        [
            ["Store Kickback", money(kickback_total)],
            ["Profit Before Kickback", money(m.get("profit_real"))],
            ["Profit After Kickback", money(m.get("profit"))],
            ["Margin Lift", pp1(as_float(m.get("margin")) - as_float(m.get("margin_real")))],
        ],
        [2.2 * inch, 2.2 * inch],
    ))

    days = len(date_range_days(start_day, end_day))
    daily_counts = {"expected": days, "rendered": 0}
    new_counts = {"expected": 0, "rendered": 0}
    kick_counts = {"expected": len(kickback_summary) if kickback_summary is not None else 0, "rendered": 0}
    if full_detail_pdf:
        add_detail_appendix_index(story, styles, include_new_customer=new_customer_daily is not None and not new_customer_daily.empty)
        daily_counts = add_full_daily_detail_section(
            story,
            styles,
            bundle.daily_df,
            start_day,
            end_day,
            "Appendix A - Full Daily Detail",
            store=bundle.abbr,
        )
        new_counts = add_full_new_customer_daily_section(
            story,
            styles,
            new_customer_daily,
            start_day,
            end_day,
            "Appendix B - Full New Customer Detail",
            store=bundle.abbr,
            authoritative_total=store_metrics.get("closing_new_customers"),
            authoritative_customers=store_metrics.get("closing_customers"),
        )
        kick_counts = add_full_kickback_detail_section(
            story,
            styles,
            kickback_summary,
            "Appendix C - Full Kickback / Deal Detail",
            include_store=False,
        )
        add_appendix_table_section(
            story,
            styles,
            "Appendix D - Category Detail",
            category_summary,
            [("category", "Category", "textwrap"), ("net_revenue", "Revenue", "money"), ("profit", "Profit", "money"), ("margin_real", "Real Margin", "pct"), ("discount_rate", "Disc Rate", "pct"), ("items", "Items", "int")],
            [2.0 * inch, 0.9 * inch, 0.82 * inch, 0.78 * inch, 0.76 * inch, 0.62 * inch],
            appendix_rows,
            full_if_rows_leq=max(30, appendix_rows),
        )
        add_appendix_table_section(
            story,
            styles,
            "Appendix E - Brand Detail",
            brand_summary,
            [("brand", "Brand", "textwrap"), ("net_revenue", "Revenue", "money"), ("profit", "Profit", "money"), ("margin_real", "Real Margin", "pct"), ("discount_rate", "Disc Rate", "pct"), ("items", "Items", "int")],
            [1.85 * inch, 0.9 * inch, 0.82 * inch, 0.76 * inch, 0.76 * inch, 0.62 * inch],
            appendix_rows,
        )
        add_appendix_table_section(
            story,
            styles,
            "Appendix F - Budtender Detail",
            budtender_summary,
            [("budtender", "Budtender", "textwrap"), ("net_revenue", "Revenue", "money"), ("tickets", "Tickets", "int"), ("basket", "Avg Cart", "money1"), ("discount_rate", "Disc Rate", "pct")],
            [2.25 * inch, 0.92 * inch, 0.68 * inch, 0.78 * inch, 0.78 * inch],
            appendix_rows,
        )
        add_appendix_table_section(
            story,
            styles,
            "Appendix G - Product Detail",
            product_summary,
            [("product", "Product", "textwrap"), ("brand", "Brand", "textwrap"), ("net_revenue", "Revenue", "money"), ("items", "Units", "int"), ("margin_real", "Real Margin", "pct"), ("discount_rate", "Disc Rate", "pct")],
            [2.0 * inch, 1.1 * inch, 0.82 * inch, 0.55 * inch, 0.72 * inch, 0.72 * inch],
            appendix_rows,
        )

    doc.build(story, onFirstPage=footer, onLaterPages=footer)
    print(f"[PDF] Store complete: {out_pdf}")
    return {
        "pdf": str(out_pdf),
        "store": bundle.abbr,
        "date_range_days": days,
        "daily_rows_expected": daily_counts["expected"],
        "daily_rows_rendered": daily_counts["rendered"],
        "kickback_rows_expected": kick_counts["expected"],
        "kickback_rows_rendered": kick_counts["rendered"],
        "new_customer_days_expected": new_counts["expected"],
        "new_customer_days_rendered": new_counts["rendered"],
        "full_detail_pdf": bool(full_detail_pdf),
    }


def summarize_for_store(bundle: StoreBundle, start_day: date, end_day: date, top_n: int) -> Dict[str, pd.DataFrame]:
    df = filter_df_date_range(bundle.raw_df, start_day, end_day)
    category = compute_category_summary(df, start_day, end_day)
    brand = compute_monthly_brand_summary(df, start_day, end_day)
    product = compute_monthly_product_summary(df, start_day, end_day)
    budtender = compute_budtender_summary(df, start_day, end_day)
    budtender = apply_adjusted_basket_to_budtenders(df, budtender if budtender is not None else pd.DataFrame(), start_day, end_day)
    cart = compute_cart_value_distribution(df, start_day, end_day)
    hourly = compute_monthly_hourly_summary(df, start_day, end_day)
    weekday = compute_weekday_summary(bundle.daily_df, start_day, end_day)
    weekday_hour = compute_weekday_hour_summary(df, start_day, end_day)
    return {
        "category": category if category is not None else pd.DataFrame(),
        "brand": brand if brand is not None else pd.DataFrame(),
        "product": product if product is not None else pd.DataFrame(),
        "budtender": budtender if budtender is not None else pd.DataFrame(),
        "cart": cart if cart is not None else pd.DataFrame(),
        "hourly": hourly if hourly is not None else pd.DataFrame(),
        "weekday": weekday if weekday is not None else pd.DataFrame(),
        "weekday_hour": weekday_hour if weekday_hour is not None else pd.DataFrame(),
    }


def load_archived_monthly_summary_metrics(month_key: str) -> Optional[Dict[str, Any]]:
    summary_path = MONTHLY_DATA_ROOT / month_key / "monthly_summary.json"
    if not summary_path.exists():
        return None
    try:
        payload = json.loads(summary_path.read_text(encoding="utf-8"))
        metrics = payload.get("metrics", {})
        if isinstance(metrics, dict):
            metrics = dict(metrics)
            metrics["_comparison_source"] = str(summary_path)
            return metrics
    except Exception:
        return None
    return None


def load_comparison_metrics(start_day: date, end_day: date, warnings: List[Dict[str, Any]]) -> Optional[Dict[str, Any]]:
    key = month_key_for_range(start_day, end_day)
    archived = load_archived_monthly_summary_metrics(key)
    if archived:
        return archived
    monthly_dir = MONTHLY_RAW_ROOT / key
    paths = list_sales_export_paths(monthly_dir)
    source = monthly_dir if paths else None
    if not paths:
        source = find_daily_folder_covering(start_day, end_day)
        paths = list_sales_export_paths(source) if source else {}
    if not paths:
        warnings.append({
            "severity": "Low",
            "message": f"Comparison data unavailable for {start_day.isoformat()} to {end_day.isoformat()}.",
        })
        return None
    try:
        local_warnings: List[Dict[str, Any]] = []
        bundles = load_store_bundles(paths, start_day, end_day, workers=min(4, len(paths)), warnings=local_warnings)
        all_daily = aggregate_daily({abbr: b.daily_df for abbr, b in bundles.items()}, start_day, end_day)
        out = metrics_for_range(all_daily, start_day, end_day)
        out["_comparison_source"] = str(source)
        return out
    except Exception as exc:
        warnings.append({
            "severity": "Low",
            "message": f"Comparison load failed for {key}: {exc}",
        })
        return None


def month_start_offset(day: date, months_back: int) -> date:
    month = day.month - months_back
    year = day.year
    while month <= 0:
        month += 12
        year -= 1
    return date(year, month, 1)


def load_trailing_three_month_average(start_day: date) -> Dict[str, Any]:
    rows: List[Dict[str, Any]] = []
    for months_back in [1, 2, 3]:
        month_start = month_start_offset(start_day, months_back)
        metrics = load_archived_monthly_summary_metrics(month_start.strftime("%Y-%m"))
        if metrics:
            rows.append(metrics)
    if not rows:
        return {}
    keys = [
        "net_revenue",
        "gross_sales",
        "profit",
        "profit_real",
        "margin_real",
        "tickets",
        "basket",
        "discount_rate",
        "closing_new_customers",
        "closing_customers",
        "closing_new_customer_rate",
        "inventory_end_value",
        "inventory_value_change",
    ]
    out: Dict[str, Any] = {"trailing_3mo_month_count": len(rows)}
    for key in keys:
        values = [as_float(row.get(key)) for row in rows if row.get(key) not in (None, "N/A")]
        if values:
            out[f"trailing_3mo_avg_{key}"] = sum(values) / len(values)
    return out


def load_archived_store_scorecards(month_key: str) -> pd.DataFrame:
    path = MONTHLY_DATA_ROOT / month_key / "monthly_store_scorecards.csv"
    if not path.exists():
        return pd.DataFrame()
    try:
        return pd.read_csv(path)
    except Exception:
        return pd.DataFrame()


def add_store_comparison_columns(
    current: pd.DataFrame,
    previous_month: pd.DataFrame,
    same_month_previous_year: pd.DataFrame,
) -> pd.DataFrame:
    if current is None or current.empty:
        return current
    out = current.copy()
    metrics = ["net_revenue", "profit", "profit_real", "margin_real", "tickets", "basket", "discount_rate"]
    for prefix, comp in [
        ("previous_month", previous_month),
        ("same_month_previous_year", same_month_previous_year),
    ]:
        if comp is None or comp.empty or "store" not in comp.columns:
            for metric in metrics:
                out[f"{prefix}_{metric}"] = "N/A"
                out[f"{prefix}_{metric}_change"] = "N/A"
            continue
        keep = ["store"] + [metric for metric in metrics if metric in comp.columns]
        renamed = comp[keep].copy().rename(columns={metric: f"{prefix}_{metric}" for metric in keep if metric != "store"})
        out = out.merge(renamed, on="store", how="left")
        for metric in metrics:
            comp_col = f"{prefix}_{metric}"
            if comp_col not in out.columns:
                out[comp_col] = "N/A"
            out[comp_col] = out[comp_col].where(out[comp_col].notna(), "N/A")
            change_col = f"{prefix}_{metric}_change"
            if metric in {"margin_real", "discount_rate"}:
                out[change_col] = out.apply(
                    lambda row: "N/A" if row[comp_col] == "N/A" else as_float(row.get(metric)) - as_float(row.get(comp_col)),
                    axis=1,
                )
            else:
                out[change_col] = out.apply(
                    lambda row: "N/A" if row[comp_col] == "N/A" else percentage_change(as_float(row.get(metric)), as_float(row.get(comp_col))),
                    axis=1,
                )
    return out


def validate_monthly_run(
    bundles: Dict[str, StoreBundle],
    all_daily: pd.DataFrame,
    all_metrics: Dict[str, Any],
    store_scorecards: pd.DataFrame,
    warnings: List[Dict[str, Any]],
) -> None:
    if not bundles:
        raise SystemExit("Validation failed: no store exports were loaded.")
    if as_float(all_metrics.get("net_revenue")) == 0.0:
        raise SystemExit("Validation failed: target month has no net revenue data.")

    required_missing = []
    for bundle in bundles.values():
        if not bundle.detected_columns.get("date"):
            required_missing.append(f"{bundle.abbr}: date")
        if not bundle.detected_columns.get("net_sales"):
            required_missing.append(f"{bundle.abbr}: net sales")
        optional_missing = [
            name for name in ["gross_sales", "profit", "cogs", "employee", "category", "product"]
            if not bundle.detected_columns.get(name)
        ]
        if optional_missing:
            warnings.append({
                "severity": "Low",
                "message": f"{bundle.abbr} missing optional columns: {', '.join(optional_missing)}.",
            })
    if required_missing:
        raise SystemExit("Validation failed: " + "; ".join(required_missing))

    scorecard_net = as_float(store_scorecards["net_revenue"].sum()) if store_scorecards is not None and not store_scorecards.empty else 0.0
    all_net = as_float(all_metrics.get("net_revenue"))
    if abs(scorecard_net - all_net) > 1.0:
        warnings.append({
            "severity": "Medium",
            "message": f"Store totals differ from all-store total by {money(scorecard_net - all_net)}.",
        })

    for key in ["margin", "margin_real", "discount_rate", "basket"]:
        if not math.isfinite(as_float(all_metrics.get(key))):
            warnings.append({
                "severity": "Medium",
                "message": f"All-store metric {key} was not finite and was treated as 0.",
            })


def json_safe(value: Any) -> Any:
    if isinstance(value, (np.integer,)):
        return int(value)
    if isinstance(value, (np.floating,)):
        return float(value)
    if isinstance(value, (datetime, pd.Timestamp)):
        return value.isoformat()
    if isinstance(value, date):
        return value.isoformat()
    if isinstance(value, Path):
        return str(value)
    if isinstance(value, dict):
        return {str(k): json_safe(v) for k, v in value.items()}
    if isinstance(value, list):
        return [json_safe(v) for v in value]
    if pd.isna(value) if not isinstance(value, (list, dict, tuple, set)) else False:
        return None
    return value


def write_json(path: Path, payload: Any) -> None:
    path.write_text(json.dumps(json_safe(payload), indent=2), encoding="utf-8")


def write_data_exports(
    data_dir: Path,
    month_key: str,
    start_day: date,
    end_day: date,
    all_metrics: Dict[str, Any],
    store_scorecards: pd.DataFrame,
    category_summary: pd.DataFrame,
    brand_summary: pd.DataFrame,
    product_summary: pd.DataFrame,
    budtender_summary: pd.DataFrame,
    cart_distribution: pd.DataFrame,
    adjusted_cart_metrics: pd.DataFrame,
    hourly_summary: pd.DataFrame,
    weekday_summary: pd.DataFrame,
    weekday_hour_summary: pd.DataFrame,
    brand_store_matrix: pd.DataFrame,
    category_store_matrix: pd.DataFrame,
    new_customer_summary: pd.DataFrame,
    new_customer_daily: pd.DataFrame,
    new_customer_raw: pd.DataFrame,
    closing_summary: pd.DataFrame,
    inventory_summary: pd.DataFrame,
    inventory_start: pd.DataFrame,
    inventory_end: pd.DataFrame,
    daily_metrics: pd.DataFrame,
    store_daily_detail: pd.DataFrame,
    kickback_summary: pd.DataFrame,
    all_kickback_detail: pd.DataFrame,
    store_kickback_summary: pd.DataFrame,
    tax_rounding_summary: pd.DataFrame,
    tax_rounding_daily: pd.DataFrame,
    action_items: List[Dict[str, str]],
    warnings: List[Dict[str, Any]],
    files_used: List[str],
    include_data_book: bool,
) -> List[Path]:
    data_dir.mkdir(parents=True, exist_ok=True)
    outputs: List[Path] = []

    summary_payload = {
        "month": month_key,
        "date_range": {"start": start_day, "end": end_day},
        "metrics": all_metrics,
        "action_items": action_items,
        "files_used": files_used,
        "generated_at": datetime.now(ZoneInfo(REPORT_TZ)).isoformat(),
    }
    summary_path = data_dir / "monthly_summary.json"
    write_json(summary_path, summary_payload)
    outputs.append(summary_path)

    warnings_path = data_dir / "monthly_warnings.json"
    write_json(warnings_path, warnings)
    outputs.append(warnings_path)

    csv_map = {
        "monthly_store_scorecards.csv": store_scorecards,
        "monthly_category_summary.csv": category_summary,
        "monthly_brand_summary.csv": brand_summary,
        "monthly_product_summary.csv": product_summary,
        "monthly_budtender_summary.csv": budtender_summary,
        "monthly_cart_distribution.csv": cart_distribution,
        "monthly_adjusted_cart_metrics.csv": adjusted_cart_metrics,
        "monthly_hourly_summary.csv": hourly_summary,
        "monthly_weekday_summary.csv": weekday_summary,
        "monthly_weekday_hour_summary.csv": weekday_hour_summary,
        "monthly_brand_store_matrix.csv": brand_store_matrix,
        "monthly_category_store_matrix.csv": category_store_matrix,
        "monthly_new_customer_summary.csv": new_customer_summary,
        "monthly_new_customer_daily.csv": new_customer_daily,
        "monthly_new_customer_raw.csv": new_customer_raw,
        "monthly_closing_report_summary.csv": closing_summary,
        "monthly_inventory_summary.csv": inventory_summary,
        "monthly_inventory_snapshot_start.csv": inventory_start,
        "monthly_inventory_snapshot_end.csv": inventory_end,
        "monthly_daily_detail.csv": daily_metrics,
        "monthly_store_daily_detail.csv": store_daily_detail,
        "monthly_kickback_detail.csv": kickback_summary,
        "monthly_store_kickback_detail.csv": all_kickback_detail,
        "monthly_store_kickback_summary.csv": store_kickback_summary,
        "monthly_tax_rounding_loss_summary.csv": tax_rounding_summary,
        "monthly_tax_rounding_loss_daily.csv": tax_rounding_daily,
    }
    for filename, df in csv_map.items():
        path = data_dir / filename
        (df if df is not None else pd.DataFrame()).to_csv(path, index=False)
        outputs.append(path)

    if include_data_book:
        book_path = data_dir / safe_filename(f"Monthly Owner Data Book - {month_key}.xlsx")
        with pd.ExcelWriter(book_path, engine="openpyxl") as writer:
            pd.DataFrame([all_metrics]).to_excel(writer, sheet_name="Executive Summary", index=False)
            store_scorecards.to_excel(writer, sheet_name="Store Scorecards", index=False)
            category_summary.to_excel(writer, sheet_name="Categories", index=False)
            brand_summary.to_excel(writer, sheet_name="Brands", index=False)
            product_summary.to_excel(writer, sheet_name="Products", index=False)
            budtender_summary.to_excel(writer, sheet_name="Budtenders", index=False)
            pd.DataFrame({
                "metric": ["discount", "discount_main", "loyalty_discount", "discount_rate"],
                "value": [
                    all_metrics.get("discount"),
                    all_metrics.get("discount_main"),
                    all_metrics.get("loyalty_discount"),
                    all_metrics.get("discount_rate"),
                ],
            }).to_excel(writer, sheet_name="Discounts", index=False)
            kickback_summary.to_excel(writer, sheet_name="Kickbacks", index=False)
            cart_distribution.to_excel(writer, sheet_name="Cart Distribution", index=False)
            adjusted_cart_metrics.to_excel(writer, sheet_name="Adjusted Cart Metrics", index=False)
            daily_metrics.to_excel(writer, sheet_name="Daily Metrics", index=False)
            store_daily_detail.to_excel(writer, sheet_name="Store Daily Detail", index=False)
            hourly_summary.to_excel(writer, sheet_name="Hourly Metrics", index=False)
            weekday_summary.to_excel(writer, sheet_name="Weekday Summary", index=False)
            weekday_hour_summary.to_excel(writer, sheet_name="Weekday Hour Metrics", index=False)
            brand_store_matrix.to_excel(writer, sheet_name="Brand Store Matrix", index=False)
            category_store_matrix.to_excel(writer, sheet_name="Category Store Matrix", index=False)
            new_customer_summary.to_excel(writer, sheet_name="New Customer Summary", index=False)
            new_customer_daily.to_excel(writer, sheet_name="New Customer Daily", index=False)
            new_customer_raw.to_excel(writer, sheet_name="New Customer Raw", index=False)
            closing_summary.to_excel(writer, sheet_name="Closing Report Summary", index=False)
            inventory_summary.to_excel(writer, sheet_name="Inventory Summary", index=False)
            inventory_start.to_excel(writer, sheet_name="Inventory Start", index=False)
            inventory_end.to_excel(writer, sheet_name="Inventory End", index=False)
            all_kickback_detail.to_excel(writer, sheet_name="Store Kickback Detail", index=False)
            store_kickback_summary.to_excel(writer, sheet_name="Store Kickbacks", index=False)
            tax_rounding_summary.to_excel(writer, sheet_name="Tax Rounding Summary", index=False)
            tax_rounding_daily.to_excel(writer, sheet_name="Tax Rounding Daily", index=False)
            pd.DataFrame(warnings).to_excel(writer, sheet_name="Warnings", index=False)
        outputs.append(book_path)

    return outputs


def try_combine_pdfs(pdf_paths: List[Path], combined_path: Path, warnings: List[Dict[str, Any]]) -> Optional[Path]:
    if len(pdf_paths) < 2:
        return None
    try:
        import fitz
    except Exception as exc:
        warnings.append({
            "severity": "Low",
            "message": f"Combined PDF skipped because PyMuPDF is unavailable: {exc}",
        })
        return None
    try:
        combined = fitz.open()
        for path in pdf_paths:
            with fitz.open(str(path)) as doc:
                combined.insert_pdf(doc)
        combined.save(str(combined_path))
        combined.close()
        return combined_path
    except Exception as exc:
        warnings.append({
            "severity": "Low",
            "message": f"Combined PDF skipped: {exc}",
        })
        return None


def biggest_concern(action_items: List[Dict[str, str]]) -> str:
    for severity in ["High", "Medium", "Low"]:
        for item in action_items:
            if item.get("severity") == severity:
                return f"{item.get('issue')} ({item.get('metric_value')})"
    return "No threshold exceptions detected"


def build_monthly_email_intro(
    month_key: str,
    start_day: date,
    end_day: date,
    all_metrics: Dict[str, Any],
    store_scorecards: pd.DataFrame,
    action_items: List[Dict[str, str]],
    attachment_count: int,
    wins: Optional[List[str]] = None,
    concerns: Optional[List[str]] = None,
) -> str:
    best_store = "N/A"
    if store_scorecards is not None and not store_scorecards.empty:
        row = store_scorecards.sort_values("net_revenue", ascending=False).iloc[0]
        best_store = f"{row['store']} ({money(row['net_revenue'])})"
    top_action = action_items[0].get("recommended_action") if action_items else "No threshold action required"
    lines = [
        f"Month: {month_key}",
        f"Net revenue: {money(all_metrics.get('net_revenue'))}",
        f"Profit: {money(all_metrics.get('profit'))}",
        f"Real margin: {pct1(all_metrics.get('margin_real'))}",
        f"Tickets: {int(as_float(all_metrics.get('tickets'))):,}",
        f"Avg cart > $1: {money1(all_metrics.get('basket'))}",
        f"OTD round-up opportunity: {money(all_metrics.get('tax_rounding_loss'))}",
        f"Customers: {int(as_float(all_metrics.get('closing_customers'))):,}" if all_metrics.get("closing_summary_has_data") else "Customers: N/A",
        f"New customers: {int(as_float(all_metrics.get('closing_new_customers'))):,} ({pct1(all_metrics.get('closing_new_customer_rate'))})" if all_metrics.get("closing_summary_has_data") else "New customers: N/A",
        f"Ending inventory: {optional_money(all_metrics.get('inventory_end_value'))}" if all_metrics.get("inventory_has_data") else "Ending inventory: N/A",
        f"Best store: {best_store}",
        f"Top win: {(wins or ['N/A'])[0]}",
        f"Top concern: {(concerns or [biggest_concern(action_items)])[0]}",
        f"Top action: {top_action}",
        f"PDF count: {attachment_count}",
        f"Date range: {start_day.isoformat()} to {end_day.isoformat()}",
    ]
    return "\n".join(lines)


def html_escape(value: Any) -> str:
    return escape_html(value)


def email_metric_card(label: str, value: str, detail: str = "", accent: str = HEX_GREEN) -> str:
    return f"""
      <td width="50%" style="padding:6px;">
        <table role="presentation" width="100%" cellspacing="0" cellpadding="0" style="border-collapse:separate;border-spacing:0;background:#FFFFFF;border:1px solid #E5E7EB;border-radius:12px;overflow:hidden;">
          <tr><td style="height:4px;background:{accent};font-size:0;line-height:0;">&nbsp;</td></tr>
          <tr>
            <td style="padding:12px 14px 11px 14px;">
              <div style="font-family:-apple-system,BlinkMacSystemFont,'Segoe UI',Arial,sans-serif;font-size:11px;line-height:14px;color:#6B7280;font-weight:800;text-transform:uppercase;letter-spacing:.4px;">{html_escape(label)}</div>
              <div style="font-family:-apple-system,BlinkMacSystemFont,'Segoe UI',Arial,sans-serif;font-size:23px;line-height:27px;color:#050505;font-weight:900;margin-top:4px;">{html_escape(value)}</div>
              <div style="font-family:-apple-system,BlinkMacSystemFont,'Segoe UI',Arial,sans-serif;font-size:12px;line-height:17px;color:#6B7280;margin-top:3px;">{html_escape(detail)}</div>
            </td>
          </tr>
        </table>
      </td>
    """


def email_panel(title: str, items: List[str], accent: str) -> str:
    list_html = ""
    for item in (items or [])[:3]:
        list_html += (
            f"<tr><td style=\"padding:7px 0;border-bottom:1px solid #EEF0F2;\">"
            f"<div style=\"font-family:-apple-system,BlinkMacSystemFont,'Segoe UI',Arial,sans-serif;font-size:13px;line-height:18px;color:#111827;\">{html_escape(item)}</div>"
            f"</td></tr>"
        )
    if not list_html:
        list_html = (
            "<tr><td style=\"padding:7px 0;\">"
            "<div style=\"font-family:-apple-system,BlinkMacSystemFont,'Segoe UI',Arial,sans-serif;font-size:13px;line-height:18px;color:#6B7280;\">No threshold exceptions detected.</div>"
            "</td></tr>"
        )
    return f"""
      <td width="33.33%" style="padding:6px;vertical-align:top;">
        <table role="presentation" width="100%" cellspacing="0" cellpadding="0" style="height:100%;border-collapse:separate;border-spacing:0;background:#FFFFFF;border:1px solid #E5E7EB;border-radius:12px;overflow:hidden;">
          <tr><td style="height:4px;background:{accent};font-size:0;line-height:0;">&nbsp;</td></tr>
          <tr>
            <td style="padding:12px 14px;">
              <div style="font-family:-apple-system,BlinkMacSystemFont,'Segoe UI',Arial,sans-serif;font-size:12px;line-height:15px;color:#050505;font-weight:900;text-transform:uppercase;letter-spacing:.5px;">{html_escape(title)}</div>
              <table role="presentation" width="100%" cellspacing="0" cellpadding="0" style="margin-top:6px;border-collapse:collapse;">{list_html}</table>
            </td>
          </tr>
        </table>
      </td>
    """


def build_monthly_email_plain_text(
    month_key: str,
    start_day: date,
    end_day: date,
    all_metrics: Dict[str, Any],
    store_scorecards: pd.DataFrame,
    action_items: List[Dict[str, str]],
    pdf_paths: List[Path],
    wins: Optional[List[str]] = None,
    concerns: Optional[List[str]] = None,
) -> str:
    return build_monthly_email_intro(
        month_key,
        start_day,
        end_day,
        all_metrics,
        store_scorecards,
        action_items,
        len(pdf_paths),
        wins=wins,
        concerns=concerns,
    )


def build_monthly_email_html(
    month_key: str,
    start_day: date,
    end_day: date,
    all_metrics: Dict[str, Any],
    store_scorecards: pd.DataFrame,
    action_items: List[Dict[str, str]],
    pdf_paths: List[Path],
    wins: Optional[List[str]] = None,
    concerns: Optional[List[str]] = None,
) -> str:
    generated = datetime.now(ZoneInfo(REPORT_TZ)).strftime("%b %-d, %Y %-I:%M %p %Z")
    attachment_count = len(pdf_paths or [])
    attachment_size = sum(path.stat().st_size for path in pdf_paths if path.exists())
    size_label = f"{attachment_size / (1024 * 1024):.1f} MB" if attachment_size >= 1024 * 1024 else f"{attachment_size / 1024:.1f} KB"

    best_store = "N/A"
    if store_scorecards is not None and not store_scorecards.empty:
        best = store_scorecards.sort_values("net_revenue", ascending=False).iloc[0]
        best_store = f"{best['store']} - {money(best['net_revenue'])}"

    concern_items = concerns or [biggest_concern(action_items)]
    action_items_text = [item.get("recommended_action", "") for item in action_items[:3] if item.get("recommended_action")]
    if not action_items_text:
        action_items_text = ["No threshold action required."]

    discount_detail = f"Total discounts {money(all_metrics.get('discount'))}"
    inventory_detail = "Inventory API unavailable"
    if all_metrics.get("inventory_has_data"):
        inventory_detail = f"Change {optional_signed_money(all_metrics.get('inventory_value_change'))}"

    kpi_rows = [
        email_metric_card("Net Revenue", money(all_metrics.get("net_revenue")), f"Best store {best_store}", HEX_GREEN),
        email_metric_card("Profit", money(all_metrics.get("profit")), f"Real profit {money(all_metrics.get('profit_real'))}", HEX_GREEN),
        email_metric_card("Real Margin", pct1(all_metrics.get("margin_real")), f"KB margin {pct1(all_metrics.get('margin'))}", HEX_YELLOW),
        email_metric_card("Customers", f"{int(as_float(all_metrics.get('closing_customers'))):,}", f"{pct1(all_metrics.get('closing_new_customer_rate'))} new", HEX_GREEN),
        email_metric_card("New Customers", f"{int(as_float(all_metrics.get('closing_new_customers'))):,}", "Official closing report total", HEX_GREEN),
        email_metric_card("Tickets", f"{int(as_float(all_metrics.get('tickets'))):,}", f"{as_float(all_metrics.get('items_per_ticket')):.2f} items / ticket", HEX_GREEN),
        email_metric_card("Avg Cart > $1", money1(all_metrics.get("basket")), f"{int(as_float(all_metrics.get('low_value_cart_count_excluded_from_basket'))):,} tiny carts excluded", HEX_GREEN),
        email_metric_card("Discount Rate", pct1(all_metrics.get("discount_rate")), discount_detail, "#F59E0B"),
        email_metric_card("OTD Round-Up Loss", money(all_metrics.get("tax_rounding_loss")), f"{money2(all_metrics.get('tax_rounding_avg_loss_per_transaction'))} avg / transaction", "#F59E0B"),
        email_metric_card("Ending Inventory", optional_money(all_metrics.get("inventory_end_value")), inventory_detail, "#111827"),
        email_metric_card("Attachments", f"{attachment_count}", f"PDF packet + data book where included, {size_label}", HEX_YELLOW),
    ]
    kpi_table = ""
    for i in range(0, len(kpi_rows), 2):
        kpi_table += f"<tr>{''.join(kpi_rows[i:i + 2])}</tr>"

    store_rows = ""
    if store_scorecards is not None and not store_scorecards.empty:
        top_stores = store_scorecards.sort_values("net_revenue", ascending=False).head(6)
        for _, row in top_stores.iterrows():
            status = str(row.get("status", "") or "")
            status_bg = "#DCFCE7" if status == "Strong" else "#FEF3C7" if status == "Watch" else "#FEE2E2"
            status_fg = "#047857" if status == "Strong" else "#92400E" if status == "Watch" else "#991B1B"
            store_rows += f"""
              <tr>
                <td style="padding:9px 10px;border-bottom:1px solid #EEF0F2;font-family:-apple-system,BlinkMacSystemFont,'Segoe UI',Arial,sans-serif;font-size:13px;color:#111827;font-weight:900;">{html_escape(row.get('store'))}</td>
                <td align="right" style="padding:9px 10px;border-bottom:1px solid #EEF0F2;font-family:-apple-system,BlinkMacSystemFont,'Segoe UI',Arial,sans-serif;font-size:13px;color:#111827;">{html_escape(money(row.get('net_revenue')))}</td>
                <td align="right" style="padding:9px 10px;border-bottom:1px solid #EEF0F2;font-family:-apple-system,BlinkMacSystemFont,'Segoe UI',Arial,sans-serif;font-size:13px;color:#111827;">{html_escape(pct1(row.get('margin_real')))}</td>
                <td align="right" style="padding:9px 10px;border-bottom:1px solid #EEF0F2;font-family:-apple-system,BlinkMacSystemFont,'Segoe UI',Arial,sans-serif;font-size:13px;color:#111827;">{html_escape(pct1(row.get('discount_rate')))}</td>
                <td align="right" style="padding:9px 10px;border-bottom:1px solid #EEF0F2;"><span style="display:inline-block;padding:4px 8px;border-radius:999px;background:{status_bg};color:{status_fg};font-family:-apple-system,BlinkMacSystemFont,'Segoe UI',Arial,sans-serif;font-size:11px;line-height:13px;font-weight:900;">{html_escape(status)}</span></td>
              </tr>
            """
    if not store_rows:
        store_rows = (
            "<tr><td colspan=\"5\" style=\"padding:12px;font-family:-apple-system,BlinkMacSystemFont,'Segoe UI',Arial,sans-serif;font-size:13px;color:#6B7280;\">Store scorecards unavailable.</td></tr>"
        )

    pdf_rows = ""
    for path in pdf_paths[:8]:
        label = path.name.replace(" - Monthly Owner Review - ", " - ").replace(".pdf", "")
        pdf_rows += (
            f"<tr><td style=\"padding:7px 0;border-bottom:1px solid #1F2937;font-family:-apple-system,BlinkMacSystemFont,'Segoe UI',Arial,sans-serif;font-size:12px;line-height:16px;color:#D1D5DB;\">{html_escape(label)}</td></tr>"
        )

    return f"""<!doctype html>
<html>
  <body style="margin:0;padding:0;background:#F3F4F6;">
    <center style="width:100%;background:#F3F4F6;">
      <table role="presentation" width="100%" cellspacing="0" cellpadding="0" style="border-collapse:collapse;background:#F3F4F6;">
        <tr>
          <td align="center" style="padding:24px 12px;">
            <table role="presentation" width="100%" cellspacing="0" cellpadding="0" style="max-width:760px;border-collapse:separate;border-spacing:0;background:#FFFFFF;border:1px solid #E5E7EB;border-radius:18px;overflow:hidden;box-shadow:0 12px 38px rgba(17,24,39,.10);">
              <tr>
                <td style="padding:0;background:#050505;">
                  <table role="presentation" width="100%" cellspacing="0" cellpadding="0" style="border-collapse:collapse;">
                    <tr>
                      <td style="padding:26px 28px 22px 28px;">
                        <div style="font-family:-apple-system,BlinkMacSystemFont,'Segoe UI',Arial,sans-serif;font-size:12px;line-height:16px;color:#FFF200;font-weight:900;text-transform:uppercase;letter-spacing:1.5px;">Buzz Cannabis</div>
                        <div style="font-family:-apple-system,BlinkMacSystemFont,'Segoe UI',Arial,sans-serif;font-size:31px;line-height:36px;color:#FFFFFF;font-weight:900;margin-top:7px;">Monthly Owner Review</div>
                        <div style="font-family:-apple-system,BlinkMacSystemFont,'Segoe UI',Arial,sans-serif;font-size:14px;line-height:20px;color:#D1D5DB;margin-top:6px;">{html_escape(month_key)} | {start_day.isoformat()} to {end_day.isoformat()}</div>
                      </td>
                      <td align="right" style="padding:26px 28px 22px 8px;vertical-align:top;">
                        <span style="display:inline-block;background:#00AE6F;color:#FFFFFF;border-radius:999px;padding:8px 11px;font-family:-apple-system,BlinkMacSystemFont,'Segoe UI',Arial,sans-serif;font-size:12px;line-height:14px;font-weight:900;">OWNER PACKET</span>
                      </td>
                    </tr>
                  </table>
                </td>
              </tr>
              <tr>
                <td style="padding:0;background:#050505;">
                  <table role="presentation" width="100%" cellspacing="0" cellpadding="0" style="border-collapse:collapse;">
                    <tr>
                      <td width="45%" style="height:6px;background:#FFF200;font-size:0;line-height:0;">&nbsp;</td>
                      <td width="55%" style="height:6px;background:#00AE6F;font-size:0;line-height:0;">&nbsp;</td>
                    </tr>
                  </table>
                </td>
              </tr>
              <tr>
                <td style="padding:18px 22px 10px 22px;background:#F9FAFB;">
                  <table role="presentation" width="100%" cellspacing="0" cellpadding="0" style="border-collapse:collapse;">{kpi_table}</table>
                </td>
              </tr>
              <tr>
                <td style="padding:4px 22px 18px 22px;background:#F9FAFB;">
                  <table role="presentation" width="100%" cellspacing="0" cellpadding="0" style="border-collapse:collapse;">
                    <tr>
                      {email_panel("Wins", wins or [], HEX_GREEN)}
                      {email_panel("Watch", concern_items, "#F59E0B")}
                      {email_panel("Actions", action_items_text, HEX_YELLOW)}
                    </tr>
                  </table>
                </td>
              </tr>
              <tr>
                <td style="padding:0 28px 20px 28px;background:#FFFFFF;">
                  <div style="font-family:-apple-system,BlinkMacSystemFont,'Segoe UI',Arial,sans-serif;font-size:16px;line-height:20px;color:#050505;font-weight:900;margin-bottom:8px;">Store Scoreboard</div>
                  <table role="presentation" width="100%" cellspacing="0" cellpadding="0" style="border-collapse:collapse;border:1px solid #E5E7EB;border-radius:12px;overflow:hidden;">
                    <tr>
                      <th align="left" style="padding:9px 10px;background:#050505;color:#FFF200;font-family:-apple-system,BlinkMacSystemFont,'Segoe UI',Arial,sans-serif;font-size:11px;text-transform:uppercase;letter-spacing:.4px;">Store</th>
                      <th align="right" style="padding:9px 10px;background:#050505;color:#FFF200;font-family:-apple-system,BlinkMacSystemFont,'Segoe UI',Arial,sans-serif;font-size:11px;text-transform:uppercase;letter-spacing:.4px;">Revenue</th>
                      <th align="right" style="padding:9px 10px;background:#050505;color:#FFF200;font-family:-apple-system,BlinkMacSystemFont,'Segoe UI',Arial,sans-serif;font-size:11px;text-transform:uppercase;letter-spacing:.4px;">Margin</th>
                      <th align="right" style="padding:9px 10px;background:#050505;color:#FFF200;font-family:-apple-system,BlinkMacSystemFont,'Segoe UI',Arial,sans-serif;font-size:11px;text-transform:uppercase;letter-spacing:.4px;">Discount</th>
                      <th align="right" style="padding:9px 10px;background:#050505;color:#FFF200;font-family:-apple-system,BlinkMacSystemFont,'Segoe UI',Arial,sans-serif;font-size:11px;text-transform:uppercase;letter-spacing:.4px;">Status</th>
                    </tr>
                    {store_rows}
                  </table>
                </td>
              </tr>
              <tr>
                <td style="padding:18px 28px 22px 28px;background:#111827;">
                  <table role="presentation" width="100%" cellspacing="0" cellpadding="0" style="border-collapse:collapse;">
                    <tr>
                      <td style="vertical-align:top;">
                        <div style="font-family:-apple-system,BlinkMacSystemFont,'Segoe UI',Arial,sans-serif;font-size:13px;line-height:17px;color:#FFFFFF;font-weight:900;">Attachments</div>
                        <div style="font-family:-apple-system,BlinkMacSystemFont,'Segoe UI',Arial,sans-serif;font-size:12px;line-height:17px;color:#9CA3AF;margin-top:3px;">{attachment_count} files attached | {html_escape(size_label)} total | Generated {html_escape(generated)}</div>
                        <table role="presentation" width="100%" cellspacing="0" cellpadding="0" style="margin-top:8px;border-collapse:collapse;">{pdf_rows}</table>
                      </td>
                    </tr>
                  </table>
                </td>
              </tr>
            </table>
          </td>
        </tr>
      </table>
    </center>
  </body>
</html>"""


def send_or_preview_email(
    args: argparse.Namespace,
    month_key: str,
    start_day: date,
    end_day: date,
    pdf_paths: List[Path],
    all_metrics: Dict[str, Any],
    store_scorecards: pd.DataFrame,
    action_items: List[Dict[str, str]],
    wins: Optional[List[str]] = None,
    concerns: Optional[List[str]] = None,
) -> str:
    if args.no_email:
        print("[EMAIL] Skipped (--no-email).")
        return "skipped"

    subject = f"Buzz Monthly Owner Review — {month_key}"
    intro = build_monthly_email_intro(month_key, start_day, end_day, all_metrics, store_scorecards, action_items, len(pdf_paths), wins=wins, concerns=concerns)
    plain_text = build_monthly_email_plain_text(month_key, start_day, end_day, all_metrics, store_scorecards, action_items, pdf_paths, wins=wins, concerns=concerns)
    html_body = build_monthly_email_html(month_key, start_day, end_day, all_metrics, store_scorecards, action_items, pdf_paths, wins=wins, concerns=concerns)
    title = f"Monthly Owner Review • {month_key}"

    recipients = [
        "anthony@buzzcannabis.com",
        "ray@buzzcannabis.com",
        "kevin@buzzcannabis.com",
        "stevei@buzzcannabis.com",
        "andyhirmez@yahoo.com",
        "stevegabbo@hotmail.com",
    ]
    if args.to_email:
        recipients = []
        for value in args.to_email:
            recipients.extend([part.strip() for part in str(value).split(",") if part.strip()])

    if args.dry_run_email:
        print("[EMAIL] Dry run only. No email sent.")
        print(f"Subject: {subject}")
        print("To: " + ", ".join(recipients))
        print(intro)
        print("Attachments:")
        for path in pdf_paths:
            print(f"- {path}")
        _, _, data_dir = ensure_monthly_dirs(month_key)
        preview_path = data_dir / "monthly_email_preview.html"
        preview_path.write_text(html_body, encoding="utf-8")
        print(f"HTML Preview: {preview_path}")
        return "dry-run"

    send_owner_snapshot_email(
        pdf_paths=[str(p) for p in pdf_paths],
        report_day=end_day,
        data_start=start_day,
        data_end=end_day,
        executive_summary=None,
        store_summaries=None,
        subject_override=subject,
        title_override=title,
        intro_override=intro,
        html_override=html_body,
        plain_text_override=plain_text,
        to_email=recipients,
    )
    return "sent"


def detail_top_n(detail_level: str) -> int:
    if detail_level == "executive":
        return 5
    if detail_level == "deep":
        return 15
    return 10


def appendix_top_n(detail_level: str) -> int:
    if detail_level == "deep":
        return 100
    if detail_level == "executive":
        return 0
    return 30


def resolve_main_table_rows(detail_level: str, main_top_n: Optional[int], legacy_override: Optional[int]) -> int:
    override = main_top_n if main_top_n is not None else legacy_override
    if override is not None:
        return max(1, int(override))
    return detail_top_n(detail_level)


def resolve_appendix_rows(detail_level: str, override: Optional[int]) -> int:
    if override is not None:
        return max(0, int(override))
    return appendix_top_n(detail_level)


def resolve_full_detail_pdf(detail_level: str, override: Optional[bool]) -> bool:
    if override is not None:
        return bool(override)
    return detail_level in {"standard", "deep"}


def main() -> None:
    args = parse_cli_args()
    if args.run_export_api:
        args.run_export = True
        args.export_source = "api"
    setup_fonts()

    warnings: List[Dict[str, Any]] = []
    if args.pdf_style == "legacy":
        warnings.append({
            "severity": "Low",
            "message": "Legacy PDF style was requested; monthly reports now use the executive-clean layout.",
        })
    start_day, end_day, explicit_dates = resolve_date_window(args)
    month_key = month_key_for_range(start_day, end_day)

    if args.detail_level == "deep":
        args.include_data_book = True
    if args.summary_only:
        args.no_store_pdfs = True

    raw_dir, pdf_dir, data_dir = ensure_monthly_dirs(month_key)
    if args.closing_summary_only:
        closing_summary = fetch_monthly_closing_summary_from_api(
            start_day=start_day,
            end_day=end_day,
            data_dir=data_dir,
            env_file=args.api_env_file,
            workers=args.workers,
            warnings=warnings,
        )
        metrics: Dict[str, Any] = {}
        add_closing_summary_metrics(metrics, closing_summary)
        write_json(data_dir / "monthly_warnings.json", warnings)
        print("\nDone ✅")
        print("Monthly Closing Report Summary Complete")
        print(f"Month: {month_key}")
        print(f"Date Range: {start_day.isoformat()} → {end_day.isoformat()}")
        print(f"Closing Summary CSV: {data_dir / 'monthly_closing_report_summary.csv'}")
        print(f"New Customers: {int(as_float(metrics.get('closing_new_customers'))):,}")
        print(f"Customers: {int(as_float(metrics.get('closing_customers'))):,}")
        print(f"Transactions: {int(as_float(metrics.get('closing_transactions'))):,}")
        print(f"Net Sales: {money(metrics.get('closing_net_sales'))}")
        print(f"Warnings: {len(warnings)}")
        return

    start_day, end_day, month_key, raw_dir, abbr_to_path = resolve_raw_exports(
        args=args,
        start_day=start_day,
        end_day=end_day,
        month_key=month_key,
        explicit_dates=explicit_dates,
        warnings=warnings,
    )
    raw_dir, pdf_dir, data_dir = ensure_monthly_dirs(month_key)

    print(f"[RANGE] Monthly report {month_key}: {start_day.isoformat()} -> {end_day.isoformat()}")
    print(f"[RAW] {raw_dir}")
    print("[PARSE] Start")
    bundles = load_store_bundles(abbr_to_path, start_day, end_day, args.workers, warnings)
    print("[PARSE] End")

    all_daily = aggregate_daily({abbr: bundle.daily_df for abbr, bundle in bundles.items()}, start_day, end_day)
    all_raw = combine_raw_dfs(bundles, start_day, end_day)

    category_summary = compute_category_summary(all_raw, start_day, end_day)
    category_summary = category_summary if category_summary is not None else pd.DataFrame()
    brand_summary = compute_monthly_brand_summary(all_raw, start_day, end_day)
    product_summary = compute_monthly_product_summary(all_raw, start_day, end_day)
    budtender_summary = compute_budtender_summary(all_raw, start_day, end_day)
    budtender_summary = budtender_summary if budtender_summary is not None else pd.DataFrame()
    budtender_summary = apply_adjusted_basket_to_budtenders(all_raw, budtender_summary, start_day, end_day)
    cart_distribution = compute_cart_value_distribution(all_raw, start_day, end_day)
    cart_distribution = cart_distribution if cart_distribution is not None else pd.DataFrame()
    adjusted_cart_metrics = compute_adjusted_cart_metrics(all_raw, start_day, end_day)
    hourly_summary = compute_monthly_hourly_summary(all_raw, start_day, end_day)
    weekday_summary = compute_weekday_summary(all_daily, start_day, end_day)
    weekday_hour_summary = compute_weekday_hour_summary(all_raw, start_day, end_day)
    brand_store_matrix = compute_store_group_matrix(all_raw, start_day, end_day, "brand", top_n=20)
    category_store_matrix = compute_store_group_matrix(all_raw, start_day, end_day, "category", top_n=20)
    kickback_summary = compute_kickback_summary(all_raw)
    all_kickback_detail = compute_kickback_detail_by_store(all_raw)

    base_all_metrics = metrics_for_range(all_daily, start_day, end_day)
    all_metrics = enrich_monthly_metrics(base_all_metrics, all_daily, category_summary, brand_summary, product_summary)
    apply_adjusted_cart_metrics(all_metrics, bundles, adjusted_cart_metrics)
    all_metrics["kickback"] = as_float(kickback_summary["kickback"].sum()) if kickback_summary is not None and not kickback_summary.empty and "kickback" in kickback_summary.columns else 0.0
    store_scorecards = build_store_scorecards(bundles, all_metrics)
    tax_rounding_summary, tax_rounding_daily = compute_tax_rounding_loss(all_raw, start_day, end_day)
    store_scorecards = apply_tax_rounding_metrics(all_metrics, store_scorecards, tax_rounding_summary)
    all_daily = apply_tax_rounding_to_daily_detail(all_daily, bundles, tax_rounding_daily)
    weekday_summary = compute_weekday_summary(all_daily, start_day, end_day)
    sync_bundle_metrics_from_scorecards(bundles, store_scorecards)
    store_kickback_summary = compute_store_kickback_summary(all_raw, store_scorecards)
    if args.fetch_inventory_api:
        inventory_start, inventory_end = fetch_monthly_inventory_snapshots_from_api(
            start_day=start_day,
            end_day=end_day,
            data_dir=data_dir,
            env_file=args.api_env_file,
            workers=args.workers,
            warnings=warnings,
        )
    else:
        inventory_start, inventory_end = load_monthly_inventory_snapshots(data_dir)
    inventory_summary = build_inventory_summary(inventory_start, inventory_end, store_scorecards)
    add_inventory_metrics(all_metrics, inventory_summary)
    if args.fetch_closing_summary_api or args.fetch_new_customers_api or args.fetch_closing_api:
        closing_summary = fetch_monthly_closing_summary_from_api(
            start_day=start_day,
            end_day=end_day,
            data_dir=data_dir,
            env_file=args.api_env_file,
            workers=args.workers,
            warnings=warnings,
        )
    else:
        closing_summary = load_monthly_closing_summary(data_dir)
    add_closing_summary_metrics(all_metrics, closing_summary)
    store_scorecards = apply_closing_summary_to_scorecards(store_scorecards, closing_summary)
    sync_bundle_metrics_from_scorecards(bundles, store_scorecards)
    closing_dir = args.closing_report_dir or (MONTHLY_CLOSING_ROOT / month_key)
    if args.fetch_closing_api or args.fetch_closing_summary_api:
        closing_raw = fetch_monthly_closing_reports_from_api(
            start_day=start_day,
            end_day=end_day,
            closing_dir=closing_dir,
            env_file=args.api_env_file,
            workers=args.workers,
            warnings=warnings,
        )
        if closing_raw.empty and args.fetch_new_customers_api:
            warnings.append({
                "severity": "Medium",
                "message": "Daily closing-report new customer data was unavailable; falling back to customer profile creation dates, which may not match Backoffice closing-report totals.",
            })
            closing_raw = fetch_new_customer_profiles_from_api(
                start_day=start_day,
                end_day=end_day,
                closing_dir=closing_dir,
                env_file=args.api_env_file,
                warnings=warnings,
            )
        if closing_raw.empty:
            closing_raw = load_monthly_closing_reports(closing_dir, start_day, end_day, warnings)
    elif args.fetch_new_customers_api:
        closing_raw = fetch_new_customer_profiles_from_api(
            start_day=start_day,
            end_day=end_day,
            closing_dir=closing_dir,
            env_file=args.api_env_file,
            warnings=warnings,
        )
        if closing_raw.empty:
            closing_raw = load_monthly_closing_reports(closing_dir, start_day, end_day, warnings)
    else:
        closing_raw = load_monthly_closing_reports(closing_dir, start_day, end_day, warnings)
    new_customer_summary, new_customer_daily = build_new_customer_summaries(closing_raw, store_scorecards)
    all_metrics["new_customers"] = as_float(new_customer_daily["new_customers"].sum()) if not new_customer_daily.empty else 0.0
    if all_metrics.get("closing_summary_has_data"):
        all_metrics["new_customer_rate"] = all_metrics.get("closing_new_customer_rate", 0.0)
    else:
        all_metrics["new_customer_rate"] = (
            as_float(new_customer_daily["new_customers"].sum()) / as_float(new_customer_daily["total_customers"].sum())
            if not new_customer_daily.empty and as_float(new_customer_daily["total_customers"].sum()) else 0.0
        )

    comparison_metrics = {
        "previous_month": load_comparison_metrics(*previous_month_range(start_day), warnings=warnings),
        "same_month_previous_year": load_comparison_metrics(*same_month_prior_year_range(start_day, end_day), warnings=warnings),
    }
    comparison_keys = [
        "net_revenue",
        "gross_sales",
        "profit",
        "profit_real",
        "margin",
        "margin_real",
        "tickets",
        "basket",
        "items",
        "discount",
        "discount_rate",
        "returns_net",
        "closing_new_customers",
        "closing_customers",
        "closing_new_customer_rate",
        "inventory_end_value",
        "inventory_value_change",
    ]
    for comparison_key, comparison_data in comparison_metrics.items():
        if comparison_data is None:
            for metric_key in comparison_keys:
                all_metrics[f"{comparison_key}_{metric_key}"] = "N/A"
                all_metrics[f"{comparison_key}_{metric_key}_change"] = "N/A"
            continue
        for metric_key in comparison_keys:
            comparison_value = comparison_data.get(metric_key, "N/A")
            all_metrics[f"{comparison_key}_{metric_key}"] = as_float(comparison_value) if comparison_value != "N/A" else "N/A"
            if comparison_value == "N/A":
                all_metrics[f"{comparison_key}_{metric_key}_change"] = "N/A"
            elif metric_key in {"margin", "margin_real", "discount_rate"}:
                all_metrics[f"{comparison_key}_{metric_key}_change"] = as_float(all_metrics.get(metric_key)) - as_float(comparison_value)
            else:
                all_metrics[f"{comparison_key}_{metric_key}_change"] = percentage_change(
                    as_float(all_metrics.get(metric_key)),
                    as_float(comparison_value),
                )
    all_metrics.update(load_trailing_three_month_average(start_day))
    prev_store_scorecards = load_archived_store_scorecards(previous_month_range(start_day)[0].strftime("%Y-%m"))
    yoy_store_scorecards = load_archived_store_scorecards(same_month_prior_year_range(start_day, end_day)[0].strftime("%Y-%m"))
    store_scorecards = add_store_comparison_columns(store_scorecards, prev_store_scorecards, yoy_store_scorecards)

    validate_monthly_run(bundles, all_daily, all_metrics, store_scorecards, warnings)
    action_items = generate_owner_action_items(
        all_metrics,
        store_scorecards,
        category_summary,
        brand_summary,
        product_summary,
        cart_distribution,
        comparison_metrics,
    )

    top_n = resolve_main_table_rows(args.detail_level, args.main_top_n, args.max_main_table_rows)
    appendix_rows = resolve_appendix_rows(args.detail_level, args.appendix_top_n)
    full_detail_pdf = resolve_full_detail_pdf(args.detail_level, args.full_detail_pdf)
    if args.summary_only:
        appendix_rows = 0
        full_detail_pdf = False
    store_rankings = build_store_rankings(store_scorecards, inventory_summary)
    monthly_wins = generate_monthly_wins(all_metrics, store_scorecards, category_summary, brand_summary, new_customer_daily, inventory_summary)
    monthly_concerns = generate_monthly_concerns(all_metrics, store_scorecards, category_summary, brand_summary, cart_distribution, inventory_summary)
    monthly_actions = generate_monthly_action_items(action_items)
    files_used = [str(bundle.path) for bundle in bundles.values()]
    store_daily_detail = build_store_daily_detail_export(bundles, start_day, end_day)

    data_outputs = write_data_exports(
        data_dir=data_dir,
        month_key=month_key,
        start_day=start_day,
        end_day=end_day,
        all_metrics=all_metrics,
        store_scorecards=store_scorecards,
        category_summary=category_summary,
        brand_summary=brand_summary,
        product_summary=product_summary,
        budtender_summary=budtender_summary,
        cart_distribution=cart_distribution,
        adjusted_cart_metrics=adjusted_cart_metrics,
        hourly_summary=hourly_summary,
        weekday_summary=weekday_summary,
        weekday_hour_summary=weekday_hour_summary,
        brand_store_matrix=brand_store_matrix,
        category_store_matrix=category_store_matrix,
        new_customer_summary=new_customer_summary,
        new_customer_daily=new_customer_daily,
        new_customer_raw=closing_raw,
        closing_summary=closing_summary,
        inventory_summary=inventory_summary,
        inventory_start=inventory_start,
        inventory_end=inventory_end,
        daily_metrics=all_daily,
        store_daily_detail=store_daily_detail,
        kickback_summary=kickback_summary,
        all_kickback_detail=all_kickback_detail,
        store_kickback_summary=store_kickback_summary,
        tax_rounding_summary=tax_rounding_summary,
        tax_rounding_daily=tax_rounding_daily,
        action_items=action_items,
        warnings=warnings,
        files_used=files_used,
        include_data_book=args.include_data_book,
    )

    comparison_summary = pd.DataFrame([
        {
            "metric": key,
            "current": all_metrics.get(key),
            "previous_month": all_metrics.get(f"previous_month_{key}", "N/A"),
            "previous_month_change": all_metrics.get(f"previous_month_{key}_change", "N/A"),
            "same_month_previous_year": all_metrics.get(f"same_month_previous_year_{key}", "N/A"),
            "same_month_previous_year_change": all_metrics.get(f"same_month_previous_year_{key}_change", "N/A"),
            "trailing_3mo_avg": all_metrics.get(f"trailing_3mo_avg_{key}", "N/A"),
        }
        for key in comparison_keys
    ])
    comparison_path = data_dir / "monthly_comparison_summary.csv"
    comparison_summary.to_csv(comparison_path, index=False)
    data_outputs.append(comparison_path)

    rankings_path = data_dir / "monthly_store_rankings.csv"
    (store_rankings if store_rankings is not None else pd.DataFrame()).to_csv(rankings_path, index=False)
    data_outputs.append(rankings_path)

    insights_payload = {
        "wins": monthly_wins,
        "concerns": monthly_concerns,
        "actions": monthly_actions,
    }
    insights_path = data_dir / "monthly_insights.json"
    write_json(insights_path, insights_payload)
    data_outputs.append(insights_path)

    scorecard_payload = {
        "month": month_key,
        "date_range": {"start": start_day, "end": end_day},
        "metrics": {
            "net_revenue": all_metrics.get("net_revenue"),
            "profit": all_metrics.get("profit"),
            "profit_real": all_metrics.get("profit_real"),
            "margin_real": all_metrics.get("margin_real"),
            "tickets": all_metrics.get("tickets"),
            "basket": all_metrics.get("basket"),
            "basket_raw_including_low_value_carts": all_metrics.get("basket_raw_including_low_value_carts"),
            "low_value_cart_count_excluded_from_basket": all_metrics.get("low_value_cart_count_excluded_from_basket"),
            "closing_new_customers": all_metrics.get("closing_new_customers", "N/A"),
            "closing_customers": all_metrics.get("closing_customers", "N/A"),
            "closing_new_customer_rate": all_metrics.get("closing_new_customer_rate", "N/A"),
            "ending_inventory": all_metrics.get("inventory_end_value", "N/A"),
            "discount_rate": all_metrics.get("discount_rate"),
            "tax_rounding_loss": all_metrics.get("tax_rounding_loss"),
            "tax_included_roundup_opportunity": all_metrics.get("tax_included_roundup_opportunity"),
            "tax_backout_rounddown_loss": all_metrics.get("tax_backout_rounddown_loss"),
            "tax_rounding_avg_loss_per_transaction": all_metrics.get("tax_rounding_avg_loss_per_transaction"),
            "inventory_gain_loss": all_metrics.get("inventory_value_change", "N/A"),
        },
        "comparison_summary_csv": str(comparison_path),
        "insights_json": str(insights_path),
    }
    scorecard_path = data_dir / "monthly_executive_scorecard.json"
    write_json(scorecard_path, scorecard_payload)
    data_outputs.append(scorecard_path)

    all_pdf = pdf_dir / safe_filename(f"ALL STORES - Monthly Owner Review - {month_key}.pdf")
    print("[PDF] Start")
    pdf_manifest: List[Dict[str, Any]] = []
    all_manifest = build_all_stores_pdf(
        out_pdf=all_pdf,
        month_key=month_key,
        start_day=start_day,
        end_day=end_day,
        generated_at=datetime.now(ZoneInfo(REPORT_TZ)).strftime("%Y-%m-%d %I:%M %p %Z"),
        bundles=bundles,
        all_metrics=all_metrics,
        all_daily=all_daily,
        store_scorecards=store_scorecards,
        category_summary=category_summary,
        brand_summary=brand_summary,
        product_summary=product_summary,
        budtender_summary=budtender_summary,
        cart_distribution=cart_distribution,
        hourly_summary=hourly_summary,
        weekday_summary=weekday_summary,
        weekday_hour_summary=weekday_hour_summary,
        brand_store_matrix=brand_store_matrix,
        category_store_matrix=category_store_matrix,
        new_customer_summary=new_customer_summary,
        new_customer_daily=new_customer_daily,
        closing_summary=closing_summary,
        inventory_summary=inventory_summary,
        kickback_summary=kickback_summary,
        store_kickback_summary=store_kickback_summary,
        action_items=action_items,
        warnings=warnings,
        detail_level=args.detail_level,
        top_n=top_n,
        appendix_rows=appendix_rows,
        full_detail_pdf=full_detail_pdf,
        all_kickback_detail=all_kickback_detail,
        summary_only=args.summary_only,
        files_used=files_used,
    )
    pdf_manifest.append(all_manifest)

    store_pdfs: List[Path] = []
    if args.detail_level != "executive" and not args.no_store_pdfs and not args.summary_only:
        for abbr, bundle in bundles.items():
            summaries = summarize_for_store(bundle, start_day, end_day, top_n)
            store_closing = closing_raw[closing_raw["store"].astype(str).str.upper() == abbr].copy() if closing_raw is not None and not closing_raw.empty else pd.DataFrame()
            store_closing_summary_rows = closing_summary[closing_summary["store"].astype(str).str.upper() == abbr] if closing_summary is not None and not closing_summary.empty else pd.DataFrame()
            store_closing_summary = store_closing_summary_rows.iloc[0].to_dict() if not store_closing_summary_rows.empty else {}
            store_inventory_rows = inventory_summary[inventory_summary["store"].astype(str).str.upper() == abbr] if inventory_summary is not None and not inventory_summary.empty else pd.DataFrame()
            store_inventory = store_inventory_rows.iloc[0].to_dict() if not store_inventory_rows.empty else {}
            store_kickback_detail = compute_kickback_summary(filter_df_date_range(bundle.raw_df, start_day, end_day))
            store_new_summary, store_new_daily = build_new_customer_summaries(
                store_closing,
                pd.DataFrame([{"store": abbr, **bundle.metrics}]),
            )
            store_action_items = generate_owner_action_items(
                {**bundle.metrics, **enrich_monthly_metrics(bundle.metrics, bundle.daily_df, summaries["category"], summaries["brand"], summaries["product"])},
                pd.DataFrame([{"store": abbr, **bundle.metrics, "status": generate_store_status(bundle.metrics, all_metrics)}]),
                summaries["category"],
                summaries["brand"],
                summaries["product"],
                summaries["cart"],
                comparison_metrics={},
            )
            store_pdf = pdf_dir / safe_filename(f"{abbr} - Monthly Owner Review - {bundle.label} - {month_key}.pdf")
            store_manifest = build_store_pdf(
                out_pdf=store_pdf,
                month_key=month_key,
                start_day=start_day,
                end_day=end_day,
                generated_at=datetime.now(ZoneInfo(REPORT_TZ)).strftime("%Y-%m-%d %I:%M %p %Z"),
                bundle=bundle,
                all_metrics=all_metrics,
                category_summary=summaries["category"],
                brand_summary=summaries["brand"],
                product_summary=summaries["product"],
                budtender_summary=summaries["budtender"],
                cart_distribution=summaries["cart"],
                hourly_summary=summaries["hourly"],
                weekday_summary=summaries["weekday"],
                weekday_hour_summary=summaries["weekday_hour"],
                new_customer_summary=store_new_summary,
                new_customer_daily=store_new_daily,
                closing_summary_row=store_closing_summary,
                inventory_row=store_inventory,
                kickback_summary=store_kickback_detail,
                action_items=store_action_items,
                top_n=top_n,
                appendix_rows=appendix_rows,
                full_detail_pdf=full_detail_pdf,
                detail_level=args.detail_level,
                store_rankings=store_rankings,
            )
            pdf_manifest.append(store_manifest)
            store_pdfs.append(store_pdf)

    pdf_paths = [all_pdf] + store_pdfs
    combined_pdf = try_combine_pdfs(
        pdf_paths,
        pdf_dir / safe_filename(f"ALL PDFS Combined - Monthly Owner Review - {month_key}.pdf"),
        warnings,
    )
    if combined_pdf:
        pdf_paths.append(combined_pdf)
    print("[PDF] End")

    for row in pdf_manifest:
        for expected_key, rendered_key, label in [
            ("daily_rows_expected", "daily_rows_rendered", "daily detail"),
            ("kickback_rows_expected", "kickback_rows_rendered", "kickback detail"),
            ("new_customer_days_expected", "new_customer_days_rendered", "new customer detail"),
        ]:
            expected = int(row.get(expected_key) or 0)
            rendered = int(row.get(rendered_key) or 0)
            if row.get("full_detail_pdf") and rendered < expected:
                warnings.append({
                    "severity": "High",
                    "message": f"{row.get('store')} {label} rendered {rendered} of {expected} rows in {Path(str(row.get('pdf'))).name}.",
                })
    manifest_path = data_dir / "monthly_pdf_manifest.json"
    write_json(manifest_path, pdf_manifest)
    data_outputs.append(manifest_path)

    # Warnings can grow during PDF combination, so rewrite warning exports at the end.
    write_json(data_dir / "monthly_warnings.json", warnings)

    email_status = send_or_preview_email(
        args=args,
        month_key=month_key,
        start_day=start_day,
        end_day=end_day,
        pdf_paths=pdf_paths,
        all_metrics=all_metrics,
        store_scorecards=store_scorecards,
        action_items=action_items,
        wins=monthly_wins,
        concerns=monthly_concerns,
    )

    print("\nDone ✅")
    print("Monthly Owner Review Complete")
    print(f"Month: {month_key}")
    print(f"Date Range: {start_day.isoformat()} → {end_day.isoformat()}")
    print(f"All Stores PDF: {all_pdf}")
    print(f"Store PDFs: {len(store_pdfs)}")
    print(f"Data Exports: {data_dir}/")
    print(f"Email: {email_status}")
    print(f"Warnings: {len(warnings)}")
    print("PDF Detail Verification:")
    for row in pdf_manifest:
        print(f"{row.get('store')} daily rows: {row.get('daily_rows_rendered')} / {row.get('daily_rows_expected')}")
        print(f"{row.get('store')} kickback rows: {row.get('kickback_rows_rendered')} / {row.get('kickback_rows_expected')}")
        print(f"{row.get('store')} new customer days: {row.get('new_customer_days_rendered')} / {row.get('new_customer_days_expected')}")


if __name__ == "__main__":
    main()
