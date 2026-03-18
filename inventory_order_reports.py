import os
import re
from collections import OrderedDict
from functools import lru_cache
import math

import pandas as pd

from getSalesReport import store_abbr_map

ORDER_REPORT_WINDOWS = (7, 14, 30)
ORDER_REPORT_PATTERN = re.compile(
    r"^inventory_order_(?P<days>7d|14d|30d)_(?P<store>[A-Za-z0-9]+)\.(?P<ext>xlsx|xls|csv)$",
    re.IGNORECASE,
)

BRAND_COLUMN_CANDIDATES = (
    "brand",
    "product brand",
    "brand name",
    "vendor",
    "vendor name",
    "producer",
    "supplier",
)
PRODUCT_COLUMN_CANDIDATES = (
    "product",
    "product name",
    "inventory name",
    "item name",
    "name",
    "sku",
)
SORT_COLUMN_CANDIDATES = ("category", "product", "product name", "sku")
LOCATION_COLUMN_CANDIDATES = ("location", "store", "dispensary")
SKU_COLUMN_CANDIDATES = ("product sku", "sku", "product id", "inventory id")
MASTER_CATEGORY_COLUMN_CANDIDATES = ("master category",)
QOH_COLUMN_CANDIDATES = ("quantity on hand", "on hand", "qty on hand", "available")
PRICE_COLUMN_CANDIDATES = ("price", "unit price", "retail price")
QTY_SOLD_COLUMN_CANDIDATES = ("quantity sold", "qty sold", "units sold")
AVG_DAILY_SALES_COLUMN_CANDIDATES = ("avg daily sales",)
SOLD_PER_DAY_COLUMN_CANDIDATES = ("sold per day", "units per day")
DAYS_REMAINING_COLUMN_CANDIDATES = ("days remaining", "days on hand")
DAYS_SINCE_RECEIVED_COLUMN_CANDIDATES = ("days since last received", "days since received")
LAST_WHOLESALE_COST_COLUMN_CANDIDATES = ("last wholesale cost", "wholesale cost", "cost")
LAST_ORDERED_QTY_COLUMN_CANDIDATES = ("last ordered quantity", "last order qty", "ordered quantity")
LAST_AUDIT_COLUMN_CANDIDATES = ("last audit",)
GRAMS_COLUMN_CANDIDATES = ("grams concentration", "grams", "weight")
VENDOR_COLUMN_CANDIDATES = ("vendor", "vendor name", "supplier")
STRAIN_COLUMN_CANDIDATES = ("strain",)
FLOWER_TYPE_COLUMN_CANDIDATES = ("flower type",)
CONCENTRATE_TYPE_COLUMN_CANDIDATES = ("concentrate type",)
UPC_COLUMN_CANDIDATES = ("upc/gtin", "upc", "gtin")
PROVINCIAL_SKU_COLUMN_CANDIDATES = ("provincial sku",)

STORE_NAME_TO_ABBR = dict(store_abbr_map)
STORE_ABBR_TO_NAME = {abbr.upper(): name for name, abbr in STORE_NAME_TO_ABBR.items()}


def normalize_text(value):
    if value is None:
        return ""
    return re.sub(r"\s+", " ", str(value).strip()).lower()


def canonical_store_code(value):
    if value is None:
        return ""

    raw = str(value).strip()
    if not raw:
        return ""

    if raw in STORE_NAME_TO_ABBR:
        return STORE_NAME_TO_ABBR[raw].upper()

    upper = raw.upper()
    if upper in STORE_ABBR_TO_NAME:
        return upper

    match = re.search(r"_([A-Z]{2,3})$", upper)
    if match:
        return match.group(1)

    return upper


def order_report_filename(store_name_or_code, window_days, extension=".xlsx"):
    store_code = canonical_store_code(store_name_or_code)
    if not store_code:
        safe_store = re.sub(r"[^A-Za-z0-9]+", "_", str(store_name_or_code)).strip("_") or "UNK"
        store_code = safe_store.upper()

    ext = extension if extension.startswith(".") else f".{extension}"
    return f"inventory_order_{int(window_days)}d_{store_code}{ext.lower()}"


def extract_store_code_from_filename(name):
    base_name = os.path.splitext(os.path.basename(name))[0]
    parts = base_name.split("_")
    if len(parts) >= 2:
        return canonical_store_code(parts[-1])
    return ""


def find_matching_column(df, candidates):
    candidate_map = {normalize_text(col): col for col in df.columns}
    for candidate in candidates:
        match = candidate_map.get(normalize_text(candidate))
        if match:
            return match
    return None


def _score_header(columns):
    normalized = {normalize_text(col) for col in columns}
    score = 0
    if normalized & set(BRAND_COLUMN_CANDIDATES):
        score += 4
    if normalized & set(PRODUCT_COLUMN_CANDIDATES):
        score += 3
    if "category" in normalized:
        score += 1
    if len(normalized) >= 4:
        score += 1
    return score


@lru_cache(maxsize=64)
def _load_order_report_table_cached(path):
    ext = os.path.splitext(path)[1].lower()
    readers = []
    if ext in (".xlsx", ".xls"):
        readers = [("excel", header_idx) for header_idx in range(0, 7)]
    elif ext == ".csv":
        readers = [("csv", 0)]
    else:
        raise ValueError(f"Unsupported order report format: {path}")

    best_df = None
    best_score = -1

    for reader_type, header_idx in readers:
        try:
            if reader_type == "excel":
                df = pd.read_excel(path, header=header_idx)
            else:
                df = pd.read_csv(path)
        except Exception:
            continue

        df = df.dropna(axis=1, how="all").dropna(axis=0, how="all")
        if df.empty:
            continue

        score = _score_header(df.columns)
        if score > best_score:
            best_score = score
            best_df = df

    if best_df is None:
        if ext in (".xlsx", ".xls"):
            best_df = pd.read_excel(path)
        else:
            best_df = pd.read_csv(path)
        best_df = best_df.dropna(axis=1, how="all").dropna(axis=0, how="all")

    return best_df


def load_order_report_table(path):
    return _load_order_report_table_cached(os.path.abspath(path)).copy()


def discover_order_report_files(directory):
    report_files = OrderedDict((days, OrderedDict()) for days in ORDER_REPORT_WINDOWS)
    if not directory or not os.path.isdir(directory):
        return report_files

    for name in sorted(os.listdir(directory)):
        match = ORDER_REPORT_PATTERN.match(name)
        if not match:
            continue

        days = int(match.group("days")[:-1])
        store = canonical_store_code(match.group("store"))
        report_files.setdefault(days, OrderedDict())[store] = os.path.join(directory, name)

    return report_files


def summarize_order_report_files(directory):
    discovered = discover_order_report_files(directory)
    parts = []
    for days, files in discovered.items():
        if files:
            parts.append(f"{days}d ({len(files)} store{'s' if len(files) != 1 else ''})")
    return ", ".join(parts)


def _filter_brand_rows(df, brand_aliases):
    if df.empty or not brand_aliases:
        return pd.DataFrame(columns=df.columns)

    normalized_aliases = {normalize_text(alias) for alias in brand_aliases if normalize_text(alias)}
    if not normalized_aliases:
        return pd.DataFrame(columns=df.columns)

    brand_col = find_matching_column(df, BRAND_COLUMN_CANDIDATES)
    if brand_col:
        brand_series = df[brand_col].map(normalize_text)
        return df[brand_series.isin(normalized_aliases)].copy()

    product_col = find_matching_column(df, PRODUCT_COLUMN_CANDIDATES)
    if product_col:
        product_series = df[product_col].map(normalize_text)
        mask = product_series.apply(
            lambda value: any(alias and alias in value for alias in normalized_aliases)
        )
        return df[mask].copy()

    return pd.DataFrame(columns=df.columns)


def _sort_order_rows(df):
    sort_cols = []
    for col in df.columns:
        if normalize_text(col) in SORT_COLUMN_CANDIDATES and col not in sort_cols:
            sort_cols.append(col)

    if sort_cols:
        return df.sort_values(by=sort_cols, na_position="last")
    return df


def _to_numeric_series(df, candidates):
    col = find_matching_column(df, candidates)
    if not col:
        return pd.Series([pd.NA] * len(df), index=df.index, dtype="object"), None
    return pd.to_numeric(df[col], errors="coerce"), col


def _ceil_non_negative(value):
    if pd.isna(value):
        return 0
    return max(0, int(math.ceil(float(value))))


def _priority_from_metrics(suggested_qty, days_remaining, sold_per_day, qty_sold, days_since_received):
    sold_per_day = 0.0 if pd.isna(sold_per_day) else float(sold_per_day)
    qty_sold = 0.0 if pd.isna(qty_sold) else float(qty_sold)
    suggested_qty = 0.0 if pd.isna(suggested_qty) else float(suggested_qty)
    days_remaining = None if pd.isna(days_remaining) else float(days_remaining)
    days_since_received = None if pd.isna(days_since_received) else float(days_since_received)

    if sold_per_day <= 0 and qty_sold <= 0:
        return "No Recent Sales"
    if suggested_qty <= 0 and days_remaining is not None and days_remaining > 30:
        return "Healthy"
    if days_remaining is not None and days_remaining <= 3:
        return "Urgent"
    if days_remaining is not None and days_remaining <= 7:
        return "Reorder Now"
    if days_remaining is not None and days_remaining <= 14:
        return "Reorder Soon"
    if suggested_qty > 0 and days_since_received is not None and days_since_received >= 14:
        return "Check PO"
    if suggested_qty > 0:
        return "Watch"
    return "Healthy"


def _priority_rank(priority):
    ranks = {
        "Urgent": 0,
        "Reorder Now": 1,
        "Reorder Soon": 2,
        "Check PO": 3,
        "Watch": 4,
        "Healthy": 5,
        "No Recent Sales": 6,
    }
    return ranks.get(priority, 99)


def _build_reorder_note(suggested_qty, days_remaining, sold_per_day, qty_sold, days_since_received):
    notes = []
    if not pd.isna(days_remaining):
        if float(days_remaining) <= 7:
            notes.append("low cover")
        elif float(days_remaining) <= 14:
            notes.append("tight cover")
    if not pd.isna(days_since_received) and float(days_since_received) >= 14:
        notes.append("not received recently")
    if not pd.isna(sold_per_day) and float(sold_per_day) > 0:
        notes.append(f"{float(sold_per_day):.2f}/day")
    elif not pd.isna(qty_sold) and float(qty_sold) > 0:
        notes.append(f"{float(qty_sold):.0f} sold")
    if not pd.isna(suggested_qty) and float(suggested_qty) > 0:
        notes.append(f"order {int(float(suggested_qty))}")
    return ", ".join(notes)


def _reorder_columns(df, ordered_names):
    normalized_map = {normalize_text(col): col for col in df.columns}
    ordered_cols = []
    for name in ordered_names:
        actual = normalized_map.get(normalize_text(name))
        if actual and actual not in ordered_cols:
            ordered_cols.append(actual)
    remaining = [col for col in df.columns if col not in ordered_cols]
    return df[ordered_cols + remaining]


def _drop_columns_by_candidates(df, candidate_groups):
    drop_cols = []
    for candidates in candidate_groups:
        col = find_matching_column(df, candidates)
        if col and col not in drop_cols:
            drop_cols.append(col)
    if not drop_cols:
        return df
    return df.drop(columns=drop_cols)


def prepare_reorder_sheet(df, window_days):
    if df.empty:
        return df

    work = df.copy()

    qty_on_hand, qoh_col = _to_numeric_series(work, QOH_COLUMN_CANDIDATES)
    qty_sold, qty_sold_col = _to_numeric_series(work, QTY_SOLD_COLUMN_CANDIDATES)
    sold_per_day, sold_per_day_col = _to_numeric_series(work, SOLD_PER_DAY_COLUMN_CANDIDATES)
    avg_daily_sales, _avg_sales_col = _to_numeric_series(work, AVG_DAILY_SALES_COLUMN_CANDIDATES)
    days_remaining, days_remaining_col = _to_numeric_series(work, DAYS_REMAINING_COLUMN_CANDIDATES)
    days_since_received, days_since_received_col = _to_numeric_series(work, DAYS_SINCE_RECEIVED_COLUMN_CANDIDATES)
    last_ordered_qty, _last_ordered_col = _to_numeric_series(work, LAST_ORDERED_QTY_COLUMN_CANDIDATES)

    target_qty_col = f"Target Qty ({window_days}d)"
    suggested_qty_col = f"Suggested Order Qty ({window_days}d)"
    priority_col = "Reorder Priority"
    note_col = "Reorder Notes"

    target_qty = sold_per_day.fillna(0).apply(lambda value: _ceil_non_negative(value * window_days))
    suggested_qty = (target_qty - qty_on_hand.fillna(0)).apply(_ceil_non_negative)

    work[target_qty_col] = target_qty
    work[suggested_qty_col] = suggested_qty
    work[priority_col] = [
        _priority_from_metrics(sugg, remain, spd, sold, dsr)
        for sugg, remain, spd, sold, dsr in zip(
            suggested_qty, days_remaining, sold_per_day, qty_sold, days_since_received
        )
    ]
    work[note_col] = [
        _build_reorder_note(sugg, remain, spd, sold, dsr)
        for sugg, remain, spd, sold, dsr in zip(
            suggested_qty, days_remaining, sold_per_day, qty_sold, days_since_received
        )
    ]

    work["_priority_rank"] = work[priority_col].map(_priority_rank)
    category_col = find_matching_column(work, ("category",))
    product_col = find_matching_column(work, PRODUCT_COLUMN_CANDIDATES)
    vendor_col = find_matching_column(work, VENDOR_COLUMN_CANDIDATES)
    work["_sort_category"] = (
        work[category_col].fillna("").astype(str).str.lower() if category_col else ""
    )
    work["_sort_product"] = (
        work[product_col].fillna("").astype(str).str.lower() if product_col else ""
    )
    work["_sort_vendor"] = (
        work[vendor_col].fillna("").astype(str).str.lower() if vendor_col else ""
    )
    work["_sort_days_remaining"] = days_remaining.fillna(999999)
    work["_sort_suggested_qty"] = suggested_qty.fillna(0)
    work["_sort_sold_per_day"] = sold_per_day.fillna(0)
    work["_sort_qty_sold"] = qty_sold.fillna(0)
    work["_sort_days_since_received"] = days_since_received.fillna(-1)

    work = work.sort_values(
        by=[
            "_sort_category",
            "_priority_rank",
            "_sort_days_remaining",
            "_sort_suggested_qty",
            "_sort_sold_per_day",
            "_sort_qty_sold",
            "_sort_days_since_received",
            "_sort_vendor",
            "_sort_product",
        ],
        ascending=[True, True, True, False, False, False, False, True, True],
        na_position="last",
    )

    work = _drop_columns_by_candidates(
        work,
        [
            LOCATION_COLUMN_CANDIDATES,
            BRAND_COLUMN_CANDIDATES,
            SKU_COLUMN_CANDIDATES,
            MASTER_CATEGORY_COLUMN_CANDIDATES,
            STRAIN_COLUMN_CANDIDATES,
            FLOWER_TYPE_COLUMN_CANDIDATES,
            GRAMS_COLUMN_CANDIDATES,
        ],
    )

    work = work.drop(
        columns=[
            "_sort_category",
            "_priority_rank",
            "_sort_days_remaining",
            "_sort_suggested_qty",
            "_sort_sold_per_day",
            "_sort_qty_sold",
            "_sort_days_since_received",
            "_sort_vendor",
            "_sort_product",
        ]
    )

    preferred_order = [
        "Reorder Priority",
        note_col,
        target_qty_col,
        suggested_qty_col,
        "Category",
        "Product Name",
        "Quantity on Hand",
        "Quantity Sold",
        "Sold Per Day",
        "Avg Daily Sales",
        "Days Remaining",
        "Days Since Last Received",
        "Last Ordered Quantity",
        "Last Wholesale Cost",
        "Price",
        "Vendor",
        "Concentrate Type",
        "UPC/GTIN",
        "Provincial SKU",
        "Last Audit",
    ]
    work = _reorder_columns(work, preferred_order)

    # Keep source numeric columns in their original names/casing; these locals
    # only exist to force column discovery during sheet preparation.
    _ = (
        qoh_col,
        qty_sold_col,
        sold_per_day_col,
        days_remaining_col,
        days_since_received_col,
        last_ordered_qty,
        avg_daily_sales,
        last_ordered_qty,
    )

    return work


def build_brand_order_sections(order_reports_dir, brand_aliases, store_code=None):
    sections = OrderedDict()
    discovered = discover_order_report_files(order_reports_dir)
    wanted_store = canonical_store_code(store_code) if store_code else ""

    for days in ORDER_REPORT_WINDOWS:
        store_map = discovered.get(days, {})
        if wanted_store:
            store_items = [(wanted_store, store_map[wanted_store])] if wanted_store in store_map else []
        else:
            store_items = list(store_map.items())

        if not store_items:
            continue

        frames = []
        include_store_column = len(store_items) > 1 and not wanted_store
        for store, path in store_items:
            try:
                df = load_order_report_table(path)
            except Exception as exc:
                print(f"[WARN] Could not read order report '{path}': {exc}")
                continue

            brand_rows = _filter_brand_rows(df, brand_aliases)
            if brand_rows.empty:
                continue

            brand_rows = _sort_order_rows(brand_rows)
            brand_rows = prepare_reorder_sheet(brand_rows, days)
            if include_store_column:
                brand_rows.insert(0, "Store", store)
            frames.append(brand_rows)

        if not frames:
            continue

        sections[f"Order_{days}d"] = pd.concat(frames, ignore_index=True) if len(frames) > 1 else frames[0]

    return sections
