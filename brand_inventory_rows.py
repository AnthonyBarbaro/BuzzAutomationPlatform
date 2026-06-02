import math
import re

import pandas as pd


DEFAULT_IDENTITY_COLUMNS = ("Brand", "Category", "Product")
DEFAULT_QUANTITY_COLUMN = "Available"
DEFAULT_COST_COLUMN = "Cost"
SORT_PREFIX = "_inventory_sort_"


def _is_missing(value):
    try:
        return pd.isna(value)
    except (TypeError, ValueError):
        return False


def _safe_display_text(value):
    if _is_missing(value):
        return ""
    return re.sub(r"\s+", " ", str(value).strip())


def normalize_inventory_text(value):
    return _safe_display_text(value).lower()


def _first_non_empty(series):
    for value in series:
        if _safe_display_text(value):
            return value
    return ""


def _format_quantity(value):
    if _is_missing(value):
        return 0
    number = float(value)
    if math.isclose(number, round(number), abs_tol=1e-9):
        return int(round(number))
    return round(number, 3)


def _weighted_cost(group, cost_column, quantity_column):
    numeric_cost = pd.to_numeric(group[cost_column], errors="coerce")
    valid_costs = numeric_cost.dropna()
    if valid_costs.empty:
        return _first_non_empty(group[cost_column])

    rounded_costs = valid_costs.round(4)
    if rounded_costs.nunique(dropna=True) == 1:
        return round(float(valid_costs.iloc[0]), 4)

    weights = pd.to_numeric(group[quantity_column], errors="coerce").fillna(0).clip(lower=0)
    valid_mask = numeric_cost.notna()
    valid_weights = weights[valid_mask]
    if valid_weights.sum() > 0:
        weighted = (numeric_cost[valid_mask] * valid_weights).sum() / valid_weights.sum()
    else:
        weighted = valid_costs.mean()
    return round(float(weighted), 4)


def consolidate_duplicate_inventory_rows(
    df,
    identity_columns=DEFAULT_IDENTITY_COLUMNS,
    quantity_column=DEFAULT_QUANTITY_COLUMN,
    cost_column=DEFAULT_COST_COLUMN,
):
    """Collapse duplicate brand/category/product rows into one summed row."""
    if df.empty or quantity_column not in df.columns:
        return df.copy()

    existing_identity_cols = [col for col in identity_columns if col in df.columns]
    if not existing_identity_cols:
        return df.copy()

    work = df.copy()
    original_columns = list(work.columns)
    work[quantity_column] = pd.to_numeric(work[quantity_column], errors="coerce").fillna(0)

    key_columns = []
    for col in existing_identity_cols:
        key_col = f"__dedupe_key_{col}"
        work[key_col] = work[col].map(normalize_inventory_text)
        key_columns.append(key_col)

    rows = []
    grouped = work.groupby(key_columns, dropna=False, sort=False)
    for _, group in grouped:
        row = {}
        for col in original_columns:
            if col == quantity_column:
                row[col] = _format_quantity(group[quantity_column].sum())
            elif col == cost_column and cost_column in group.columns:
                row[col] = _weighted_cost(group, cost_column, quantity_column)
            else:
                row[col] = _first_non_empty(group[col])
        rows.append(row)

    return pd.DataFrame(rows, columns=original_columns)


def _split_product_segments(product_name):
    text = _safe_display_text(product_name)
    return [segment.strip() for segment in text.split("|") if segment.strip()]


def _remove_weight_tokens(value):
    value = re.sub(r"\b\d+(?:\.\d+)?\s*G\b", " ", value, flags=re.IGNORECASE)
    return re.sub(r"\s+", " ", value).strip(" -")


def _product_line_segment(product_name):
    segments = _split_product_segments(product_name)
    if len(segments) >= 3:
        return segments[1]
    if len(segments) >= 2:
        return segments[0]
    return segments[0] if segments else ""


def _strain_segment(product_name):
    segments = _split_product_segments(product_name)
    if len(segments) >= 3:
        return segments[-1]
    if len(segments) == 2:
        return segments[-1]
    return segments[0] if segments else ""


def _extract_grams(product_name):
    matches = re.findall(r"(\d+(?:\.\d+)?)\s*G\b", _safe_display_text(product_name), flags=re.IGNORECASE)
    if not matches:
        return 999999.0
    try:
        return float(matches[0])
    except ValueError:
        return 999999.0


def _strain_sort_parts(product_name):
    strain = _strain_segment(product_name)
    variant_match = re.search(r"\(([^)]*)\)\s*$", strain)
    variant = normalize_inventory_text(variant_match.group(1)) if variant_match else ""
    base = re.sub(r"\([^)]*\)\s*$", "", strain).strip()
    return normalize_inventory_text(base), variant


def _product_sort_parts(product_name):
    line = normalize_inventory_text(_remove_weight_tokens(_product_line_segment(product_name)))
    strain, variant = _strain_sort_parts(product_name)
    full_name = normalize_inventory_text(product_name)
    return line, _extract_grams(product_name), strain, variant, full_name


def sort_inventory_rows(df, include_cost_as_tiebreaker=False):
    """Sort products by category, product family, pack size, strain, then name."""
    if df.empty:
        return df.copy()

    work = df.copy()
    sort_columns = []

    if "Category" in work.columns:
        category_col = f"{SORT_PREFIX}category"
        work[category_col] = work["Category"].map(normalize_inventory_text)
        sort_columns.append(category_col)

    if "Product" in work.columns:
        sort_part_columns = [
            f"{SORT_PREFIX}line",
            f"{SORT_PREFIX}grams",
            f"{SORT_PREFIX}strain",
            f"{SORT_PREFIX}variant",
            f"{SORT_PREFIX}product",
        ]
        parts = work["Product"].map(_product_sort_parts)
        for idx, col in enumerate(sort_part_columns):
            work[col] = parts.map(lambda values, part_idx=idx: values[part_idx])
        sort_columns.extend(sort_part_columns)

    if include_cost_as_tiebreaker and "Cost" in work.columns:
        cost_col = f"{SORT_PREFIX}cost"
        work[cost_col] = pd.to_numeric(work["Cost"], errors="coerce")
        sort_columns.append(cost_col)

    if not sort_columns:
        return work

    work = work.sort_values(by=sort_columns, na_position="last", kind="mergesort")
    return work.drop(columns=[col for col in work.columns if col.startswith(SORT_PREFIX)])
