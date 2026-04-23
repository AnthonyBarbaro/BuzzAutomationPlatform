#!/usr/bin/env python3
from __future__ import annotations

import argparse
import json
import logging
import math
import os
import re
import tempfile
from collections import Counter, OrderedDict
from decimal import Decimal, ROUND_HALF_UP
from datetime import date, datetime, timedelta
from pathlib import Path
from typing import Any, Callable, Mapping, Sequence
from zoneinfo import ZoneInfo

import numpy as np
import pandas as pd

os.environ.setdefault("MPLCONFIGDIR", str(Path(tempfile.gettempdir()) / "matplotlib"))

import brand_meeting_packet as bmp

from dutchie_api_reports import (
    DEFAULT_ENV_FILE,
    REPORT_SPECS,
    STORE_CODES,
    canonical_env_map,
    create_session,
    local_date_range_to_utc_strings,
    parse_store_codes,
    request_json,
    resolve_integrator_key,
    resolve_store_keys,
)
from weekly_store_ordering_sheets import (
    authenticate_sheets,
    build_summary_rows,
    merge_preserved_review_columns,
    move_latest_tabs_next_to_readme,
    parse_spreadsheet_target,
    read_sheet_values,
    upsert_readme_tab,
    upsert_ordering_tab,
)


BASE_DIR = Path(__file__).resolve().parent
DEFAULT_CONFIG_PATH = BASE_DIR / "weekly_store_ordering_config.json"
DEFAULT_SPREADSHEET_TARGET_KEY = "DEFAULT"

AUTO_COLUMNS = [
    "Row Key",
    "Brand",
    "Category",
    "Product",
    "Available",
    "Par Level",
    "Cost",
    "Price",
    "Units Sold 7d",
    "Units Sold 14d",
    "Units Sold 30d",
]

METRIC_EXPORT_COLUMNS = AUTO_COLUMNS + [
    "Vendor",
    "Reorder Priority",
    "Inventory Value",
    "Sell-Through 7D/14D/30D",
    "Avg Daily Sold 14d",
    "Days of Supply",
    "Suggested Order Qty",
    "Reorder Notes / Reason",
    "Last Sale Date",
    "Needs Order",
    "SKU",
    "Sell-Through 7d",
    "Sell-Through 14d",
    "Sell-Through 30d",
    "Target Qty",
    "Priority Rank",
    "store_code",
    "eligible_brand_30d",
    "eligible_vendor_30d",
]

SUMMARY_FIELD_ORDER = [
    "Store",
    "Week Of",
    "Snapshot Generated At",
    "Total Inventory Value",
]


def load_ordering_config(config_path: str | os.PathLike[str] = DEFAULT_CONFIG_PATH) -> dict[str, Any]:
    path = Path(config_path)
    with path.open("r", encoding="utf-8") as handle:
        config = json.load(handle)

    config.setdefault("timezone", "America/Los_Angeles")
    config.setdefault("stores", list(STORE_CODES))
    config.setdefault("output_root", "reports/store_weekly_ordering")
    config.setdefault("sheet_names", {})
    config["sheet_names"].setdefault("auto_suffix", "Auto")
    config["sheet_names"].setdefault("review_suffix", "Review")
    config.setdefault("sheet_outputs", {})
    config["sheet_outputs"].setdefault("write_auto_tab", True)
    config["sheet_outputs"].setdefault("write_review_tab", True)
    config.setdefault("sales", {})
    config["sales"].setdefault("window_days", 30)
    config["sales"].setdefault("excluded_statuses", ["cancelled", "canceled", "void", "voided", "deleted"])
    config.setdefault("inventory_api_params", {})
    config.setdefault("eligibility", {})
    config["eligibility"].setdefault("mode", "brand_or_vendor")
    config["eligibility"].setdefault("include_sales_only_rows", True)
    config["eligibility"].setdefault("min_units_sold_30d", 3)
    config.setdefault("exclusions", {})
    config["exclusions"].setdefault("pattern", r"\b(sample|samples|promo|promos|promotional|display|tester)\b")
    config["exclusions"].setdefault("fields", ["product", "category", "tags", "brand", "vendor"])
    config["exclusions"].setdefault("extra_keywords", [])
    config["exclusions"].setdefault("low_cost_threshold", 1.0)
    config["exclusions"].setdefault("exclude_low_cost_rows", True)
    config.setdefault("reorder", {})
    config["reorder"].setdefault("velocity_window_days", 14)
    config["reorder"].setdefault("target_cover_days", 14)
    config["reorder"].setdefault("needs_order_min_qty", 1)
    config["reorder"].setdefault("days_of_supply_urgent", 3.0)
    config["reorder"].setdefault("days_of_supply_low", 7.0)
    config["reorder"].setdefault("high_sell_through_30d", 0.6)
    config["reorder"].setdefault("par_weight_7d", 0.5)
    config["reorder"].setdefault("par_weight_14d", 0.3)
    config["reorder"].setdefault("par_weight_30d", 0.2)
    config["reorder"].setdefault("par_stale_30d_dampener", 0.6)
    config["reorder"].setdefault("par_stockout_uplift_max", 0.25)
    config.setdefault("review_manual_columns", [])
    return config


def sheet_output_flags(config: Mapping[str, Any]) -> dict[str, bool]:
    outputs = dict(config.get("sheet_outputs", {}) or {})
    flags = {
        "auto": bool(outputs.get("write_auto_tab", True)),
        "review": bool(outputs.get("write_review_tab", True)),
    }
    if not any(flags.values()):
        raise ValueError("At least one weekly ordering sheet tab must be enabled.")
    return flags


def resolve_as_of_day(as_of_text: str | None, tz_name: str) -> date:
    if as_of_text:
        return datetime.fromisoformat(str(as_of_text)).date()
    return datetime.now(ZoneInfo(tz_name)).date()


def resolve_week_of(week_text: str | None, as_of_day: date) -> date:
    if week_text:
        source_day = datetime.fromisoformat(str(week_text)).date()
        return source_day - timedelta(days=source_day.weekday())

    current_monday = as_of_day - timedelta(days=as_of_day.weekday())
    if as_of_day > current_monday:
        return current_monday + timedelta(days=7)
    return current_monday


def build_tab_title(store_code: str, week_of: date, suffix: str) -> str:
    return f"{store_code} {week_of.isoformat()} {suffix}"


def build_store_tab_titles(store_code: str, week_of: date, config: Mapping[str, Any]) -> dict[str, str]:
    return {
        "auto": build_tab_title(store_code, week_of, str(config.get("sheet_names", {}).get("auto_suffix", "Auto"))),
        "review": build_tab_title(store_code, week_of, str(config.get("sheet_names", {}).get("review_suffix", "Review"))),
    }


def parse_spreadsheet_targets_text(text: str) -> dict[str, str]:
    stripped = str(text or "").strip()
    if not stripped:
        return {}

    if stripped.startswith("{"):
        parsed = json.loads(stripped)
        if isinstance(parsed, str):
            clean_value = parsed.strip()
            return {DEFAULT_SPREADSHEET_TARGET_KEY: clean_value} if clean_value else {}
        if not isinstance(parsed, dict):
            raise ValueError("Spreadsheet target JSON must be a string or object.")

        targets: dict[str, str] = {}
        for raw_key, raw_value in parsed.items():
            key = str(raw_key or "").strip()
            value = str(raw_value or "").strip()
            if not key or not value:
                continue
            normalized_key = (
                DEFAULT_SPREADSHEET_TARGET_KEY if key.lower() == "default" else key.upper()
            )
            targets[normalized_key] = value
        return targets

    targets: dict[str, str] = {}
    plain_lines: list[str] = []
    for raw_line in stripped.splitlines():
        line = raw_line.strip()
        if not line or line.startswith("#"):
            continue
        if "=" not in line:
            plain_lines.append(line)
            continue

        raw_key, raw_value = line.split("=", 1)
        key = str(raw_key or "").strip()
        value = str(raw_value or "").strip()
        if not key or not value:
            continue
        normalized_key = DEFAULT_SPREADSHEET_TARGET_KEY if key.lower() == "default" else key.upper()
        targets[normalized_key] = value

    if plain_lines:
        if len(plain_lines) == 1 and not targets:
            return {DEFAULT_SPREADSHEET_TARGET_KEY: plain_lines[0]}
        raise ValueError(
            "Spreadsheet target text must be a single URL/ID or KEY=VALUE lines such as MV=https://... ."
        )

    return targets


def resolve_spreadsheet_targets(args: argparse.Namespace, config: Mapping[str, Any]) -> dict[str, str]:
    if args.sheet_url:
        return parse_spreadsheet_targets_text(str(args.sheet_url))

    env_name = str(config.get("spreadsheet_url_env", "")).strip()
    if env_name and os.environ.get(env_name):
        return parse_spreadsheet_targets_text(str(os.environ.get(env_name, "")))

    file_name = str(config.get("spreadsheet_url_file", "")).strip()
    if file_name:
        path = BASE_DIR / file_name
        if path.exists():
            return parse_spreadsheet_targets_text(path.read_text(encoding="utf-8"))

    return {}


def resolve_store_spreadsheet_target(spreadsheet_targets: Mapping[str, str], store_code: str) -> str:
    normalized_store = str(store_code or "").strip().upper()
    if normalized_store and spreadsheet_targets.get(normalized_store):
        return str(spreadsheet_targets[normalized_store]).strip()
    if spreadsheet_targets.get(DEFAULT_SPREADSHEET_TARGET_KEY):
        return str(spreadsheet_targets[DEFAULT_SPREADSHEET_TARGET_KEY]).strip()
    return ""


def default_store_selection(args: argparse.Namespace, config: Mapping[str, Any]) -> list[str]:
    if args.all_stores:
        return parse_store_codes(list(config.get("stores", []))) or list(STORE_CODES)
    if args.store:
        return parse_store_codes([args.store])
    if args.stores:
        return parse_store_codes(args.stores)
    return parse_store_codes(list(config.get("stores", []))) or list(STORE_CODES)


def load_store_payloads(
    store_code: str,
    as_of_day: date,
    config: Mapping[str, Any],
    env_file: str,
    fixture_root: Path | None,
    logger: logging.Logger,
) -> dict[str, Any]:
    if fixture_root is not None:
        return load_fixture_payloads(store_code, fixture_root)
    return fetch_api_payloads(store_code, as_of_day, config, env_file, logger)


def load_fixture_payloads(store_code: str, fixture_root: Path) -> dict[str, Any]:
    store_root = fixture_root / store_code
    return {
        "inventory": json.loads((store_root / "inventory.json").read_text(encoding="utf-8")),
        "products": json.loads((store_root / "products.json").read_text(encoding="utf-8")),
        "transactions": json.loads((store_root / "transactions.json").read_text(encoding="utf-8")),
    }


def fetch_api_payloads(
    store_code: str,
    as_of_day: date,
    config: Mapping[str, Any],
    env_file: str,
    logger: logging.Logger,
) -> dict[str, Any]:
    env_map = canonical_env_map(env_file)
    store_keys = resolve_store_keys(env_map, [store_code])
    if store_code not in store_keys:
        raise RuntimeError(
            f"Missing Dutchie API location key for {store_code} in {env_file}. "
            "Expected names like DUTCHIE_API_KEY_MV or MV."
        )

    session = create_session(store_keys[store_code], resolve_integrator_key(env_map))
    window_days = int(config.get("sales", {}).get("window_days", 30))
    sales_start = as_of_day - timedelta(days=max(window_days - 1, 0))
    from_utc, to_utc = local_date_range_to_utc_strings(
        sales_start.isoformat(),
        as_of_day.isoformat(),
        str(config.get("timezone", "America/Los_Angeles")),
    )
    sales_params = {
        "FromDateUTC": from_utc,
        "ToDateUTC": to_utc,
        "IncludeDetail": True,
        "IncludeTaxes": True,
        "IncludeOrderIds": True,
        "IncludeFeesAndDonations": True,
    }

    logger.info("[%s] Fetching %s", store_code, REPORT_SPECS["inventory"].endpoint)
    inventory_payload = request_json(
        session,
        REPORT_SPECS["inventory"].endpoint,
        params=dict(config.get("inventory_api_params", {})),
    )
    logger.info("[%s] Fetching %s", store_code, REPORT_SPECS["catalog"].endpoint)
    products_payload = request_json(session, REPORT_SPECS["catalog"].endpoint, params={})
    logger.info("[%s] Fetching %s", store_code, REPORT_SPECS["sales"].endpoint)
    transactions_payload = request_json(session, REPORT_SPECS["sales"].endpoint, params=sales_params)

    return {
        "inventory": inventory_payload,
        "products": products_payload,
        "transactions": transactions_payload,
    }


def filter_transaction_payload(
    transactions_payload: Sequence[Mapping[str, Any]] | Any,
    config: Mapping[str, Any],
) -> tuple[list[dict[str, Any]], Counter]:
    status_blocklist = {
        str(value).strip().lower()
        for value in config.get("sales", {}).get("excluded_statuses", [])
        if str(value).strip()
    }
    counts: Counter[str] = Counter()
    kept: list[dict[str, Any]] = []
    for transaction in transactions_payload or []:
        if not isinstance(transaction, dict):
            continue
        text_status = _first_nonempty(
            transaction.get("status"),
            transaction.get("transactionStatus"),
            transaction.get("state"),
            transaction.get("transactionState"),
        ).strip().lower()
        if text_status and text_status in status_blocklist:
            counts[f"status:{text_status}"] += 1
            continue
        if bool(transaction.get("isCancelled")) or bool(transaction.get("isCanceled")):
            counts["flag:cancelled"] += 1
            continue
        if bool(transaction.get("isVoided")) or bool(transaction.get("isVoid")):
            counts["flag:voided"] += 1
            continue
        if bool(transaction.get("isDeleted")):
            counts["flag:deleted"] += 1
            continue
        kept.append(dict(transaction))
    return kept, counts


def normalize_inventory_payload(inventory_payload: Any, store_code: str) -> pd.DataFrame:
    normalized = bmp._normalize_inventory_api_catalog_rows(inventory_payload, store_code)
    if normalized.empty:
        return pd.DataFrame()
    normalized["_store_abbr"] = store_code
    return bmp.prepare_catalog_for_all_brands(normalized, [store_code])


def normalize_sales_payload(
    transactions_payload: Any,
    products_payload: Any,
    store_code: str,
    catalog_prepared_df: pd.DataFrame,
    logger: logging.Logger,
) -> pd.DataFrame:
    normalized = bmp._normalize_transactions_api_sales_rows(transactions_payload, products_payload, store_code)
    if normalized.empty:
        return pd.DataFrame()
    catalog_maps = bmp.build_catalog_merge_maps(catalog_prepared_df) if catalog_prepared_df is not None and not catalog_prepared_df.empty else {}
    brand_display_map = bmp.build_brand_display_map(catalog_prepared_df) if catalog_prepared_df is not None and not catalog_prepared_df.empty else {}
    return bmp._prepare_sales_df_all_brands(
        normalized,
        store_code,
        logger=_log_adapter(logger),
        catalog_merge_maps=catalog_maps,
        brand_display_map=brand_display_map,
    )


def build_ordering_bundle(
    store_code: str,
    week_of: date,
    as_of_day: date,
    payloads: Mapping[str, Any],
    config: Mapping[str, Any],
    snapshot_generated_at: datetime,
    logger: logging.Logger,
) -> dict[str, Any]:
    filtered_transactions, transaction_drop_counts = filter_transaction_payload(payloads.get("transactions") or [], config)
    inventory_prepared = normalize_inventory_payload(payloads.get("inventory"), store_code)
    sales_prepared = normalize_sales_payload(
        filtered_transactions,
        payloads.get("products"),
        store_code,
        inventory_prepared,
        logger,
    )

    inventory_filtered, inventory_exclusion_counts = apply_exclusion_rules(inventory_prepared, "inventory", config)
    sales_filtered, sales_exclusion_counts = apply_exclusion_rules(sales_prepared, "sales", config)

    inventory_rows = inventory_rows_for_ordering(inventory_filtered, store_code)
    sales_rows = sales_rows_for_ordering(sales_filtered, store_code)
    inventory_agg = aggregate_inventory_rows(inventory_rows)
    sales_agg = aggregate_sales_rows(
        sales_rows=sales_rows,
        as_of_day=as_of_day,
        velocity_window_days=int(config.get("reorder", {}).get("velocity_window_days", 30)),
    )

    eligible_brand_keys = set(sales_rows["brand_key"].dropna().astype(str).str.strip())
    eligible_brand_keys.discard("")
    eligible_vendor_keys = set(sales_rows["vendor_key"].dropna().astype(str).str.strip())
    eligible_vendor_keys.discard("")

    merged = merge_inventory_sales(inventory_agg, sales_agg)
    merged = apply_eligibility_rules(merged, eligible_brand_keys, eligible_vendor_keys, config)
    if not bool(config.get("eligibility", {}).get("include_sales_only_rows", True)):
        merged = merged[merged["inventory_row_count"] > 0].copy()
    metrics_df = compute_ordering_metrics(merged, config)
    metrics_df, metric_filter_counts = apply_ordering_filters(metrics_df, config)
    metrics_df = sort_ordering_rows(metrics_df)
    auto_df = build_auto_sheet_df(metrics_df)
    review_df = build_review_sheet_df(metrics_df, config)
    summary = build_store_summary(
        inventory_prepared=inventory_prepared,
        store_code=store_code,
        week_of=week_of,
        snapshot_generated_at=snapshot_generated_at,
    )
    tab_titles = build_store_tab_titles(store_code, week_of, config)

    return {
        "store_code": store_code,
        "week_of": week_of.isoformat(),
        "snapshot_generated_at": snapshot_generated_at.isoformat(timespec="seconds"),
        "tab_titles": tab_titles,
        "summary": summary,
        "auto_df": auto_df,
        "review_df": review_df,
        "normalized_inventory": inventory_prepared,
        "normalized_sales": sales_prepared,
        "sku_metrics": metrics_df,
        "logs": {
            "transaction_drop_counts": dict(transaction_drop_counts),
            "inventory_exclusion_counts": dict(inventory_exclusion_counts),
            "sales_exclusion_counts": dict(sales_exclusion_counts),
            "eligible_brand_count": len(eligible_brand_keys),
            "eligible_vendor_count": len(eligible_vendor_keys),
            "inventory_rows_in": int(len(inventory_prepared)),
            "inventory_rows_out": int(len(inventory_filtered)),
            "sales_rows_in": int(len(sales_prepared)),
            "sales_rows_out": int(len(sales_filtered)),
            "metric_filter_counts": dict(metric_filter_counts),
            "final_rows": int(len(metrics_df)),
            "needs_order_rows": int((metrics_df["Needs Order"] == "Y").sum()) if not metrics_df.empty else 0,
        },
    }


def apply_exclusion_rules(
    prepared_df: pd.DataFrame,
    dataset_name: str,
    config: Mapping[str, Any],
) -> tuple[pd.DataFrame, Counter]:
    if prepared_df is None or prepared_df.empty:
        return pd.DataFrame(columns=prepared_df.columns if prepared_df is not None else []), Counter()

    work = prepared_df.copy()
    pattern = re.compile(str(config.get("exclusions", {}).get("pattern", "")), flags=re.IGNORECASE)
    fields = [str(value).strip().lower() for value in config.get("exclusions", {}).get("fields", [])]
    extra_keywords = [
        str(value).strip().lower()
        for value in config.get("exclusions", {}).get("extra_keywords", [])
        if str(value).strip()
    ]
    low_cost_threshold = float(config.get("exclusions", {}).get("low_cost_threshold", 1.01))
    exclude_low_cost_rows = bool(config.get("exclusions", {}).get("exclude_low_cost_rows", False))

    counts: Counter[str] = Counter()
    keep_mask: list[bool] = []
    for _, row in work.iterrows():
        product = _clean_text(row.get("Product"), row.get("Product Name"), row.get("_product_raw"), row.get("display_product"))
        category = _clean_text(row.get("Category"), row.get("category_normalized"))
        tags = _clean_text(row.get("Tags"))
        brand = _clean_text(row.get("brand_name"), row.get("Brand"))
        vendor = _clean_text(row.get("Vendor"), row.get("Vendor Name"), row.get("Producer"))
        cost = _maybe_float(row.get("Cost"), row.get("merge_cost_basis"))

        texts = {
            "product": product.lower(),
            "category": category.lower(),
            "tags": tags.lower(),
            "brand": brand.lower(),
            "vendor": vendor.lower(),
        }
        reason = ""
        for field in fields:
            if pattern.pattern and pattern.search(texts.get(field, "")):
                reason = f"pattern:{field}"
                break
        if not reason and extra_keywords:
            for field in fields:
                field_value = texts.get(field, "")
                if any(keyword in field_value for keyword in extra_keywords):
                    reason = f"keyword:{field}"
                    break
        if not reason and exclude_low_cost_rows and cost is not None and cost < low_cost_threshold:
            reason = "low_cost"

        keep_mask.append(not reason)
        if reason:
            counts[reason] += 1

    filtered = work.loc[pd.Series(keep_mask, index=work.index)].copy()
    return filtered, counts


def inventory_rows_for_ordering(prepared_df: pd.DataFrame, store_code: str) -> pd.DataFrame:
    if prepared_df is None or prepared_df.empty:
        return pd.DataFrame(columns=_ordering_row_columns("inventory"))

    work = prepared_df.copy()
    work["store_code"] = store_code
    work["store_name"] = work.get("Store", bmp._store_name_from_abbr(store_code))
    work["sku"] = work.get("SKU", "").fillna("").astype(str).str.strip()
    work["vendor"] = work.apply(lambda row: _clean_text(row.get("Vendor"), row.get("Producer"), "Unknown Vendor"), axis=1)
    work["vendor_key"] = work["vendor"].map(_key_text)
    work["brand"] = work.apply(lambda row: _clean_text(row.get("brand_name"), row.get("Brand"), "Unknown"), axis=1)
    work["brand_key"] = work.get("brand_key", pd.Series("unknown", index=work.index)).fillna("unknown").astype(str)
    work["category"] = work.apply(lambda row: _clean_text(row.get("Category"), row.get("category_normalized"), "Unknown"), axis=1)
    work["product"] = work.apply(
        lambda row: _clean_text(row.get("Product"), row.get("_product_raw"), row.get("display_product"), "Unknown Product"),
        axis=1,
    )
    work["row_key"] = work.apply(
        lambda row: build_row_key(
            store_code=store_code,
            sku=row.get("sku", ""),
            brand_product_key=row.get("brand_product_key", ""),
            product=row.get("product", ""),
            brand_key=row.get("brand_key", ""),
        ),
        axis=1,
    )
    work["available"] = pd.to_numeric(work.get("Available", 0.0), errors="coerce").fillna(0.0).astype(float)
    work["cost"] = pd.to_numeric(work.get("Cost", 0.0), errors="coerce").fillna(0.0).astype(float)
    work["price"] = pd.to_numeric(work.get("Price_Used", work.get("Price", 0.0)), errors="coerce").fillna(0.0).astype(float)
    work["inventory_value"] = pd.to_numeric(work.get("Inventory_Value", 0.0), errors="coerce").fillna(0.0).astype(float)
    return work[_ordering_row_columns("inventory")].copy()


def sales_rows_for_ordering(prepared_df: pd.DataFrame, store_code: str) -> pd.DataFrame:
    if prepared_df is None or prepared_df.empty:
        return pd.DataFrame(columns=_ordering_row_columns("sales"))

    work = prepared_df.copy()
    work = work[~work.get("_is_return", False)].copy() if "_is_return" in work.columns else work
    if work.empty:
        return pd.DataFrame(columns=_ordering_row_columns("sales"))

    work["store_code"] = store_code
    work["store_name"] = work.get("Store", bmp._store_name_from_abbr(store_code))
    work["sku"] = work.get("SKU", "").fillna("").astype(str).str.strip()
    work["vendor"] = work.apply(lambda row: _clean_text(row.get("Vendor Name"), row.get("Producer"), "Unknown Vendor"), axis=1)
    work["vendor_key"] = work["vendor"].map(_key_text)
    work["brand"] = work.apply(lambda row: _clean_text(row.get("brand_name"), row.get("Brand"), "Unknown"), axis=1)
    work["brand_key"] = work.get("brand_key", pd.Series("unknown", index=work.index)).fillna("unknown").astype(str)
    work["category"] = work.apply(lambda row: _clean_text(row.get("Category"), row.get("category_normalized"), "Unknown"), axis=1)
    work["product"] = work.apply(
        lambda row: _clean_text(row.get("Product Name"), row.get("_product_raw"), row.get("display_product"), "Unknown Product"),
        axis=1,
    )
    work["row_key"] = work.apply(
        lambda row: build_row_key(
            store_code=store_code,
            sku=row.get("sku", ""),
            brand_product_key=row.get("brand_product_key", ""),
            product=row.get("product", ""),
            brand_key=row.get("brand_key", ""),
        ),
        axis=1,
    )
    work["qty"] = pd.to_numeric(work.get("_qty", 0.0), errors="coerce").fillna(0.0).clip(lower=0.0).astype(float)
    work = work[work["qty"] > 0].copy()
    if work.empty:
        return pd.DataFrame(columns=_ordering_row_columns("sales"))
    work["sale_date"] = pd.to_datetime(work.get("_date"), errors="coerce").dt.date
    work["price"] = pd.to_numeric(work.get("merge_price_basis", work.get("Price", 0.0)), errors="coerce").fillna(0.0).astype(float)
    merge_cost = pd.to_numeric(work.get("merge_cost_basis", 0.0), errors="coerce")
    realized_cost = (
        pd.to_numeric(work.get("_cogs_real", 0.0), errors="coerce").fillna(0.0)
        / work["qty"].replace({0: np.nan})
    ).replace([np.inf, -np.inf], np.nan)
    work["cost"] = realized_cost.where(realized_cost > 0, merge_cost).fillna(0.0).astype(float)
    return work[_ordering_row_columns("sales")].copy()


def aggregate_inventory_rows(row_df: pd.DataFrame) -> pd.DataFrame:
    if row_df is None or row_df.empty:
        return pd.DataFrame(columns=_inventory_agg_columns())

    work = row_df.copy()
    work["value_weight"] = np.where(work["available"] > 0, work["available"], 1.0)
    grouped = work.groupby("row_key", as_index=False).agg(
        store_code=("store_code", _first_mode),
        store_name=("store_name", _first_mode),
        sku=("sku", _first_mode),
        vendor=("vendor", _first_mode),
        vendor_key=("vendor_key", _first_mode),
        brand=("brand", _first_mode),
        brand_key=("brand_key", _first_mode),
        category=("category", _first_mode),
        product=("product", _first_mode),
        available=("available", "sum"),
        inventory_value=("inventory_value", "sum"),
        value_weight=("value_weight", "sum"),
        cost_num=("cost", lambda series: float((series * work.loc[series.index, "value_weight"]).sum())),
        price_num=("price", lambda series: float((series * work.loc[series.index, "value_weight"]).sum())),
        inventory_row_count=("row_key", "size"),
    )
    grouped["cost"] = grouped["cost_num"] / grouped["value_weight"].replace({0: np.nan})
    grouped["price"] = grouped["price_num"] / grouped["value_weight"].replace({0: np.nan})
    grouped = grouped.replace([np.inf, -np.inf], np.nan).fillna({"cost": 0.0, "price": 0.0})
    return grouped[_inventory_agg_columns()].copy()


def aggregate_sales_rows(sales_rows: pd.DataFrame, as_of_day: date, velocity_window_days: int) -> pd.DataFrame:
    if sales_rows is None or sales_rows.empty:
        return pd.DataFrame(columns=_sales_agg_columns())

    work = sales_rows.copy()
    work["sale_date"] = pd.to_datetime(work["sale_date"], errors="coerce").dt.date
    work = work[work["sale_date"].notna()].copy()
    if work.empty:
        return pd.DataFrame(columns=_sales_agg_columns())

    window_specs = {
        "7": 7,
        "14": 14,
        "30": 30,
        "velocity": velocity_window_days,
    }
    for label, days in window_specs.items():
        start_day = as_of_day - timedelta(days=max(days - 1, 0))
        work[f"qty_{label}"] = np.where(work["sale_date"] >= start_day, work["qty"], 0.0)

    work["qty_weight"] = np.where(work["qty"] > 0, work["qty"], 1.0)
    grouped = work.groupby("row_key", as_index=False).agg(
        store_code=("store_code", _first_mode),
        store_name=("store_name", _first_mode),
        sku=("sku", _first_mode),
        vendor=("vendor", _first_mode),
        vendor_key=("vendor_key", _first_mode),
        brand=("brand", _first_mode),
        brand_key=("brand_key", _first_mode),
        category=("category", _first_mode),
        product=("product", _first_mode),
        units_sold_7d=("qty_7", "sum"),
        units_sold_14d=("qty_14", "sum"),
        units_sold_30d=("qty_30", "sum"),
        units_sold_velocity=("qty_velocity", "sum"),
        last_sale_date=("sale_date", "max"),
        qty_weight=("qty_weight", "sum"),
        cost_num=("cost", lambda series: float((series * work.loc[series.index, "qty_weight"]).sum())),
        price_num=("price", lambda series: float((series * work.loc[series.index, "qty_weight"]).sum())),
        sales_row_count=("row_key", "size"),
    )
    grouped["cost"] = grouped["cost_num"] / grouped["qty_weight"].replace({0: np.nan})
    grouped["price"] = grouped["price_num"] / grouped["qty_weight"].replace({0: np.nan})
    grouped = grouped.replace([np.inf, -np.inf], np.nan).fillna({"cost": 0.0, "price": 0.0})
    return grouped[_sales_agg_columns()].copy()


def merge_inventory_sales(inventory_agg: pd.DataFrame, sales_agg: pd.DataFrame) -> pd.DataFrame:
    if inventory_agg.empty and sales_agg.empty:
        return pd.DataFrame(columns=_merged_columns())
    if inventory_agg.empty:
        inventory_agg = pd.DataFrame(columns=_inventory_agg_columns())
    if sales_agg.empty:
        sales_agg = pd.DataFrame(columns=_sales_agg_columns())

    merged = inventory_agg.merge(sales_agg, on="row_key", how="outer", suffixes=("_inv", "_sales"))
    for field, default in [
        ("store_code", ""),
        ("store_name", ""),
        ("sku", ""),
        ("vendor", "Unknown Vendor"),
        ("vendor_key", ""),
        ("brand", "Unknown"),
        ("brand_key", "unknown"),
        ("category", "Unknown"),
        ("product", "Unknown Product"),
    ]:
        merged[field] = _coalesce_text(merged, f"{field}_inv", f"{field}_sales", default=default)

    merged["available"] = pd.to_numeric(merged.get("available", 0.0), errors="coerce").fillna(0.0).astype(float)
    merged["inventory_value"] = pd.to_numeric(merged.get("inventory_value", 0.0), errors="coerce").fillna(0.0).astype(float)
    merged["inventory_row_count"] = pd.to_numeric(merged.get("inventory_row_count", 0), errors="coerce").fillna(0).astype(int)
    merged["units_sold_7d"] = pd.to_numeric(merged.get("units_sold_7d", 0.0), errors="coerce").fillna(0.0).astype(float)
    merged["units_sold_14d"] = pd.to_numeric(merged.get("units_sold_14d", 0.0), errors="coerce").fillna(0.0).astype(float)
    merged["units_sold_30d"] = pd.to_numeric(merged.get("units_sold_30d", 0.0), errors="coerce").fillna(0.0).astype(float)
    merged["units_sold_velocity"] = pd.to_numeric(merged.get("units_sold_velocity", 0.0), errors="coerce").fillna(0.0).astype(float)
    merged["sales_row_count"] = pd.to_numeric(merged.get("sales_row_count", 0), errors="coerce").fillna(0).astype(int)
    merged["cost"] = np.where(merged["cost_sales"].fillna(0.0) > 0, merged["cost_sales"], merged["cost_inv"]).astype(float)
    merged["price"] = np.where(merged["price_inv"].fillna(0.0) > 0, merged["price_inv"], merged["price_sales"]).astype(float)
    merged["last_sale_date"] = pd.to_datetime(merged.get("last_sale_date", pd.NaT), errors="coerce").dt.date
    merged["has_inventory"] = merged["inventory_row_count"].fillna(0).astype(float) > 0
    merged["eligible_brand_30d"] = False
    merged["eligible_vendor_30d"] = False
    return merged[_merged_columns()].copy()


def apply_eligibility_rules(
    merged_df: pd.DataFrame,
    eligible_brand_keys: set[str],
    eligible_vendor_keys: set[str],
    config: Mapping[str, Any],
) -> pd.DataFrame:
    if merged_df is None or merged_df.empty:
        return pd.DataFrame(columns=_merged_columns())

    mode = str(config.get("eligibility", {}).get("mode", "brand_or_vendor")).strip().lower()
    work = merged_df.copy()
    brand_match = work["brand_key"].fillna("").astype(str).isin(eligible_brand_keys)
    vendor_match = work["vendor_key"].fillna("").astype(str).isin(eligible_vendor_keys)

    if mode == "brand_only":
        mask = brand_match
    elif mode == "vendor_only":
        mask = vendor_match
    elif mode == "brand_and_vendor":
        mask = brand_match & vendor_match
    else:
        mask = brand_match | vendor_match

    filtered = work.loc[mask].copy()
    filtered["eligible_brand_30d"] = brand_match.loc[filtered.index].astype(bool)
    filtered["eligible_vendor_30d"] = vendor_match.loc[filtered.index].astype(bool)
    return filtered


def aggregate_family_par_rows(merged_df: pd.DataFrame) -> pd.DataFrame:
    if merged_df is None or merged_df.empty:
        return pd.DataFrame(columns=_merged_columns())

    work = merged_df.copy()
    group_meta = pd.DataFrame(
        [
            _build_family_group_metadata(
                store_code=row.get("store_code", ""),
                vendor_key=row.get("vendor_key", ""),
                brand_key=row.get("brand_key", ""),
                category=row.get("category", ""),
                product=row.get("product", ""),
                row_key=row.get("row_key", ""),
            )
            for _, row in work.iterrows()
        ],
        index=work.index,
    )
    work = pd.concat([work, group_meta], axis=1)
    work["last_sale_date"] = pd.to_datetime(work.get("last_sale_date"), errors="coerce")
    work["group_weight"] = np.where(
        pd.to_numeric(work["available"], errors="coerce").fillna(0.0) > 0,
        pd.to_numeric(work["available"], errors="coerce").fillna(0.0),
        np.where(
            pd.to_numeric(work["units_sold_14d"], errors="coerce").fillna(0.0) > 0,
            pd.to_numeric(work["units_sold_14d"], errors="coerce").fillna(0.0),
            np.where(
                pd.to_numeric(work["units_sold_30d"], errors="coerce").fillna(0.0) > 0,
                pd.to_numeric(work["units_sold_30d"], errors="coerce").fillna(0.0),
                1.0,
            ),
        ),
    ).astype(float)

    grouped = work.groupby("group_row_key", as_index=False).agg(
        row_key=("group_row_key", _first_mode),
        store_code=("store_code", _first_mode),
        store_name=("store_name", _first_mode),
        sku=("sku", _first_mode),
        vendor=("vendor", _first_mode),
        vendor_key=("vendor_key", _first_mode),
        brand=("brand", _first_mode),
        brand_key=("brand_key", _first_mode),
        category=("category", _first_mode),
        product=("group_product", _combine_group_products),
        group_is_family=("group_is_family", "max"),
        available=("available", "sum"),
        inventory_value=("inventory_value", "sum"),
        group_weight=("group_weight", "sum"),
        cost_num=("cost", lambda series: float((pd.to_numeric(series, errors="coerce").fillna(0.0) * work.loc[series.index, "group_weight"]).sum())),
        price_num=("price", lambda series: float((pd.to_numeric(series, errors="coerce").fillna(0.0) * work.loc[series.index, "group_weight"]).sum())),
        inventory_row_count=("inventory_row_count", "sum"),
        units_sold_7d=("units_sold_7d", "sum"),
        units_sold_14d=("units_sold_14d", "sum"),
        units_sold_30d=("units_sold_30d", "sum"),
        units_sold_velocity=("units_sold_velocity", "sum"),
        last_sale_date=("last_sale_date", _latest_timestamp),
        sales_row_count=("sales_row_count", "sum"),
        has_inventory=("has_inventory", "max"),
        eligible_brand_30d=("eligible_brand_30d", "max"),
        eligible_vendor_30d=("eligible_vendor_30d", "max"),
    )
    grouped["cost"] = grouped["cost_num"] / grouped["group_weight"].replace({0: np.nan})
    grouped["price"] = grouped["price_num"] / grouped["group_weight"].replace({0: np.nan})
    grouped["sku"] = np.where(grouped["group_is_family"].astype(bool), "", grouped["sku"].fillna("").astype(str))
    grouped["last_sale_date"] = pd.to_datetime(grouped["last_sale_date"], errors="coerce").dt.date
    grouped = grouped.replace([np.inf, -np.inf], np.nan).fillna({"cost": 0.0, "price": 0.0})
    return grouped[_merged_columns()].copy()


def compute_ordering_metrics(merged_df: pd.DataFrame, config: Mapping[str, Any]) -> pd.DataFrame:
    if merged_df is None or merged_df.empty:
        return pd.DataFrame(columns=METRIC_EXPORT_COLUMNS)

    work = merged_df.copy()
    velocity_window_days = int(config.get("reorder", {}).get("velocity_window_days", 30))
    target_cover_days = int(config.get("reorder", {}).get("target_cover_days", 14))
    needs_order_min_qty = int(config.get("reorder", {}).get("needs_order_min_qty", 1))
    urgent_days = float(config.get("reorder", {}).get("days_of_supply_urgent", 3.0))
    low_days = float(config.get("reorder", {}).get("days_of_supply_low", 7.0))
    high_sell_through = float(config.get("reorder", {}).get("high_sell_through_30d", 0.6))

    work["avg_daily_sold_14d"] = work["units_sold_14d"] / 14.0
    work["avg_daily_sold_30d"] = work["units_sold_30d"] / 30.0
    work["avg_daily_sold_velocity"] = work["units_sold_velocity"] / float(max(velocity_window_days, 1))
    work["sell_through_7d"] = _safe_sell_through(work["units_sold_7d"], work["available"])
    work["sell_through_14d"] = _safe_sell_through(work["units_sold_14d"], work["available"])
    work["sell_through_30d"] = _safe_sell_through(work["units_sold_30d"], work["available"])
    work["days_of_supply"] = np.where(
        work["avg_daily_sold_velocity"] > 0,
        work["available"] / work["avg_daily_sold_velocity"].replace({0: np.nan}),
        np.nan,
    )
    work["Par Level"] = work.apply(lambda row: _estimate_par_level(row, config, target_cover_days), axis=1)
    work["Target Qty"] = work["Par Level"].astype(int)
    work["Suggested Order Qty"] = np.ceil((work["Target Qty"] - work["available"]).clip(lower=0.0)).astype(int)
    work["Needs Order"] = np.where(work["Suggested Order Qty"] >= needs_order_min_qty, "Y", "N")

    priorities: list[str] = []
    notes: list[str] = []
    ranks: list[int] = []
    for _, row in work.iterrows():
        priority, rank, note = classify_reorder_priority(
            available=float(row.get("available", 0.0) or 0.0),
            units_sold_7d=float(row.get("units_sold_7d", 0.0) or 0.0),
            units_sold_14d=float(row.get("units_sold_14d", 0.0) or 0.0),
            units_sold_30d=float(row.get("units_sold_30d", 0.0) or 0.0),
            avg_daily_sold_14d=float(row.get("avg_daily_sold_14d", 0.0) or 0.0),
            days_of_supply=row.get("days_of_supply", np.nan),
            sell_through_7d=float(row.get("sell_through_7d", 0.0) or 0.0),
            sell_through_14d=float(row.get("sell_through_14d", 0.0) or 0.0),
            sell_through_30d=float(row.get("sell_through_30d", 0.0) or 0.0),
            suggested_order_qty=int(row.get("Suggested Order Qty", 0) or 0),
            target_cover_days=target_cover_days,
            urgent_days=urgent_days,
            low_days=low_days,
            high_sell_through=high_sell_through,
        )
        priorities.append(priority)
        notes.append(note)
        ranks.append(rank)

    work["Reorder Priority"] = priorities
    work["Priority Rank"] = ranks
    work["Reorder Notes / Reason"] = notes
    work["Last Sale Date"] = pd.to_datetime(work["last_sale_date"], errors="coerce").dt.date
    work["Available"] = work["available"].fillna(0.0).astype(float)
    work["Par Level"] = work["Par Level"].fillna(0).astype(int)
    work["Cost"] = work["cost"].fillna(0.0).astype(float)
    work["Price"] = work["price"].fillna(0.0).astype(float)
    work["Inventory Value"] = work["inventory_value"].fillna(0.0).astype(float)
    work["Units Sold 7d"] = work["units_sold_7d"].fillna(0.0).astype(float)
    work["Units Sold 14d"] = work["units_sold_14d"].fillna(0.0).astype(float)
    work["Units Sold 30d"] = work["units_sold_30d"].fillna(0.0).astype(float)
    work["Sell-Through 7d"] = work["sell_through_7d"].fillna(0.0).astype(float)
    work["Sell-Through 14d"] = work["sell_through_14d"].fillna(0.0).astype(float)
    work["Sell-Through 30d"] = work["sell_through_30d"].fillna(0.0).astype(float)
    work["Sell-Through 7D/14D/30D"] = work.apply(
        lambda row: _format_sell_through_triplet(
            row.get("Sell-Through 7d", 0.0),
            row.get("Sell-Through 14d", 0.0),
            row.get("Sell-Through 30d", 0.0),
        ),
        axis=1,
    )
    work["Avg Daily Sold 14d"] = work["avg_daily_sold_14d"].fillna(0.0).astype(float)
    work["Days of Supply"] = pd.to_numeric(work["days_of_supply"], errors="coerce")
    work["SKU"] = work["sku"].fillna("").astype(str)
    work["Vendor"] = work["vendor"].fillna("Unknown Vendor").astype(str)
    work["Brand"] = work["brand"].fillna("Unknown").astype(str)
    work["Category"] = work["category"].fillna("Unknown").astype(str)
    work["Product"] = work["product"].fillna("Unknown Product").astype(str)
    work["Row Key"] = work["row_key"].fillna("").astype(str)

    return work[METRIC_EXPORT_COLUMNS].copy()


def apply_ordering_filters(
    metrics_df: pd.DataFrame,
    config: Mapping[str, Any],
) -> tuple[pd.DataFrame, Counter]:
    if metrics_df is None or metrics_df.empty:
        return pd.DataFrame(columns=metrics_df.columns if metrics_df is not None else AUTO_COLUMNS), Counter()

    work = metrics_df.copy()
    keep_mask = pd.Series(True, index=work.index, dtype=bool)
    counts: Counter[str] = Counter()

    exclude_low_cost_rows = bool(config.get("exclusions", {}).get("exclude_low_cost_rows", True))
    low_cost_threshold = float(config.get("exclusions", {}).get("low_cost_threshold", 1.0))
    if exclude_low_cost_rows:
        cost_series = pd.to_numeric(work.get("Cost", 0.0), errors="coerce")
        low_cost_mask = cost_series.notna() & (cost_series < low_cost_threshold)
        excluded_low_cost = keep_mask & low_cost_mask
        if excluded_low_cost.any():
            counts["low_cost"] = int(excluded_low_cost.sum())
        keep_mask &= ~low_cost_mask

    min_units_sold_30d = float(config.get("eligibility", {}).get("min_units_sold_30d", 3))
    if min_units_sold_30d > 0:
        units_sold_30d = pd.to_numeric(work.get("Units Sold 30d", 0.0), errors="coerce").fillna(0.0)
        low_sales_mask = units_sold_30d < min_units_sold_30d
        excluded_low_sales = keep_mask & low_sales_mask
        if excluded_low_sales.any():
            counts["min_units_sold_30d"] = int(excluded_low_sales.sum())
        keep_mask &= ~low_sales_mask

    return work.loc[keep_mask].copy(), counts


def classify_reorder_priority(
    available: float,
    units_sold_7d: float,
    units_sold_14d: float,
    units_sold_30d: float,
    avg_daily_sold_14d: float,
    days_of_supply: Any,
    sell_through_7d: float,
    sell_through_14d: float,
    sell_through_30d: float,
    suggested_order_qty: int,
    target_cover_days: int,
    urgent_days: float,
    low_days: float,
    high_sell_through: float,
) -> tuple[str, int, str]:
    sales_summary = f"{units_sold_7d:.0f}/7d, {units_sold_14d:.0f}/14d, {units_sold_30d:.0f}/30d"
    sell_through_summary = _format_sell_through_triplet(sell_through_7d, sell_through_14d, sell_through_30d)
    pace_summary = f"{avg_daily_sold_14d:.1f}/day over 14d"
    trend_summary = _sales_trend_summary(units_sold_7d, units_sold_14d)

    if suggested_order_qty <= 0:
        if units_sold_30d <= 0:
            return "Healthy", 0, "No reorder: no sales in the last 30 days."
        return "Healthy", 0, f"No reorder: current stock covers the {target_cover_days}d target at {pace_summary}."

    finite_dos = pd.notna(days_of_supply)
    if available <= 0 and units_sold_30d > 0:
        note = f"Out of stock with recent sales ({sales_summary}); sell-through {sell_through_summary}; suggest {suggested_order_qty}."
        return "Urgent", 3, _append_note_part(note, trend_summary)
    if finite_dos and float(days_of_supply) <= urgent_days:
        note = (
            f"{float(days_of_supply):.1f} days of supply vs {target_cover_days}d target; "
            f"{pace_summary}; suggest {suggested_order_qty}."
        )
        return "Urgent", 3, _append_note_part(note, trend_summary)
    if finite_dos and float(days_of_supply) <= low_days:
        note = (
            f"{float(days_of_supply):.1f} days of supply is below the reorder threshold; "
            f"{sales_summary}; suggest {suggested_order_qty}."
        )
        return "Low Cover", 2, _append_note_part(note, trend_summary)
    if sell_through_30d >= high_sell_through:
        note = (
            f"Fast turn item: sell-through {sell_through_summary}; "
            f"{pace_summary}; suggest {suggested_order_qty}."
        )
        return "Reorder", 1, _append_note_part(note, trend_summary)
    note = f"Below target cover based on recent demand ({sales_summary}); suggest {suggested_order_qty}."
    return "Reorder", 1, _append_note_part(note, trend_summary)


def sort_ordering_rows(metrics_df: pd.DataFrame) -> pd.DataFrame:
    if metrics_df is None or metrics_df.empty:
        return pd.DataFrame(columns=metrics_df.columns if metrics_df is not None else AUTO_COLUMNS)

    work = metrics_df.copy()
    work["_brand_sort"] = work["Brand"].fillna("").astype(str).str.upper()
    work["_category_sort"] = work["Category"].fillna("").astype(str).str.upper()
    work["_cost_sort"] = pd.to_numeric(work["Cost"], errors="coerce").fillna(0.0)
    work["_price_sort"] = pd.to_numeric(work["Price"], errors="coerce").fillna(0.0)
    work["_priority_sort"] = pd.to_numeric(work["Priority Rank"], errors="coerce").fillna(0).astype(int)
    work["_product_sort"] = work["Product"].fillna("").astype(str).str.upper()
    work["_sku_sort"] = work["SKU"].fillna("").astype(str).str.upper()
    work = work.sort_values(
        [
            "_brand_sort",
            "_category_sort",
            "_cost_sort",
            "_price_sort",
            "_product_sort",
            "_priority_sort",
            "_sku_sort",
        ],
        ascending=[True, True, True, True, True, False, True],
    )
    return work.drop(
        columns=[
            "_brand_sort",
            "_category_sort",
            "_cost_sort",
            "_price_sort",
            "_priority_sort",
            "_product_sort",
            "_sku_sort",
        ]
    )


def build_auto_sheet_df(metrics_df: pd.DataFrame) -> pd.DataFrame:
    if metrics_df is None or metrics_df.empty:
        return pd.DataFrame(columns=AUTO_COLUMNS)
    return metrics_df[AUTO_COLUMNS].copy()


def build_review_sheet_df(metrics_df: pd.DataFrame, config: Mapping[str, Any]) -> pd.DataFrame:
    manual_columns = list(config.get("review_manual_columns", []))
    review_columns = AUTO_COLUMNS + manual_columns
    if metrics_df is None or metrics_df.empty:
        return pd.DataFrame(columns=review_columns)

    work = metrics_df[AUTO_COLUMNS].copy()
    for column in manual_columns:
        work[column] = ""
    return work[review_columns].copy()


def build_store_summary(
    inventory_prepared: pd.DataFrame,
    store_code: str,
    week_of: date,
    snapshot_generated_at: datetime,
) -> OrderedDict[str, Any]:
    store_name = bmp._store_name_from_abbr(store_code)
    summary_values = {
        "Store": f"{store_code} - {store_name}",
        "Week Of": week_of.isoformat(),
        "Snapshot Generated At": snapshot_generated_at.astimezone(ZoneInfo(bmp.REPORT_TZ)).strftime("%Y-%m-%d %H:%M %Z"),
        "Total Inventory Value": compute_total_inventory_value(inventory_prepared),
    }
    return OrderedDict((field, summary_values[field]) for field in SUMMARY_FIELD_ORDER)


def compute_total_inventory_value(inventory_prepared: pd.DataFrame) -> float:
    if inventory_prepared is None or inventory_prepared.empty:
        return 0.0

    if "Inventory_Value" in inventory_prepared.columns:
        return float(pd.to_numeric(inventory_prepared["Inventory_Value"], errors="coerce").fillna(0.0).sum())

    available = pd.to_numeric(inventory_prepared.get("Available", 0.0), errors="coerce").fillna(0.0)
    cost = pd.to_numeric(inventory_prepared.get("Cost", 0.0), errors="coerce").fillna(0.0)
    return float((available * cost).sum())


def write_store_artifacts(
    bundle: Mapping[str, Any],
    output_root: Path,
) -> dict[str, str]:
    store_root = output_root / str(bundle["week_of"]) / str(bundle["store_code"])
    store_root.mkdir(parents=True, exist_ok=True)

    artifact_paths = {
        "normalized_inventory": store_root / "normalized_inventory.csv",
        "normalized_sales": store_root / "normalized_sales.csv",
        "sku_metrics": store_root / "sku_metrics.csv",
        "auto_preview": store_root / "auto_preview.csv",
        "review_preview": store_root / "review_preview.csv",
        "sheet_payload": store_root / "sheet_payload.json",
    }

    bundle["normalized_inventory"].to_csv(artifact_paths["normalized_inventory"], index=False)
    bundle["normalized_sales"].to_csv(artifact_paths["normalized_sales"], index=False)
    bundle["sku_metrics"].to_csv(artifact_paths["sku_metrics"], index=False)
    bundle["auto_df"].to_csv(artifact_paths["auto_preview"], index=False)
    bundle["review_df"].to_csv(artifact_paths["review_preview"], index=False)

    payload = {
        "store_code": bundle["store_code"],
        "week_of": bundle["week_of"],
        "tab_titles": bundle["tab_titles"],
        "summary": bundle["summary"],
        "logs": bundle["logs"],
        "auto_row_count": int(len(bundle["auto_df"])),
        "review_row_count": int(len(bundle["review_df"])),
        "auto_preview_rows": [_json_safe_record(row) for row in bundle["auto_df"].head(5).to_dict(orient="records")],
        "review_preview_rows": [_json_safe_record(row) for row in bundle["review_df"].head(5).to_dict(orient="records")],
    }
    artifact_paths["sheet_payload"].write_text(json.dumps(payload, indent=2, default=str), encoding="utf-8")
    return {name: str(path) for name, path in artifact_paths.items()}


def write_store_tabs_to_google_sheet(
    bundle: Mapping[str, Any],
    spreadsheet_target: str,
    config: Mapping[str, Any],
    logger: logging.Logger,
) -> dict[str, Any]:
    output_flags = sheet_output_flags(config)
    spreadsheet_id, _gid = parse_spreadsheet_target(spreadsheet_target)
    service = authenticate_sheets()

    summary_rows = build_summary_rows(bundle["summary"])
    write_result: dict[str, Any] = {
        "spreadsheet_id": spreadsheet_id,
    }

    written_titles: list[str] = []
    if output_flags["auto"]:
        auto_write = upsert_ordering_tab(
            service=service,
            spreadsheet_id=spreadsheet_id,
            title=str(bundle["tab_titles"]["auto"]),
            summary_rows=summary_rows,
            df=bundle["auto_df"],
            sheet_kind="auto",
            hidden_headers={"Row Key"},
        )
        write_result["auto"] = auto_write
        written_titles.append(auto_write["title"])
    else:
        write_result["auto"] = {"enabled": False, "title": str(bundle["tab_titles"]["auto"])}

    if output_flags["review"]:
        review_title = str(bundle["tab_titles"]["review"])
        existing_review_values = read_sheet_values(service, spreadsheet_id, review_title)
        preserved_review_df = merge_preserved_review_columns(
            bundle["review_df"],
            existing_review_values,
            manual_columns=config.get("review_manual_columns", []),
        )
        review_write = upsert_ordering_tab(
            service=service,
            spreadsheet_id=spreadsheet_id,
            title=review_title,
            summary_rows=summary_rows,
            df=preserved_review_df,
            sheet_kind="review",
            hidden_headers={"Row Key"},
        )
        write_result["review"] = review_write
        written_titles.append(review_write["title"])
    else:
        write_result["review"] = {"enabled": False, "title": str(bundle["tab_titles"]["review"])}

    store_name = bmp._store_name_from_abbr(str(bundle["store_code"]))
    readme_write = upsert_readme_tab(
        service=service,
        spreadsheet_id=spreadsheet_id,
        store_code=str(bundle["store_code"]),
        store_name=store_name,
        output_flags=output_flags,
        week_of=str(bundle["week_of"]),
        tab_titles=bundle["tab_titles"],
        manual_columns=config.get("review_manual_columns", []),
        snapshot_generated_at=str(bundle["snapshot_generated_at"]),
    )
    write_result["readme"] = readme_write

    latest_tab_order = []
    if output_flags["review"]:
        latest_tab_order.append(str(bundle["tab_titles"]["review"]))
    if output_flags["auto"]:
        latest_tab_order.append(str(bundle["tab_titles"]["auto"]))
    moved_titles = move_latest_tabs_next_to_readme(service, spreadsheet_id, latest_tab_order)
    write_result["front_tab_order"] = ["README"] + moved_titles

    logger.info("[%s] Wrote tabs: %s", bundle["store_code"], ", ".join(written_titles))
    return write_result


def build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(
        description="Build store-first weekly Dutchie reorder sheets and upsert them into Google Sheets.",
    )
    parser.add_argument("--config", default=str(DEFAULT_CONFIG_PATH), help="Path to the weekly ordering config JSON.")
    parser.add_argument("--env-file", default=DEFAULT_ENV_FILE, help="Path to the Dutchie API .env file.")
    parser.add_argument("--store", help="Single store code, for example MV.")
    parser.add_argument("--stores", nargs="*", help="One or more store codes, for example MV LG.")
    parser.add_argument("--all-stores", action="store_true", help="Process every store in the config.")
    parser.add_argument("--week", help="Week-of date. Any date in the target week is normalized to Monday.")
    parser.add_argument("--as-of-date", help="As-of date for sales windows and snapshots, YYYY-MM-DD.")
    parser.add_argument("--sheet-url", help="Editable Google Sheets URL or raw spreadsheet ID.")
    parser.add_argument("--dry-run", action="store_true", help="Write local proof artifacts only; do not call Google Sheets.")
    parser.add_argument("--fixture-root", help="Read raw Dutchie JSON fixtures from this directory instead of the live API.")
    parser.add_argument("--output-root", help="Override artifact output root directory.")
    parser.add_argument("--log-level", default="INFO", help="Logging level. Default: INFO")
    return parser


def configure_logging(level_name: str) -> logging.Logger:
    logging.basicConfig(level=getattr(logging, str(level_name).upper(), logging.INFO), format="%(message)s")
    return logging.getLogger("weekly_store_ordering")


def main(argv: Sequence[str] | None = None) -> int:
    parser = build_parser()
    args = parser.parse_args(argv)
    config = load_ordering_config(args.config)
    output_flags = sheet_output_flags(config)
    logger = configure_logging(args.log_level)
    tz_name = str(config.get("timezone", "America/Los_Angeles"))
    as_of_day = resolve_as_of_day(args.as_of_date, tz_name)
    week_of = resolve_week_of(args.week, as_of_day)
    store_codes = default_store_selection(args, config)
    fixture_root = Path(args.fixture_root).resolve() if args.fixture_root else None
    output_root = Path(args.output_root or config.get("output_root", "reports/store_weekly_ordering")).resolve()
    spreadsheet_targets = resolve_spreadsheet_targets(args, config)

    if not args.dry_run:
        missing_targets = [code for code in store_codes if not resolve_store_spreadsheet_target(spreadsheet_targets, code)]
        if missing_targets:
            if len(missing_targets) == len(store_codes):
                parser.error(
                    "Google Sheets target missing. Provide --sheet-url or configure the sheet target in the config."
                )
            parser.error(
                "Missing Google Sheets targets for stores: "
                + ", ".join(missing_targets)
                + ". Add store-specific targets or a DEFAULT target."
            )

    snapshot_generated_at = datetime.now(ZoneInfo(tz_name))
    proof_rows: list[dict[str, Any]] = []
    failed_stores: list[str] = []
    for store_code in store_codes:
        tab_titles = build_store_tab_titles(store_code, week_of, config)
        try:
            logger.info("[%s] Building weekly ordering bundle for week %s as of %s", store_code, week_of.isoformat(), as_of_day.isoformat())
            payloads = load_store_payloads(store_code, as_of_day, config, args.env_file, fixture_root, logger)
            bundle = build_ordering_bundle(
                store_code=store_code,
                week_of=week_of,
                as_of_day=as_of_day,
                payloads=payloads,
                config=config,
                snapshot_generated_at=snapshot_generated_at,
                logger=logger,
            )
            artifact_paths = write_store_artifacts(bundle, output_root)
            enabled_tab_titles = [
                str(bundle["tab_titles"][tab_name])
                for tab_name in ("auto", "review")
                if output_flags[tab_name]
            ]
            logger.info(
                "[%s] rows=%s needs_order=%s tabs=%s",
                store_code,
                len(bundle["auto_df"]),
                bundle["logs"]["needs_order_rows"],
                ", ".join(enabled_tab_titles),
            )
            logger.info(
                "[%s] excluded inventory=%s sales=%s tx=%s",
                store_code,
                bundle["logs"]["inventory_exclusion_counts"],
                bundle["logs"]["sales_exclusion_counts"],
                bundle["logs"]["transaction_drop_counts"],
            )
            write_result = {"mode": "dry-run"}
            if not args.dry_run:
                spreadsheet_target = resolve_store_spreadsheet_target(spreadsheet_targets, store_code)
                write_result = write_store_tabs_to_google_sheet(bundle, spreadsheet_target, config, logger)
            proof_rows.append(
                {
                    "store_code": store_code,
                    "status": "ok",
                    "tab_auto": bundle["tab_titles"]["auto"],
                    "tab_review": bundle["tab_titles"]["review"],
                    "tab_auto_enabled": output_flags["auto"],
                    "tab_review_enabled": output_flags["review"],
                    "rows": int(len(bundle["auto_df"])),
                    "needs_order_rows": int(bundle["logs"]["needs_order_rows"]),
                    "artifact_payload": artifact_paths["sheet_payload"],
                    "write_result": write_result,
                }
            )
        except Exception as exc:
            failed_stores.append(store_code)
            logger.exception("[%s] Failed weekly ordering run: %s", store_code, exc)
            proof_rows.append(
                {
                    "store_code": store_code,
                    "status": "failed",
                    "tab_auto": tab_titles["auto"],
                    "tab_review": tab_titles["review"],
                    "tab_auto_enabled": output_flags["auto"],
                    "tab_review_enabled": output_flags["review"],
                    "error_type": type(exc).__name__,
                    "error": str(exc),
                }
            )
            continue

    proof_path = output_root / week_of.isoformat() / "run_summary.json"
    proof_path.parent.mkdir(parents=True, exist_ok=True)
    proof_path.write_text(json.dumps(proof_rows, indent=2, default=str), encoding="utf-8")
    logger.info("Run summary saved to %s", proof_path)
    if failed_stores:
        logger.warning("Weekly ordering completed with failures for stores: %s", ", ".join(failed_stores))
        return 1
    return 0


def _ordering_row_columns(kind: str) -> list[str]:
    common = [
        "row_key",
        "store_code",
        "store_name",
        "sku",
        "vendor",
        "vendor_key",
        "brand",
        "brand_key",
        "category",
        "product",
        "cost",
        "price",
    ]
    if kind == "inventory":
        return common + ["available", "inventory_value"]
    return common + ["qty", "sale_date"]


def _inventory_agg_columns() -> list[str]:
    return [
        "row_key",
        "store_code",
        "store_name",
        "sku",
        "vendor",
        "vendor_key",
        "brand",
        "brand_key",
        "category",
        "product",
        "available",
        "cost",
        "price",
        "inventory_value",
        "inventory_row_count",
    ]


def _sales_agg_columns() -> list[str]:
    return [
        "row_key",
        "store_code",
        "store_name",
        "sku",
        "vendor",
        "vendor_key",
        "brand",
        "brand_key",
        "category",
        "product",
        "units_sold_7d",
        "units_sold_14d",
        "units_sold_30d",
        "units_sold_velocity",
        "cost",
        "price",
        "last_sale_date",
        "sales_row_count",
    ]


def _merged_columns() -> list[str]:
    return [
        "row_key",
        "store_code",
        "store_name",
        "sku",
        "vendor",
        "vendor_key",
        "brand",
        "brand_key",
        "category",
        "product",
        "available",
        "cost",
        "price",
        "inventory_value",
        "inventory_row_count",
        "units_sold_7d",
        "units_sold_14d",
        "units_sold_30d",
        "units_sold_velocity",
        "last_sale_date",
        "sales_row_count",
        "has_inventory",
        "eligible_brand_30d",
        "eligible_vendor_30d",
    ]


def build_row_key(
    store_code: str,
    sku: Any,
    brand_product_key: Any,
    product: Any,
    brand_key: Any,
) -> str:
    sku_text = str(sku or "").strip()
    if sku_text:
        return f"{store_code}|sku:{sku_text}"
    brand_product_text = str(brand_product_key or "").strip()
    if brand_product_text:
        return f"{store_code}|product:{brand_product_text}"
    product_key = _key_text(product)
    brand_value = str(brand_key or "unknown").strip() or "unknown"
    return f"{store_code}|fallback:{brand_value}|{product_key or 'unknown'}"


def _safe_sell_through(units_sold: pd.Series, available: pd.Series) -> pd.Series:
    denom = (units_sold.fillna(0.0) + available.fillna(0.0)).replace({0: np.nan})
    return (units_sold.fillna(0.0) / denom).replace([np.inf, -np.inf], np.nan).fillna(0.0)


def _format_sell_through_triplet(value_7d: Any, value_14d: Any, value_30d: Any) -> str:
    parts = [_format_percent_display(value) for value in [value_7d, value_14d, value_30d]]
    if len(set(parts)) == 1:
        return parts[0]
    return " / ".join(parts)


def _format_percent_display(value: Any) -> str:
    try:
        numeric = Decimal(str(float(value)))
    except Exception:
        numeric = Decimal("0")
    percent = (numeric * Decimal("100")).quantize(Decimal("1"), rounding=ROUND_HALF_UP)
    return f"{int(percent)}%"


def _build_family_group_metadata(
    store_code: Any,
    vendor_key: Any,
    brand_key: Any,
    category: Any,
    product: Any,
    row_key: Any,
) -> dict[str, str]:
    raw_product = str(product or "").strip()
    parts = [part.strip() for part in raw_product.split("|") if part.strip()]
    product_parts = parts[1:] if len(parts) > 1 else ([raw_product] if raw_product else [])

    strain_index = None
    strain_bucket = ""
    for index, token in enumerate(product_parts):
        normalized = _normalize_strain_bucket(token)
        if normalized:
            strain_index = index
            strain_bucket = normalized
            break

    if strain_index is not None and strain_index > 0:
        base_parts = product_parts[:strain_index]
        family_display = " | ".join(base_parts + [strain_bucket]).strip()
        family_key = _key_text(family_display)
        category_key = _key_text(category)
        vendor_text = str(vendor_key or "").strip() or "unknownvendor"
        brand_text = str(brand_key or "").strip() or "unknownbrand"
        return {
            "group_row_key": f"{store_code}|family:{vendor_text}|{brand_text}|{category_key}|{family_key}",
            "group_product": _display_product_without_brand(raw_product),
            "group_is_family": True,
        }

    return {
        "group_row_key": str(row_key or "").strip(),
        "group_product": _display_product_without_brand(raw_product),
        "group_is_family": False,
    }


def _display_product_without_brand(product: Any) -> str:
    raw_product = str(product or "").strip()
    if not raw_product:
        return ""
    parts = [part.strip() for part in raw_product.split("|") if part.strip()]
    if len(parts) <= 1:
        return raw_product
    return " | ".join(parts[1:])


def _combine_group_products(values: Any) -> str:
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
        candidate = tokenized[0][index]
        candidate_key = _key_text(candidate)
        if all(_key_text(tokens[index]) == candidate_key for tokens in tokenized[1:]):
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


def _normalize_strain_bucket(value: Any) -> str:
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


def _latest_timestamp(series: pd.Series) -> Any:
    timestamps = pd.to_datetime(series, errors="coerce")
    if timestamps.isna().all():
        return pd.NaT
    return timestamps.max()


def _estimate_par_level(row: Mapping[str, Any], config: Mapping[str, Any], target_cover_days: int) -> int:
    reorder_config = dict(config.get("reorder", {}) or {})
    units_sold_7d = max(0.0, _to_float(row.get("units_sold_7d", row.get("Units Sold 7d", 0.0))))
    units_sold_14d = max(0.0, _to_float(row.get("units_sold_14d", row.get("Units Sold 14d", 0.0))))
    units_sold_30d = max(0.0, _to_float(row.get("units_sold_30d", row.get("Units Sold 30d", 0.0))))
    available = max(0.0, _to_float(row.get("available", row.get("Available", 0.0))))
    sell_through_7d = max(0.0, _to_float(row.get("sell_through_7d", row.get("Sell-Through 7d", 0.0))))
    sell_through_14d = max(0.0, _to_float(row.get("sell_through_14d", row.get("Sell-Through 14d", 0.0))))
    sell_through_30d = max(0.0, _to_float(row.get("sell_through_30d", row.get("Sell-Through 30d", 0.0))))

    if units_sold_7d <= 0 and units_sold_14d <= 0 and units_sold_30d <= 0:
        return 0

    target_cover_days = max(int(target_cover_days or 14), 1)
    projected_7d = units_sold_7d * (target_cover_days / 7.0)
    projected_14d = units_sold_14d * (target_cover_days / 14.0)
    projected_30d = units_sold_30d * (target_cover_days / 30.0)

    weight_7d = float(reorder_config.get("par_weight_7d", 0.5) or 0.0)
    weight_14d = float(reorder_config.get("par_weight_14d", 0.3) or 0.0)
    weight_30d = float(reorder_config.get("par_weight_30d", 0.2) or 0.0)
    weight_total = weight_7d + weight_14d + weight_30d
    if weight_total <= 0:
        weight_7d, weight_14d, weight_30d = 0.5, 0.3, 0.2
        weight_total = 1.0

    projected_two_weeks = (
        (projected_7d * weight_7d)
        + (projected_14d * weight_14d)
        + (projected_30d * weight_30d)
    ) / weight_total

    # If the last 14 days are empty, keep older 30-day sales on a short leash so stale items do not overinflate.
    if units_sold_7d <= 0 and units_sold_14d <= 0 and units_sold_30d > 0:
        projected_two_weeks *= float(reorder_config.get("par_stale_30d_dampener", 0.6) or 0.6)

    # Low available units plus high sell-through usually means observed sales were capped by stock on hand.
    max_sell_through = max(sell_through_7d, sell_through_14d, sell_through_30d)
    projected_floor = max(projected_two_weeks, projected_14d)
    if projected_floor > 0:
        coverage_ratio = min(1.0, available / projected_floor)
        sell_pressure = max(0.0, (max_sell_through - 0.55) / 0.45)
        uplift_max = float(reorder_config.get("par_stockout_uplift_max", 0.25) or 0.25)
        uplift = min(uplift_max, max(0.0, (1.0 - coverage_ratio) * sell_pressure * uplift_max))
        projected_two_weeks *= 1.0 + uplift

    return int(math.ceil(max(projected_two_weeks, 0.0)))


def _sales_trend_summary(units_sold_7d: float, units_sold_14d: float) -> str:
    if units_sold_14d <= 0:
        return ""

    baseline_7d = units_sold_14d / 2.0
    if baseline_7d <= 0:
        return ""
    if units_sold_7d >= baseline_7d * 1.25 and units_sold_14d >= 4:
        return "7d pace is accelerating."
    if units_sold_7d <= baseline_7d * 0.75 and units_sold_14d >= 4:
        return "7d pace is slowing."
    return ""


def _append_note_part(note: str, extra: str) -> str:
    base = str(note or "").strip()
    tail = str(extra or "").strip()
    if not tail:
        return base
    if not base:
        return tail
    return f"{base} {tail}"


def _coalesce_text(df: pd.DataFrame, left_col: str, right_col: str, default: str = "") -> pd.Series:
    left = df[left_col].fillna("").astype(str).str.strip() if left_col in df.columns else pd.Series("", index=df.index)
    right = df[right_col].fillna("").astype(str).str.strip() if right_col in df.columns else pd.Series("", index=df.index)
    out = left.where(left != "", right)
    out = out.where(out != "", default)
    return out.astype(str)


def _coalesce_numeric(df: pd.DataFrame, left_col: str, right_col: str, default: float) -> pd.Series:
    left = pd.to_numeric(df[left_col], errors="coerce") if left_col in df.columns else pd.Series(np.nan, index=df.index)
    right = pd.to_numeric(df[right_col], errors="coerce") if right_col in df.columns else pd.Series(np.nan, index=df.index)
    return left.fillna(right).fillna(default)


def _first_nonempty(*values: Any) -> str:
    for value in values:
        text = str(value or "").strip()
        if text:
            return text
    return ""


def _clean_text(*values: Any) -> str:
    return _first_nonempty(*values)


def _to_float(*values: Any) -> float:
    for value in values:
        try:
            if value is None or value == "":
                continue
            if pd.isna(value):
                continue
            return float(value)
        except Exception:
            continue
    return 0.0


def _maybe_float(*values: Any) -> float | None:
    for value in values:
        try:
            if value is None or value == "":
                continue
            if pd.isna(value):
                continue
            return float(value)
        except Exception:
            continue
    return None


def _key_text(value: Any) -> str:
    text = str(value or "").strip().lower()
    text = re.sub(r"\s+", " ", text)
    return text


def _first_mode(series: pd.Series) -> Any:
    cleaned = [str(value).strip() for value in series if str(value).strip()]
    if not cleaned:
        return ""
    return Counter(cleaned).most_common(1)[0][0]


def _log_adapter(logger: logging.Logger) -> Callable[[str], None]:
    def _log(message: str) -> None:
        logger.info(message)

    return _log


def _json_safe_record(record: Mapping[str, Any]) -> dict[str, Any]:
    clean: dict[str, Any] = {}
    for key, value in record.items():
        if value is None:
            clean[str(key)] = None
            continue
        try:
            if pd.isna(value):
                clean[str(key)] = None
                continue
        except Exception:
            pass
        clean[str(key)] = value
    return clean


if __name__ == "__main__":
    raise SystemExit(main())
