#!/usr/bin/env python3

from __future__ import annotations

import argparse
from concurrent.futures import ThreadPoolExecutor, as_completed
from datetime import date, datetime, timedelta
from pathlib import Path
import math
import re
from threading import Lock
from typing import Any

import pandas as pd

from dutchie_api_reports import (
    DEFAULT_API_WORKERS,
    DEFAULT_ENV_FILE,
    STORE_CODES,
    canonical_env_map,
    create_session,
    local_date_range_to_utc_strings,
    parse_store_codes,
    positive_int,
    print_threadsafe,
    request_json,
    resolve_integrator_key,
    resolve_store_keys,
    resolve_worker_count,
)
from inventory_order_reports import ORDER_REPORT_WINDOWS, order_report_filename

TRANSACTION_STATUS_BLOCKLIST = {
    "cancelled",
    "canceled",
    "void",
    "voided",
    "deleted",
}

ORDER_REPORT_COLUMNS = [
    "Location",
    "Store",
    "Brand",
    "Vendor",
    "Category",
    "Product Name",
    "SKU",
    "Quantity on Hand",
    "Quantity Sold",
    "Sold Per Day",
    "Avg Daily Sales",
    "Days Remaining",
    "Last Wholesale Cost",
    "Price",
    "Last Ordered Quantity",
    "Days Since Last Received",
    "Master Category",
    "Strain",
    "Strain Type",
    "Flower Type",
    "Concentrate Type",
    "UPC/GTIN",
    "Provincial SKU",
    "Last Audit",
    "Tags",
]


def _first_nonempty(*values):
    for value in values:
        if value is None:
            continue
        if isinstance(value, str):
            text = value.strip()
            if text:
                return text
            continue
        return value
    return ""


def _clean_text(value):
    return re.sub(r"\s+", " ", str(value or "").strip())


def _to_float(value, default=0.0):
    try:
        if value in (None, ""):
            return float(default)
        return float(value)
    except Exception:
        return float(default)


def _to_date(value):
    if value in (None, ""):
        return None
    try:
        return pd.to_datetime(value, errors="coerce").date()
    except Exception:
        return None


def _safe_divide(numerator, denominator):
    try:
        numerator_value = float(numerator or 0.0)
        denominator_value = float(denominator or 0.0)
    except Exception:
        return math.nan
    if denominator_value <= 0:
        return math.nan
    return numerator_value / denominator_value


def _coalesce_series(frame, *columns, default=""):
    for column in columns:
        if column in frame.columns:
            values = frame[column]
            if not pd.api.types.is_object_dtype(values.dtype):
                values = values.astype("object")
            mask = values.notna()
            if mask.any():
                result = values.where(mask, None)
                for next_column in columns[columns.index(column) + 1 :]:
                    if next_column not in frame.columns:
                        continue
                    result = result.where(result.notna(), frame[next_column])
                if default != "":
                    result = result.fillna(default)
                return result
    return pd.Series([default] * len(frame), index=frame.index, dtype="object")


def _weighted_average(series, weights):
    valid = pd.to_numeric(series, errors="coerce")
    valid_weights = pd.to_numeric(weights, errors="coerce").fillna(0.0)
    mask = valid.notna()
    if not mask.any():
        return math.nan
    masked_values = valid[mask]
    masked_weights = valid_weights[mask]
    total_weight = float(masked_weights.sum())
    if total_weight <= 0:
        return float(masked_values.mean())
    return float((masked_values * masked_weights).sum() / total_weight)


def _parse_brand_from_name(product_name):
    text = _clean_text(product_name)
    if not text:
        return ""
    if "|" in text:
        return _clean_text(text.split("|", 1)[0])
    return _clean_text(text.split("-", 1)[0])


def _tag_text(tags):
    if not tags:
        return ""
    names = []
    for tag in tags:
        if isinstance(tag, dict):
            text = _clean_text(_first_nonempty(tag.get("name"), tag.get("label"), tag.get("value")))
        else:
            text = _clean_text(tag)
        if text:
            names.append(text)
    return ", ".join(names)


def _join_key(sku, product_name):
    sku_text = _clean_text(sku)
    if sku_text:
        return f"sku:{sku_text.upper()}"
    return f"name:{_clean_text(product_name).lower()}"


def _first_mode(series):
    cleaned = [_clean_text(value) for value in series if _clean_text(value)]
    if not cleaned:
        return ""
    counts = pd.Series(cleaned).value_counts()
    return str(counts.index[0]) if not counts.empty else cleaned[0]


def compute_windows(anchor_day=None):
    end_day = anchor_day or date.today()
    windows = []
    for days in ORDER_REPORT_WINDOWS:
        start_day = end_day - timedelta(days=int(days))
        windows.append((int(days), start_day, end_day))
    return windows


def _build_product_lookup(products_payload):
    lookup = {}
    for row in products_payload or []:
        if not isinstance(row, dict):
            continue
        product_id = row.get("productId")
        try:
            key = int(product_id)
        except Exception:
            continue
        lookup[key] = row
    return lookup


def build_inventory_frame(inventory_payload, store_code):
    store_label = STORE_CODES.get(store_code, store_code)
    rows = []
    for item in inventory_payload or []:
        if not isinstance(item, dict):
            continue

        product_name = _clean_text(_first_nonempty(item.get("productName"), item.get("alternateName")))
        sku = _clean_text(item.get("sku"))
        quantity_on_hand = _to_float(item.get("quantityAvailable"))
        row = {
            "Join Key": _join_key(sku, product_name),
            "Location": store_label,
            "Store": store_code,
            "Brand": _clean_text(_first_nonempty(item.get("brandName"), _parse_brand_from_name(product_name))),
            "Vendor": _clean_text(_first_nonempty(item.get("vendor"), item.get("producer"))),
            "Category": _clean_text(_first_nonempty(item.get("category"), item.get("masterCategory"), "Unknown")),
            "Product Name": product_name,
            "SKU": sku,
            "Quantity on Hand": quantity_on_hand,
            "Last Wholesale Cost": _to_float(item.get("unitCost")),
            "Price": _to_float(_first_nonempty(item.get("unitPrice"), item.get("recUnitPrice"), item.get("medUnitPrice"))),
            "Master Category": _clean_text(item.get("masterCategory")),
            "Strain": _clean_text(item.get("strain")),
            "Strain Type": _clean_text(item.get("strainType")),
            "Flower Type": _clean_text(item.get("flowerType")),
            "Concentrate Type": _clean_text(item.get("concentrateType")),
            "UPC/GTIN": _clean_text(_first_nonempty(item.get("upc"), item.get("gtin"), item.get("upcGtin"))),
            "Provincial SKU": _clean_text(item.get("provincialSku")),
            "Tags": _tag_text(item.get("tags")),
        }
        rows.append(row)

    if not rows:
        return pd.DataFrame(columns=["Join Key"] + ORDER_REPORT_COLUMNS)

    frame = pd.DataFrame(rows)
    frame["weight"] = frame["Quantity on Hand"].where(frame["Quantity on Hand"] > 0, 1.0)
    grouped = (
        frame.groupby("Join Key", as_index=False)
        .agg(
            Location=("Location", _first_mode),
            Store=("Store", _first_mode),
            Brand=("Brand", _first_mode),
            Vendor=("Vendor", _first_mode),
            Category=("Category", _first_mode),
            Product_Name=("Product Name", _first_mode),
            SKU=("SKU", _first_mode),
            Quantity_on_Hand=("Quantity on Hand", "sum"),
            Last_Wholesale_Cost=("Last Wholesale Cost", lambda series: _weighted_average(series, frame.loc[series.index, "weight"])),
            Price=("Price", lambda series: _weighted_average(series, frame.loc[series.index, "weight"])),
            Master_Category=("Master Category", _first_mode),
            Strain=("Strain", _first_mode),
            Strain_Type=("Strain Type", _first_mode),
            Flower_Type=("Flower Type", _first_mode),
            Concentrate_Type=("Concentrate Type", _first_mode),
            UPC_GTIN=("UPC/GTIN", _first_mode),
            Provincial_SKU=("Provincial SKU", _first_mode),
            Tags=("Tags", _first_mode),
        )
    )

    grouped = grouped.rename(
        columns={
            "Product_Name": "Product Name",
            "Quantity_on_Hand": "Quantity on Hand",
            "Last_Wholesale_Cost": "Last Wholesale Cost",
            "Master_Category": "Master Category",
            "Strain_Type": "Strain Type",
            "Flower_Type": "Flower Type",
            "Concentrate_Type": "Concentrate Type",
            "UPC_GTIN": "UPC/GTIN",
            "Provincial_SKU": "Provincial SKU",
        }
    )
    return grouped


def _transaction_is_excluded(transaction):
    if not isinstance(transaction, dict):
        return True

    status = _clean_text(
        _first_nonempty(
            transaction.get("status"),
            transaction.get("transactionStatus"),
            transaction.get("state"),
            transaction.get("transactionState"),
        )
    ).lower()
    if status and status in TRANSACTION_STATUS_BLOCKLIST:
        return True
    if bool(transaction.get("isCancelled")) or bool(transaction.get("isCanceled")):
        return True
    if bool(transaction.get("isVoided")) or bool(transaction.get("isVoid")):
        return True
    if bool(transaction.get("isDeleted")):
        return True
    return False


def build_sales_frame(transactions_payload, products_payload, store_code, start_day, end_day):
    store_label = STORE_CODES.get(store_code, store_code)
    product_lookup = _build_product_lookup(products_payload)
    rows = []

    for transaction in transactions_payload or []:
        if _transaction_is_excluded(transaction):
            continue

        transaction_day = _to_date(
            _first_nonempty(
                transaction.get("transactionDateLocalTime"),
                transaction.get("transactionDate"),
                transaction.get("lastModifiedDateUTC"),
            )
        )
        if transaction_day is None or transaction_day < start_day or transaction_day > end_day:
            continue

        transaction_is_return = bool(transaction.get("isReturn"))

        for item in transaction.get("items") or []:
            if not isinstance(item, dict):
                continue

            try:
                product_key = int(item.get("productId") or 0)
            except Exception:
                product_key = 0
            product_info = product_lookup.get(product_key, {})

            product_name = _clean_text(
                _first_nonempty(
                    product_info.get("productName"),
                    product_info.get("internalName"),
                    item.get("productName"),
                    f"Product {product_key or 'Unknown'}",
                )
            )
            sku = _clean_text(_first_nonempty(product_info.get("sku"), item.get("sku")))
            is_return = bool(item.get("isReturned")) or transaction_is_return
            sign = -1.0 if is_return else 1.0
            quantity = abs(_to_float(item.get("quantity"))) * sign

            rows.append(
                {
                    "Join Key": _join_key(sku, product_name),
                    "Location": store_label,
                    "Store": store_code,
                    "Brand": _clean_text(
                        _first_nonempty(product_info.get("brandName"), _parse_brand_from_name(product_name))
                    ),
                    "Vendor": _clean_text(
                        _first_nonempty(item.get("vendor"), product_info.get("vendorName"), product_info.get("producerName"))
                    ),
                    "Category": _clean_text(
                        _first_nonempty(product_info.get("category"), product_info.get("masterCategory"), "Unknown")
                    ),
                    "Product Name": product_name,
                    "SKU": sku,
                    "Quantity Sold": quantity,
                    "Last Wholesale Cost": _to_float(_first_nonempty(item.get("unitCost"), product_info.get("unitCost"))),
                    "Price": _to_float(_first_nonempty(item.get("unitPrice"), product_info.get("price"))),
                    "Master Category": _clean_text(product_info.get("masterCategory")),
                }
            )

    if not rows:
        return pd.DataFrame(columns=["Join Key"] + ORDER_REPORT_COLUMNS)

    frame = pd.DataFrame(rows)
    frame["qty_weight"] = frame["Quantity Sold"].abs().where(frame["Quantity Sold"].abs() > 0, 1.0)
    grouped = (
        frame.groupby("Join Key", as_index=False)
        .agg(
            Location=("Location", _first_mode),
            Store=("Store", _first_mode),
            Brand=("Brand", _first_mode),
            Vendor=("Vendor", _first_mode),
            Category=("Category", _first_mode),
            Product_Name=("Product Name", _first_mode),
            SKU=("SKU", _first_mode),
            Quantity_Sold=("Quantity Sold", "sum"),
            Last_Wholesale_Cost=("Last Wholesale Cost", lambda series: _weighted_average(series, frame.loc[series.index, "qty_weight"])),
            Price=("Price", lambda series: _weighted_average(series, frame.loc[series.index, "qty_weight"])),
            Master_Category=("Master Category", _first_mode),
        )
    )

    grouped["Quantity Sold"] = pd.to_numeric(grouped["Quantity_Sold"], errors="coerce").fillna(0.0).clip(lower=0.0)

    grouped = grouped.rename(
        columns={
            "Product_Name": "Product Name",
            "Last_Wholesale_Cost": "Last Wholesale Cost",
            "Master_Category": "Master Category",
        }
    )
    return grouped.drop(columns=["Quantity_Sold"], errors="ignore")


def build_inventory_order_report_frame(
    inventory_payload,
    products_payload,
    transactions_payload,
    store_code,
    window_days,
    start_day,
    end_day,
):
    inventory_frame = build_inventory_frame(inventory_payload, store_code)
    sales_frame = build_sales_frame(transactions_payload, products_payload, store_code, start_day, end_day)

    if inventory_frame.empty and sales_frame.empty:
        return pd.DataFrame(columns=ORDER_REPORT_COLUMNS)
    if inventory_frame.empty:
        inventory_frame = pd.DataFrame(columns=["Join Key"] + list(sales_frame.columns))
    if sales_frame.empty:
        sales_frame = pd.DataFrame(columns=["Join Key"] + list(inventory_frame.columns))

    merged = inventory_frame.merge(sales_frame, on="Join Key", how="outer", suffixes=("_inv", "_sales"))

    report = pd.DataFrame(index=merged.index)
    for column in [
        "Location",
        "Store",
        "Brand",
        "Vendor",
        "Category",
        "Product Name",
        "SKU",
        "Master Category",
        "Strain",
        "Strain Type",
        "Flower Type",
        "Concentrate Type",
        "UPC/GTIN",
        "Provincial SKU",
        "Tags",
    ]:
        report[column] = _coalesce_series(merged, f"{column}_inv", f"{column}_sales", default="")

    report["Quantity on Hand"] = pd.to_numeric(
        merged.get("Quantity on Hand", merged.get("Quantity on Hand_inv", 0.0)),
        errors="coerce",
    ).fillna(0.0)
    report["Quantity Sold"] = pd.to_numeric(
        merged.get("Quantity Sold", merged.get("Quantity Sold_sales", 0.0)),
        errors="coerce",
    ).fillna(0.0).clip(lower=0.0)
    report["Last Wholesale Cost"] = pd.to_numeric(
        _coalesce_series(merged, "Last Wholesale Cost_inv", "Last Wholesale Cost_sales"),
        errors="coerce",
    ).fillna(0.0)
    report["Price"] = pd.to_numeric(
        _coalesce_series(merged, "Price_inv", "Price_sales"),
        errors="coerce",
    ).fillna(0.0)

    report["Sold Per Day"] = report["Quantity Sold"].apply(
        lambda value: _safe_divide(value, float(window_days))
    )
    report["Avg Daily Sales"] = report["Sold Per Day"]
    report["Days Remaining"] = report.apply(
        lambda row: _safe_divide(row["Quantity on Hand"], row["Sold Per Day"]),
        axis=1,
    )
    report["Last Ordered Quantity"] = ""
    report["Days Since Last Received"] = ""
    report["Last Audit"] = ""

    for numeric_column in [
        "Quantity on Hand",
        "Quantity Sold",
        "Sold Per Day",
        "Avg Daily Sales",
        "Days Remaining",
        "Last Wholesale Cost",
        "Price",
    ]:
        report[numeric_column] = pd.to_numeric(report[numeric_column], errors="coerce")

    report = report[ORDER_REPORT_COLUMNS].copy()
    report = report.sort_values(by=["Category", "Brand", "Product Name"], na_position="last").reset_index(drop=True)
    return report


def clear_existing_order_reports(output_dir):
    output_path = Path(output_dir)
    output_path.mkdir(parents=True, exist_ok=True)
    for existing in output_path.iterdir():
        if existing.is_file() and re.match(r"^inventory_order_(7d|14d|30d)_[A-Za-z0-9]+\.(xlsx|xls|csv)$", existing.name, re.IGNORECASE):
            existing.unlink()
            print(f"[INFO] Deleted old order report: {existing}")


def _sales_params_for_window(start_day, end_day):
    from_utc, to_utc = local_date_range_to_utc_strings(
        start_day.isoformat(),
        end_day.isoformat(),
    )
    return {
        "FromDateUTC": from_utc,
        "ToDateUTC": to_utc,
        "IncludeDetail": True,
        "IncludeTaxes": True,
        "IncludeOrderIds": True,
        "IncludeFeesAndDonations": True,
    }


def _run_inventory_order_report_store(
    store_code,
    location_key,
    integrator_key,
    windows,
    output_path,
    print_lock=None,
):
    store_label = STORE_CODES.get(store_code, store_code)
    failures = []
    session = create_session(location_key, integrator_key)

    try:
        print_threadsafe(
            f"[INFO] Preparing Dutchie API order-report exports for {store_code} ({store_label})",
            print_lock,
        )
        try:
            print_threadsafe(f"[FETCH] {store_code} ({store_label}) -> /reporting/inventory", print_lock)
            inventory_payload = request_json(session, "/reporting/inventory")
            print_threadsafe(f"[FETCH] {store_code} ({store_label}) -> /reporting/products", print_lock)
            products_payload = request_json(session, "/reporting/products")
            print_threadsafe(
                f"[INFO] Loaded inventory/products for {store_code}: "
                f"{len(inventory_payload or [])} inventory row(s), {len(products_payload or [])} product row(s).",
                print_lock,
            )
        except Exception as exc:
            failures.append(f"{store_code}: inventory/products fetch failed ({exc})")
            return failures

        largest_window_days, largest_start_day, largest_end_day = max(windows, key=lambda window: window[0])
        try:
            sales_params = _sales_params_for_window(largest_start_day, largest_end_day)
            print_threadsafe(
                f"[FETCH] {store_code} inventory order {largest_window_days}d transaction cache: "
                f"/reporting/transactions {largest_start_day.isoformat()} -> {largest_end_day.isoformat()}",
                print_lock,
            )
            transactions_payload = request_json(session, "/reporting/transactions", params=sales_params)
            print_threadsafe(
                f"[INFO] Loaded {len(transactions_payload or [])} transaction row(s) for {store_code}; "
                "reusing that cache for all order windows.",
                print_lock,
            )
        except Exception as exc:
            failures.append(f"{store_code}: transactions fetch failed ({exc})")
            return failures

        for window_days, start_day, end_day in windows:
            try:
                report_df = build_inventory_order_report_frame(
                    inventory_payload=inventory_payload,
                    products_payload=products_payload,
                    transactions_payload=transactions_payload,
                    store_code=store_code,
                    window_days=window_days,
                    start_day=start_day,
                    end_day=end_day,
                )
                destination = output_path / order_report_filename(store_code, window_days, extension=".csv")
                report_df.to_csv(destination, index=False)
                print_threadsafe(f"[INFO] Saved {destination.name} with {len(report_df)} row(s).", print_lock)
            except Exception as exc:
                failures.append(f"{store_code}: {window_days}d ({exc})")
    finally:
        session.close()

    return failures


def run_inventory_order_report_api(
    output_dir="files",
    anchor_day=None,
    stores=None,
    env_file=DEFAULT_ENV_FILE,
    workers=DEFAULT_API_WORKERS,
):
    output_path = Path(output_dir).resolve()
    output_path.mkdir(parents=True, exist_ok=True)
    clear_existing_order_reports(output_path)

    requested_stores = parse_store_codes(list(stores or [])) if stores else list(STORE_CODES.keys())
    env_map = canonical_env_map(env_file)
    store_keys = resolve_store_keys(env_map, requested_stores)
    missing_codes = [code for code in requested_stores if code not in store_keys]
    if missing_codes:
        missing_text = ", ".join(missing_codes)
        raise RuntimeError(
            f"Missing Dutchie API location key(s) for: {missing_text}. "
            f"Expected these in {env_file} before running the API order-report export."
        )

    integrator_key = resolve_integrator_key(env_map)
    windows = compute_windows(anchor_day=anchor_day)
    failures = []
    worker_count = resolve_worker_count(workers, len(requested_stores))
    worker_label = "serial mode" if worker_count == 1 else f"{worker_count} store worker threads"
    print(f"[INFO] Running Dutchie API order reports with {worker_label}.")
    print("[INFO] Transaction pulls use the largest configured order window once per store, then reuse it.")
    print_lock = Lock()

    if worker_count == 1:
        for store_code in requested_stores:
            failures.extend(
                _run_inventory_order_report_store(
                    store_code=store_code,
                    location_key=store_keys[store_code],
                    integrator_key=integrator_key,
                    windows=windows,
                    output_path=output_path,
                    print_lock=print_lock,
                )
            )
    else:
        with ThreadPoolExecutor(max_workers=worker_count) as executor:
            futures = {
                executor.submit(
                    _run_inventory_order_report_store,
                    store_code,
                    store_keys[store_code],
                    integrator_key,
                    windows,
                    output_path,
                    print_lock,
                ): store_code
                for store_code in requested_stores
            }
            for future in as_completed(futures):
                store_code = futures[future]
                try:
                    failures.extend(future.result())
                except Exception as exc:
                    failures.append(f"{store_code}: {exc}")

    if failures:
        raise RuntimeError("Dutchie API inventory order export failed for: " + ", ".join(failures))


def parse_args():
    parser = argparse.ArgumentParser(
        description="Build Dutchie inventory order report source files through the API."
    )
    parser.add_argument(
        "output_dir",
        nargs="?",
        default="files",
        help="Directory where inventory_order_*.csv files will be saved.",
    )
    parser.add_argument(
        "--env-file",
        default=DEFAULT_ENV_FILE,
        help="Path to the .env file containing Dutchie API credentials.",
    )
    parser.add_argument(
        "--stores",
        nargs="*",
        help="Optional list of store codes like MV LG LM.",
    )
    parser.add_argument(
        "--workers",
        type=positive_int,
        default=DEFAULT_API_WORKERS,
        help=(
            "Number of stores to build concurrently. "
            f"Default: {DEFAULT_API_WORKERS}. Use 1 for serial API calls."
        ),
    )
    parser.add_argument(
        "--end-date",
        help="Anchor end date in YYYY-MM-DD format. Defaults to today.",
    )
    return parser.parse_args()


def main():
    args = parse_args()
    anchor_day = datetime.strptime(args.end_date, "%Y-%m-%d").date() if args.end_date else None
    run_inventory_order_report_api(
        output_dir=args.output_dir,
        anchor_day=anchor_day,
        stores=args.stores,
        env_file=args.env_file,
        workers=args.workers,
    )


if __name__ == "__main__":
    main()
