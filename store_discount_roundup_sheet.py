#!/usr/bin/env python3
from __future__ import annotations

import argparse
import socket
import time
from collections import OrderedDict
from dataclasses import dataclass
from datetime import datetime
from pathlib import Path
from typing import Any, Iterable

import pandas as pd
from googleapiclient.errors import HttpError

import owner_snapshot as osnap
from deals_brand_config_sync import authenticate_sheets
from dutchie_api_reports import (
    STORE_CODES,
    canonical_env_map,
    create_session,
    request_json,
    resolve_integrator_key,
    resolve_store_keys,
)
from weekly_store_ordering_sheets import ensure_sheet, get_sheet_info_by_title, move_sheet_to_index


THIS_DIR = Path(__file__).resolve().parent
DEFAULT_ENV_FILE = THIS_DIR / ".env"
SPREADSHEET_ID = "1MPE5ndMe3KKHr3KhO82aifusnl__hsCsRywzEYWsMk4"
README_TITLE = "README"
ALL_PRICING_TITLE = "All Pricing"
STORE_TAB_TITLES = OrderedDict(
    [
        ("MV", "MV Pricing"),
        ("LG", "LG Pricing"),
        ("LM", "LM Pricing"),
        ("WP", "WP Pricing"),
        ("SV", "SV Pricing"),
        ("NC", "NC Pricing"),
    ]
)
TITLE_ROW = 1
CONTROL_TITLE_ROW = 1
CONTROL_HEADER_ROW = 2
CONTROL_START_ROW = 3
HEADER_ROW = 2
DATA_START_ROW = HEADER_ROW + 1
LEGACY_CONTROL_START_COL = 20  # T
CONTROL_START_COL = 25  # Y
TOTAL_COLUMNS = 29  # A:AC
DEFAULT_DISCOUNT_CELL = "R1"
TAX_MULTIPLIER_CELL = "P1"
KICKBACK_DISCOUNT_THRESHOLD = 0.30
DEFAULT_KICKBACK_PCT = 0.25
SHEETS_WRITE_CHUNK_ROWS = 250
SHEETS_WRITE_CHUNK_CHARS = 900_000
SHEETS_WRITE_RETRY_ATTEMPTS = 4

MAIN_HEADERS = [
    "Brand",
    "Category",
    "Product",
    "Available",
    "Current Shelf Price",
    "Suggested Shelf Price",
    "Applied Discount %",
    "Current Discounted Shelf",
    "Current Discounted OTD",
    "Rounded OTD Target",
    "Needed Discounted Shelf",
    "Suggested New OTD",
    "Price Change",
    "Price Change %",
    "Status",
    "Brand Notes",
    "Finished Helper",
    "Cost Price",
    "Kickback %",
    "Old Margin %",
    "New Projected Margin %",
]
CONTROL_HEADERS = ["Brand", "Discount %", "Kickback %", "Finished?", "Notes"]
LEGACY_CONTROL_HEADERS = ["Brand", "Discount %", "Finished?", "Notes"]
ALL_PRICING_HEADERS = [
    "Price Scope",
    "Stores",
    "Brand",
    "Category",
    "Product",
    "Available",
    "Current Shelf Price",
    "Cost Price",
    "Suggested Shared Shelf Price",
    "Projected Shared OTD",
    "Max Discount %",
    "Max Kickback %",
    "Worst Old Margin %",
    "Worst Projected Margin %",
    "Price Notes",
    "Store Rows",
]
LEFT_ALIGN_HEADERS = {"Brand", "Category", "Product", "Brand Notes"}
CURRENCY_HEADERS = {
    "Current Shelf Price",
    "Cost Price",
    "Current Discounted Shelf",
    "Current Discounted OTD",
    "Rounded OTD Target",
    "Needed Discounted Shelf",
    "Suggested Shelf Price",
    "Suggested New OTD",
    "Price Change",
}
PERCENT_HEADERS = {"Applied Discount %", "Price Change %", "Kickback %", "Old Margin %", "New Projected Margin %"}
INTEGER_HEADERS = {"Available"}
HIDDEN_HEADERS = {"Finished Helper"}
ALL_PRICING_LEFT_ALIGN_HEADERS = {"Price Scope", "Stores", "Brand", "Category", "Product", "Price Notes", "Store Rows"}
ALL_PRICING_CURRENCY_HEADERS = {
    "Current Shelf Price",
    "Cost Price",
    "Suggested Shared Shelf Price",
    "Projected Shared OTD",
}
ALL_PRICING_PERCENT_HEADERS = {"Max Discount %", "Max Kickback %", "Worst Old Margin %", "Worst Projected Margin %"}
ALL_PRICING_INTEGER_HEADERS = {"Available"}
ALL_PRICING_HIDDEN_HEADERS = {"Store Rows"}


@dataclass(frozen=True)
class StoreTaxConfig:
    city_tax: float
    excise_tax: float
    state_tax: float
    city_label: str
    excise_label: str
    state_label: str


STORE_TAXES: dict[str, StoreTaxConfig] = {
    "LM": StoreTaxConfig(
        city_tax=0.0400,
        excise_tax=0.1560,
        state_tax=0.1017,
        city_label="La Mesa City Tax",
        excise_label="Excise Tax",
        state_label="State Sales Tax",
    ),
    "LG": StoreTaxConfig(
        city_tax=0.0500,
        excise_tax=0.1575,
        state_tax=0.1056,
        city_label="Lemon Grove City Tax",
        excise_label="Excise Tax",
        state_label="Sales Tax",
    ),
    "MV": StoreTaxConfig(
        city_tax=0.1000,
        excise_tax=0.1650,
        state_tax=0.0980,
        city_label="San Diego City Tax",
        excise_label="Excise Tax",
        state_label="State Sales Tax",
    ),
    "NC": StoreTaxConfig(
        city_tax=0.1000,
        excise_tax=0.1650,
        state_tax=0.0980,
        city_label="San Diego City Tax",
        excise_label="Excise Tax",
        state_label="State Sales Tax",
    ),
    "SV": StoreTaxConfig(
        city_tax=0.1000,
        excise_tax=0.1650,
        state_tax=0.0980,
        city_label="San Diego City Tax",
        excise_label="Excise Tax",
        state_label="State Sales Tax",
    ),
    "WP": StoreTaxConfig(
        city_tax=0.0300,
        excise_tax=0.1545,
        state_tax=0.1036,
        city_label="Wildomar City Tax",
        excise_label="Excise Tax",
        state_label="State Sales Tax",
    ),
}


def _canon(text: Any) -> str:
    return "".join(ch.lower() for ch in str(text or "").strip() if ch.isalnum())


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


def _to_float(value: Any) -> float:
    try:
        if value in (None, ""):
            return 0.0
        number = float(value)
        if pd.isna(number):
            return 0.0
        return number
    except Exception:
        return 0.0


def _first_positive_float(*values: Any) -> float:
    for value in values:
        number = _to_float(value)
        if number > 0:
            return number
    return 0.0


SOURCE_COLUMNS = [
    "merge_key",
    "Brand",
    "Category",
    "Product",
    "Vendor",
    "Current Shelf Price",
    "Inventory Cost",
    "Available",
    "Product ID",
    "Catalog Source",
    "Inventory Source",
]


def _empty_source_frame() -> pd.DataFrame:
    return pd.DataFrame(columns=SOURCE_COLUMNS)


def _should_exclude_product(product_name: str, current_price: float) -> bool:
    normalized_name = str(product_name or "").strip().lower()
    if "sample" in normalized_name or "promo" in normalized_name:
        return True
    return round(float(current_price or 0.0), 2) == 0.01


def _key_for_row(product_name: str, sku: str, brand: str, category: str, product_id: Any) -> str:
    product_id_text = str(product_id or "").strip()
    if product_id_text:
        return f"id:{product_id_text}"
    sku_text = str(sku or "").strip()
    if sku_text:
        return f"sku:{sku_text.lower()}"
    return f"name:{_canon(brand)}|{_canon(category)}|{_canon(product_name)}"


def _normalize_products_payload(products_payload: Any) -> pd.DataFrame:
    rows: list[dict[str, Any]] = []
    for item in products_payload or []:
        if not isinstance(item, dict):
            continue
        product_name = str(
            _first_nonempty(
                item.get("productName"),
                item.get("internalName"),
                item.get("alternateName"),
                "",
            )
        ).strip()
        if not product_name:
            continue

        brand = str(_first_nonempty(item.get("brandName"), osnap.parse_brand_from_product(product_name), "")).strip()
        category = str(_first_nonempty(item.get("category"), item.get("masterCategory"), "")).strip()
        sku = str(_first_nonempty(item.get("sku"), "")).strip()
        vendor = str(_first_nonempty(item.get("vendorName"), item.get("producerName"), item.get("vendor"), "")).strip()
        current_price = _to_float(
            _first_nonempty(
                item.get("price"),
                item.get("unitPrice"),
                item.get("recPrice"),
                item.get("medPrice"),
            )
        )
        inventory_cost = _to_float(
            _first_nonempty(
                item.get("unitCost"),
                item.get("cost"),
                item.get("wholesaleCost"),
                item.get("lastWholesaleCost"),
            )
        )
        product_id = _first_nonempty(item.get("productId"), item.get("id"), "")
        rows.append(
            {
                "merge_key": _key_for_row(product_name, sku, brand, category, product_id),
                "Brand": brand,
                "Category": category,
                "Product": product_name,
                "Vendor": vendor,
                "Current Shelf Price": current_price,
                "Inventory Cost": inventory_cost,
                "Available": 0.0,
                "Product ID": str(product_id).strip(),
                "Catalog Source": True,
                "Inventory Source": False,
            }
        )

    frame = pd.DataFrame(rows)
    if frame.empty:
        return _empty_source_frame()
    return frame


def _normalize_inventory_payload(inventory_payload: Any) -> pd.DataFrame:
    rows: list[dict[str, Any]] = []
    for item in inventory_payload or []:
        if not isinstance(item, dict):
            continue
        product_name = str(_first_nonempty(item.get("productName"), item.get("alternateName"), "")).strip()
        if not product_name:
            continue

        brand = str(_first_nonempty(item.get("brandName"), osnap.parse_brand_from_product(product_name), "")).strip()
        category = str(_first_nonempty(item.get("category"), item.get("masterCategory"), "")).strip()
        sku = str(_first_nonempty(item.get("sku"), "")).strip()
        vendor = str(_first_nonempty(item.get("vendor"), item.get("producer"), "")).strip()
        current_price = _to_float(
            _first_nonempty(
                item.get("unitPrice"),
                item.get("recUnitPrice"),
                item.get("medUnitPrice"),
                item.get("price"),
            )
        )
        inventory_cost = _to_float(
            _first_nonempty(
                item.get("unitCost"),
                item.get("cost"),
                item.get("wholesaleCost"),
                item.get("lastWholesaleCost"),
            )
        )
        available = _to_float(item.get("quantityAvailable"))
        product_id = _first_nonempty(item.get("productId"), item.get("id"), "")
        rows.append(
            {
                "merge_key": _key_for_row(product_name, sku, brand, category, product_id),
                "Brand": brand,
                "Category": category,
                "Product": product_name,
                "Vendor": vendor,
                "Current Shelf Price": current_price,
                "Inventory Cost": inventory_cost,
                "Available": available,
                "Product ID": str(product_id).strip(),
                "Catalog Source": False,
                "Inventory Source": True,
            }
        )

    frame = pd.DataFrame(rows)
    if frame.empty:
        return _empty_source_frame()
    return frame


def _dedupe_source_frame(frame: pd.DataFrame) -> pd.DataFrame:
    if frame.empty:
        return frame

    working = frame.copy()
    for column in SOURCE_COLUMNS:
        if column not in working.columns:
            working[column] = "" if column not in {"Current Shelf Price", "Inventory Cost", "Available"} else 0.0

    rows: list[dict[str, Any]] = []
    for merge_key, group in working.groupby("merge_key", dropna=False, sort=False):
        costs = pd.to_numeric(group["Inventory Cost"], errors="coerce")
        available = pd.to_numeric(group["Available"], errors="coerce").fillna(0.0)
        valid_costs = costs.notna() & (costs > 0)

        if bool(valid_costs.any()) and float(available[valid_costs].sum()) > 0:
            inventory_cost = float((costs[valid_costs] * available[valid_costs]).sum() / available[valid_costs].sum())
        elif bool(valid_costs.any()):
            inventory_cost = float(costs[valid_costs].max())
        else:
            inventory_cost = 0.0

        rows.append(
            {
                "merge_key": merge_key,
                "Brand": group["Brand"].iloc[0],
                "Category": group["Category"].iloc[0],
                "Product": group["Product"].iloc[0],
                "Vendor": group["Vendor"].iloc[0],
                "Current Shelf Price": float(pd.to_numeric(group["Current Shelf Price"], errors="coerce").fillna(0.0).max()),
                "Inventory Cost": inventory_cost,
                "Available": float(available.sum()),
                "Product ID": group["Product ID"].iloc[0],
                "Catalog Source": bool(group["Catalog Source"].max()),
                "Inventory Source": bool(group["Inventory Source"].max()),
            }
        )

    return pd.DataFrame(rows, columns=SOURCE_COLUMNS)


def _merge_similar_brand_rows(frame: pd.DataFrame) -> pd.DataFrame:
    if frame.empty:
        return frame

    working = frame.copy()
    working["Current Shelf Price"] = pd.to_numeric(working["Current Shelf Price"], errors="coerce").fillna(0).round(2)
    working["Inventory Cost"] = pd.to_numeric(working.get("Inventory Cost", 0), errors="coerce").fillna(0).round(2)
    working["Available"] = pd.to_numeric(working["Available"], errors="coerce").fillna(0)

    merged_rows: list[dict[str, Any]] = []
    for _, group in working.groupby(["Brand", "Category", "Current Shelf Price", "Inventory Cost"], dropna=False, sort=False):
        if group.empty:
            continue

        row = group.iloc[0].copy()
        product_names = sorted({str(value).strip() for value in group["Product"].dropna() if str(value).strip()})

        if not product_names:
            display_name = ""
        elif len(product_names) == 1:
            display_name = product_names[0]
        else:
            display_name = f"{product_names[0]} (+{len(product_names) - 1} more)"

        row["Product"] = display_name
        row["Available"] = float(group["Available"].sum())
        merged_rows.append(row[["Brand", "Category", "Product", "Available", "Current Shelf Price", "Inventory Cost"]].to_dict())

    if not merged_rows:
        return frame.iloc[0:0].copy()

    return pd.DataFrame(merged_rows)


def build_store_catalog_frame(products_payload: Any, inventory_payload: Any) -> pd.DataFrame:
    products_frame = _dedupe_source_frame(_normalize_products_payload(products_payload))
    inventory_frame = _dedupe_source_frame(_normalize_inventory_payload(inventory_payload))

    merged = products_frame.merge(
        inventory_frame,
        on="merge_key",
        how="outer",
        suffixes=("_catalog", "_inventory"),
    )

    rows: list[dict[str, Any]] = []
    for row in merged.to_dict("records"):
        brand = str(
            _first_nonempty(
                row.get("Brand_inventory"),
                row.get("Brand_catalog"),
                "",
            )
        ).strip()
        category = str(
            _first_nonempty(
                row.get("Category_inventory"),
                row.get("Category_catalog"),
                "",
            )
        ).strip()
        product = str(
            _first_nonempty(
                row.get("Product_inventory"),
                row.get("Product_catalog"),
                "",
            )
        ).strip()
        vendor = str(_first_nonempty(row.get("Vendor_inventory"), row.get("Vendor_catalog"), "")).strip()
        current_price = _to_float(
            _first_nonempty(
                row.get("Current Shelf Price_inventory"),
                row.get("Current Shelf Price_catalog"),
                0.0,
            )
        )
        cost = _first_positive_float(row.get("Inventory Cost_inventory"), row.get("Inventory Cost_catalog"))
        available = _to_float(row.get("Available_inventory"))

        if not product:
            continue

        if not brand:
            brand = osnap.parse_brand_from_product(product)

        if _should_exclude_product(product, current_price):
            continue

        rows.append(
            {
                "Brand": str(brand or "").strip() or "Unknown",
                "Category": category or "Unknown",
                "Product": product,
                "Available": available,
                "Current Shelf Price": current_price,
                "Inventory Cost": cost,
            }
        )

    frame = pd.DataFrame(rows)
    if frame.empty:
        return pd.DataFrame(columns=["Brand", "Category", "Product", "Available", "Current Shelf Price", "Inventory Cost"])

    frame = (
        frame.groupby(["Brand", "Category", "Product", "Current Shelf Price", "Inventory Cost"], as_index=False)
        .agg({"Available": "sum"})
        .copy()
    )
    frame = frame[["Brand", "Category", "Product", "Available", "Current Shelf Price", "Inventory Cost"]]
    frame = _merge_similar_brand_rows(frame)
    frame = frame.sort_values(
        by=["Brand", "Category", "Current Shelf Price", "Inventory Cost", "Product"],
        kind="stable",
    ).reset_index(drop=True)
    return frame


def fetch_store_frames(store_codes: Iterable[str], env_file: Path) -> dict[str, pd.DataFrame]:
    env_map = canonical_env_map(str(env_file))
    requested_codes = [str(code).strip().upper() for code in store_codes if str(code).strip()]
    store_keys = resolve_store_keys(env_map, requested_codes)
    missing_codes = [code for code in requested_codes if code not in store_keys]
    if missing_codes:
        raise ValueError(
            "Missing Dutchie API location key(s) for: "
            + ", ".join(missing_codes)
            + f". Add them to {env_file} using names like DUTCHIE_API_KEY_MV or MV."
        )
    integrator_key = resolve_integrator_key(env_map)

    out: dict[str, pd.DataFrame] = {}
    for store_code in requested_codes:
        store_name = STORE_CODES.get(store_code, store_code)
        print(f"[FETCH] {store_code} ({store_name}) -> /reporting/products + /reporting/inventory")
        session = create_session(store_keys[store_code], integrator_key)
        products_payload = request_json(session, "/reporting/products")
        inventory_payload = request_json(session, "/reporting/inventory")
        frame = build_store_catalog_frame(products_payload, inventory_payload)
        out[store_code] = frame
        print(
            f"[READY] {store_code}: {len(frame)} consolidated row(s), "
            f"{int((frame['Available'] > 0).sum()) if not frame.empty else 0} in-stock row(s)"
        )
    return out


def _column_letter(index: int) -> str:
    value = int(index)
    result = ""
    while value > 0:
        value, remainder = divmod(value - 1, 26)
        result = chr(65 + remainder) + result
    return result


def _rgb(hex_text: str) -> dict[str, float]:
    text = str(hex_text).strip().lstrip("#")
    if len(text) != 6:
        raise ValueError(f"Expected #RRGGBB color, got {hex_text!r}")
    return {
        "red": int(text[0:2], 16) / 255.0,
        "green": int(text[2:4], 16) / 255.0,
        "blue": int(text[4:6], 16) / 255.0,
    }


def _number_format_type(pattern: str) -> str:
    if "%" in pattern:
        return "PERCENT"
    if any(token in pattern for token in ("$", "€", "£")):
        return "CURRENCY"
    return "NUMBER"


def _values_get(
    service: Any,
    spreadsheet_id: str,
    range_name: str,
    value_render_option: str | None = None,
) -> list[list[Any]]:
    kwargs: dict[str, Any] = {}
    if value_render_option:
        kwargs["valueRenderOption"] = value_render_option
    return (
        service.spreadsheets()
        .values()
        .get(
            spreadsheetId=spreadsheet_id,
            range=range_name,
            **kwargs,
        )
        .execute()
        .get("values", [])
    )


def _a1(sheet_title: str, a1_range: str) -> str:
    escaped = sheet_title.replace("'", "''")
    return f"'{escaped}'!{a1_range}"


def _sheet_cell_ref(sheet_title: str, col_index: int, row_number: int) -> str:
    escaped = sheet_title.replace("'", "''")
    return f"'{escaped}'!${_column_letter(col_index)}${row_number}"


def _control_read_range(start_col: int, header_count: int) -> str:
    start_letter = _column_letter(start_col)
    end_letter = _column_letter(start_col + header_count - 1)
    return f"{start_letter}{CONTROL_HEADER_ROW}:{end_letter}"


def _parse_control_rows(values: list[list[Any]]) -> dict[str, dict[str, Any]]:
    controls: dict[str, dict[str, Any]] = {}
    if not values:
        return controls

    rows = list(values)
    header = [str(value).strip() for value in rows[0]]
    if header[: len(CONTROL_HEADERS)] == CONTROL_HEADERS:
        mode = "current"
    elif header[: len(LEGACY_CONTROL_HEADERS)] == LEGACY_CONTROL_HEADERS:
        mode = "legacy"
    else:
        return controls

    for row in rows[1:]:
        if mode == "current":
            padded = list(row) + [""] * (len(CONTROL_HEADERS) - len(row))
            brand, discount, kickback, finished, notes = padded[:5]
        else:
            padded = list(row) + [""] * (len(LEGACY_CONTROL_HEADERS) - len(row))
            brand, discount, finished, notes = padded[:4]
            kickback = ""

        brand_text = str(brand).strip()
        if not brand_text:
            continue
        controls[brand_text.casefold()] = {
            "brand": brand_text,
            "discount": discount,
            "kickback": kickback,
            "finished": finished,
            "notes": notes,
        }
    return controls


def read_existing_store_controls(service: Any, spreadsheet_id: str, title: str) -> tuple[Any, dict[str, dict[str, Any]]]:
    current_values = _values_get(
        service,
        spreadsheet_id,
        _a1(title, _control_read_range(CONTROL_START_COL, len(CONTROL_HEADERS))),
        value_render_option="FORMULA",
    )
    controls = _parse_control_rows(current_values)
    if not controls:
        legacy_values = _values_get(
            service,
            spreadsheet_id,
            _a1(title, _control_read_range(LEGACY_CONTROL_START_COL, len(LEGACY_CONTROL_HEADERS))),
            value_render_option="FORMULA",
        )
        controls = _parse_control_rows(legacy_values)

    default_discount_values = _values_get(service, spreadsheet_id, _a1(title, DEFAULT_DISCOUNT_CELL), value_render_option="FORMULA")
    default_discount = default_discount_values[0][0] if default_discount_values and default_discount_values[0] else 0.30
    return default_discount, controls


def _normalize_checkbox_value(value: Any) -> bool | str:
    if isinstance(value, bool):
        return value
    text = str(value or "").strip()
    if not text:
        return False
    upper = text.upper()
    if upper == "TRUE":
        return True
    if upper == "FALSE":
        return False
    return text


def build_brand_control_rows(
    brands: list[str],
    existing_controls: dict[str, dict[str, Any]],
    default_discount_formula: str,
) -> list[list[Any]]:
    control_brands = {brand for brand in brands if brand}
    control_brands.update(control.get("brand", "") for control in existing_controls.values())
    ordered = sorted((brand for brand in control_brands if brand), key=str.casefold)

    rows: list[list[Any]] = []
    discount_col_letter = _column_letter(CONTROL_START_COL + 1)
    for offset, brand in enumerate(ordered):
        row_number = CONTROL_START_ROW + offset
        existing = existing_controls.get(brand.casefold(), {})
        discount = existing.get("discount", "") if existing else ""
        kickback = existing.get("kickback", "") if existing else ""
        finished = existing.get("finished", "") if existing else ""
        notes = existing.get("notes", "") if existing else ""
        default_kickback_formula = f'=IF({discount_col_letter}{row_number}>{KICKBACK_DISCOUNT_THRESHOLD},{DEFAULT_KICKBACK_PCT},0)'
        rows.append(
            [
                brand,
                discount if str(discount).strip() else default_discount_formula,
                kickback if str(kickback).strip() else default_kickback_formula,
                _normalize_checkbox_value(finished),
                notes,
            ]
        )
    return rows


def _sheet_value(value: Any) -> Any:
    if value is None:
        return ""
    if isinstance(value, pd.Timestamp):
        if pd.isna(value):
            return ""
        return value.isoformat()
    if isinstance(value, float):
        if pd.isna(value):
            return ""
        return float(value)
    if isinstance(value, bool):
        return value
    return value


def _open_col_range(col_index: int, start_row: int = CONTROL_START_ROW) -> str:
    letter = _column_letter(col_index)
    return f"${letter}${start_row}:${letter}"


def _formula_for_row(row_number: int, column_name: str) -> str:
    control_brand_range = _open_col_range(CONTROL_START_COL)
    control_discount_range = _open_col_range(CONTROL_START_COL + 1)
    control_kickback_range = _open_col_range(CONTROL_START_COL + 2)
    control_finished_range = _open_col_range(CONTROL_START_COL + 3)
    control_notes_range = _open_col_range(CONTROL_START_COL + 4)
    if column_name == "Applied Discount %":
        return f'=IFNA(XLOOKUP($A{row_number},{control_brand_range},{control_discount_range}),${DEFAULT_DISCOUNT_CELL})'
    if column_name == "Suggested Shelf Price":
        return f'=IF(OR($K{row_number}="",$G{row_number}>=1),"",ROUNDUP($K{row_number}/(1-$G{row_number}),2))'
    if column_name == "Current Discounted Shelf":
        return f'=IF($E{row_number}<=0,"",ROUND($E{row_number}*(1-$G{row_number}),2))'
    if column_name == "Current Discounted OTD":
        return f'=IF($H{row_number}="","",ROUND($H{row_number}*${TAX_MULTIPLIER_CELL},2))'
    if column_name == "Rounded OTD Target":
        return f'=IF($I{row_number}="","",ROUNDUP($I{row_number},0))'
    if column_name == "Needed Discounted Shelf":
        return f'=IF($J{row_number}="","",ROUNDUP($J{row_number}/${TAX_MULTIPLIER_CELL},2))'
    if column_name == "Suggested New OTD":
        return f'=IF($F{row_number}="","",ROUND($F{row_number}*(1-$G{row_number})*${TAX_MULTIPLIER_CELL},2))'
    if column_name == "Price Change":
        return f'=IF($F{row_number}="","",ROUND($F{row_number}-$E{row_number},2))'
    if column_name == "Price Change %":
        return f'=IF(OR($M{row_number}="",$E{row_number}=0),"",$M{row_number}/$E{row_number})'
    if column_name == "Finished Helper":
        return f'=IFNA(XLOOKUP($A{row_number},{control_brand_range},{control_finished_range}),FALSE)'
    if column_name == "Status":
        return f'=IF($Q{row_number},"Done","")'
    if column_name == "Brand Notes":
        return f'=IFNA(XLOOKUP($A{row_number},{control_brand_range},{control_notes_range}),"")'
    if column_name == "Kickback %":
        return f'=IFNA(XLOOKUP($A{row_number},{control_brand_range},{control_kickback_range}),0)'
    if column_name == "Old Margin %":
        revenue = f'ROUNDDOWN($I{row_number}/${TAX_MULTIPLIER_CELL},2)'
        adjusted_cost = f'$R{row_number}*(1-$S{row_number})'
        return f'=IFERROR(IF(OR($R{row_number}<=0,{revenue}<=0),"",({revenue}-({adjusted_cost}))/{revenue}),"")'
    if column_name == "New Projected Margin %":
        revenue = f'ROUNDDOWN($L{row_number}/${TAX_MULTIPLIER_CELL},2)'
        adjusted_cost = f'$R{row_number}*(1-$S{row_number})'
        return f'=IFERROR(IF(OR($R{row_number}<=0,$F{row_number}="",{revenue}<=0),"",({revenue}-({adjusted_cost}))/{revenue}),"")'
    return ""


def build_store_sheet_matrix(
    store_code: str,
    store_name: str,
    frame: pd.DataFrame,
    tax_config: StoreTaxConfig,
    generated_at: str,
    default_discount_value: Any,
    control_rows: list[list[Any]],
) -> list[list[Any]]:
    total_rows = max(DATA_START_ROW + len(frame) - 1, CONTROL_START_ROW + len(control_rows) - 1, HEADER_ROW)
    total_rows = max(total_rows, CONTROL_START_ROW)
    matrix: list[list[Any]] = [["" for _ in range(TOTAL_COLUMNS)] for _ in range(total_rows)]

    def set_cell(row_number: int, column_number: int, value: Any) -> None:
        matrix[row_number - 1][column_number - 1] = _sheet_value(value)

    title = f"{store_name} Discount Round-Up"
    set_cell(1, 1, title)
    set_cell(1, 5, "Generated")
    set_cell(1, 6, generated_at)
    set_cell(1, 8, "Store")
    set_cell(1, 9, f"{store_code} - {store_name}")
    set_cell(1, 11, "Taxes")
    set_cell(1, 12, f"{tax_config.city_tax:.2%} / {tax_config.excise_tax:.2%} / {tax_config.state_tax:.2%}")
    set_cell(1, 15, "Multiplier")
    set_cell(1, 16, f"=1+{tax_config.city_tax}+{tax_config.excise_tax}+{tax_config.state_tax}")
    set_cell(1, 17, "Default")
    set_cell(1, 18, default_discount_value)

    set_cell(CONTROL_TITLE_ROW, CONTROL_START_COL, "Brand Controls")
    for index, header in enumerate(CONTROL_HEADERS, start=CONTROL_START_COL):
        set_cell(CONTROL_HEADER_ROW, index, header)
    for row_offset, row_values in enumerate(control_rows, start=CONTROL_START_ROW):
        for column_offset, value in enumerate(row_values, start=CONTROL_START_COL):
            set_cell(row_offset, column_offset, value)

    for col_index, header in enumerate(MAIN_HEADERS, start=1):
        set_cell(HEADER_ROW, col_index, header)

    for row_offset, values in enumerate(frame.to_dict("records"), start=DATA_START_ROW):
        for col_index, header in enumerate(MAIN_HEADERS, start=1):
            if header in values:
                set_cell(row_offset, col_index, values[header])
            elif header == "Cost Price" and "Inventory Cost" in values:
                set_cell(row_offset, col_index, values["Inventory Cost"])
            else:
                formula = _formula_for_row(row_offset, header)
                set_cell(row_offset, col_index, formula)

    return matrix


def build_readme_rows(generated_at: str) -> list[list[Any]]:
    return [
        ["Buzz Discount Round-Up Planner", ""],
        ["Generated At", generated_at],
        [
            "Purpose",
            "Round discounted out-the-door pricing up to the next whole dollar and show the shelf price needed to hit that rounded target.",
        ],
        [
            "How To Use",
            "Each store tab has a Brand Controls box on the right. Change brand-level Discount % and Kickback % there, check Finished? when a brand is done, and the product rows update automatically. Use All Pricing for shared price buckets across stores.",
        ],
        [
            "Main Formula",
            "Current Discounted OTD = Current Shelf Price x (1 - discount) x tax multiplier. Rounded OTD Target = ROUNDUP(Current Discounted OTD, 0). Suggested Shelf Price = ROUNDUP(ROUNDUP(Rounded OTD Target / tax multiplier, 2) / (1 - discount), 2).",
        ],
        [
            "Margins",
            "Old Margin % and New Projected Margin % use tax-backed revenue rounded down from OTD, then subtract Cost Price after Kickback %. New brand controls default Kickback % to 25% when Discount % is above the default 30%, and you can override it.",
        ],
        [
            "Cost Price Reference",
            "Cost Price comes from Dutchie unitCost when available and is shown on each store tab plus the All Pricing rollup.",
        ],
        [
            "All Pricing",
            "The All Pricing tab rolls store rows into shared price buckets. If the same product/cost has different shelf prices by store, those location-price buckets stay separate.",
        ],
        [
            "Training Video",
            "https://youtu.be/La_JT4Pir0I",
        ],
        [
            "Rerun Safety",
            "Brand-level Discount %, Kickback %, Finished?, and Notes are preserved on rerun for each store tab.",
        ],
        [
            "Excluded Rows",
            "Items with shelf price 0.01 and products with Sample or Promo in the name are excluded automatically.",
        ],
        [
            "Consolidation",
            "Products from the same brand and category with the same shelf price and inventory cost are grouped into one row, with product names rolled up automatically.",
        ],
        [
            "Store Tax Assumption",
            "Taxes are applied additively using the provided city + excise + state percentages for each store.",
        ],
        [
            "Store Tabs",
            ", ".join([ALL_PRICING_TITLE, *STORE_TAB_TITLES.values()]),
        ],
    ]


def _is_retryable_google_error(error: HttpError) -> bool:
    status = getattr(getattr(error, "resp", None), "status", None)
    try:
        status_int = int(status)
    except Exception:
        return False
    return status_int in {429, 500, 502, 503, 504}


def _execute_sheet_request(request: Any, label: str) -> Any:
    for attempt in range(1, SHEETS_WRITE_RETRY_ATTEMPTS + 1):
        try:
            return request.execute()
        except (TimeoutError, socket.timeout) as exc:
            if attempt >= SHEETS_WRITE_RETRY_ATTEMPTS:
                raise
            wait_seconds = min(30, 2**attempt)
            print(f"[RETRY] {label}: timed out ({exc}); retrying in {wait_seconds}s")
            time.sleep(wait_seconds)
        except HttpError as exc:
            if attempt >= SHEETS_WRITE_RETRY_ATTEMPTS or not _is_retryable_google_error(exc):
                raise
            wait_seconds = min(30, 2**attempt)
            print(f"[RETRY] {label}: Google API {exc.resp.status}; retrying in {wait_seconds}s")
            time.sleep(wait_seconds)
    return None


def _matrix_batches(
    matrix: list[list[Any]],
    max_rows: int = SHEETS_WRITE_CHUNK_ROWS,
    max_chars: int = SHEETS_WRITE_CHUNK_CHARS,
) -> Iterable[tuple[int, list[list[Any]]]]:
    start_index = 0
    while start_index < len(matrix):
        batch: list[list[Any]] = []
        batch_chars = 0
        index = start_index
        while index < len(matrix):
            row = matrix[index]
            row_chars = sum(len(str(value)) for value in row)
            if batch and (len(batch) >= max_rows or batch_chars + row_chars > max_chars):
                break
            batch.append(row)
            batch_chars += row_chars
            index += 1

        yield start_index + 1, batch
        start_index += len(batch)


def write_values(service: Any, spreadsheet_id: str, title: str, matrix: list[list[Any]], total_columns: int | None = None) -> None:
    width = total_columns or max((len(row) for row in matrix), default=TOTAL_COLUMNS)
    _execute_sheet_request(
        service.spreadsheets().values().clear(
            spreadsheetId=spreadsheet_id,
            range=_a1(title, f"A1:{_column_letter(width)}"),
            body={},
        ),
        f"clear {title}",
    )
    if not matrix:
        return

    batches = list(_matrix_batches(matrix))
    if len(batches) > 1:
        print(f"[WRITE] {title}: sending {len(matrix)} rows in {len(batches)} chunk(s)")
    for start_row, rows in batches:
        end_row = start_row + len(rows) - 1
        _execute_sheet_request(
            service.spreadsheets().values().update(
                spreadsheetId=spreadsheet_id,
                range=_a1(title, f"A{start_row}"),
                valueInputOption="USER_ENTERED",
                body={"values": rows},
            ),
            f"write {title} rows {start_row}-{end_row}",
        )


def _delete_existing_banding_and_rules(sheet_info: dict[str, Any]) -> list[dict[str, Any]]:
    requests: list[dict[str, Any]] = []
    for banded_range in sheet_info.get("banded_ranges", []):
        banded_range_id = banded_range.get("bandedRangeId")
        if banded_range_id is not None:
            requests.append({"deleteBanding": {"bandedRangeId": banded_range_id}})
    conditional_rules = list(sheet_info.get("conditional_rules", []))
    for index in range(len(conditional_rules) - 1, -1, -1):
        requests.append({"deleteConditionalFormatRule": {"sheetId": int(sheet_info["sheet_id"]), "index": index}})
    return requests


def format_store_sheet(
    service: Any,
    spreadsheet_id: str,
    title: str,
    total_rows: int,
    data_row_count: int,
    control_row_count: int,
) -> None:
    sheet_info = get_sheet_info_by_title(service, spreadsheet_id, title)
    if not sheet_info or sheet_info.get("sheet_id") is None:
        return
    sheet_id = int(sheet_info["sheet_id"])
    requests: list[dict[str, Any]] = _delete_existing_banding_and_rules(sheet_info)

    data_end_row = max(DATA_START_ROW, DATA_START_ROW + data_row_count)
    control_end_row = max(CONTROL_START_ROW, CONTROL_START_ROW + control_row_count)
    main_data_range = {
        "sheetId": sheet_id,
        "startRowIndex": HEADER_ROW - 1,
        "endRowIndex": max(total_rows, HEADER_ROW),
        "startColumnIndex": 0,
        "endColumnIndex": len(MAIN_HEADERS),
    }
    control_range = {
        "sheetId": sheet_id,
        "startRowIndex": CONTROL_HEADER_ROW - 1,
        "endRowIndex": max(control_end_row, CONTROL_HEADER_ROW),
        "startColumnIndex": CONTROL_START_COL - 1,
        "endColumnIndex": CONTROL_START_COL - 1 + len(CONTROL_HEADERS),
    }

    requests.extend(
        [
            {
                "updateSheetProperties": {
                    "properties": {
                        "sheetId": sheet_id,
                        "gridProperties": {
                            "frozenRowCount": HEADER_ROW,
                            "frozenColumnCount": 6,
                        },
                        "tabColor": _rgb("#B8662E"),
                    },
                    "fields": "gridProperties.frozenRowCount,gridProperties.frozenColumnCount,tabColor",
                }
            },
            {
                "repeatCell": {
                    "range": {"sheetId": sheet_id, "startRowIndex": 0, "endRowIndex": total_rows, "startColumnIndex": 0, "endColumnIndex": TOTAL_COLUMNS},
                    "cell": {
                        "userEnteredFormat": {
                            "backgroundColor": _rgb("#FFFDF8"),
                            "verticalAlignment": "MIDDLE",
                            "wrapStrategy": "WRAP",
                            "textFormat": {"fontSize": 10},
                        }
                    },
                    "fields": "userEnteredFormat(backgroundColor,verticalAlignment,wrapStrategy,textFormat)",
                }
            },
            {
                "repeatCell": {
                    "range": {"sheetId": sheet_id, "startRowIndex": 0, "endRowIndex": 1, "startColumnIndex": 0, "endColumnIndex": 18},
                    "cell": {
                        "userEnteredFormat": {
                            "backgroundColor": _rgb("#173F3A"),
                            "wrapStrategy": "CLIP",
                            "textFormat": {"fontSize": 10, "foregroundColor": _rgb("#FFFFFF")},
                        }
                    },
                    "fields": "userEnteredFormat(backgroundColor,wrapStrategy,textFormat)",
                }
            },
            {
                "repeatCell": {
                    "range": {"sheetId": sheet_id, "startRowIndex": 0, "endRowIndex": 1, "startColumnIndex": 0, "endColumnIndex": 1},
                    "cell": {
                        "userEnteredFormat": {
                            "textFormat": {"bold": True, "fontSize": 13, "foregroundColor": _rgb("#FFFFFF")},
                        }
                    },
                    "fields": "userEnteredFormat.textFormat",
                }
            },
            {
                "repeatCell": {
                    "range": {"sheetId": sheet_id, "startRowIndex": HEADER_ROW - 1, "endRowIndex": HEADER_ROW, "startColumnIndex": 0, "endColumnIndex": len(MAIN_HEADERS)},
                    "cell": {
                        "userEnteredFormat": {
                            "backgroundColor": _rgb("#244B3C"),
                            "horizontalAlignment": "CENTER",
                            "textFormat": {"bold": True, "foregroundColor": _rgb("#FFFFFF")},
                        }
                    },
                    "fields": "userEnteredFormat(backgroundColor,horizontalAlignment,textFormat)",
                }
            },
            {
                "repeatCell": {
                    "range": {"sheetId": sheet_id, "startRowIndex": CONTROL_TITLE_ROW - 1, "endRowIndex": CONTROL_TITLE_ROW, "startColumnIndex": CONTROL_START_COL - 1, "endColumnIndex": CONTROL_START_COL - 1 + len(CONTROL_HEADERS)},
                    "cell": {
                        "userEnteredFormat": {
                            "backgroundColor": _rgb("#C56B2D"),
                            "textFormat": {"bold": True, "foregroundColor": _rgb("#FFFFFF")},
                        }
                    },
                    "fields": "userEnteredFormat(backgroundColor,textFormat)",
                }
            },
            {
                "repeatCell": {
                    "range": {"sheetId": sheet_id, "startRowIndex": CONTROL_HEADER_ROW - 1, "endRowIndex": CONTROL_HEADER_ROW, "startColumnIndex": CONTROL_START_COL - 1, "endColumnIndex": CONTROL_START_COL - 1 + len(CONTROL_HEADERS)},
                    "cell": {
                        "userEnteredFormat": {
                            "backgroundColor": _rgb("#EAD3BE"),
                            "textFormat": {"bold": True},
                        }
                    },
                    "fields": "userEnteredFormat(backgroundColor,textFormat)",
                }
            },
        ]
    )

    if total_rows > HEADER_ROW:
        requests.append(
            {
                "addBanding": {
                    "bandedRange": {
                        "range": main_data_range,
                        "rowProperties": {
                            "headerColor": _rgb("#244B3C"),
                            "firstBandColor": _rgb("#FFFDF8"),
                            "secondBandColor": _rgb("#F3F6F1"),
                        },
                    }
                }
            }
        )
        requests.append(
            {
                "setBasicFilter": {
                    "filter": {
                        "range": {
                            "sheetId": sheet_id,
                            "startRowIndex": HEADER_ROW - 1,
                            "endRowIndex": max(total_rows, HEADER_ROW),
                            "startColumnIndex": 0,
                            "endColumnIndex": len(MAIN_HEADERS),
                        }
                    }
                }
            }
        )

    if control_row_count:
        if LEGACY_CONTROL_START_COL != CONTROL_START_COL:
            requests.append(
                {
                    "setDataValidation": {
                        "range": {
                            "sheetId": sheet_id,
                            "startRowIndex": CONTROL_START_ROW - 1,
                            "endRowIndex": control_end_row,
                            "startColumnIndex": LEGACY_CONTROL_START_COL + 1,
                            "endColumnIndex": LEGACY_CONTROL_START_COL + 2,
                        },
                        "rule": None,
                    }
                }
            )
        requests.append(
            {
                "addBanding": {
                    "bandedRange": {
                        "range": control_range,
                        "rowProperties": {
                            "headerColor": _rgb("#EAD3BE"),
                            "firstBandColor": _rgb("#FFF8F1"),
                            "secondBandColor": _rgb("#FFF2E5"),
                        },
                    }
                }
            }
        )
        requests.append(
            {
                "setDataValidation": {
                    "range": {
                        "sheetId": sheet_id,
                        "startRowIndex": CONTROL_START_ROW - 1,
                        "endRowIndex": control_end_row,
                        "startColumnIndex": CONTROL_START_COL + 2,
                        "endColumnIndex": CONTROL_START_COL + 3,
                    },
                    "rule": {
                        "condition": {"type": "BOOLEAN"},
                        "showCustomUi": True,
                        "strict": True,
                    },
                }
            }
        )

    for index, header in enumerate(MAIN_HEADERS, start=1):
        requests.append(
            {
                "updateDimensionProperties": {
                    "range": {
                        "sheetId": sheet_id,
                        "dimension": "COLUMNS",
                        "startIndex": index - 1,
                        "endIndex": index,
                    },
                    "properties": {"pixelSize": _column_width(header)},
                    "fields": "pixelSize",
                }
            }
        )
        if header in HIDDEN_HEADERS:
            requests.append(
                {
                    "updateDimensionProperties": {
                        "range": {
                            "sheetId": sheet_id,
                            "dimension": "COLUMNS",
                            "startIndex": index - 1,
                            "endIndex": index,
                        },
                        "properties": {"hiddenByUser": True},
                        "fields": "hiddenByUser",
                    }
                }
            )

    for offset, header in enumerate(CONTROL_HEADERS):
        requests.append(
            {
                "updateDimensionProperties": {
                    "range": {
                        "sheetId": sheet_id,
                        "dimension": "COLUMNS",
                        "startIndex": CONTROL_START_COL - 1 + offset,
                        "endIndex": CONTROL_START_COL + offset,
                    },
                    "properties": {"pixelSize": _control_column_width(header)},
                    "fields": "pixelSize",
                }
            }
        )

    requests.append(
        {
            "updateDimensionProperties": {
                "range": {
                    "sheetId": sheet_id,
                    "dimension": "ROWS",
                    "startIndex": 0,
                    "endIndex": 1,
                },
                "properties": {"pixelSize": 34},
                "fields": "pixelSize",
            }
        }
    )

    requests.append(
        {
            "updateDimensionProperties": {
                "range": {
                    "sheetId": sheet_id,
                    "dimension": "ROWS",
                    "startIndex": HEADER_ROW - 1,
                    "endIndex": HEADER_ROW,
                },
                "properties": {"pixelSize": 36},
                "fields": "pixelSize",
            }
        }
    )

    if total_rows > DATA_START_ROW:
        requests.append(
            {
                "updateDimensionProperties": {
                    "range": {
                        "sheetId": sheet_id,
                        "dimension": "ROWS",
                        "startIndex": DATA_START_ROW - 1,
                        "endIndex": total_rows,
                    },
                    "properties": {"pixelSize": 32},
                    "fields": "pixelSize",
                }
            }
        )

    requests.extend(_alignment_requests(sheet_id, total_rows))
    requests.extend(_number_format_requests(sheet_id, total_rows, control_end_row))
    requests.extend(_conditional_format_requests(sheet_id, total_rows, control_end_row))

    if requests:
        service.spreadsheets().batchUpdate(
            spreadsheetId=spreadsheet_id,
            body={"requests": requests},
        ).execute()


def _column_width(header: str) -> int:
    widths = {
        "Brand": 150,
        "Category": 130,
        "Product": 320,
        "Available": 90,
        "Current Shelf Price": 120,
        "Suggested Shelf Price": 135,
        "Applied Discount %": 110,
        "Current Discounted Shelf": 140,
        "Current Discounted OTD": 135,
        "Rounded OTD Target": 130,
        "Needed Discounted Shelf": 145,
        "Suggested New OTD": 120,
        "Price Change": 110,
        "Price Change %": 110,
        "Status": 90,
        "Brand Notes": 220,
        "Finished Helper": 90,
        "Cost Price": 115,
        "Kickback %": 95,
        "Old Margin %": 110,
        "New Projected Margin %": 140,
    }
    return widths.get(header, 110)


def _control_column_width(header: str) -> int:
    widths = {
        "Brand": 150,
        "Discount %": 110,
        "Kickback %": 110,
        "Finished?": 90,
        "Notes": 180,
    }
    return widths.get(header, 120)


def _alignment_requests(sheet_id: int, total_rows: int) -> list[dict[str, Any]]:
    requests: list[dict[str, Any]] = []
    if total_rows <= DATA_START_ROW - 1:
        return requests
    for index, header in enumerate(MAIN_HEADERS, start=1):
        alignment = "LEFT" if header in LEFT_ALIGN_HEADERS else "CENTER"
        requests.append(
            {
                "repeatCell": {
                    "range": {
                        "sheetId": sheet_id,
                        "startRowIndex": DATA_START_ROW - 1,
                        "endRowIndex": total_rows,
                        "startColumnIndex": index - 1,
                        "endColumnIndex": index,
                    },
                    "cell": {"userEnteredFormat": {"horizontalAlignment": alignment}},
                    "fields": "userEnteredFormat.horizontalAlignment",
                }
            }
        )
    return requests


def _number_format_requests(sheet_id: int, total_rows: int, control_end_row: int) -> list[dict[str, Any]]:
    requests: list[dict[str, Any]] = []

    def add_repeat(col_index: int, pattern: str, start_row: int, end_row: int) -> None:
        requests.append(
            {
                "repeatCell": {
                    "range": {
                        "sheetId": sheet_id,
                        "startRowIndex": start_row - 1,
                        "endRowIndex": end_row,
                        "startColumnIndex": col_index - 1,
                        "endColumnIndex": col_index,
                    },
                    "cell": {
                        "userEnteredFormat": {
                            "numberFormat": {
                                "type": _number_format_type(pattern),
                                "pattern": pattern,
                            }
                        }
                    },
                    "fields": "userEnteredFormat.numberFormat",
                }
            }
        )

    add_repeat(16, "0.0000", 1, 1)
    add_repeat(18, "0.00%", 1, 1)

    if total_rows >= DATA_START_ROW:
        for header in CURRENCY_HEADERS:
            col_index = MAIN_HEADERS.index(header) + 1
            add_repeat(col_index, "$#,##0.00", DATA_START_ROW, total_rows)
        for header in PERCENT_HEADERS:
            col_index = MAIN_HEADERS.index(header) + 1
            add_repeat(col_index, "0.00%", DATA_START_ROW, total_rows)
        for header in INTEGER_HEADERS:
            col_index = MAIN_HEADERS.index(header) + 1
            add_repeat(col_index, "0", DATA_START_ROW, total_rows)

    if control_end_row >= CONTROL_START_ROW:
        add_repeat(CONTROL_START_COL + 1, "0.00%", CONTROL_START_ROW, control_end_row)
        add_repeat(CONTROL_START_COL + 2, "0.00%", CONTROL_START_ROW, control_end_row)

    return requests


def _conditional_format_requests(sheet_id: int, total_rows: int, control_end_row: int) -> list[dict[str, Any]]:
    requests: list[dict[str, Any]] = []
    if total_rows >= DATA_START_ROW:
        data_range = {
            "sheetId": sheet_id,
            "startRowIndex": DATA_START_ROW - 1,
            "endRowIndex": total_rows,
            "startColumnIndex": 0,
            "endColumnIndex": len(MAIN_HEADERS),
        }
        requests.extend(
            [
                {
                    "addConditionalFormatRule": {
                        "index": 0,
                        "rule": {
                            "ranges": [data_range],
                            "booleanRule": {
                                "condition": {
                                    "type": "CUSTOM_FORMULA",
                                    "values": [{"userEnteredValue": f"=$D{DATA_START_ROW}=0"}],
                                },
                                "format": {
                                    "backgroundColor": _rgb("#E8F3FF"),
                                },
                            },
                        },
                    }
                },
                {
                    "addConditionalFormatRule": {
                        "index": 0,
                        "rule": {
                            "ranges": [data_range],
                            "booleanRule": {
                                "condition": {
                                    "type": "CUSTOM_FORMULA",
                                    "values": [{"userEnteredValue": f"=$M{DATA_START_ROW}>0.009"}],
                                },
                                "format": {
                                    "backgroundColor": _rgb("#FFF3D6"),
                                },
                            },
                        },
                    }
                },
                {
                    "addConditionalFormatRule": {
                        "index": 0,
                        "rule": {
                            "ranges": [data_range],
                            "booleanRule": {
                                "condition": {
                                    "type": "CUSTOM_FORMULA",
                                    "values": [{"userEnteredValue": f"=$Q{DATA_START_ROW}=TRUE"}],
                                },
                                "format": {
                                    "backgroundColor": _rgb("#ECECEC"),
                                    "textFormat": {"strikethrough": True, "foregroundColor": _rgb("#6B7280")},
                                },
                            },
                        },
                    }
                },
            ]
        )

    if control_end_row >= CONTROL_START_ROW:
        control_range = {
            "sheetId": sheet_id,
            "startRowIndex": CONTROL_START_ROW - 1,
            "endRowIndex": control_end_row,
            "startColumnIndex": CONTROL_START_COL - 1,
            "endColumnIndex": CONTROL_START_COL - 1 + len(CONTROL_HEADERS),
        }
        requests.append(
            {
                "addConditionalFormatRule": {
                    "index": 0,
                    "rule": {
                        "ranges": [control_range],
                        "booleanRule": {
                            "condition": {
                                "type": "CUSTOM_FORMULA",
                                "values": [{"userEnteredValue": f"=${_column_letter(CONTROL_START_COL + 3)}{CONTROL_START_ROW}=TRUE"}],
                            },
                            "format": {
                                "backgroundColor": _rgb("#ECECEC"),
                                "textFormat": {"strikethrough": True, "foregroundColor": _rgb("#6B7280")},
                            },
                        },
                    },
                }
            }
        )
    return requests


def _main_col(header: str) -> int:
    return MAIN_HEADERS.index(header) + 1


def _all_pricing_col(header: str) -> int:
    return ALL_PRICING_HEADERS.index(header) + 1


def _filtered_aggregate_inner(function_name: str, expressions: list[str]) -> str:
    if not expressions:
        return ""
    array_literal = "{" + ";".join(expressions) + "}"
    return f'IFERROR(LET(v,{array_literal},{function_name}(FILTER(v,v<>""))),"")'


def _formula_max(expressions: list[str]) -> str:
    inner = _filtered_aggregate_inner("MAX", expressions)
    return f"={inner}" if inner else ""


def _formula_min(expressions: list[str]) -> str:
    inner = _filtered_aggregate_inner("MIN", expressions)
    return f"={inner}" if inner else ""


def _store_sort_key(store_code: str) -> int:
    try:
        return list(STORE_TAB_TITLES.keys()).index(str(store_code).upper())
    except ValueError:
        return 999


def _store_list_label(store_codes: Iterable[str]) -> str:
    ordered = sorted({str(code).upper() for code in store_codes if str(code).strip()}, key=_store_sort_key)
    return ", ".join(ordered)


def _display_product_name(values: Iterable[Any]) -> str:
    names = sorted({str(value).strip() for value in values if str(value or "").strip()}, key=str.casefold)
    if not names:
        return ""
    if len(names) == 1:
        return names[0]
    return f"{names[0]} (+{len(names) - 1} more)"


def build_all_pricing_entries(
    store_frames: dict[str, pd.DataFrame],
    requested_codes: Iterable[str],
) -> list[dict[str, Any]]:
    requested = [str(code).upper() for code in requested_codes if str(code).strip()]
    requested_set = set(requested)
    source_rows: list[dict[str, Any]] = []
    for store_code in requested:
        frame = store_frames.get(store_code, pd.DataFrame())
        if frame.empty:
            continue
        title = STORE_TAB_TITLES[store_code]
        for index, row in frame.reset_index(drop=True).iterrows():
            price = round(_to_float(row.get("Current Shelf Price")), 2)
            cost = round(_to_float(row.get("Inventory Cost")), 2)
            if price <= 0:
                continue
            source_rows.append(
                {
                    "store": store_code,
                    "title": title,
                    "row_number": DATA_START_ROW + int(index),
                    "Brand": str(row.get("Brand", "")).strip() or "Unknown",
                    "Category": str(row.get("Category", "")).strip() or "Unknown",
                    "Product": str(row.get("Product", "")).strip(),
                    "Available": _to_float(row.get("Available")),
                    "Current Shelf Price": price,
                    "Cost Price": cost,
                }
            )

    grouped: dict[tuple[str, str, str, float], list[dict[str, Any]]] = {}
    for source in source_rows:
        key = (
            _canon(source["Brand"]),
            _canon(source["Category"]),
            _canon(source["Product"]),
            round(_to_float(source["Cost Price"]), 2),
        )
        grouped.setdefault(key, []).append(source)

    entries: list[dict[str, Any]] = []
    for group_rows in grouped.values():
        price_groups: dict[float, list[dict[str, Any]]] = {}
        for source in group_rows:
            price_groups.setdefault(round(_to_float(source["Current Shelf Price"]), 2), []).append(source)

        common_price = max(
            price_groups,
            key=lambda price: (
                len({row["store"] for row in price_groups[price]}),
                sum(_to_float(row["Available"]) for row in price_groups[price]),
                -price,
            ),
        )
        group_store_set = {row["store"] for row in group_rows}

        for price, price_rows in price_groups.items():
            stores = sorted({row["store"] for row in price_rows}, key=_store_sort_key)
            if len(price_groups) == 1 and set(stores) == requested_set and len(stores) > 1:
                scope = "All Stores"
                notes = "All requested stores share this price."
            elif len(price_groups) == 1 and len(stores) > 1:
                scope = "Shared Price"
                notes = "Shared by every requested store where this product is stocked."
            elif len(price_groups) == 1:
                scope = "Store Price"
                notes = "Only one requested store has this product/price bucket."
            elif price == common_price and len(stores) > 1:
                scope = "Shared Price"
                notes = "Common price bucket; location-price buckets are split separately."
            else:
                scope = "Location Price"
                notes = "Location price kept separate from the common price bucket."

            first = price_rows[0]
            entries.append(
                {
                    "Price Scope": scope,
                    "Stores": _store_list_label(stores),
                    "Brand": first["Brand"],
                    "Category": first["Category"],
                    "Product": _display_product_name(row["Product"] for row in price_rows),
                    "Available": sum(_to_float(row["Available"]) for row in price_rows),
                    "Current Shelf Price": price,
                    "Cost Price": round(_to_float(first["Cost Price"]), 2),
                    "Price Notes": notes,
                    "Store Rows": "; ".join(f'{row["store"]} row {row["row_number"]}' for row in price_rows),
                    "source_refs": price_rows,
                    "_scope_rank": {"All Stores": 0, "Shared Price": 1, "Store Price": 2, "Location Price": 3}.get(scope, 9),
                    "_store_count": len(stores),
                    "_group_store_count": len(group_store_set),
                }
            )

    entries.sort(
        key=lambda row: (
            row["_scope_rank"],
            str(row["Brand"]).casefold(),
            str(row["Category"]).casefold(),
            float(row["Current Shelf Price"] or 0.0),
            str(row["Product"]).casefold(),
        )
    )
    return entries


def _all_pricing_formula_values(entry: dict[str, Any], row_number: int) -> dict[str, Any]:
    refs = list(entry.get("source_refs") or [])
    suggested_refs = [_sheet_cell_ref(ref["title"], _main_col("Suggested Shelf Price"), ref["row_number"]) for ref in refs]
    discount_refs = [_sheet_cell_ref(ref["title"], _main_col("Applied Discount %"), ref["row_number"]) for ref in refs]
    kickback_refs = [_sheet_cell_ref(ref["title"], _main_col("Kickback %"), ref["row_number"]) for ref in refs]
    old_margin_refs = [_sheet_cell_ref(ref["title"], _main_col("Old Margin %"), ref["row_number"]) for ref in refs]

    shared_price_cell = f'${_column_letter(_all_pricing_col("Suggested Shared Shelf Price"))}{row_number}'
    projected_otd_expressions: list[str] = []
    projected_margin_expressions: list[str] = []
    for ref in refs:
        title = ref["title"]
        store_row = ref["row_number"]
        discount_ref = _sheet_cell_ref(title, _main_col("Applied Discount %"), store_row)
        kickback_ref = _sheet_cell_ref(title, _main_col("Kickback %"), store_row)
        cost_ref = _sheet_cell_ref(title, _main_col("Cost Price"), store_row)
        tax_ref = _sheet_cell_ref(title, 16, 1)
        otd_expr = f"ROUND({shared_price_cell}*(1-{discount_ref})*{tax_ref},2)"
        revenue_expr = f"ROUNDDOWN({otd_expr}/{tax_ref},2)"
        projected_otd_expressions.append(otd_expr)
        projected_margin_expressions.append(
            f'IFERROR(IF(OR({cost_ref}<=0,{revenue_expr}<=0),"",({revenue_expr}-({cost_ref}*(1-{kickback_ref})))/{revenue_expr}),"")'
        )

    return {
        "Suggested Shared Shelf Price": _formula_max(suggested_refs),
        "Projected Shared OTD": f'=IF({shared_price_cell}="","",IFERROR(MAX({",".join(projected_otd_expressions)}),""))'
        if projected_otd_expressions
        else "",
        "Max Discount %": _formula_max(discount_refs),
        "Max Kickback %": _formula_max(kickback_refs),
        "Worst Old Margin %": _formula_min(old_margin_refs),
        "Worst Projected Margin %": f'=IF({shared_price_cell}="","",{_filtered_aggregate_inner("MIN", projected_margin_expressions)})'
        if projected_margin_expressions
        else "",
    }


def build_all_pricing_sheet_matrix(
    store_frames: dict[str, pd.DataFrame],
    requested_codes: Iterable[str],
    generated_at: str,
) -> list[list[Any]]:
    entries = build_all_pricing_entries(store_frames, requested_codes)
    total_rows = max(DATA_START_ROW + len(entries) - 1, HEADER_ROW)
    matrix: list[list[Any]] = [["" for _ in range(len(ALL_PRICING_HEADERS))] for _ in range(total_rows)]

    def set_cell(row_number: int, column_number: int, value: Any) -> None:
        matrix[row_number - 1][column_number - 1] = _sheet_value(value)

    set_cell(1, 1, "All Pricing")
    set_cell(1, 5, "Generated")
    set_cell(1, 6, generated_at)
    set_cell(1, 8, "Shared Price Logic")
    set_cell(1, 9, "Use the max suggested shelf price across source store rows; location-price buckets stay separate.")

    for col_index, header in enumerate(ALL_PRICING_HEADERS, start=1):
        set_cell(HEADER_ROW, col_index, header)

    for row_offset, entry in enumerate(entries, start=DATA_START_ROW):
        formulas = _all_pricing_formula_values(entry, row_offset)
        for col_index, header in enumerate(ALL_PRICING_HEADERS, start=1):
            if header in formulas:
                set_cell(row_offset, col_index, formulas[header])
            else:
                set_cell(row_offset, col_index, entry.get(header, ""))

    return matrix


def _all_pricing_column_width(header: str) -> int:
    widths = {
        "Price Scope": 115,
        "Stores": 110,
        "Brand": 150,
        "Category": 130,
        "Product": 320,
        "Available": 90,
        "Current Shelf Price": 125,
        "Cost Price": 115,
        "Suggested Shared Shelf Price": 160,
        "Projected Shared OTD": 145,
        "Max Discount %": 115,
        "Max Kickback %": 115,
        "Worst Old Margin %": 135,
        "Worst Projected Margin %": 155,
        "Price Notes": 260,
        "Store Rows": 220,
    }
    return widths.get(header, 120)


def format_all_pricing_sheet(service: Any, spreadsheet_id: str, title: str, total_rows: int) -> None:
    sheet_info = get_sheet_info_by_title(service, spreadsheet_id, title)
    if not sheet_info or sheet_info.get("sheet_id") is None:
        return
    sheet_id = int(sheet_info["sheet_id"])
    total_columns = len(ALL_PRICING_HEADERS)
    requests: list[dict[str, Any]] = _delete_existing_banding_and_rules(sheet_info)
    requests.extend(
        [
            {
                "updateSheetProperties": {
                    "properties": {
                        "sheetId": sheet_id,
                        "gridProperties": {"frozenRowCount": HEADER_ROW, "frozenColumnCount": 5},
                        "tabColor": _rgb("#2F6F73"),
                    },
                    "fields": "gridProperties.frozenRowCount,gridProperties.frozenColumnCount,tabColor",
                }
            },
            {
                "repeatCell": {
                    "range": {"sheetId": sheet_id, "startRowIndex": 0, "endRowIndex": total_rows, "startColumnIndex": 0, "endColumnIndex": total_columns},
                    "cell": {
                        "userEnteredFormat": {
                            "backgroundColor": _rgb("#F8FBFB"),
                            "verticalAlignment": "MIDDLE",
                            "wrapStrategy": "WRAP",
                            "textFormat": {"fontSize": 10},
                        }
                    },
                    "fields": "userEnteredFormat(backgroundColor,verticalAlignment,wrapStrategy,textFormat)",
                }
            },
            {
                "repeatCell": {
                    "range": {"sheetId": sheet_id, "startRowIndex": 0, "endRowIndex": 1, "startColumnIndex": 0, "endColumnIndex": total_columns},
                    "cell": {
                        "userEnteredFormat": {
                            "backgroundColor": _rgb("#173F3A"),
                            "wrapStrategy": "CLIP",
                            "textFormat": {"fontSize": 10, "foregroundColor": _rgb("#FFFFFF")},
                        }
                    },
                    "fields": "userEnteredFormat(backgroundColor,wrapStrategy,textFormat)",
                }
            },
            {
                "repeatCell": {
                    "range": {"sheetId": sheet_id, "startRowIndex": 0, "endRowIndex": 1, "startColumnIndex": 0, "endColumnIndex": 1},
                    "cell": {"userEnteredFormat": {"textFormat": {"bold": True, "fontSize": 13, "foregroundColor": _rgb("#FFFFFF")}}},
                    "fields": "userEnteredFormat.textFormat",
                }
            },
            {
                "repeatCell": {
                    "range": {"sheetId": sheet_id, "startRowIndex": HEADER_ROW - 1, "endRowIndex": HEADER_ROW, "startColumnIndex": 0, "endColumnIndex": total_columns},
                    "cell": {
                        "userEnteredFormat": {
                            "backgroundColor": _rgb("#244B3C"),
                            "horizontalAlignment": "CENTER",
                            "textFormat": {"bold": True, "foregroundColor": _rgb("#FFFFFF")},
                        }
                    },
                    "fields": "userEnteredFormat(backgroundColor,horizontalAlignment,textFormat)",
                }
            },
        ]
    )

    if total_rows > HEADER_ROW:
        data_range = {
            "sheetId": sheet_id,
            "startRowIndex": HEADER_ROW - 1,
            "endRowIndex": total_rows,
            "startColumnIndex": 0,
            "endColumnIndex": total_columns,
        }
        requests.append(
            {
                "addBanding": {
                    "bandedRange": {
                        "range": data_range,
                        "rowProperties": {
                            "headerColor": _rgb("#244B3C"),
                            "firstBandColor": _rgb("#F8FBFB"),
                            "secondBandColor": _rgb("#EEF5F5"),
                        },
                    }
                }
            }
        )
        requests.append({"setBasicFilter": {"filter": {"range": data_range}}})
        requests.append(
            {
                "addConditionalFormatRule": {
                    "index": 0,
                    "rule": {
                        "ranges": [
                            {
                                "sheetId": sheet_id,
                                "startRowIndex": DATA_START_ROW - 1,
                                "endRowIndex": total_rows,
                                "startColumnIndex": 0,
                                "endColumnIndex": total_columns,
                            }
                        ],
                        "booleanRule": {
                            "condition": {
                                "type": "CUSTOM_FORMULA",
                                "values": [{"userEnteredValue": f'=$A{DATA_START_ROW}="Location Price"'}],
                            },
                            "format": {"backgroundColor": _rgb("#FFF3D6")},
                        },
                    },
                }
            }
        )

    for index, header in enumerate(ALL_PRICING_HEADERS, start=1):
        requests.append(
            {
                "updateDimensionProperties": {
                    "range": {"sheetId": sheet_id, "dimension": "COLUMNS", "startIndex": index - 1, "endIndex": index},
                    "properties": {"pixelSize": _all_pricing_column_width(header)},
                    "fields": "pixelSize",
                }
            }
        )
        if header in ALL_PRICING_HIDDEN_HEADERS:
            requests.append(
                {
                    "updateDimensionProperties": {
                        "range": {"sheetId": sheet_id, "dimension": "COLUMNS", "startIndex": index - 1, "endIndex": index},
                        "properties": {"hiddenByUser": True},
                        "fields": "hiddenByUser",
                    }
                }
            )

    requests.append(
        {
            "updateDimensionProperties": {
                "range": {"sheetId": sheet_id, "dimension": "ROWS", "startIndex": 0, "endIndex": 1},
                "properties": {"pixelSize": 34},
                "fields": "pixelSize",
            }
        }
    )
    requests.append(
        {
            "updateDimensionProperties": {
                "range": {"sheetId": sheet_id, "dimension": "ROWS", "startIndex": HEADER_ROW - 1, "endIndex": HEADER_ROW},
                "properties": {"pixelSize": 36},
                "fields": "pixelSize",
            }
        }
    )

    def add_number_format(col_index: int, pattern: str, start_row: int, end_row: int) -> None:
        requests.append(
            {
                "repeatCell": {
                    "range": {
                        "sheetId": sheet_id,
                        "startRowIndex": start_row - 1,
                        "endRowIndex": end_row,
                        "startColumnIndex": col_index - 1,
                        "endColumnIndex": col_index,
                    },
                    "cell": {"userEnteredFormat": {"numberFormat": {"type": _number_format_type(pattern), "pattern": pattern}}},
                    "fields": "userEnteredFormat.numberFormat",
                }
            }
        )

    if total_rows >= DATA_START_ROW:
        for header in ALL_PRICING_HEADERS:
            col_index = _all_pricing_col(header)
            alignment = "LEFT" if header in ALL_PRICING_LEFT_ALIGN_HEADERS else "CENTER"
            requests.append(
                {
                    "repeatCell": {
                        "range": {
                            "sheetId": sheet_id,
                            "startRowIndex": DATA_START_ROW - 1,
                            "endRowIndex": total_rows,
                            "startColumnIndex": col_index - 1,
                            "endColumnIndex": col_index,
                        },
                        "cell": {"userEnteredFormat": {"horizontalAlignment": alignment}},
                        "fields": "userEnteredFormat.horizontalAlignment",
                    }
                }
            )
            if header in ALL_PRICING_CURRENCY_HEADERS:
                add_number_format(col_index, "$#,##0.00", DATA_START_ROW, total_rows)
            elif header in ALL_PRICING_PERCENT_HEADERS:
                add_number_format(col_index, "0.00%", DATA_START_ROW, total_rows)
            elif header in ALL_PRICING_INTEGER_HEADERS:
                add_number_format(col_index, "0", DATA_START_ROW, total_rows)

    if requests:
        service.spreadsheets().batchUpdate(spreadsheetId=spreadsheet_id, body={"requests": requests}).execute()


def upsert_all_pricing_tab(
    service: Any,
    spreadsheet_id: str,
    store_frames: dict[str, pd.DataFrame],
    requested_codes: Iterable[str],
    generated_at: str,
) -> None:
    ensure_sheet(service, spreadsheet_id, ALL_PRICING_TITLE, "review")
    matrix = build_all_pricing_sheet_matrix(store_frames, requested_codes, generated_at)
    write_values(service, spreadsheet_id, ALL_PRICING_TITLE, matrix, total_columns=len(ALL_PRICING_HEADERS))
    format_all_pricing_sheet(service, spreadsheet_id, ALL_PRICING_TITLE, len(matrix))


def format_readme_sheet(service: Any, spreadsheet_id: str, title: str, total_rows: int) -> None:
    sheet_info = get_sheet_info_by_title(service, spreadsheet_id, title)
    if not sheet_info or sheet_info.get("sheet_id") is None:
        return
    sheet_id = int(sheet_info["sheet_id"])
    requests = _delete_existing_banding_and_rules(sheet_info)
    requests.extend(
        [
            {
                "updateSheetProperties": {
                    "properties": {"sheetId": sheet_id, "gridProperties": {"frozenRowCount": 1}, "tabColor": _rgb("#D4A761")},
                    "fields": "gridProperties.frozenRowCount,tabColor",
                }
            },
            {
                "repeatCell": {
                    "range": {"sheetId": sheet_id, "startRowIndex": 0, "endRowIndex": total_rows, "startColumnIndex": 0, "endColumnIndex": 2},
                    "cell": {
                        "userEnteredFormat": {
                            "backgroundColor": _rgb("#FFFFFF"),
                            "verticalAlignment": "MIDDLE",
                            "wrapStrategy": "WRAP",
                            "textFormat": {"fontSize": 10},
                        }
                    },
                    "fields": "userEnteredFormat(backgroundColor,verticalAlignment,wrapStrategy,textFormat)",
                }
            },
            {
                "repeatCell": {
                    "range": {"sheetId": sheet_id, "startRowIndex": 0, "endRowIndex": 1, "startColumnIndex": 0, "endColumnIndex": 2},
                    "cell": {
                        "userEnteredFormat": {
                            "backgroundColor": _rgb("#173F3A"),
                            "textFormat": {"bold": True, "fontSize": 14, "foregroundColor": _rgb("#FFFFFF")},
                        }
                    },
                    "fields": "userEnteredFormat(backgroundColor,textFormat)",
                }
            },
            {
                "repeatCell": {
                    "range": {"sheetId": sheet_id, "startRowIndex": 1, "endRowIndex": total_rows, "startColumnIndex": 0, "endColumnIndex": 1},
                    "cell": {
                        "userEnteredFormat": {
                            "backgroundColor": _rgb("#F5F5F5"),
                            "textFormat": {"bold": True},
                        }
                    },
                    "fields": "userEnteredFormat(backgroundColor,textFormat)",
                }
            },
            {
                "updateDimensionProperties": {
                    "range": {"sheetId": sheet_id, "dimension": "COLUMNS", "startIndex": 0, "endIndex": 1},
                    "properties": {"pixelSize": 230},
                    "fields": "pixelSize",
                }
            },
            {
                "updateDimensionProperties": {
                    "range": {"sheetId": sheet_id, "dimension": "COLUMNS", "startIndex": 1, "endIndex": 2},
                    "properties": {"pixelSize": 760},
                    "fields": "pixelSize",
                }
            },
        ]
    )
    if requests:
        service.spreadsheets().batchUpdate(
            spreadsheetId=spreadsheet_id,
            body={"requests": requests},
        ).execute()


def upsert_readme(service: Any, spreadsheet_id: str, generated_at: str) -> None:
    ensure_sheet(service, spreadsheet_id, README_TITLE, "readme")
    rows = build_readme_rows(generated_at)
    service.spreadsheets().values().clear(
        spreadsheetId=spreadsheet_id,
        range=_a1(README_TITLE, "A1:B"),
        body={},
    ).execute()
    service.spreadsheets().values().update(
        spreadsheetId=spreadsheet_id,
        range=_a1(README_TITLE, "A1"),
        valueInputOption="USER_ENTERED",
        body={"values": rows},
    ).execute()
    format_readme_sheet(service, spreadsheet_id, README_TITLE, len(rows))


def upsert_store_tab(
    service: Any,
    spreadsheet_id: str,
    store_code: str,
    store_name: str,
    frame: pd.DataFrame,
    generated_at: str,
) -> None:
    title = STORE_TAB_TITLES[store_code]
    ensure_sheet(service, spreadsheet_id, title, "review")
    tax_config = STORE_TAXES[store_code]
    default_discount_value, existing_controls = read_existing_store_controls(service, spreadsheet_id, title)
    brand_names = frame["Brand"].dropna().astype(str).str.strip().tolist() if not frame.empty else []
    control_rows = build_brand_control_rows(
        brands=brand_names,
        existing_controls=existing_controls,
        default_discount_formula=f"=${DEFAULT_DISCOUNT_CELL}",
    )
    matrix = build_store_sheet_matrix(
        store_code=store_code,
        store_name=store_name,
        frame=frame,
        tax_config=tax_config,
        generated_at=generated_at,
        default_discount_value=default_discount_value if str(default_discount_value).strip() else 0.30,
        control_rows=control_rows,
    )
    write_values(service, spreadsheet_id, title, matrix)
    format_store_sheet(
        service=service,
        spreadsheet_id=spreadsheet_id,
        title=title,
        total_rows=len(matrix),
        data_row_count=len(frame),
        control_row_count=len(control_rows),
    )


def build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(
        description="Build the store discount round-up Google Sheet from Dutchie catalog + inventory data.",
    )
    parser.add_argument(
        "--env-file",
        default=str(DEFAULT_ENV_FILE),
        help=f"Dutchie API .env path. Default: {DEFAULT_ENV_FILE}",
    )
    parser.add_argument(
        "--stores",
        nargs="*",
        help="Optional store codes to refresh, for example: MV LG LM WP SV NC",
    )
    parser.add_argument(
        "--dry-run",
        action="store_true",
        help="Fetch and build the store frames, but do not write to Google Sheets.",
    )
    return parser


def main() -> int:
    parser = build_parser()
    args = parser.parse_args()

    requested_codes = [code.upper() for code in (args.stores or STORE_TAB_TITLES.keys())]
    invalid = [code for code in requested_codes if code not in STORE_TAB_TITLES]
    if invalid:
        parser.error("Unknown store code(s): " + ", ".join(invalid))
        return 2

    generated_at = datetime.now().strftime("%Y-%m-%d %I:%M %p")
    store_frames = fetch_store_frames(requested_codes, Path(args.env_file).expanduser().resolve())

    if args.dry_run:
        for store_code in requested_codes:
            frame = store_frames.get(store_code, pd.DataFrame())
            print(
                f"[DRY RUN] {store_code}: {len(frame)} row(s), "
                f"{int((frame['Available'] > 0).sum()) if not frame.empty else 0} with stock"
            )
        all_pricing_rows = build_all_pricing_entries(store_frames, requested_codes)
        print(f"[DRY RUN] {ALL_PRICING_TITLE}: {len(all_pricing_rows)} shared/location pricing row(s)")
        return 0

    service = authenticate_sheets()
    upsert_readme(service, SPREADSHEET_ID, generated_at)
    move_sheet_to_index(service, SPREADSHEET_ID, README_TITLE, 0)

    next_index = 2
    for store_code in STORE_TAB_TITLES:
        if store_code not in requested_codes:
            continue
        frame = store_frames.get(store_code, pd.DataFrame())
        upsert_store_tab(
            service=service,
            spreadsheet_id=SPREADSHEET_ID,
            store_code=store_code,
            store_name=STORE_CODES[store_code],
            frame=frame,
            generated_at=generated_at,
        )
        move_sheet_to_index(service, SPREADSHEET_ID, STORE_TAB_TITLES[store_code], next_index)
        next_index += 1
        print(f"[SHEET] {store_code}: wrote {STORE_TAB_TITLES[store_code]}")

    upsert_all_pricing_tab(
        service=service,
        spreadsheet_id=SPREADSHEET_ID,
        store_frames=store_frames,
        requested_codes=requested_codes,
        generated_at=generated_at,
    )
    move_sheet_to_index(service, SPREADSHEET_ID, ALL_PRICING_TITLE, 1)
    print(f"[SHEET] wrote {ALL_PRICING_TITLE}")

    print(f"[DONE] Updated spreadsheet: https://docs.google.com/spreadsheets/d/{SPREADSHEET_ID}/edit")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
