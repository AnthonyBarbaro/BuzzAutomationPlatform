#!/usr/bin/env python3
from __future__ import annotations

import argparse
from collections import OrderedDict
from dataclasses import dataclass
from datetime import datetime
from pathlib import Path
from typing import Any, Iterable

import pandas as pd

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
CONTROL_START_COL = 20  # T
TOTAL_COLUMNS = 23  # A:W
DEFAULT_DISCOUNT_CELL = "R1"
TAX_MULTIPLIER_CELL = "P1"

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
]
CONTROL_HEADERS = ["Brand", "Discount %", "Finished?", "Notes"]
LEFT_ALIGN_HEADERS = {"Brand", "Category", "Product", "Brand Notes"}
CURRENCY_HEADERS = {
    "Current Shelf Price",
    "Current Discounted Shelf",
    "Current Discounted OTD",
    "Rounded OTD Target",
    "Needed Discounted Shelf",
    "Suggested Shelf Price",
    "Suggested New OTD",
    "Price Change",
}
PERCENT_HEADERS = {"Applied Discount %", "Price Change %"}
INTEGER_HEADERS = {"Available"}
HIDDEN_HEADERS = {"Finished Helper"}


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
        return float(value)
    except Exception:
        return 0.0


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
        product_id = _first_nonempty(item.get("productId"), item.get("id"), "")
        rows.append(
            {
                "merge_key": _key_for_row(product_name, sku, brand, category, product_id),
                "Brand": brand,
                "Category": category,
                "Product": product_name,
                "Vendor": vendor,
                "Current Shelf Price": current_price,
                "Available": 0.0,
                "Product ID": str(product_id).strip(),
                "Catalog Source": True,
                "Inventory Source": False,
            }
        )

    frame = pd.DataFrame(rows)
    if frame.empty:
        return pd.DataFrame(
            columns=[
                "merge_key",
                "Brand",
                "Category",
                "Product",
                "Vendor",
                "Current Shelf Price",
                "Available",
                "Product ID",
                "Catalog Source",
                "Inventory Source",
            ]
        )
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
                "Available": available,
                "Product ID": str(product_id).strip(),
                "Catalog Source": False,
                "Inventory Source": True,
            }
        )

    frame = pd.DataFrame(rows)
    if frame.empty:
        return pd.DataFrame(
            columns=[
                "merge_key",
                "Brand",
                "Category",
                "Product",
                "Vendor",
                "Current Shelf Price",
                "Available",
                "Product ID",
                "Catalog Source",
                "Inventory Source",
            ]
        )
    return frame


def _dedupe_source_frame(frame: pd.DataFrame) -> pd.DataFrame:
    if frame.empty:
        return frame

    group_cols = ["merge_key"]
    aggregated = (
        frame.groupby(group_cols, as_index=False)
        .agg(
            {
                "Brand": "first",
                "Category": "first",
                "Product": "first",
                "Vendor": "first",
                "Current Shelf Price": "max",
                "Available": "sum",
                "Product ID": "first",
                "Catalog Source": "max",
                "Inventory Source": "max",
            }
        )
        .copy()
    )
    return aggregated


def _merge_similar_brand_rows(frame: pd.DataFrame) -> pd.DataFrame:
    if frame.empty:
        return frame

    working = frame.copy()
    working["Current Shelf Price"] = pd.to_numeric(working["Current Shelf Price"], errors="coerce").fillna(0).round(2)
    working["Available"] = pd.to_numeric(working["Available"], errors="coerce").fillna(0)

    merged_rows: list[dict[str, Any]] = []
    for _, group in working.groupby(["Brand", "Category", "Current Shelf Price"], dropna=False, sort=False):
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
        merged_rows.append(row[["Brand", "Category", "Product", "Available", "Current Shelf Price"]].to_dict())

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
            }
        )

    frame = pd.DataFrame(rows)
    if frame.empty:
        return pd.DataFrame(columns=["Brand", "Category", "Product", "Available", "Current Shelf Price"])

    frame = (
        frame.groupby(["Brand", "Category", "Product", "Current Shelf Price"], as_index=False)
        .agg({"Available": "sum"})
        .copy()
    )
    frame = frame[["Brand", "Category", "Product", "Available", "Current Shelf Price"]]
    frame = _merge_similar_brand_rows(frame)
    frame = frame.sort_values(
        by=["Brand", "Category", "Current Shelf Price", "Product"],
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


def read_existing_store_controls(service: Any, spreadsheet_id: str, title: str) -> tuple[Any, dict[str, dict[str, Any]]]:
    values = _values_get(service, spreadsheet_id, _a1(title, "T2:W"), value_render_option="FORMULA")
    controls: dict[str, dict[str, Any]] = {}
    if values:
        rows = list(values)
        header = [str(value).strip() for value in rows[0]]
        if header[:4] == CONTROL_HEADERS:
            for row in rows[1:]:
                padded = list(row) + [""] * (4 - len(row))
                brand = str(padded[0]).strip()
                if not brand:
                    continue
                controls[brand.casefold()] = {
                    "brand": brand,
                    "discount": padded[1],
                    "finished": padded[2],
                    "notes": padded[3],
                }

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
    for brand in ordered:
        existing = existing_controls.get(brand.casefold(), {})
        discount = existing.get("discount", "") if existing else ""
        finished = existing.get("finished", "") if existing else ""
        notes = existing.get("notes", "") if existing else ""
        rows.append(
            [
                brand,
                discount if str(discount).strip() else default_discount_formula,
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


def _formula_for_row(row_number: int, column_name: str) -> str:
    if column_name == "Applied Discount %":
        return f'=IFNA(XLOOKUP($A{row_number},$T$3:$T,$U$3:$U),${DEFAULT_DISCOUNT_CELL})'
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
        return f'=IFNA(XLOOKUP($A{row_number},$T$3:$T,$V$3:$V),FALSE)'
    if column_name == "Status":
        return f'=IF($Q{row_number},"Done","")'
    if column_name == "Brand Notes":
        return f'=IFNA(XLOOKUP($A{row_number},$T$3:$T,$W$3:$W),"")'
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
            "Each store tab has a Brand Controls box on the right. Change the brand-level discount there from the default 30% to 40% or 50% when needed, check Finished? when a brand is done, and the product rows update automatically.",
        ],
        [
            "Main Formula",
            "Current Discounted OTD = Current Shelf Price x (1 - discount) x tax multiplier. Rounded OTD Target = ROUNDUP(Current Discounted OTD, 0). Suggested Shelf Price = ROUNDUP(ROUNDUP(Rounded OTD Target / tax multiplier, 2) / (1 - discount), 2).",
        ],
        [
        "Rerun Safety",
            "Brand-level Discount %, Finished?, and Notes are preserved on rerun for each store tab.",
        ],
        [
            "Excluded Rows",
            "Items with shelf price 0.01 and products with Sample or Promo in the name are excluded automatically.",
        ],
        [
            "Consolidation",
            "Products from the same brand and category with the same shelf price are grouped into one row, with product names rolled up automatically.",
        ],
        [
            "Store Tax Assumption",
            "Taxes are applied additively using the provided city + excise + state percentages for each store.",
        ],
        [
            "Store Tabs",
            ", ".join(STORE_TAB_TITLES.values()),
        ],
    ]


def write_values(service: Any, spreadsheet_id: str, title: str, matrix: list[list[Any]]) -> None:
    service.spreadsheets().values().clear(
        spreadsheetId=spreadsheet_id,
        range=_a1(title, "A1:W"),
        body={},
    ).execute()
    service.spreadsheets().values().update(
        spreadsheetId=spreadsheet_id,
        range=_a1(title, "A1"),
        valueInputOption="USER_ENTERED",
        body={"values": matrix},
    ).execute()


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
                            "endColumnIndex": len(MAIN_HEADERS) - 1,
                        }
                    }
                }
            }
        )

    if control_row_count:
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
                        "startColumnIndex": CONTROL_START_COL + 1,
                        "endColumnIndex": CONTROL_START_COL + 2,
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
    }
    return widths.get(header, 110)


def _control_column_width(header: str) -> int:
    widths = {
        "Brand": 150,
        "Discount %": 110,
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

    return requests


def _conditional_format_requests(sheet_id: int, total_rows: int, control_end_row: int) -> list[dict[str, Any]]:
    requests: list[dict[str, Any]] = []
    if total_rows >= DATA_START_ROW:
        data_range = {
            "sheetId": sheet_id,
            "startRowIndex": DATA_START_ROW - 1,
            "endRowIndex": total_rows,
            "startColumnIndex": 0,
            "endColumnIndex": len(MAIN_HEADERS) - 1,
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
                                "values": [{"userEnteredValue": f"=$V{CONTROL_START_ROW}=TRUE"}],
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
        return 0

    service = authenticate_sheets()
    upsert_readme(service, SPREADSHEET_ID, generated_at)
    move_sheet_to_index(service, SPREADSHEET_ID, README_TITLE, 0)

    next_index = 1
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

    print(f"[DONE] Updated spreadsheet: https://docs.google.com/spreadsheets/d/{SPREADSHEET_ID}/edit")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
