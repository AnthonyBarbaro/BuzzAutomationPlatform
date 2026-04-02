#!/usr/bin/env python3
"""
Build a live Dutchie API dashboard for today's sales activity.

The script writes a self-contained HTML dashboard so it is easy to open in a
browser, refresh on an interval, and share around internally.
"""

from __future__ import annotations

import argparse
import math
import time
import webbrowser
from collections.abc import Sequence
from dataclasses import dataclass
from datetime import datetime, time as dt_time, timedelta
from html import escape
from pathlib import Path
from typing import Any
from zoneinfo import ZoneInfo

import pandas as pd
import requests

from dutchie_api_reports import (
    DEFAULT_ENV_FILE,
    DEFAULT_TIMEZONE,
    STORE_CODES,
    canonical_env_map,
    create_session,
    discover_configured_store_codes,
    isoformat_utc,
    parse_store_codes,
    request_json,
    resolve_integrator_key,
    resolve_store_keys,
)

TRANSACTIONS_ENDPOINT = "/reporting/transactions"
PRODUCTS_ENDPOINT = "/reporting/products"
INVENTORY_ENDPOINT = "/reporting/inventory"
DEFAULT_OUTPUT_PATH = Path("reports/live_dashboard/dutchie_today_dashboard.html")
DEFAULT_REFRESH_SECONDS = 90
LOW_STOCK_UNITS_THRESHOLD = 12.0
LOW_STOCK_DAYS_THRESHOLD = 3.0


@dataclass
class StoreSnapshot:
    store_code: str
    store_name: str
    transactions_today: pd.DataFrame
    items_today: pd.DataFrame
    transactions_yesterday: pd.DataFrame
    items_yesterday: pd.DataFrame
    transactions_last_week: pd.DataFrame
    items_last_week: pd.DataFrame
    inventory_now: pd.DataFrame


def _to_float(value: Any) -> float:
    if value in (None, "", "None"):
        return 0.0
    try:
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


def _safe_int(value: Any) -> int:
    try:
        return int(value)
    except Exception:
        return 0


def _display_customer_type(value: Any) -> str:
    raw = str(_first_nonempty(value, "")).strip()
    if not raw:
        return "Unknown"
    return f"Type {raw}" if raw.isdigit() else raw


def _normalize_store_name(store_code: str) -> str:
    return STORE_CODES.get(store_code, store_code)


def _product_lookup_by_id(products_payload: Any) -> dict[int, dict[str, Any]]:
    lookup: dict[int, dict[str, Any]] = {}
    if not isinstance(products_payload, list):
        return lookup
    for row in products_payload:
        if not isinstance(row, dict):
            continue
        product_id = row.get("productId")
        try:
            lookup[int(product_id)] = row
        except Exception:
            continue
    return lookup


def _build_sales_params(start_local: datetime, end_local: datetime) -> dict[str, Any]:
    return {
        "FromDateUTC": isoformat_utc(start_local),
        "ToDateUTC": isoformat_utc(end_local),
        "IncludeDetail": True,
        "IncludeTaxes": True,
        "IncludeOrderIds": True,
        "IncludeFeesAndDonations": True,
    }


def _window_bounds(now_local: datetime, days_back: int) -> tuple[datetime, datetime]:
    target_day = now_local.date() - timedelta(days=days_back)
    elapsed = now_local - datetime.combine(now_local.date(), dt_time.min, tzinfo=now_local.tzinfo)
    start_local = datetime.combine(target_day, dt_time.min, tzinfo=now_local.tzinfo)
    end_local = start_local + elapsed
    return start_local, end_local


def _normalize_transactions_api_sales_rows(
    transactions_payload: Any,
    products_payload: Any,
    store_code: str,
) -> tuple[pd.DataFrame, pd.DataFrame]:
    transaction_rows: list[dict[str, Any]] = []
    item_rows: list[dict[str, Any]] = []
    product_lookup = _product_lookup_by_id(products_payload)
    store_name = _normalize_store_name(store_code)

    for tx in transactions_payload or []:
        if not isinstance(tx, dict):
            continue

        tx_id = _first_nonempty(tx.get("transactionId"), tx.get("globalId"), tx.get("referenceId"), "")
        tx_time = pd.to_datetime(
            _first_nonempty(
                tx.get("transactionDateLocalTime"),
                tx.get("transactionDate"),
                tx.get("lastModifiedDateUTC"),
            ),
            errors="coerce",
        )
        budtender = str(_first_nonempty(tx.get("completedByUser"), tx.get("terminalName"), "Unknown"))
        order_source = str(_first_nonempty(tx.get("orderSource"), tx.get("orderMethod"), "Unknown"))
        order_type = str(_first_nonempty(tx.get("orderType"), tx.get("transactionType"), "Unknown"))
        customer_type = _display_customer_type(
            _first_nonempty(tx.get("customerTypeName"), tx.get("customerType"), tx.get("customerTypeId"))
        )
        total = _to_float(tx.get("total"))
        subtotal = _to_float(tx.get("subtotal"))
        total_discount = _to_float(tx.get("totalDiscount"))
        total_before_tax = _to_float(tx.get("totalBeforeTax"))
        tax = _to_float(tx.get("tax"))
        tip_amount = _to_float(tx.get("tipAmount"))
        total_items = _to_float(tx.get("totalItems"))
        is_return = bool(tx.get("isReturn")) or total < 0
        is_void = bool(tx.get("isVoid"))

        transaction_rows.append(
            {
                "transaction_id": str(tx_id),
                "order_time": tx_time,
                "budtender": budtender,
                "order_source": order_source,
                "order_type": order_type,
                "customer_type": customer_type,
                "transaction_type": str(_first_nonempty(tx.get("transactionType"), "")),
                "total": total,
                "subtotal": subtotal,
                "discount": total_discount,
                "before_tax": total_before_tax,
                "tax": tax,
                "tip": tip_amount,
                "total_items": total_items,
                "is_return": is_return,
                "is_void": is_void,
                "store_code": store_code,
                "store_name": store_name,
            }
        )

        for item in tx.get("items") or []:
            if not isinstance(item, dict):
                continue

            product_id = _safe_int(item.get("productId"))
            inventory_id = _safe_int(item.get("inventoryId"))
            product_info = product_lookup.get(product_id, {})
            product_name = str(
                _first_nonempty(
                    product_info.get("productName"),
                    product_info.get("internalName"),
                    product_info.get("alternateName"),
                    f"Unknown Product {product_id or '?'}",
                )
            )
            category = str(_first_nonempty(product_info.get("category"), product_info.get("masterCategory"), "Unknown"))
            brand_name = str(_first_nonempty(product_info.get("brandName"), product_info.get("brand"), ""))
            vendor_name = str(
                _first_nonempty(
                    item.get("vendor"),
                    product_info.get("vendorName"),
                    product_info.get("producerName"),
                    "",
                )
            )
            quantity = _to_float(item.get("quantity"))
            unit_weight = _to_float(item.get("unitWeight"))
            gross_sales = _to_float(item.get("totalPrice"))
            discount_amount = _to_float(item.get("totalDiscount"))
            net_sales = gross_sales - discount_amount
            unit_cost = _to_float(_first_nonempty(item.get("unitCost"), product_info.get("unitCost")))
            inventory_cost = unit_cost * quantity
            unit_price = _to_float(
                _first_nonempty(
                    item.get("unitPrice"),
                    product_info.get("price"),
                    product_info.get("recPrice"),
                    product_info.get("medPrice"),
                )
            )
            is_return_item = bool(item.get("isReturned")) or is_return
            sign = -1.0 if is_return_item else 1.0

            quantity = abs(quantity) * sign
            gross_sales = abs(gross_sales) * sign
            discount_amount = abs(discount_amount) * sign
            net_sales = abs(net_sales) * sign
            inventory_cost = abs(inventory_cost) * sign
            total_weight = abs(quantity) * unit_weight * (1.0 if sign >= 0 else -1.0)
            order_profit = net_sales - inventory_cost

            item_rows.append(
                {
                    "transaction_id": str(tx_id),
                    "order_time": tx_time,
                    "budtender": budtender,
                    "order_source": order_source,
                    "order_type": order_type,
                    "customer_type": customer_type,
                    "store_code": store_code,
                    "store_name": store_name,
                    "product_id": product_id,
                    "inventory_id": inventory_id,
                    "sku": str(_first_nonempty(product_info.get("sku"), "")),
                    "product_name": product_name,
                    "brand_name": brand_name,
                    "category": category,
                    "vendor_name": vendor_name,
                    "package_id": str(_first_nonempty(item.get("packageId"), "")),
                    "quantity": quantity,
                    "unit_weight": unit_weight,
                    "total_weight": total_weight,
                    "gross_sales": gross_sales,
                    "discount_amount": discount_amount,
                    "net_sales": net_sales,
                    "inventory_cost": inventory_cost,
                    "order_profit": order_profit,
                    "unit_price": unit_price,
                    "is_return": is_return_item,
                }
            )

    transactions_df = pd.DataFrame(transaction_rows)
    if not transactions_df.empty:
        transactions_df["order_time"] = pd.to_datetime(transactions_df["order_time"], errors="coerce")

    items_df = pd.DataFrame(item_rows)
    if not items_df.empty:
        items_df["order_time"] = pd.to_datetime(items_df["order_time"], errors="coerce")

    return transactions_df, items_df


def _normalize_inventory_api_rows(inventory_payload: Any, store_code: str) -> pd.DataFrame:
    rows: list[dict[str, Any]] = []
    store_name = _normalize_store_name(store_code)

    for item in inventory_payload or []:
        if not isinstance(item, dict):
            continue
        rows.append(
            {
                "store_code": store_code,
                "store_name": store_name,
                "product_id": _safe_int(item.get("productId")),
                "inventory_id": _safe_int(item.get("inventoryId")),
                "sku": str(_first_nonempty(item.get("sku"), "")),
                "product_name": str(_first_nonempty(item.get("productName"), item.get("alternateName"), "Unknown")),
                "brand_name": str(_first_nonempty(item.get("brandName"), item.get("brand"), "")),
                "category": str(_first_nonempty(item.get("category"), item.get("masterCategory"), "Unknown")),
                "vendor_name": str(_first_nonempty(item.get("vendor"), item.get("producer"), "")),
                "available": _to_float(item.get("quantityAvailable")),
                "unit_cost": _to_float(item.get("unitCost")),
                "unit_price": _to_float(
                    _first_nonempty(item.get("unitPrice"), item.get("recUnitPrice"), item.get("medUnitPrice"))
                ),
            }
        )

    return pd.DataFrame(rows)


def _summarize_period(transactions_df: pd.DataFrame, items_df: pd.DataFrame) -> dict[str, Any]:
    if transactions_df.empty:
        return {
            "sales_total": 0.0,
            "subtotal": 0.0,
            "before_tax": 0.0,
            "discount": 0.0,
            "tax": 0.0,
            "tip": 0.0,
            "tickets": 0,
            "units": 0.0,
            "avg_ticket": 0.0,
            "discount_rate": 0.0,
            "profit_estimate": 0.0,
            "margin_estimate": None,
            "returns_total": 0.0,
            "returns_count": 0,
            "last_ticket_time": None,
        }

    sales_total = float(transactions_df["total"].sum())
    subtotal = float(transactions_df["subtotal"].sum())
    before_tax = float(transactions_df["before_tax"].sum())
    discount = float(transactions_df["discount"].sum())
    tax = float(transactions_df["tax"].sum())
    tip = float(transactions_df["tip"].sum())
    tickets = int(transactions_df["transaction_id"].nunique())
    returns_mask = transactions_df["is_return"].fillna(False) | (transactions_df["total"] < 0)
    returns_total = float(-transactions_df.loc[returns_mask, "total"].sum()) if returns_mask.any() else 0.0
    returns_count = int(returns_mask.sum())
    last_ticket_time = transactions_df["order_time"].max()

    units = 0.0
    profit_estimate = 0.0
    margin_estimate: float | None = None
    if not items_df.empty:
        units = float(items_df["quantity"].sum())
        profit_estimate = float(items_df["order_profit"].sum())
        net_sales = float(items_df["net_sales"].sum())
        margin_estimate = (profit_estimate / net_sales) if abs(net_sales) > 1e-9 else None

    avg_ticket = sales_total / tickets if tickets else 0.0
    discount_rate = discount / subtotal if abs(subtotal) > 1e-9 else 0.0

    return {
        "sales_total": sales_total,
        "subtotal": subtotal,
        "before_tax": before_tax,
        "discount": discount,
        "tax": tax,
        "tip": tip,
        "tickets": tickets,
        "units": units,
        "avg_ticket": avg_ticket,
        "discount_rate": discount_rate,
        "profit_estimate": profit_estimate,
        "margin_estimate": margin_estimate,
        "returns_total": returns_total,
        "returns_count": returns_count,
        "last_ticket_time": last_ticket_time,
    }


def _compare_metrics(current: float, previous: float) -> dict[str, Any]:
    delta = current - previous
    pct = None if abs(previous) <= 1e-9 else delta / abs(previous)
    return {"current": current, "previous": previous, "delta": delta, "pct": pct}


def _hours_for_chart(transactions_df: pd.DataFrame, now_local: datetime) -> list[dict[str, Any]]:
    if transactions_df.empty:
        current_hour = now_local.hour
        return [{"label": f"{current_hour:02d}:00", "sales_total": 0.0, "tickets": 0}]

    hourly = transactions_df.copy()
    hourly["hour"] = hourly["order_time"].dt.hour.fillna(now_local.hour).astype(int)
    grouped = (
        hourly.groupby("hour", dropna=False)
        .agg(sales_total=("total", "sum"), tickets=("transaction_id", "nunique"))
        .reset_index()
    )
    hour_map = {int(row["hour"]): row for _, row in grouped.iterrows()}
    min_hour = max(0, min(hour_map) - 1)
    max_hour = now_local.hour
    rows: list[dict[str, Any]] = []
    for hour in range(min_hour, max_hour + 1):
        row = hour_map.get(hour)
        rows.append(
            {
                "label": f"{hour:02d}:00",
                "sales_total": float(row["sales_total"]) if row is not None else 0.0,
                "tickets": int(row["tickets"]) if row is not None else 0,
            }
        )
    return rows


def _group_top_products(items_df: pd.DataFrame, limit: int = 12) -> list[dict[str, Any]]:
    if items_df.empty:
        return []
    grouped = (
        items_df.groupby(["product_name", "brand_name", "category"], dropna=False)
        .agg(
            net_sales=("net_sales", "sum"),
            gross_sales=("gross_sales", "sum"),
            quantity=("quantity", "sum"),
            tickets=("transaction_id", "nunique"),
            discount=("discount_amount", "sum"),
        )
        .reset_index()
        .sort_values(["net_sales", "quantity"], ascending=[False, False])
    )
    return grouped.head(limit).to_dict("records")


def _group_top_categories(items_df: pd.DataFrame, limit: int = 10) -> list[dict[str, Any]]:
    if items_df.empty:
        return []
    grouped = (
        items_df.groupby("category", dropna=False)
        .agg(net_sales=("net_sales", "sum"), quantity=("quantity", "sum"), tickets=("transaction_id", "nunique"))
        .reset_index()
        .sort_values("net_sales", ascending=False)
    )
    return grouped.head(limit).to_dict("records")


def _group_top_vendors(items_df: pd.DataFrame, limit: int = 10) -> list[dict[str, Any]]:
    if items_df.empty:
        return []
    grouped = (
        items_df.groupby("vendor_name", dropna=False)
        .agg(net_sales=("net_sales", "sum"), quantity=("quantity", "sum"), tickets=("transaction_id", "nunique"))
        .reset_index()
        .sort_values("net_sales", ascending=False)
    )
    return grouped.head(limit).to_dict("records")


def _group_top_budtenders(transactions_df: pd.DataFrame, limit: int = 10) -> list[dict[str, Any]]:
    if transactions_df.empty:
        return []
    grouped = (
        transactions_df.groupby("budtender", dropna=False)
        .agg(
            sales_total=("total", "sum"),
            tickets=("transaction_id", "nunique"),
            discounts=("discount", "sum"),
        )
        .reset_index()
    )
    grouped["avg_ticket"] = grouped.apply(
        lambda row: (row["sales_total"] / row["tickets"]) if row["tickets"] else 0.0,
        axis=1,
    )
    grouped = grouped.sort_values(["sales_total", "tickets"], ascending=[False, False])
    return grouped.head(limit).to_dict("records")


def _group_source_mix(transactions_df: pd.DataFrame, column: str, limit: int = 10) -> list[dict[str, Any]]:
    if transactions_df.empty:
        return []
    grouped = (
        transactions_df.groupby(column, dropna=False)
        .agg(sales_total=("total", "sum"), tickets=("transaction_id", "nunique"))
        .reset_index()
        .sort_values("sales_total", ascending=False)
    )
    return grouped.head(limit).to_dict("records")


def _store_rows(snapshots: Sequence[StoreSnapshot]) -> list[dict[str, Any]]:
    rows: list[dict[str, Any]] = []
    for snapshot in snapshots:
        today = _summarize_period(snapshot.transactions_today, snapshot.items_today)
        yesterday = _summarize_period(snapshot.transactions_yesterday, snapshot.items_yesterday)
        last_week = _summarize_period(snapshot.transactions_last_week, snapshot.items_last_week)
        rows.append(
            {
                "store_code": snapshot.store_code,
                "store_name": snapshot.store_name,
                "sales_total": today["sales_total"],
                "tickets": today["tickets"],
                "units": today["units"],
                "avg_ticket": today["avg_ticket"],
                "margin_estimate": today["margin_estimate"],
                "last_ticket_time": today["last_ticket_time"],
                "vs_yesterday": _compare_metrics(today["sales_total"], yesterday["sales_total"]),
                "vs_last_week": _compare_metrics(today["sales_total"], last_week["sales_total"]),
            }
        )
    rows.sort(key=lambda row: row["sales_total"], reverse=True)
    return rows


def _low_stock_rows(snapshots: Sequence[StoreSnapshot], limit: int = 14) -> list[dict[str, Any]]:
    inventory_frames = [snapshot.inventory_now for snapshot in snapshots if not snapshot.inventory_now.empty]
    item_frames = [snapshot.items_today for snapshot in snapshots if not snapshot.items_today.empty]
    if not inventory_frames or not item_frames:
        return []

    inventory_df = pd.concat(inventory_frames, ignore_index=True)
    sales_df = pd.concat(item_frames, ignore_index=True)
    sales_df = sales_df[sales_df["quantity"] > 0]
    if sales_df.empty:
        return []

    velocity = (
        sales_df.groupby(["store_code", "product_id"], dropna=False)
        .agg(
            units_today=("quantity", "sum"),
            revenue_today=("net_sales", "sum"),
            tickets=("transaction_id", "nunique"),
        )
        .reset_index()
    )

    merged = inventory_df.merge(velocity, on=["store_code", "product_id"], how="left")
    merged["units_today"] = merged["units_today"].fillna(0.0)
    merged["revenue_today"] = merged["revenue_today"].fillna(0.0)
    merged["tickets"] = merged["tickets"].fillna(0).astype(int)
    merged["days_left"] = merged.apply(
        lambda row: (row["available"] / row["units_today"]) if row["units_today"] > 0 else math.inf,
        axis=1,
    )
    flagged = merged[
        (merged["units_today"] > 0)
        & (
            (merged["available"] <= LOW_STOCK_UNITS_THRESHOLD)
            | (merged["days_left"] <= LOW_STOCK_DAYS_THRESHOLD)
        )
    ].copy()
    if flagged.empty:
        return []

    flagged = flagged.sort_values(
        ["days_left", "units_today", "revenue_today", "available"],
        ascending=[True, False, False, True],
    )
    return flagged.head(limit).to_dict("records")


def _fmt_money(value: Any) -> str:
    return f"${_to_float(value):,.2f}"


def _fmt_number(value: Any) -> str:
    return f"{_to_float(value):,.0f}"


def _fmt_decimal(value: Any) -> str:
    return f"{_to_float(value):,.1f}"


def _fmt_pct(value: Any) -> str:
    if value is None or (isinstance(value, float) and not math.isfinite(value)):
        return "n/a"
    return f"{_to_float(value) * 100:,.1f}%"


def _fmt_delta(metrics: dict[str, Any]) -> str:
    delta = _to_float(metrics.get("delta"))
    pct = metrics.get("pct")
    sign = "+" if delta >= 0 else "-"
    pct_text = "n/a" if pct is None else f"{abs(pct) * 100:,.1f}%"
    return f"{sign}{_fmt_money(abs(delta))} ({pct_text})"


def _delta_class(metrics: dict[str, Any]) -> str:
    return "up" if _to_float(metrics.get("delta")) >= 0 else "down"


def _fmt_time(value: Any) -> str:
    if value is None or (isinstance(value, float) and not math.isfinite(value)):
        return "n/a"
    ts = pd.to_datetime(value, errors="coerce")
    if pd.isna(ts):
        return "n/a"
    return ts.strftime("%-I:%M %p")


def _minutes_since(last_ticket_time: Any, now_local: datetime) -> str:
    ts = pd.to_datetime(last_ticket_time, errors="coerce")
    if pd.isna(ts):
        return "n/a"
    ts_py = ts.to_pydatetime()
    if ts_py.tzinfo is None and now_local.tzinfo is not None:
        ts_py = ts_py.replace(tzinfo=now_local.tzinfo)
    elif ts_py.tzinfo is not None and now_local.tzinfo is not None:
        ts_py = ts_py.astimezone(now_local.tzinfo)
    delta = now_local - ts_py
    minutes = max(0, int(delta.total_seconds() // 60))
    return f"{minutes} min ago"


def _table_html(title: str, subtitle: str, columns: Sequence[tuple[str, str]], rows: Sequence[dict[str, Any]]) -> str:
    if not rows:
        return (
            f"<section class='panel'><div class='panel-head'><h3>{escape(title)}</h3>"
            f"<p>{escape(subtitle)}</p></div><div class='empty'>No data yet for this section.</div></section>"
        )

    head_html = "".join(f"<th>{escape(label)}</th>" for label, _ in columns)
    body_parts: list[str] = []
    for row in rows:
        cells = "".join(f"<td>{row.get(key, '')}</td>" for _, key in columns)
        body_parts.append(f"<tr>{cells}</tr>")
    body_html = "".join(body_parts)
    return (
        f"<section class='panel'><div class='panel-head'><h3>{escape(title)}</h3><p>{escape(subtitle)}</p></div>"
        f"<div class='table-wrap'><table><thead><tr>{head_html}</tr></thead><tbody>{body_html}</tbody></table></div></section>"
    )


def _render_dashboard_html(
    report: dict[str, Any],
    output_path: Path,
    auto_refresh_seconds: int,
) -> str:
    overview = report["overview"]
    compare_yesterday = report["compare"]["yesterday"]
    compare_last_week = report["compare"]["last_week"]
    store_rows = report["store_rows"]
    hourly_points = report["hourly_points"]
    low_stock = report["low_stock_rows"]
    generated_at = report["generated_at"]
    now_local = report["now_local"]

    bar_max = max((point["sales_total"] for point in hourly_points), default=0.0) or 1.0
    hourly_html = "".join(
        (
            "<div class='hour-bar'>"
            f"<div class='bar' style='height:{max(8.0, (point['sales_total'] / bar_max) * 100):.1f}%'></div>"
            f"<div class='hour-label'>{escape(point['label'])}</div>"
            f"<div class='hour-value'>{escape(_fmt_money(point['sales_total']))}</div>"
            f"<div class='hour-sub'>{escape(str(point['tickets']))} tix</div>"
            "</div>"
        )
        for point in hourly_points
    )

    store_table_rows = []
    for row in store_rows:
        store_table_rows.append(
            {
                "store": f"<strong>{escape(row['store_code'])}</strong><span class='muted-line'>{escape(row['store_name'])}</span>",
                "sales": _fmt_money(row["sales_total"]),
                "tickets": _fmt_number(row["tickets"]),
                "avg_ticket": _fmt_money(row["avg_ticket"]),
                "units": _fmt_decimal(row["units"]),
                "margin": _fmt_pct(row["margin_estimate"]),
                "last_ticket": f"{escape(_fmt_time(row['last_ticket_time']))}<span class='muted-line'>{escape(_minutes_since(row['last_ticket_time'], now_local))}</span>",
                "vs_yesterday": f"<span class='delta-pill {_delta_class(row['vs_yesterday'])}'>{escape(_fmt_delta(row['vs_yesterday']))}</span>",
                "vs_last_week": f"<span class='delta-pill {_delta_class(row['vs_last_week'])}'>{escape(_fmt_delta(row['vs_last_week']))}</span>",
            }
        )

    product_rows = [
        {
            "product": f"<strong>{escape(str(row.get('product_name') or 'Unknown'))}</strong><span class='muted-line'>{escape(str(row.get('brand_name') or ''))}</span>",
            "category": escape(str(row.get("category") or "")),
            "sales": _fmt_money(row.get("net_sales")),
            "units": _fmt_decimal(row.get("quantity")),
            "tickets": _fmt_number(row.get("tickets")),
            "discounts": _fmt_money(row.get("discount")),
        }
        for row in report["top_products"]
    ]

    category_rows = [
        {
            "category": escape(str(row.get("category") or "Unknown")),
            "sales": _fmt_money(row.get("net_sales")),
            "units": _fmt_decimal(row.get("quantity")),
            "tickets": _fmt_number(row.get("tickets")),
        }
        for row in report["top_categories"]
    ]

    vendor_rows = [
        {
            "vendor": escape(str(row.get("vendor_name") or "Unknown")),
            "sales": _fmt_money(row.get("net_sales")),
            "units": _fmt_decimal(row.get("quantity")),
            "tickets": _fmt_number(row.get("tickets")),
        }
        for row in report["top_vendors"]
    ]

    budtender_rows = [
        {
            "budtender": escape(str(row.get("budtender") or "Unknown")),
            "sales": _fmt_money(row.get("sales_total")),
            "tickets": _fmt_number(row.get("tickets")),
            "avg_ticket": _fmt_money(row.get("avg_ticket")),
            "discounts": _fmt_money(row.get("discounts")),
        }
        for row in report["top_budtenders"]
    ]

    source_rows = [
        {
            "source": escape(str(row.get("order_source") or "Unknown")),
            "sales": _fmt_money(row.get("sales_total")),
            "tickets": _fmt_number(row.get("tickets")),
        }
        for row in report["source_mix"]
    ]

    order_type_rows = [
        {
            "type": escape(str(row.get("order_type") or "Unknown")),
            "sales": _fmt_money(row.get("sales_total")),
            "tickets": _fmt_number(row.get("tickets")),
        }
        for row in report["order_type_mix"]
    ]

    low_stock_rows = [
        {
            "store": f"<strong>{escape(str(row.get('store_code') or ''))}</strong><span class='muted-line'>{escape(str(row.get('store_name') or ''))}</span>",
            "product": f"<strong>{escape(str(row.get('product_name') or 'Unknown'))}</strong><span class='muted-line'>{escape(str(row.get('brand_name') or ''))}</span>",
            "available": _fmt_decimal(row.get("available")),
            "sold_today": _fmt_decimal(row.get("units_today")),
            "days_left": "same-day sellout"
            if row.get("days_left") not in (None, math.inf) and _to_float(row.get("days_left")) < 1
            else ("n/a" if row.get("days_left") in (None, math.inf) else f"{_to_float(row.get('days_left')):,.1f} days"),
            "sales": _fmt_money(row.get("revenue_today")),
        }
        for row in low_stock
    ]

    refresh_meta = (
        f"<meta http-equiv='refresh' content='{int(auto_refresh_seconds)}'>"
        if auto_refresh_seconds and auto_refresh_seconds > 0
        else ""
    )

    errors_html = ""
    if report["errors"]:
        error_items = "".join(f"<li>{escape(err)}</li>" for err in report["errors"])
        errors_html = (
            "<details class='panel error-panel'><summary>API notes and fetch warnings</summary>"
            f"<ul>{error_items}</ul></details>"
        )

    return f"""<!doctype html>
<html lang="en">
<head>
  <meta charset="utf-8">
  <meta name="viewport" content="width=device-width, initial-scale=1">
  {refresh_meta}
  <title>Dutchie Today Dashboard</title>
  <style>
    :root {{
      --bg: #f4efe7;
      --card: rgba(255, 251, 245, 0.86);
      --ink: #1f2933;
      --muted: #5f6c76;
      --line: rgba(32, 55, 68, 0.12);
      --accent: #0f766e;
      --accent-2: #d97706;
      --accent-3: #1d4ed8;
      --good: #0f766e;
      --bad: #b42318;
      --shadow: 0 18px 48px rgba(22, 38, 46, 0.14);
      --radius: 24px;
    }}
    * {{ box-sizing: border-box; }}
    body {{
      margin: 0;
      color: var(--ink);
      font-family: "Avenir Next", "Segoe UI", "Helvetica Neue", sans-serif;
      background:
        radial-gradient(circle at top left, rgba(15, 118, 110, 0.16), transparent 30%),
        radial-gradient(circle at top right, rgba(217, 119, 6, 0.15), transparent 26%),
        linear-gradient(180deg, #f8f4ed 0%, var(--bg) 100%);
      min-height: 100vh;
    }}
    .page {{
      width: min(1440px, calc(100vw - 32px));
      margin: 0 auto;
      padding: 28px 0 40px;
    }}
    .hero {{
      display: grid;
      grid-template-columns: 1.5fr 1fr;
      gap: 18px;
      margin-bottom: 20px;
    }}
    .hero-card, .panel, .kpi-card, .compare-card, .mini-card {{
      background: var(--card);
      backdrop-filter: blur(18px);
      border: 1px solid rgba(255, 255, 255, 0.8);
      border-radius: var(--radius);
      box-shadow: var(--shadow);
      animation: fade-up 420ms ease both;
    }}
    .hero-card {{
      padding: 28px;
    }}
    .hero-kicker {{
      display: inline-flex;
      align-items: center;
      gap: 8px;
      padding: 8px 12px;
      border-radius: 999px;
      background: rgba(15, 118, 110, 0.09);
      color: var(--accent);
      font-size: 12px;
      letter-spacing: 0.08em;
      text-transform: uppercase;
      font-weight: 700;
    }}
    .hero h1 {{
      margin: 16px 0 12px;
      font-family: "Gill Sans", "Avenir Next", sans-serif;
      font-size: clamp(32px, 5vw, 58px);
      line-height: 0.95;
      letter-spacing: -0.04em;
    }}
    .hero p {{
      margin: 0;
      color: var(--muted);
      font-size: 15px;
      line-height: 1.6;
    }}
    .hero-meta {{
      display: grid;
      grid-template-columns: repeat(2, minmax(0, 1fr));
      gap: 12px;
      margin-top: 20px;
    }}
    .mini-card {{
      padding: 18px 20px;
    }}
    .mini-label {{
      font-size: 11px;
      letter-spacing: 0.08em;
      text-transform: uppercase;
      color: var(--muted);
      margin-bottom: 10px;
      font-weight: 700;
    }}
    .mini-value {{
      font-size: 22px;
      font-weight: 800;
      letter-spacing: -0.03em;
    }}
    .mini-note {{
      color: var(--muted);
      font-size: 13px;
      margin-top: 8px;
    }}
    .kpi-grid {{
      display: grid;
      grid-template-columns: repeat(4, minmax(0, 1fr));
      gap: 16px;
      margin-bottom: 18px;
    }}
    .kpi-card {{
      padding: 20px 22px;
      position: relative;
      overflow: hidden;
    }}
    .kpi-card::after {{
      content: "";
      position: absolute;
      inset: auto -24px -24px auto;
      width: 120px;
      height: 120px;
      border-radius: 999px;
      opacity: 0.12;
      background: linear-gradient(135deg, var(--accent), transparent 70%);
    }}
    .kpi-label {{
      color: var(--muted);
      font-size: 12px;
      text-transform: uppercase;
      letter-spacing: 0.08em;
      font-weight: 700;
    }}
    .kpi-value {{
      margin-top: 12px;
      font-size: clamp(24px, 3vw, 38px);
      font-weight: 800;
      letter-spacing: -0.05em;
    }}
    .kpi-sub {{
      margin-top: 8px;
      font-size: 13px;
      color: var(--muted);
    }}
    .compare-grid {{
      display: grid;
      grid-template-columns: repeat(2, minmax(0, 1fr));
      gap: 16px;
      margin-bottom: 18px;
    }}
    .compare-card {{
      padding: 22px;
    }}
    .compare-title {{
      display: flex;
      align-items: baseline;
      justify-content: space-between;
      gap: 12px;
      margin-bottom: 16px;
    }}
    .compare-title h3 {{
      margin: 0;
      font-size: 18px;
    }}
    .compare-title span {{
      color: var(--muted);
      font-size: 13px;
    }}
    .compare-main {{
      display: flex;
      align-items: flex-end;
      justify-content: space-between;
      gap: 20px;
    }}
    .compare-value {{
      font-size: 34px;
      font-weight: 800;
      letter-spacing: -0.05em;
    }}
    .compare-note {{
      margin-top: 8px;
      font-size: 13px;
      color: var(--muted);
    }}
    .delta-pill {{
      display: inline-flex;
      align-items: center;
      gap: 8px;
      border-radius: 999px;
      padding: 10px 14px;
      font-size: 13px;
      font-weight: 700;
      white-space: nowrap;
    }}
    .delta-pill.up {{
      color: var(--good);
      background: rgba(15, 118, 110, 0.1);
    }}
    .delta-pill.down {{
      color: var(--bad);
      background: rgba(180, 35, 24, 0.1);
    }}
    .layout {{
      display: grid;
      grid-template-columns: 1.35fr 1fr;
      gap: 18px;
      margin-bottom: 18px;
    }}
    .panel {{
      padding: 22px;
    }}
    .panel-head {{
      margin-bottom: 16px;
    }}
    .panel-head h3 {{
      margin: 0 0 6px;
      font-size: 20px;
    }}
    .panel-head p {{
      margin: 0;
      color: var(--muted);
      font-size: 14px;
      line-height: 1.5;
    }}
    .hour-grid {{
      display: grid;
      grid-template-columns: repeat(auto-fit, minmax(58px, 1fr));
      gap: 10px;
      align-items: end;
      min-height: 240px;
    }}
    .hour-bar {{
      display: flex;
      flex-direction: column;
      align-items: center;
      gap: 8px;
      justify-content: end;
      min-height: 240px;
    }}
    .bar {{
      width: 100%;
      min-height: 6px;
      border-radius: 18px 18px 8px 8px;
      background: linear-gradient(180deg, var(--accent-2), var(--accent));
      box-shadow: inset 0 1px 0 rgba(255, 255, 255, 0.45);
    }}
    .hour-label {{
      font-size: 12px;
      font-weight: 700;
      color: var(--muted);
    }}
    .hour-value {{
      font-size: 12px;
      font-weight: 700;
      text-align: center;
    }}
    .hour-sub {{
      font-size: 11px;
      color: var(--muted);
    }}
    .table-wrap {{
      overflow: auto;
    }}
    table {{
      width: 100%;
      border-collapse: collapse;
      min-width: 640px;
    }}
    th {{
      text-align: left;
      font-size: 11px;
      text-transform: uppercase;
      letter-spacing: 0.08em;
      color: var(--muted);
      border-bottom: 1px solid var(--line);
      padding: 0 0 12px;
    }}
    td {{
      padding: 14px 0;
      border-bottom: 1px solid var(--line);
      font-size: 14px;
      vertical-align: top;
    }}
    tbody tr:last-child td {{
      border-bottom: none;
    }}
    .muted-line {{
      display: block;
      margin-top: 4px;
      font-size: 12px;
      color: var(--muted);
    }}
    .empty {{
      color: var(--muted);
      font-size: 14px;
      padding: 8px 2px 2px;
    }}
    .error-panel {{
      margin-top: 18px;
      padding: 20px 22px;
    }}
    .error-panel summary {{
      cursor: pointer;
      font-weight: 700;
    }}
    .error-panel ul {{
      margin: 14px 0 0 18px;
      color: var(--muted);
    }}
    .footer-note {{
      text-align: right;
      color: var(--muted);
      font-size: 13px;
      margin-top: 20px;
    }}
    @keyframes fade-up {{
      from {{
        opacity: 0;
        transform: translateY(12px);
      }}
      to {{
        opacity: 1;
        transform: translateY(0);
      }}
    }}
    @media (max-width: 1100px) {{
      .hero,
      .layout,
      .kpi-grid,
      .compare-grid {{
        grid-template-columns: 1fr;
      }}
    }}
    @media (max-width: 720px) {{
      .page {{
        width: min(100vw - 18px, 1440px);
        padding-top: 18px;
      }}
      .hero-card,
      .panel,
      .kpi-card,
      .compare-card,
      .mini-card {{
        border-radius: 20px;
      }}
      .hero-meta {{
        grid-template-columns: 1fr;
      }}
    }}
  </style>
</head>
<body>
  <div class="page">
    <section class="hero">
      <div class="hero-card">
        <span class="hero-kicker">Dutchie Live View</span>
        <h1>Today at a glance, while the day is still moving.</h1>
        <p>
          This dashboard blends live Dutchie sales activity, same-time pace checks,
          and stock pressure signals so you can watch today's business without
          digging through raw exports.
        </p>
        <div class="hero-meta">
          <div class="mini-card">
            <div class="mini-label">Generated</div>
            <div class="mini-value">{escape(generated_at.strftime('%b %-d, %Y  %-I:%M:%S %p'))}</div>
            <div class="mini-note">{escape(report['timezone'])}</div>
          </div>
          <div class="mini-card">
            <div class="mini-label">Stores Loaded</div>
            <div class="mini-value">{len(report['snapshots'])}/{len(report['selected_stores'])}</div>
            <div class="mini-note">{escape(', '.join(report['selected_stores']))}</div>
          </div>
        </div>
      </div>
      <div class="hero-card">
        <div class="mini-label">Live window</div>
        <div class="mini-value">{escape(now_local.strftime('%A, %B %-d'))}</div>
        <div class="mini-note">Midnight through the latest available ticket.</div>
        <div class="hero-meta">
          <div class="mini-card">
            <div class="mini-label">Output file</div>
            <div class="mini-value">{escape(output_path.name)}</div>
            <div class="mini-note">{escape(str(output_path.parent))}</div>
          </div>
          <div class="mini-card">
            <div class="mini-label">Refresh mode</div>
            <div class="mini-value">{'On' if auto_refresh_seconds else 'One-time'}</div>
            <div class="mini-note">
              {'Browser refreshes every ' + str(auto_refresh_seconds) + ' seconds.' if auto_refresh_seconds else 'Re-run the script whenever you want a fresh pull.'}
            </div>
          </div>
        </div>
      </div>
    </section>

    <section class="kpi-grid">
      <article class="kpi-card">
        <div class="kpi-label">Sales Total</div>
        <div class="kpi-value">{escape(_fmt_money(overview['sales_total']))}</div>
        <div class="kpi-sub">{escape(_fmt_number(overview['tickets']))} tickets, {escape(_fmt_money(overview['avg_ticket']))} avg ticket</div>
      </article>
      <article class="kpi-card">
        <div class="kpi-label">Net Before Tax</div>
        <div class="kpi-value">{escape(_fmt_money(overview['before_tax']))}</div>
        <div class="kpi-sub">{escape(_fmt_money(overview['tax']))} tax collected today</div>
      </article>
      <article class="kpi-card">
        <div class="kpi-label">Discount Pressure</div>
        <div class="kpi-value">{escape(_fmt_money(overview['discount']))}</div>
        <div class="kpi-sub">{escape(_fmt_pct(overview['discount_rate']))} of subtotal discounted</div>
      </article>
      <article class="kpi-card">
        <div class="kpi-label">Profit Estimate</div>
        <div class="kpi-value">{escape(_fmt_money(overview['profit_estimate']))}</div>
        <div class="kpi-sub">{escape(_fmt_pct(overview['margin_estimate']))} detail-based margin estimate</div>
      </article>
      <article class="kpi-card">
        <div class="kpi-label">Units</div>
        <div class="kpi-value">{escape(_fmt_decimal(overview['units']))}</div>
        <div class="kpi-sub">Net item quantity from detailed line items</div>
      </article>
      <article class="kpi-card">
        <div class="kpi-label">Returns</div>
        <div class="kpi-value">{escape(_fmt_money(overview['returns_total']))}</div>
        <div class="kpi-sub">{escape(_fmt_number(overview['returns_count']))} return tickets</div>
      </article>
      <article class="kpi-card">
        <div class="kpi-label">Latest Ticket</div>
        <div class="kpi-value">{escape(_fmt_time(overview['last_ticket_time']))}</div>
        <div class="kpi-sub">{escape(_minutes_since(overview['last_ticket_time'], now_local))}</div>
      </article>
      <article class="kpi-card">
        <div class="kpi-label">Tips</div>
        <div class="kpi-value">{escape(_fmt_money(overview['tip']))}</div>
        <div class="kpi-sub">Tracked at the transaction level</div>
      </article>
    </section>

    <section class="compare-grid">
      <article class="compare-card">
        <div class="compare-title">
          <h3>Same-time pace vs yesterday</h3>
          <span>midnight to current time</span>
        </div>
        <div class="compare-main">
          <div>
            <div class="compare-value">{escape(_fmt_money(compare_yesterday['current']))}</div>
            <div class="compare-note">Yesterday at this same point: {escape(_fmt_money(compare_yesterday['previous']))}</div>
          </div>
          <span class="delta-pill {_delta_class(compare_yesterday)}">{escape(_fmt_delta(compare_yesterday))}</span>
        </div>
      </article>
      <article class="compare-card">
        <div class="compare-title">
          <h3>Same-time pace vs last week</h3>
          <span>same weekday checkpoint</span>
        </div>
        <div class="compare-main">
          <div>
            <div class="compare-value">{escape(_fmt_money(compare_last_week['current']))}</div>
            <div class="compare-note">Last week at this same point: {escape(_fmt_money(compare_last_week['previous']))}</div>
          </div>
          <span class="delta-pill {_delta_class(compare_last_week)}">{escape(_fmt_delta(compare_last_week))}</span>
        </div>
      </article>
    </section>

    <section class="layout">
      <section class="panel">
        <div class="panel-head">
          <h3>Hourly Sales Flow</h3>
          <p>Sales total and ticket count by hour for the current day.</p>
        </div>
        <div class="hour-grid">{hourly_html}</div>
      </section>
      {_table_html(
          "Store Scoreboard",
          "Quick read on where the day is strongest and how each store is pacing.",
          [
              ("Store", "store"),
              ("Sales", "sales"),
              ("Tickets", "tickets"),
              ("Avg Ticket", "avg_ticket"),
              ("Units", "units"),
              ("Margin", "margin"),
              ("Last Ticket", "last_ticket"),
              ("Vs Yesterday", "vs_yesterday"),
              ("Vs Last Week", "vs_last_week"),
          ],
          store_table_rows,
      )}
    </section>

    {_table_html(
        "Top Products",
        "Best performers from today's detailed sales rows.",
        [
            ("Product", "product"),
            ("Category", "category"),
            ("Net Sales", "sales"),
            ("Units", "units"),
            ("Tickets", "tickets"),
            ("Discounts", "discounts"),
        ],
        product_rows,
    )}

    <section class="layout">
      {_table_html(
          "Category Mix",
          "Which categories are driving today's dollars and unit movement.",
          [
              ("Category", "category"),
              ("Net Sales", "sales"),
              ("Units", "units"),
              ("Tickets", "tickets"),
          ],
          category_rows,
      )}
      {_table_html(
          "Budtender Leaders",
          "Transaction-level performance, ranked by sales total.",
          [
              ("Budtender", "budtender"),
              ("Sales", "sales"),
              ("Tickets", "tickets"),
              ("Avg Ticket", "avg_ticket"),
              ("Discounts", "discounts"),
          ],
          budtender_rows,
      )}
    </section>

    <section class="layout">
      {_table_html(
          "Vendor Leaders",
          "Today's strongest vendor movement from item-level detail.",
          [
              ("Vendor", "vendor"),
              ("Net Sales", "sales"),
              ("Units", "units"),
              ("Tickets", "tickets"),
          ],
          vendor_rows,
      )}
      {_table_html(
          "Channel Mix",
          "Order source and order type distribution for the current day.",
          [
              ("Order Source", "source"),
              ("Sales", "sales"),
              ("Tickets", "tickets"),
          ],
          source_rows,
      )}
    </section>

    {_table_html(
        "Order Type Mix",
        "How the day is split across in-store, pickup, and other order types.",
        [
            ("Order Type", "type"),
            ("Sales", "sales"),
            ("Tickets", "tickets"),
        ],
        order_type_rows,
    )}

    {_table_html(
        "Inventory Pressure",
        "Items selling today that could run tight based on current on-hand quantity.",
        [
            ("Store", "store"),
            ("Product", "product"),
            ("Available", "available"),
            ("Sold Today", "sold_today"),
            ("Days Left", "days_left"),
            ("Today's Sales", "sales"),
        ],
        low_stock_rows,
    )}

    {errors_html}
    <div class="footer-note">Generated from Dutchie POS API data for {escape(', '.join(report['selected_stores']))}.</div>
  </div>
</body>
</html>
"""


def _build_dashboard_report(
    snapshots: Sequence[StoreSnapshot],
    selected_stores: Sequence[str],
    timezone_name: str,
    errors: Sequence[str],
    now_local: datetime,
) -> dict[str, Any]:
    tx_today_frames = [snapshot.transactions_today for snapshot in snapshots if not snapshot.transactions_today.empty]
    item_today_frames = [snapshot.items_today for snapshot in snapshots if not snapshot.items_today.empty]
    tx_yesterday_frames = [snapshot.transactions_yesterday for snapshot in snapshots if not snapshot.transactions_yesterday.empty]
    item_yesterday_frames = [snapshot.items_yesterday for snapshot in snapshots if not snapshot.items_yesterday.empty]
    tx_last_week_frames = [snapshot.transactions_last_week for snapshot in snapshots if not snapshot.transactions_last_week.empty]
    item_last_week_frames = [snapshot.items_last_week for snapshot in snapshots if not snapshot.items_last_week.empty]

    tx_today = pd.concat(tx_today_frames, ignore_index=True) if tx_today_frames else pd.DataFrame()
    items_today = pd.concat(item_today_frames, ignore_index=True) if item_today_frames else pd.DataFrame()
    tx_yesterday = pd.concat(tx_yesterday_frames, ignore_index=True) if tx_yesterday_frames else pd.DataFrame()
    items_yesterday = pd.concat(item_yesterday_frames, ignore_index=True) if item_yesterday_frames else pd.DataFrame()
    tx_last_week = pd.concat(tx_last_week_frames, ignore_index=True) if tx_last_week_frames else pd.DataFrame()
    items_last_week = pd.concat(item_last_week_frames, ignore_index=True) if item_last_week_frames else pd.DataFrame()

    overview = _summarize_period(tx_today, items_today)
    yesterday = _summarize_period(tx_yesterday, items_yesterday)
    last_week = _summarize_period(tx_last_week, items_last_week)

    report = {
        "snapshots": list(snapshots),
        "selected_stores": list(selected_stores),
        "timezone": timezone_name,
        "generated_at": now_local,
        "now_local": now_local,
        "errors": list(errors),
        "overview": overview,
        "compare": {
            "yesterday": _compare_metrics(overview["sales_total"], yesterday["sales_total"]),
            "last_week": _compare_metrics(overview["sales_total"], last_week["sales_total"]),
        },
        "store_rows": _store_rows(snapshots),
        "hourly_points": _hours_for_chart(tx_today, now_local),
        "top_products": _group_top_products(items_today),
        "top_categories": _group_top_categories(items_today),
        "top_vendors": _group_top_vendors(items_today),
        "top_budtenders": _group_top_budtenders(tx_today),
        "source_mix": _group_source_mix(tx_today, "order_source"),
        "order_type_mix": _group_source_mix(tx_today, "order_type"),
        "low_stock_rows": _low_stock_rows(snapshots),
    }
    return report


def _fetch_store_snapshot(
    store_code: str,
    store_key: str,
    integrator_key: str,
    now_local: datetime,
    include_inventory: bool,
    products_cache: dict[str, Any],
) -> StoreSnapshot:
    session = create_session(store_key, integrator_key)
    try:
        products_payload = products_cache.get(store_code)
        if products_payload is None:
            products_payload = request_json(session, PRODUCTS_ENDPOINT)
            products_cache[store_code] = products_payload

        today_start, today_end = _window_bounds(now_local, 0)
        yesterday_start, yesterday_end = _window_bounds(now_local, 1)
        week_start, week_end = _window_bounds(now_local, 7)

        today_payload = request_json(session, TRANSACTIONS_ENDPOINT, params=_build_sales_params(today_start, today_end))
        yesterday_payload = request_json(
            session,
            TRANSACTIONS_ENDPOINT,
            params=_build_sales_params(yesterday_start, yesterday_end),
        )
        last_week_payload = request_json(session, TRANSACTIONS_ENDPOINT, params=_build_sales_params(week_start, week_end))
        inventory_payload = request_json(session, INVENTORY_ENDPOINT) if include_inventory else []

        tx_today, items_today = _normalize_transactions_api_sales_rows(today_payload, products_payload, store_code)
        tx_yesterday, items_yesterday = _normalize_transactions_api_sales_rows(yesterday_payload, products_payload, store_code)
        tx_last_week, items_last_week = _normalize_transactions_api_sales_rows(last_week_payload, products_payload, store_code)
        inventory_now = _normalize_inventory_api_rows(inventory_payload, store_code)

        return StoreSnapshot(
            store_code=store_code,
            store_name=_normalize_store_name(store_code),
            transactions_today=tx_today,
            items_today=items_today,
            transactions_yesterday=tx_yesterday,
            items_yesterday=items_yesterday,
            transactions_last_week=tx_last_week,
            items_last_week=items_last_week,
            inventory_now=inventory_now,
        )
    finally:
        session.close()


def _resolve_requested_stores(env_file: str, raw_store_args: list[str] | None) -> tuple[list[str], dict[str, str], str]:
    env_map = canonical_env_map(env_file)
    configured_store_codes = discover_configured_store_codes(env_map)
    requested_store_codes = parse_store_codes(raw_store_args) or configured_store_codes
    if not requested_store_codes:
        raise SystemExit(
            "No Dutchie stores were selected or discovered. Add store keys to .env or pass --stores mv lg lm wp sv nc."
        )

    store_keys = resolve_store_keys(env_map, requested_store_codes)
    missing = [code for code in requested_store_codes if code not in store_keys]
    if missing:
        raise SystemExit(
            "Missing Dutchie location key(s) for: "
            f"{', '.join(missing)}. Add them to {env_file} using names like DUTCHIE_API_KEY_MV or just MV."
        )

    return requested_store_codes, store_keys, resolve_integrator_key(env_map)


def _write_dashboard(output_path: Path, html: str) -> None:
    output_path.parent.mkdir(parents=True, exist_ok=True)
    output_path.write_text(html, encoding="utf-8")


def build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(description="Build a live same-day Dutchie dashboard as a local HTML file.")
    parser.add_argument(
        "--env-file",
        default=DEFAULT_ENV_FILE,
        help=f"Path to the .env file. Default: {DEFAULT_ENV_FILE}",
    )
    parser.add_argument(
        "--stores",
        nargs="*",
        help="Store codes to include, for example: mv lg lm wp sv nc",
    )
    parser.add_argument(
        "--timezone",
        default=DEFAULT_TIMEZONE,
        help=f"Timezone for today's window and comparisons. Default: {DEFAULT_TIMEZONE}",
    )
    parser.add_argument(
        "--output",
        default=str(DEFAULT_OUTPUT_PATH),
        help=f"Where to write the HTML dashboard. Default: {DEFAULT_OUTPUT_PATH}",
    )
    parser.add_argument(
        "--watch",
        action="store_true",
        help="Keep refreshing the dashboard on an interval.",
    )
    parser.add_argument(
        "--refresh-seconds",
        type=int,
        default=DEFAULT_REFRESH_SECONDS,
        help=f"Refresh interval for --watch mode. Default: {DEFAULT_REFRESH_SECONDS}",
    )
    parser.add_argument(
        "--open",
        action="store_true",
        help="Open the dashboard in your default browser after the first render.",
    )
    parser.add_argument(
        "--no-inventory",
        action="store_true",
        help="Skip the inventory API call if you only want sales-side live metrics.",
    )
    return parser


def run_dashboard(args: argparse.Namespace) -> int:
    requested_store_codes, store_keys, integrator_key = _resolve_requested_stores(args.env_file, args.stores)
    timezone_name = args.timezone
    tz = ZoneInfo(timezone_name)
    output_path = Path(args.output)
    include_inventory = not args.no_inventory
    products_cache: dict[str, Any] = {}
    opened = False

    while True:
        now_local = datetime.now(tz)
        snapshots: list[StoreSnapshot] = []
        errors: list[str] = []

        for store_code in requested_store_codes:
            try:
                snapshot = _fetch_store_snapshot(
                    store_code=store_code,
                    store_key=store_keys[store_code],
                    integrator_key=integrator_key,
                    now_local=now_local,
                    include_inventory=include_inventory,
                    products_cache=products_cache,
                )
                snapshots.append(snapshot)
                overview = _summarize_period(snapshot.transactions_today, snapshot.items_today)
                print(
                    f"[LIVE] {store_code}: {_fmt_money(overview['sales_total'])} | "
                    f"{overview['tickets']} tickets | last {_fmt_time(overview['last_ticket_time'])}"
                )
            except Exception as exc:
                errors.append(f"{store_code}: {exc}")
                print(f"[WARN] {store_code} failed: {exc}")

        report = _build_dashboard_report(
            snapshots=snapshots,
            selected_stores=requested_store_codes,
            timezone_name=timezone_name,
            errors=errors,
            now_local=now_local,
        )
        html = _render_dashboard_html(
            report=report,
            output_path=output_path,
            auto_refresh_seconds=args.refresh_seconds if args.watch else 0,
        )
        _write_dashboard(output_path, html)
        print(f"[WRITE] {output_path}")

        if args.open and not opened:
            webbrowser.open(output_path.resolve().as_uri())
            opened = True

        if not args.watch:
            break

        time.sleep(max(5, int(args.refresh_seconds)))

    return 0


def main() -> int:
    parser = build_parser()
    args = parser.parse_args()
    try:
        return run_dashboard(args)
    except (ValueError, requests.RequestException) as exc:
        parser.error(str(exc))
        return 2


if __name__ == "__main__":
    raise SystemExit(main())
