#!/usr/bin/env python3
from __future__ import annotations

import argparse
import queue
import threading
import time
import tkinter as tk
from dataclasses import dataclass
from datetime import date, datetime
from pathlib import Path
from tkinter import font as tkfont
from tkinter import messagebox, ttk
from typing import Any, Callable, Dict, List, Optional, Sequence

import pandas as pd
from matplotlib.backends.backend_tkagg import FigureCanvasTkAgg
from matplotlib.figure import Figure
from matplotlib.ticker import FuncFormatter

import owner_snapshot as osnap
from dutchie_api_reports import (
    DEFAULT_ENV_FILE,
    STORE_CODES,
    canonical_env_map,
    discover_configured_store_codes,
)
from dutchie_today_dashboard import (
    StoreSnapshot,
    _compare_metrics,
    _fetch_store_snapshot,
    _low_stock_rows,
    _resolve_requested_stores,
    _summarize_period,
)


FLAT_SALES_COLUMNS = [
    "Order ID",
    "Order Time",
    "Budtender Name",
    "Customer Type",
    "Vendor Name",
    "Product Name",
    "Category",
    "Major Category",
    "Package ID",
    "Batch ID",
    "External Package ID",
    "Total Inventory Sold",
    "Unit Weight Sold",
    "Total Weight Sold",
    "Gross Sales",
    "Inventory Cost",
    "Discounted Amount",
    "Loyalty as Discount",
    "Net Sales",
    "Return Date",
    "Producer",
    "Order Profit",
    "Unit Price",
    "Price",
    "Location price",
    "Store",
    "Store Code",
    "SKU",
]

INVENTORY_COLUMNS = [
    "Store",
    "Store Code",
    "Product Name",
    "Brand",
    "Category",
    "Vendor",
    "Available",
    "Unit Cost",
    "Unit Price",
    "Inventory Value",
    "Revenue Potential",
    "Potential Profit",
    "SKU",
]


@dataclass
class DashboardBundle:
    now_local: datetime
    selected_stores: list[str]
    snapshots: list[StoreSnapshot]
    errors: list[str]
    today_flat: pd.DataFrame
    yesterday_flat: pd.DataFrame
    last_week_flat: pd.DataFrame
    per_store_today: dict[str, pd.DataFrame]
    overview_metrics: dict[str, float]
    compare_yesterday: dict[str, Any]
    compare_last_week: dict[str, Any]
    store_summary: pd.DataFrame
    pace_summary: pd.DataFrame
    operational_watch: pd.DataFrame
    hourly_all: pd.DataFrame
    product_net: pd.DataFrame
    product_units: pd.DataFrame
    brand_summary: pd.DataFrame
    category_summary: pd.DataFrame
    vendor_summary: pd.DataFrame
    budtender_summary: pd.DataFrame
    customer_type_summary: pd.DataFrame
    cart_distribution: pd.DataFrame
    source_mix: pd.DataFrame
    order_type_mix: pd.DataFrame
    low_stock: pd.DataFrame
    inventory_category: pd.DataFrame
    inventory_brand: pd.DataFrame
    inventory_product: pd.DataFrame
    story_lines: list[str]


def _zero_metrics() -> dict[str, float]:
    return {key: 0.0 for key in osnap.METRIC_KEYS}


def _to_float(value: Any) -> float:
    try:
        if value in (None, "", "None"):
            return 0.0
        return float(value)
    except Exception:
        return 0.0


def _fmt_money(value: Any) -> str:
    return f"${_to_float(value):,.2f}"


def _fmt_int(value: Any) -> str:
    return f"{int(round(_to_float(value))):,}"


def _fmt_decimal(value: Any) -> str:
    return f"{_to_float(value):,.1f}"


def _fmt_pct(value: Any) -> str:
    if value is None:
        return "n/a"
    try:
        value_f = float(value)
    except Exception:
        return "n/a"
    return f"{value_f * 100:,.1f}%"


def _fmt_time(value: Any) -> str:
    ts = pd.to_datetime(value, errors="coerce")
    if pd.isna(ts):
        return "n/a"
    return ts.strftime("%-I:%M %p")


def _fmt_delta(metrics: dict[str, Any]) -> str:
    delta = _to_float(metrics.get("delta"))
    pct = metrics.get("pct")
    pct_txt = "n/a" if pct is None else f"{abs(_to_float(pct)) * 100:,.1f}%"
    sign = "+" if delta >= 0 else "-"
    return f"{sign}{_fmt_money(abs(delta))} ({pct_txt})"


def _short_label(value: Any, max_len: int = 22) -> str:
    text = str(value or "").strip()
    if len(text) <= max_len:
        return text
    return text[: max_len - 1].rstrip() + "…"


def _combine_snapshot_attr(snapshots: Sequence[StoreSnapshot], attr_name: str) -> pd.DataFrame:
    frames = [getattr(snapshot, attr_name) for snapshot in snapshots if not getattr(snapshot, attr_name).empty]
    if not frames:
        return pd.DataFrame()
    return pd.concat(frames, ignore_index=True)


def _items_df_to_flat_sales(items_df: pd.DataFrame) -> pd.DataFrame:
    if items_df.empty:
        return pd.DataFrame(columns=FLAT_SALES_COLUMNS)

    flat = pd.DataFrame(
        {
            "Order ID": items_df.get("transaction_id", pd.Series("", index=items_df.index)).astype(str),
            "Order Time": pd.to_datetime(items_df.get("order_time"), errors="coerce"),
            "Budtender Name": items_df.get("budtender", pd.Series("", index=items_df.index)).fillna("").astype(str),
            "Customer Type": items_df.get("customer_type", pd.Series("", index=items_df.index)).fillna("").astype(str),
            "Vendor Name": items_df.get("vendor_name", pd.Series("", index=items_df.index)).fillna("").astype(str),
            "Product Name": items_df.get("product_name", pd.Series("", index=items_df.index)).fillna("").astype(str),
            "Category": items_df.get("category", pd.Series("Unknown", index=items_df.index)).fillna("Unknown").astype(str),
            "Major Category": items_df.get("category", pd.Series("Unknown", index=items_df.index)).fillna("Unknown").astype(str),
            "Package ID": items_df.get("package_id", pd.Series("", index=items_df.index)).fillna("").astype(str),
            "Batch ID": items_df.get("package_id", pd.Series("", index=items_df.index)).fillna("").astype(str),
            "External Package ID": items_df.get("package_id", pd.Series("", index=items_df.index)).fillna("").astype(str),
            "Total Inventory Sold": items_df.get("quantity", pd.Series(0.0, index=items_df.index)).fillna(0.0).astype(float),
            "Unit Weight Sold": items_df.get("unit_weight", pd.Series(0.0, index=items_df.index)).fillna(0.0).astype(float),
            "Total Weight Sold": items_df.get("total_weight", pd.Series(0.0, index=items_df.index)).fillna(0.0).astype(float),
            "Gross Sales": items_df.get("gross_sales", pd.Series(0.0, index=items_df.index)).fillna(0.0).astype(float),
            "Inventory Cost": items_df.get("inventory_cost", pd.Series(0.0, index=items_df.index)).fillna(0.0).astype(float),
            "Discounted Amount": items_df.get("discount_amount", pd.Series(0.0, index=items_df.index)).fillna(0.0).astype(float),
            "Loyalty as Discount": 0.0,
            "Net Sales": items_df.get("net_sales", pd.Series(0.0, index=items_df.index)).fillna(0.0).astype(float),
            "Return Date": pd.to_datetime(items_df.get("order_time"), errors="coerce").where(
                items_df.get("is_return", pd.Series(False, index=items_df.index)).fillna(False)
            ),
            "Producer": items_df.get("vendor_name", pd.Series("", index=items_df.index)).fillna("").astype(str),
            "Order Profit": items_df.get("order_profit", pd.Series(0.0, index=items_df.index)).fillna(0.0).astype(float),
            "Unit Price": items_df.get("unit_price", pd.Series(0.0, index=items_df.index)).fillna(0.0).astype(float),
            "Price": items_df.get("unit_price", pd.Series(0.0, index=items_df.index)).fillna(0.0).astype(float),
            "Location price": items_df.get("unit_price", pd.Series(0.0, index=items_df.index)).fillna(0.0).astype(float),
            "Store": items_df.get("store_name", pd.Series("", index=items_df.index)).fillna("").astype(str),
            "Store Code": items_df.get("store_code", pd.Series("", index=items_df.index)).fillna("").astype(str),
            "SKU": items_df.get("sku", pd.Series("", index=items_df.index)).fillna("").astype(str),
        }
    )
    return flat[FLAT_SALES_COLUMNS].copy()


def _inventory_df_to_reference(inventory_df: pd.DataFrame) -> pd.DataFrame:
    if inventory_df.empty:
        return pd.DataFrame(columns=INVENTORY_COLUMNS)

    ref = pd.DataFrame(
        {
            "Store": inventory_df.get("store_name", pd.Series("", index=inventory_df.index)).fillna("").astype(str),
            "Store Code": inventory_df.get("store_code", pd.Series("", index=inventory_df.index)).fillna("").astype(str),
            "Product Name": inventory_df.get("product_name", pd.Series("", index=inventory_df.index)).fillna("").astype(str),
            "Brand": inventory_df.get("brand_name", pd.Series("", index=inventory_df.index)).fillna("").astype(str),
            "Category": inventory_df.get("category", pd.Series("Unknown", index=inventory_df.index)).fillna("Unknown").astype(str),
            "Vendor": inventory_df.get("vendor_name", pd.Series("", index=inventory_df.index)).fillna("").astype(str),
            "Available": inventory_df.get("available", pd.Series(0.0, index=inventory_df.index)).fillna(0.0).astype(float),
            "Unit Cost": inventory_df.get("unit_cost", pd.Series(0.0, index=inventory_df.index)).fillna(0.0).astype(float),
            "Unit Price": inventory_df.get("unit_price", pd.Series(0.0, index=inventory_df.index)).fillna(0.0).astype(float),
            "SKU": inventory_df.get("sku", pd.Series("", index=inventory_df.index)).fillna("").astype(str),
        }
    )
    ref["Inventory Value"] = ref["Available"] * ref["Unit Cost"]
    ref["Revenue Potential"] = ref["Available"] * ref["Unit Price"]
    ref["Potential Profit"] = ref["Revenue Potential"] - ref["Inventory Value"]
    return ref[INVENTORY_COLUMNS].copy()


def _safe_optional_df(df: Optional[pd.DataFrame], columns: Sequence[str]) -> pd.DataFrame:
    if df is None or df.empty:
        return pd.DataFrame(columns=list(columns))
    out = df.copy()
    for column in columns:
        if column not in out.columns:
            out[column] = ""
    return out


def _build_mix_summary(transactions_df: pd.DataFrame, column: str, label: str) -> pd.DataFrame:
    if transactions_df.empty or column not in transactions_df.columns:
        return pd.DataFrame(columns=[label, "net_revenue", "tickets"])
    tmp = transactions_df.copy()
    tmp[column] = tmp[column].fillna("Unknown").astype(str)
    out = (
        tmp.groupby(column, dropna=False)
        .agg(net_revenue=("total", "sum"), tickets=("transaction_id", "nunique"))
        .reset_index()
        .rename(columns={column: label})
        .sort_values("net_revenue", ascending=False)
    )
    return out


def _build_store_summary(snapshots: Sequence[StoreSnapshot], now_local: datetime) -> pd.DataFrame:
    rows: list[dict[str, Any]] = []
    for snapshot in snapshots:
        today = _summarize_period(snapshot.transactions_today, snapshot.items_today)
        yesterday = _summarize_period(snapshot.transactions_yesterday, snapshot.items_yesterday)
        last_week = _summarize_period(snapshot.transactions_last_week, snapshot.items_last_week)
        vs_yesterday = _compare_metrics(today["sales_total"], yesterday["sales_total"])
        vs_last_week = _compare_metrics(today["sales_total"], last_week["sales_total"])
        mins_since_value = _minutes_since_value(today["last_ticket_time"], now_local)
        rows.append(
            {
                "store_code": snapshot.store_code,
                "store_name": snapshot.store_name,
                "net_revenue": today["sales_total"],
                "tickets": today["tickets"],
                "units": today["units"],
                "avg_ticket": today["avg_ticket"],
                "margin_real": today["margin_estimate"],
                "last_ticket": today["last_ticket_time"],
                "vs_yesterday": _fmt_delta(vs_yesterday),
                "vs_last_week": _fmt_delta(vs_last_week),
                "vs_yesterday_delta": _to_float(vs_yesterday.get("delta")),
                "vs_yesterday_pct": vs_yesterday.get("pct"),
                "vs_last_week_delta": _to_float(vs_last_week.get("delta")),
                "vs_last_week_pct": vs_last_week.get("pct"),
                "minutes_since_ticket": "n/a" if mins_since_value is None else f"{mins_since_value} min ago",
                "minutes_since_ticket_value": mins_since_value,
            }
        )
    if not rows:
        return pd.DataFrame(
            columns=[
                "store_code",
                "store_name",
                "net_revenue",
                "tickets",
                "units",
                "avg_ticket",
                "margin_real",
                "last_ticket",
                "vs_yesterday",
                "vs_last_week",
                "vs_yesterday_delta",
                "vs_yesterday_pct",
                "vs_last_week_delta",
                "vs_last_week_pct",
                "minutes_since_ticket",
                "minutes_since_ticket_value",
            ]
        )
    return pd.DataFrame(rows).sort_values("net_revenue", ascending=False)


def _minutes_since_value(last_ticket_time: Any, now_local: datetime) -> Optional[int]:
    ts = pd.to_datetime(last_ticket_time, errors="coerce")
    if pd.isna(ts):
        return None
    ts_py = ts.to_pydatetime()
    if ts_py.tzinfo is None and now_local.tzinfo is not None:
        ts_py = ts_py.replace(tzinfo=now_local.tzinfo)
    elif ts_py.tzinfo is not None and now_local.tzinfo is not None:
        ts_py = ts_py.astimezone(now_local.tzinfo)
    delta = now_local - ts_py
    return max(0, int(delta.total_seconds() // 60))


def _minutes_since(last_ticket_time: Any, now_local: datetime) -> str:
    mins = _minutes_since_value(last_ticket_time, now_local)
    if mins is None:
        return "n/a"
    return f"{mins} min ago"


def _hourly_period_rollup(flat_sales_df: pd.DataFrame) -> pd.DataFrame:
    base = pd.DataFrame({"hour": list(range(24))})
    if flat_sales_df is None or flat_sales_df.empty:
        base["net_revenue"] = 0.0
        base["tickets"] = 0.0
        base["basket"] = 0.0
        return base

    tmp = flat_sales_df.copy()
    tmp["Order Time"] = pd.to_datetime(tmp.get("Order Time"), errors="coerce")
    tmp = tmp[tmp["Order Time"].notna()]
    if tmp.empty:
        base["net_revenue"] = 0.0
        base["tickets"] = 0.0
        base["basket"] = 0.0
        return base

    tmp["hour"] = tmp["Order Time"].dt.hour.astype(int)
    grouped = (
        tmp.groupby("hour", dropna=False)
        .agg(
            net_revenue=("Net Sales", "sum"),
            tickets=("Order ID", "nunique"),
        )
        .reset_index()
    )
    grouped["basket"] = grouped["net_revenue"] / grouped["tickets"].replace({0: None})
    grouped["basket"] = grouped["basket"].fillna(0.0)

    out = base.merge(grouped, on="hour", how="left")
    for column in ("net_revenue", "tickets", "basket"):
        out[column] = out[column].fillna(0.0)
    return out


def _build_pace_summary(
    today_flat: pd.DataFrame,
    yesterday_flat: pd.DataFrame,
    last_week_flat: pd.DataFrame,
    now_local: datetime,
) -> pd.DataFrame:
    today_hourly = _hourly_period_rollup(today_flat).rename(
        columns={"net_revenue": "today_net", "tickets": "today_tickets", "basket": "today_basket"}
    )
    yesterday_hourly = _hourly_period_rollup(yesterday_flat).rename(
        columns={"net_revenue": "yesterday_net", "tickets": "yesterday_tickets", "basket": "yesterday_basket"}
    )
    last_week_hourly = _hourly_period_rollup(last_week_flat).rename(
        columns={"net_revenue": "last_week_net", "tickets": "last_week_tickets", "basket": "last_week_basket"}
    )

    pace = today_hourly.merge(yesterday_hourly, on="hour", how="left").merge(last_week_hourly, on="hour", how="left")
    pace["hour_label"] = pace["hour"].apply(lambda hour: osnap.fmt_hour_ampm(int(hour)))
    pace["today_cum"] = pace["today_net"].cumsum()
    pace["yesterday_cum"] = pace["yesterday_net"].cumsum()
    pace["last_week_cum"] = pace["last_week_net"].cumsum()

    future_mask = pace["hour"] > int(now_local.hour)
    for column in ("today_net", "today_tickets", "today_basket", "today_cum"):
        pace.loc[future_mask, column] = float("nan")
    return pace


def _build_operational_watch(
    store_summary: pd.DataFrame,
    low_stock: pd.DataFrame,
    compare_yesterday: dict[str, Any],
    compare_last_week: dict[str, Any],
) -> pd.DataFrame:
    rows: list[dict[str, str]] = []

    rows.append(
        {
            "priority": "Pace",
            "area": "Company",
            "detail": "Same-time pace vs yesterday",
            "impact": _fmt_delta(compare_yesterday),
        }
    )
    rows.append(
        {
            "priority": "Pace",
            "area": "Company",
            "detail": "Same-time pace vs last week",
            "impact": _fmt_delta(compare_last_week),
        }
    )

    if not store_summary.empty:
        leader = store_summary.iloc[0]
        rows.append(
            {
                "priority": "Leader",
                "area": str(leader["store_code"]).upper(),
                "detail": "Top store by same-day net revenue",
                "impact": _fmt_money(leader["net_revenue"]),
            }
        )

        lagging = store_summary.sort_values("vs_last_week_delta", ascending=True).iloc[0]
        if _to_float(lagging.get("vs_last_week_delta")) < 0:
            rows.append(
                {
                    "priority": "Gap",
                    "area": str(lagging["store_code"]).upper(),
                    "detail": "Largest pace gap vs last week",
                    "impact": _fmt_delta(
                        {"delta": lagging["vs_last_week_delta"], "pct": lagging.get("vs_last_week_pct")}
                    ),
                }
            )

        stalled = (
            store_summary[store_summary["minutes_since_ticket_value"].notna()]
            .sort_values("minutes_since_ticket_value", ascending=False)
            .head(2)
        )
        for _, row in stalled.iterrows():
            mins = int(_to_float(row.get("minutes_since_ticket_value")))
            if mins >= 35:
                rows.append(
                    {
                        "priority": "Stale",
                        "area": str(row["store_code"]).upper(),
                        "detail": f"No ticket for {mins} minutes",
                        "impact": _fmt_money(row["net_revenue"]),
                    }
                )

    if low_stock is not None and not low_stock.empty:
        for _, row in low_stock.head(2).iterrows():
            rows.append(
                {
                    "priority": "Stock",
                    "area": str(row.get("store_code", "")).upper(),
                    "detail": f"{_short_label(row.get('product_name', ''), 30)} running low",
                    "impact": f"{_fmt_decimal(row.get('available'))} left | {_fmt_money(row.get('revenue_today'))}",
                }
            )

    if not rows:
        return pd.DataFrame(columns=["priority", "area", "detail", "impact"])
    return pd.DataFrame(rows[:8], columns=["priority", "area", "detail", "impact"])


def _build_inventory_rollups(inventory_ref: pd.DataFrame) -> tuple[pd.DataFrame, pd.DataFrame, pd.DataFrame]:
    if inventory_ref.empty:
        empty_category = pd.DataFrame(columns=["Category", "Available Units", "Inventory Value", "Revenue Potential", "Potential Profit", "SKU Count"])
        empty_brand = pd.DataFrame(columns=["Brand", "Available Units", "Inventory Value", "Revenue Potential", "Potential Profit", "SKU Count"])
        empty_product = pd.DataFrame(columns=["Store Code", "Product Name", "Brand", "Available", "Inventory Value", "Revenue Potential", "Potential Profit"])
        return empty_category, empty_brand, empty_product

    category_df = (
        inventory_ref.groupby("Category", dropna=False)
        .agg(
            **{
                "Available Units": ("Available", "sum"),
                "Inventory Value": ("Inventory Value", "sum"),
                "Revenue Potential": ("Revenue Potential", "sum"),
                "Potential Profit": ("Potential Profit", "sum"),
                "SKU Count": ("SKU", "nunique"),
            }
        )
        .reset_index()
        .sort_values("Inventory Value", ascending=False)
    )

    brand_df = (
        inventory_ref.groupby("Brand", dropna=False)
        .agg(
            **{
                "Available Units": ("Available", "sum"),
                "Inventory Value": ("Inventory Value", "sum"),
                "Revenue Potential": ("Revenue Potential", "sum"),
                "Potential Profit": ("Potential Profit", "sum"),
                "SKU Count": ("SKU", "nunique"),
            }
        )
        .reset_index()
        .sort_values("Inventory Value", ascending=False)
    )

    product_df = inventory_ref.sort_values(["Inventory Value", "Available"], ascending=[False, False]).copy()
    product_df = product_df[
        ["Store Code", "Product Name", "Brand", "Available", "Inventory Value", "Revenue Potential", "Potential Profit"]
    ].head(40)

    return category_df, brand_df, product_df


def _build_story_lines(
    store_summary: pd.DataFrame,
    product_net: pd.DataFrame,
    category_summary: pd.DataFrame,
    low_stock: pd.DataFrame,
    compare_last_week: dict[str, Any],
) -> list[str]:
    lines: list[str] = []
    if not store_summary.empty:
        leader = store_summary.iloc[0]
        lines.append(f"{leader['store_code']} is leading today at {_fmt_money(leader['net_revenue'])}.")
    if not product_net.empty:
        top_product = product_net.iloc[0]
        lines.append(f"Top product right now is {top_product['Product Name']} at {_fmt_money(top_product['net_revenue'])}.")
    if not category_summary.empty:
        top_category = category_summary.iloc[0]
        lines.append(f"{top_category['category']} is the top category at {_fmt_money(top_category['net_revenue'])}.")
    if not low_stock.empty:
        top_alert = low_stock.iloc[0]
        lines.append(
            f"Low-stock watch: {top_alert['store_code']} has {top_alert['product_name']} down to {_fmt_decimal(top_alert['available'])} units."
        )
    if not lines:
        lines.append("No live sales data has landed yet for the selected stores.")
    lines.append(f"Same-time pace vs last week: {_fmt_delta(compare_last_week)}.")
    return lines[:5]


def build_dashboard_bundle(
    snapshots: Sequence[StoreSnapshot],
    selected_stores: Sequence[str],
    now_local: datetime,
    errors: Sequence[str],
) -> DashboardBundle:
    day = now_local.date()
    per_store_today = {snapshot.store_code: _items_df_to_flat_sales(snapshot.items_today) for snapshot in snapshots}
    today_frames = [df for df in per_store_today.values() if not df.empty]
    today_flat = pd.concat(today_frames, ignore_index=True) if today_frames else pd.DataFrame(columns=FLAT_SALES_COLUMNS)

    yesterday_frames = [_items_df_to_flat_sales(snapshot.items_yesterday) for snapshot in snapshots if not snapshot.items_yesterday.empty]
    last_week_frames = [_items_df_to_flat_sales(snapshot.items_last_week) for snapshot in snapshots if not snapshot.items_last_week.empty]
    yesterday_flat = pd.concat(yesterday_frames, ignore_index=True) if yesterday_frames else pd.DataFrame(columns=FLAT_SALES_COLUMNS)
    last_week_flat = pd.concat(last_week_frames, ignore_index=True) if last_week_frames else pd.DataFrame(columns=FLAT_SALES_COLUMNS)

    if today_flat.empty:
        overview_metrics = _zero_metrics()
        hourly_all = pd.DataFrame(columns=["hour", "net_revenue", "profit", "profit_real", "tickets", "basket", "margin", "margin_real", "hour_label"])
        product_net = pd.DataFrame(columns=["Product Name", "net_revenue"])
        product_units = pd.DataFrame(columns=["Product Name", "units_sold"])
        brand_summary = pd.DataFrame(columns=["brand", "net_revenue", "profit", "profit_real", "margin", "margin_real"])
        category_summary = pd.DataFrame(columns=["category", "net_revenue", "profit", "profit_real", "margin", "margin_real", "items", "discount_rate"])
        vendor_summary = pd.DataFrame(columns=["Vendor Name", "net_revenue"])
        budtender_summary = pd.DataFrame(columns=["budtender", "net_revenue", "tickets", "basket", "discount_rate"])
        customer_type_summary = pd.DataFrame(columns=["customer_type", "net_revenue", "tickets", "basket"])
        cart_distribution = pd.DataFrame(columns=["bucket", "count", "pct"])
    else:
        daily = osnap.compute_daily_metrics(today_flat)
        overview_metrics = osnap.metrics_for_day(daily, day)
        hourly_all = _safe_optional_df(
            osnap.compute_hourly_metrics(today_flat, day),
            ["hour", "net_revenue", "profit", "profit_real", "tickets", "basket", "margin", "margin_real"],
        )
        if not hourly_all.empty:
            hourly_all["hour_label"] = hourly_all["hour"].apply(lambda hour: osnap.fmt_hour_ampm(int(hour)))

        product_net = _safe_optional_df(
            osnap.compute_breakdown_net(today_flat, ["Product Name"], day, day, top_n=30),
            ["Product Name", "net_revenue"],
        )
        product_units = _safe_optional_df(
            osnap.compute_breakdown_units(today_flat, ["Product Name"], day, day, top_n=30),
            ["Product Name", "units_sold"],
        )
        brand_summary = _safe_optional_df(
            osnap.compute_brand_summary(today_flat, day, day, top_n=25),
            ["brand", "net_revenue", "profit", "profit_real", "margin", "margin_real"],
        )
        category_summary = _safe_optional_df(
            osnap.compute_category_summary(today_flat, day, day),
            ["category", "net_revenue", "profit", "profit_real", "margin", "margin_real", "items", "discount_rate"],
        )
        vendor_summary = _safe_optional_df(
            osnap.compute_breakdown_net(today_flat, ["Vendor Name"], day, day, top_n=25),
            ["Vendor Name", "net_revenue"],
        )
        budtender_summary = _safe_optional_df(
            osnap.compute_budtender_summary(today_flat, day, day),
            ["budtender", "net_revenue", "tickets", "basket", "discount_rate"],
        )
        customer_type_summary = _safe_optional_df(
            osnap.compute_customer_type_summary(today_flat, day, day),
            ["Customer Type", "net_revenue", "tickets", "basket"],
        )
        if "Customer Type" in customer_type_summary.columns:
            customer_type_summary = customer_type_summary.rename(columns={"Customer Type": "customer_type"})
        cart_distribution = _safe_optional_df(
            osnap.compute_cart_value_distribution(today_flat, day, day),
            ["bucket", "count", "pct"],
        )

    transactions_today = _combine_snapshot_attr(snapshots, "transactions_today")
    source_mix = _build_mix_summary(transactions_today, "order_source", "order_source")
    order_type_mix = _build_mix_summary(transactions_today, "order_type", "order_type")

    today_summary = _summarize_period(
        _combine_snapshot_attr(snapshots, "transactions_today"),
        _combine_snapshot_attr(snapshots, "items_today"),
    )
    yesterday_summary = _summarize_period(
        _combine_snapshot_attr(snapshots, "transactions_yesterday"),
        _combine_snapshot_attr(snapshots, "items_yesterday"),
    )
    last_week_summary = _summarize_period(
        _combine_snapshot_attr(snapshots, "transactions_last_week"),
        _combine_snapshot_attr(snapshots, "items_last_week"),
    )

    compare_yesterday = _compare_metrics(today_summary["sales_total"], yesterday_summary["sales_total"])
    compare_last_week = _compare_metrics(today_summary["sales_total"], last_week_summary["sales_total"])

    inventory_frames = [_inventory_df_to_reference(snapshot.inventory_now) for snapshot in snapshots if not snapshot.inventory_now.empty]
    inventory_ref = pd.concat(inventory_frames, ignore_index=True) if inventory_frames else pd.DataFrame(columns=INVENTORY_COLUMNS)
    inventory_category, inventory_brand, inventory_product = _build_inventory_rollups(inventory_ref)

    low_stock = pd.DataFrame(_low_stock_rows(snapshots, limit=60))
    store_summary = _build_store_summary(snapshots, now_local)
    pace_summary = _build_pace_summary(today_flat, yesterday_flat, last_week_flat, now_local)
    operational_watch = _build_operational_watch(store_summary, low_stock, compare_yesterday, compare_last_week)
    story_lines = _build_story_lines(store_summary, product_net, category_summary, low_stock, compare_last_week)

    return DashboardBundle(
        now_local=now_local,
        selected_stores=list(selected_stores),
        snapshots=list(snapshots),
        errors=list(errors),
        today_flat=today_flat,
        yesterday_flat=yesterday_flat,
        last_week_flat=last_week_flat,
        per_store_today=per_store_today,
        overview_metrics=overview_metrics,
        compare_yesterday=compare_yesterday,
        compare_last_week=compare_last_week,
        store_summary=store_summary,
        pace_summary=pace_summary,
        operational_watch=operational_watch,
        hourly_all=hourly_all,
        product_net=product_net,
        product_units=product_units,
        brand_summary=brand_summary,
        category_summary=category_summary,
        vendor_summary=vendor_summary,
        budtender_summary=budtender_summary,
        customer_type_summary=customer_type_summary,
        cart_distribution=cart_distribution,
        source_mix=source_mix,
        order_type_mix=order_type_mix,
        low_stock=low_stock,
        inventory_category=inventory_category,
        inventory_brand=inventory_brand,
        inventory_product=inventory_product,
        story_lines=story_lines,
    )


class DutchieLiveDashboardGUI:
    def __init__(self, root: tk.Tk, env_file: str = DEFAULT_ENV_FILE, default_refresh_seconds: int = 120, default_no_inventory: bool = False):
        self.root = root
        self.root.title("Dutchie Live Command Center")
        self.root.geometry("1760x1000")
        self.root.minsize(1180, 760)
        self.root.maxsize(1880, 1040)
        self.env_file = env_file
        self.log_queue: "queue.Queue[tuple[str, object]]" = queue.Queue()
        self.worker_running = False
        self.auto_refresh_job: Optional[str] = None
        self.max_log_rows = 320
        self.current_bundle: Optional[DashboardBundle] = None
        self.chart_canvases: dict[str, FigureCanvasTkAgg] = {}
        self.table_views: dict[str, ttk.Treeview] = {}
        self.table_columns: dict[str, list[str]] = {}
        self.chart_frames: dict[str, tk.Frame] = {}
        self.dynamic_store_pages: dict[str, tk.Frame] = {}
        self.scroll_canvases: dict[str, tk.Canvas] = {}

        self.colors = {
            "bg": "#FFFFFF",
            "card": "#FFFFFF",
            "card_alt": "#FFFFFF",
            "hero": "#000000",
            "hero_chip": "#FFFFFF",
            "hero_border": "#000000",
            "border": "#000000",
            "text": "#000000",
            "muted": "#000000",
            "accent": "#000000",
            "accent_dark": "#000000",
            "ghost": "#FFFFFF",
            "ghost_dark": "#FFFFFF",
            "success": "#000000",
            "warn": "#000000",
            "error": "#000000",
            "log_bg": "#FFFFFF",
            "input_bg": "#FFFFFF",
            "canvas": "#FFFFFF",
        }

        self.status_var = tk.StringVar(value="Ready")
        self.last_refresh_var = tk.StringVar(value="Not refreshed yet")
        self.loaded_store_var = tk.StringVar(value="0 stores loaded")
        self.mode_var = tk.StringVar(value=f"Refresh every {default_refresh_seconds}s")
        self.story_var = tk.StringVar(value="Refresh to load today's sales and store updates.")
        self.summary_chip_var = tk.StringVar(value="Today's live sales board")
        self.clock_var = tk.StringVar(value="")
        self.refresh_age_var = tk.StringVar(value="Refresh age: waiting")
        self.selected_stores_var = tk.StringVar(value="Watching stores")

        self.refresh_seconds_var = tk.StringVar(value=str(default_refresh_seconds))
        self.auto_refresh_var = tk.BooleanVar(value=True)
        self.no_inventory_var = tk.BooleanVar(value=default_no_inventory)
        self.pin_window_var = tk.BooleanVar(value=False)

        env_map = canonical_env_map(self.env_file)
        self.configured_store_codes = discover_configured_store_codes(env_map)
        if not self.configured_store_codes:
            self.configured_store_codes = list(STORE_CODES)

        self.store_vars: dict[str, tk.BooleanVar] = {}
        for code in STORE_CODES:
            self.store_vars[code] = tk.BooleanVar(value=(code in self.configured_store_codes))

        self.kpi_vars = {
            "net_revenue": tk.StringVar(value="$0.00"),
            "gross_sales": tk.StringVar(value="$0.00"),
            "tickets": tk.StringVar(value="0"),
            "basket": tk.StringVar(value="$0.00"),
            "items": tk.StringVar(value="0"),
            "profit_real": tk.StringVar(value="$0.00"),
            "margin_real": tk.StringVar(value="0.0%"),
            "discount": tk.StringVar(value="$0.00"),
            "discount_rate": tk.StringVar(value="0.0%"),
            "returns_net": tk.StringVar(value="$0.00"),
            "last_ticket": tk.StringVar(value="n/a"),
            "compare_yesterday": tk.StringVar(value="n/a"),
            "compare_last_week": tk.StringVar(value="n/a"),
        }

        self._configure_theme()
        self._build_ui()
        self._update_selected_stores_summary()
        self._apply_window_pin()
        self._tick_clock()
        self.root.bind("<Control-r>", lambda _event: self.refresh_now())
        self.root.bind("<F5>", lambda _event: self.refresh_now())
        self.root.bind_all("<MouseWheel>", self._on_global_mousewheel, add="+")
        self.root.bind_all("<Button-4>", self._on_global_mousewheel, add="+")
        self.root.bind_all("<Button-5>", self._on_global_mousewheel, add="+")
        self.root.after(140, self._drain_log_queue)
        self.root.after(250, self.refresh_now)

    def _configure_theme(self) -> None:
        self.root.configure(bg=self.colors["bg"])
        self.fonts = {
            "title": tkfont.Font(family="Helvetica", size=22, weight="bold"),
            "section": tkfont.Font(family="Helvetica", size=13, weight="bold"),
            "label": tkfont.Font(family="Helvetica", size=11),
            "small": tkfont.Font(family="Helvetica", size=10),
            "chip": tkfont.Font(family="Helvetica", size=10, weight="bold"),
            "big": tkfont.Font(family="Helvetica", size=18, weight="bold"),
            "metric": tkfont.Font(family="Helvetica", size=16, weight="bold"),
        }
        self.style = ttk.Style(self.root)
        try:
            self.style.theme_use("clam")
        except tk.TclError:
            pass
        self.style.configure(".", font=self.fonts["label"])
        self.style.configure("Accent.TButton", padding=(14, 10), font=("Helvetica", 11, "bold"), background=self.colors["accent"], foreground="#FFFFFF", borderwidth=1, relief="solid")
        self.style.map("Accent.TButton", background=[("active", self.colors["accent_dark"]), ("disabled", self.colors["border"])], foreground=[("active", "#FFFFFF"), ("disabled", "#FFFFFF")])
        self.style.configure("Ghost.TButton", padding=(10, 7), font=("Helvetica", 10), background=self.colors["ghost"], foreground=self.colors["text"], borderwidth=1, relief="solid")
        self.style.map("Ghost.TButton", background=[("active", self.colors["ghost_dark"])], foreground=[("active", self.colors["text"])])
        self.style.configure("Card.TCheckbutton", background=self.colors["card"], foreground=self.colors["text"])
        self.style.map("Card.TCheckbutton", background=[("active", self.colors["card"])])
        self.style.configure("Dashboard.Treeview", background=self.colors["input_bg"], foreground=self.colors["text"], fieldbackground=self.colors["input_bg"], rowheight=28, bordercolor=self.colors["border"], relief="flat")
        self.style.configure("Dashboard.Treeview.Heading", background=self.colors["text"], foreground="#FFFFFF", font=("Helvetica", 10, "bold"), relief="flat")
        self.style.map("Dashboard.Treeview", background=[("selected", self.colors["accent"])], foreground=[("selected", "#FFFFFF")])
        self.style.configure("TEntry", fieldbackground=self.colors["input_bg"], foreground=self.colors["text"], padding=6, bordercolor=self.colors["border"])
        self.style.configure("Vertical.TScrollbar", background="#FFFFFF", troughcolor="#000000", bordercolor="#000000", arrowcolor="#FFFFFF", darkcolor="#000000", lightcolor="#000000")
        self.style.configure("Horizontal.TScrollbar", background="#FFFFFF", troughcolor="#000000", bordercolor="#000000", arrowcolor="#FFFFFF", darkcolor="#000000", lightcolor="#000000")
        self.style.configure("Studio.TNotebook", background=self.colors["bg"], borderwidth=0)
        self.style.configure("Studio.TNotebook.Tab", padding=(16, 10), font=("Helvetica", 10, "bold"), background="#FFFFFF", foreground="#000000")
        self.style.map("Studio.TNotebook.Tab", background=[("selected", "#000000")], foreground=[("selected", "#FFFFFF")])

    def _make_card(self, parent: tk.Widget, title: str, subtitle: str = "") -> tuple[tk.Frame, tk.Frame, tk.Frame]:
        card = tk.Frame(parent, bg=self.colors["card"], highlightbackground=self.colors["border"], highlightthickness=1, bd=0)
        header = tk.Frame(card, bg=self.colors["card"])
        header.pack(fill="x", padx=18, pady=(16, 8))
        tk.Label(header, text=title, bg=self.colors["card"], fg=self.colors["text"], font=self.fonts["section"], anchor="w").pack(anchor="w")
        if subtitle:
            tk.Label(header, text=subtitle, bg=self.colors["card"], fg=self.colors["muted"], font=self.fonts["small"], anchor="w", justify="left", wraplength=520).pack(anchor="w", pady=(4, 0))
        body = tk.Frame(card, bg=self.colors["card"])
        body.pack(fill="both", expand=True, padx=18, pady=(0, 18))
        return card, header, body

    def _make_hero_chip(self, parent: tk.Widget, variable: tk.StringVar, row: int, column: int, columnspan: int = 1) -> None:
        chip = tk.Frame(parent, bg=self.colors["hero_chip"], padx=8, pady=6, highlightbackground=self.colors["hero_border"], highlightthickness=1)
        chip.grid(row=row, column=column, columnspan=columnspan, padx=(0, 8), pady=(0, 8), sticky="w")
        tk.Label(chip, textvariable=variable, bg=self.colors["hero_chip"], fg=self.colors["text"], font=self.fonts["chip"]).pack(anchor="w")

    def _bind_responsive_grid(
        self,
        parent: tk.Widget,
        widgets: Sequence[tk.Widget],
        *,
        max_columns: int,
        min_column_width: int,
        row_start: int = 0,
        pad_x: int = 10,
        pad_y: int = 10,
    ) -> Callable[[Optional[tk.Event]], None]:
        state = {"columns": None}

        def _layout(_event=None) -> None:
            width = parent.winfo_width() or parent.winfo_reqwidth() or (min_column_width * max_columns)
            columns = max(1, min(max_columns, max(1, width // max(1, min_column_width))))
            if state["columns"] == columns and len(widgets) == len(getattr(parent, "_responsive_widgets", widgets)):
                return
            state["columns"] = columns
            setattr(parent, "_responsive_widgets", widgets)

            for column in range(max_columns + 1):
                parent.grid_columnconfigure(column, weight=0, uniform="")
            for column in range(columns):
                parent.grid_columnconfigure(column, weight=1, uniform=str(parent))

            for index, widget in enumerate(widgets):
                widget.grid_forget()
                row = row_start + (index // columns)
                column = index % columns
                right_pad = pad_x if column < columns - 1 else 0
                widget.grid(row=row, column=column, sticky="nsew", padx=(0, right_pad), pady=(0, pad_y))

        parent.bind("<Configure>", _layout, add="+")
        parent.after_idle(_layout)
        return _layout

    def _humanize_elapsed(self, total_seconds: int) -> str:
        seconds = max(0, int(total_seconds))
        hours, rem = divmod(seconds, 3600)
        minutes, secs = divmod(rem, 60)
        if hours:
            return f"{hours}h {minutes}m"
        if minutes:
            return f"{minutes}m {secs}s"
        return f"{secs}s"

    def _tick_clock(self) -> None:
        now_local = datetime.now().astimezone()
        self.clock_var.set(now_local.strftime("Local %b %-d, %-I:%M:%S %p"))
        if self.current_bundle is None:
            self.refresh_age_var.set("Refresh age: waiting")
        else:
            elapsed = int((now_local - self.current_bundle.now_local).total_seconds())
            self.refresh_age_var.set(f"Refresh age: {self._humanize_elapsed(elapsed)}")
        self.root.after(1000, self._tick_clock)

    def _apply_window_pin(self) -> None:
        try:
            self.root.attributes("-topmost", bool(self.pin_window_var.get()))
        except tk.TclError:
            pass

    def _update_selected_stores_summary(self) -> None:
        selected = [code.upper() for code in self._selected_store_codes()]
        if not selected:
            self.selected_stores_var.set("Watching: none")
            return
        if len(selected) <= 5:
            self.selected_stores_var.set("Watching: " + ", ".join(selected))
            return
        self.selected_stores_var.set(f"Watching: {', '.join(selected[:5])} +{len(selected) - 5}")

    def _open_store_tab(self, store_code: str) -> None:
        page = self.dynamic_store_pages.get(store_code)
        if page is not None:
            self.notebook.select(page)

    def _on_store_summary_activate(self, _event=None) -> None:
        tree = self.table_views.get("store_summary")
        if tree is None:
            return
        selection = tree.selection()
        if not selection:
            return
        values = tree.item(selection[0], "values")
        if values:
            self._open_store_tab(str(values[0]))

    def _on_global_mousewheel(self, event) -> Optional[str]:
        if not hasattr(self, "notebook"):
            return None
        current_tab = self.notebook.select()
        canvas = self.scroll_canvases.get(current_tab)
        if canvas is None or not canvas.winfo_exists():
            return None

        if event.delta:
            steps = int(-event.delta / 120)
        elif getattr(event, "num", None) == 4:
            steps = -1
        elif getattr(event, "num", None) == 5:
            steps = 1
        else:
            return None

        if steps:
            canvas.yview_scroll(steps, "units")
            return "break"
        return None

    def _make_scrollable_tab(self, notebook: ttk.Notebook, *, bg: Optional[str] = None) -> tuple[tk.Frame, tk.Frame]:
        page = tk.Frame(notebook, bg=bg or self.colors["bg"])
        page.grid_rowconfigure(0, weight=1)
        page.grid_columnconfigure(0, weight=1)

        canvas = tk.Canvas(page, bg=bg or self.colors["bg"], highlightthickness=0, bd=0)
        canvas.grid(row=0, column=0, sticky="nsew")
        y_scroll = ttk.Scrollbar(page, orient="vertical", command=canvas.yview)
        y_scroll.grid(row=0, column=1, sticky="ns")
        canvas.configure(yscrollcommand=y_scroll.set)
        self.scroll_canvases[str(page)] = canvas

        content = tk.Frame(canvas, bg=bg or self.colors["bg"], padx=6, pady=10)
        window_id = canvas.create_window((0, 0), window=content, anchor="nw")

        def _sync_scroll_region(_event=None) -> None:
            bbox = canvas.bbox("all")
            if bbox:
                canvas.configure(scrollregion=bbox)

        def _sync_width(event) -> None:
            canvas.itemconfigure(window_id, width=event.width)

        content.bind("<Configure>", _sync_scroll_region)
        canvas.bind("<Configure>", _sync_width)

        return page, content

    def _relayout_chrome(self, _event=None) -> None:
        width = self.root.winfo_width() or self.root.winfo_reqwidth()
        if width <= 1:
            return

        subtitle_wrap = max(340, min(760, width - 420))
        story_wrap = max(360, min(820, width - 400))
        self.hero_subtitle_label.configure(wraplength=subtitle_wrap)
        self.hero_story_label.configure(wraplength=story_wrap)

        if width < 1100:
            self.hero_summary.grid_configure(row=3, column=0, rowspan=1, sticky="w", pady=(8, 0))
        else:
            self.hero_summary.grid_configure(row=0, column=1, rowspan=3, sticky="ne", pady=(0, 0))

    def _build_ui(self) -> None:
        outer = tk.Frame(self.root, bg=self.colors["bg"], padx=12, pady=12)
        outer.pack(fill="both", expand=True)

        command_card = tk.Frame(outer, bg=self.colors["card"], highlightbackground=self.colors["border"], highlightthickness=1, bd=0)
        command_card.pack(fill="x")

        hero = tk.Frame(command_card, bg=self.colors["hero"], padx=16, pady=10, highlightthickness=0)
        hero.pack(fill="x")
        hero.grid_columnconfigure(0, weight=1)
        self.hero = hero

        tk.Label(hero, text="Dutchie Live Command Center", bg=self.colors["hero"], fg="#FFFFFF", font=self.fonts["title"], anchor="w").grid(row=0, column=0, sticky="w")
        self.hero_subtitle_label = tk.Label(
            hero,
            text="Today's sales, pace, and alerts across all stores.",
            bg=self.colors["hero"],
            fg="#FFFFFF",
            font=self.fonts["label"],
            anchor="w",
            justify="left",
            wraplength=760,
        )
        self.hero_subtitle_label.grid(row=1, column=0, sticky="w", pady=(3, 0))
        self.hero_story_label = tk.Label(
            hero,
            textvariable=self.story_var,
            bg=self.colors["hero"],
            fg="#FFFFFF",
            font=self.fonts["small"],
            anchor="w",
            justify="left",
            wraplength=820,
        )
        self.hero_story_label.grid(row=2, column=0, sticky="w", pady=(4, 0))

        hero_summary = tk.Frame(hero, bg=self.colors["hero"])
        hero_summary.grid(row=0, column=1, rowspan=3, sticky="ne")
        self.hero_summary = hero_summary
        self._make_hero_chip(hero_summary, self.status_var, 0, 0)
        self._make_hero_chip(hero_summary, self.loaded_store_var, 0, 1)
        self._make_hero_chip(hero_summary, self.last_refresh_var, 1, 0)
        self._make_hero_chip(hero_summary, self.refresh_age_var, 1, 1)
        self._make_hero_chip(hero_summary, self.mode_var, 2, 0)
        self._make_hero_chip(hero_summary, self.summary_chip_var, 2, 1)
        self._make_hero_chip(hero_summary, self.selected_stores_var, 3, 0, columnspan=2)
        self._make_hero_chip(hero_summary, self.clock_var, 4, 0, columnspan=2)

        notebook_wrap = tk.Frame(outer, bg=self.colors["bg"])
        notebook_wrap.pack(fill="both", expand=True, pady=(10, 0))
        self.notebook = ttk.Notebook(notebook_wrap, style="Studio.TNotebook")
        self.notebook.pack(fill="both", expand=True)

        self.overview_page, self.overview_tab = self._make_scrollable_tab(self.notebook)
        self.sales_page, self.sales_tab = self._make_scrollable_tab(self.notebook)
        self.inventory_page, self.inventory_tab = self._make_scrollable_tab(self.notebook)
        self.activity_tab = tk.Frame(self.notebook, bg=self.colors["bg"], padx=6, pady=10)
        self.settings_page, self.settings_tab = self._make_scrollable_tab(self.notebook)

        self.notebook.add(self.overview_page, text="Overview")
        self.notebook.add(self.sales_page, text="Sales Mix")
        self.notebook.add(self.inventory_page, text="Inventory")
        self.notebook.add(self.activity_tab, text="Activity")
        self.notebook.add(self.settings_page, text="Settings")

        self._build_overview_tab()
        self._build_sales_tab()
        self._build_inventory_tab()
        self._build_activity_tab()
        self._build_settings_tab()
        self.root.bind("<Configure>", self._relayout_chrome, add="+")
        self.root.after_idle(self._relayout_chrome)

    def _build_kpi_tile(self, parent: tk.Widget, title: str, var_name: str, note: str) -> tk.Frame:
        tile = tk.Frame(parent, bg=self.colors["card_alt"], padx=14, pady=12, highlightbackground=self.colors["border"], highlightthickness=1)
        tk.Label(tile, text=title, bg=self.colors["card_alt"], fg=self.colors["muted"], font=self.fonts["small"], anchor="w").pack(anchor="w")
        tk.Label(tile, textvariable=self.kpi_vars[var_name], bg=self.colors["card_alt"], fg=self.colors["text"], font=self.fonts["metric"], anchor="w").pack(anchor="w", pady=(6, 0))
        tk.Label(tile, text=note, bg=self.colors["card_alt"], fg=self.colors["muted"], font=self.fonts["small"], anchor="w", justify="left", wraplength=220).pack(anchor="w", pady=(4, 0))
        return tile

    def _build_tree_card(
        self,
        parent: tk.Widget,
        key: str,
        title: str,
        subtitle: str,
        columns: Sequence[tuple[str, str, int]],
    ) -> tk.Frame:
        card, _, body = self._make_card(parent, title, subtitle)
        body.grid_rowconfigure(0, weight=1)
        body.grid_columnconfigure(0, weight=1)
        tree = ttk.Treeview(body, columns=[col_id for col_id, _label, _width in columns], show="headings", style="Dashboard.Treeview")
        for col_id, label, width in columns:
            tree.heading(col_id, text=label)
            anchor = "e" if any(token in col_id for token in ("revenue", "profit", "value", "price", "basket", "margin", "discount", "pct", "units", "tickets", "available")) else "w"
            tree.column(col_id, width=width, minwidth=max(60, width // 2), anchor=anchor)
        tree.grid(row=0, column=0, sticky="nsew")
        scroll = ttk.Scrollbar(body, orient="vertical", command=tree.yview)
        scroll.grid(row=0, column=1, sticky="ns")
        x_scroll = ttk.Scrollbar(body, orient="horizontal", command=tree.xview)
        x_scroll.grid(row=1, column=0, sticky="ew")
        tree.configure(yscrollcommand=scroll.set, xscrollcommand=x_scroll.set)
        self.table_views[key] = tree
        self.table_columns[key] = [col_id for col_id, _label, _width in columns]
        return card

    def _build_local_tree_card(
        self,
        parent: tk.Widget,
        title: str,
        subtitle: str,
        columns: Sequence[tuple[str, str, int]],
        df: pd.DataFrame,
        formatters: Optional[dict[str, Callable[[Any], str]]] = None,
    ) -> tk.Frame:
        card, _, body = self._make_card(parent, title, subtitle)
        body.grid_rowconfigure(0, weight=1)
        body.grid_columnconfigure(0, weight=1)
        tree = ttk.Treeview(body, columns=[col_id for col_id, _label, _width in columns], show="headings", style="Dashboard.Treeview", height=10)
        for col_id, label, width in columns:
            tree.heading(col_id, text=label)
            anchor = "e" if any(token in col_id for token in ("revenue", "profit", "value", "price", "basket", "margin", "discount", "pct", "units", "tickets", "available")) else "w"
            tree.column(col_id, width=width, minwidth=max(60, width // 2), anchor=anchor)
        tree.grid(row=0, column=0, sticky="nsew")
        y_scroll = ttk.Scrollbar(body, orient="vertical", command=tree.yview)
        y_scroll.grid(row=0, column=1, sticky="ns")
        x_scroll = ttk.Scrollbar(body, orient="horizontal", command=tree.xview)
        x_scroll.grid(row=1, column=0, sticky="ew")
        tree.configure(yscrollcommand=y_scroll.set, xscrollcommand=x_scroll.set)
        if df is not None and not df.empty:
            for _, row in df.iterrows():
                values: list[str] = []
                for col_id, _label, _width in columns:
                    value = row.get(col_id, "")
                    if formatters and col_id in formatters:
                        values.append(formatters[col_id](value))
                    else:
                        values.append("" if pd.isna(value) else str(value))
                tree.insert("", tk.END, values=values)
        return card

    def _build_overview_tab(self) -> None:
        self.overview_tab.grid_columnconfigure(0, weight=3)
        self.overview_tab.grid_columnconfigure(1, weight=2)
        self.overview_tab.grid_rowconfigure(1, weight=1)
        self.overview_tab.grid_rowconfigure(2, weight=1)

        kpi_card, _, kpi_body = self._make_card(
            self.overview_tab,
            "Today's Snapshot",
            "The key numbers most people want first when checking the day.",
        )
        kpi_card.grid(row=0, column=0, columnspan=2, sticky="ew")
        kpi_tiles_widgets: list[tk.Widget] = []
        kpi_tiles = [
            ("Sales Today", "net_revenue", "Money sold so far today."),
            ("Gross Sales", "gross_sales", "Before discounts and adjustments."),
            ("Orders", "tickets", "Number of completed sales."),
            ("Avg Sale", "basket", "Average dollars per order."),
            ("Units", "items", "Total item quantity sold."),
            ("Profit", "profit_real", "Real order profit from line items."),
            ("Margin", "margin_real", "Real margin from profit and net."),
            ("Discounts", "discount", "Total discounts on current sales."),
            ("Discount Rate", "discount_rate", "Discounts as a share of gross."),
            ("Returns", "returns_net", "Net value returned so far."),
            ("Last Sale", "last_ticket", "Most recent completed sale."),
            ("Vs Yesterday", "compare_yesterday", "How today compares to this time yesterday."),
        ]
        for idx, (title, var_name, note) in enumerate(kpi_tiles):
            tile = self._build_kpi_tile(kpi_body, title, var_name, note)
            kpi_tiles_widgets.append(tile)
        self.kpi_last_week_tile = self._build_kpi_tile(kpi_body, "Vs Last Week", "compare_last_week", "Same weekday checkpoint.")
        kpi_tiles_widgets.append(self.kpi_last_week_tile)
        self._bind_responsive_grid(kpi_body, kpi_tiles_widgets, max_columns=4, min_column_width=220, pad_x=10, pad_y=10)

        chart_card, _, chart_body = self._make_card(
            self.overview_tab,
            "How Today Is Pacing",
            "Use these charts first to see whether the day is on track and which stores are carrying the most sales.",
        )
        chart_card.grid(row=1, column=0, sticky="nsew", padx=(0, 10), pady=(14, 0))
        chart_body.grid_columnconfigure(0, weight=1)
        chart_body.grid_columnconfigure(1, weight=1)
        chart_body.grid_rowconfigure(0, weight=1)
        pace_frame = tk.Frame(chart_body, bg=self.colors["card"], height=280)
        pace_frame.grid(row=0, column=0, sticky="nsew", padx=(0, 10))
        pace_frame.grid_propagate(False)
        store_frame = tk.Frame(chart_body, bg=self.colors["card"], height=280)
        store_frame.grid(row=0, column=1, sticky="nsew")
        store_frame.grid_propagate(False)
        self.chart_frames["pace_all"] = pace_frame
        self.chart_frames["store_sales"] = store_frame
        self._bind_responsive_grid(chart_body, [pace_frame, store_frame], max_columns=2, min_column_width=480, pad_x=10, pad_y=10)

        mix_card, _, mix_body = self._make_card(
            self.overview_tab,
            "What's Driving Sales",
            "These charts show where today's sales are coming from.",
        )
        mix_card.grid(row=2, column=0, sticky="nsew", padx=(0, 10), pady=(14, 0))
        mix_body.grid_columnconfigure(0, weight=1)
        mix_body.grid_columnconfigure(1, weight=1)
        mix_body.grid_rowconfigure(0, weight=1)
        category_frame = tk.Frame(mix_body, bg=self.colors["card"], height=260)
        category_frame.grid(row=0, column=0, sticky="nsew", padx=(0, 10))
        category_frame.grid_propagate(False)
        source_frame = tk.Frame(mix_body, bg=self.colors["card"], height=260)
        source_frame.grid(row=0, column=1, sticky="nsew")
        source_frame.grid_propagate(False)
        self.chart_frames["category_mix"] = category_frame
        self.chart_frames["source_mix"] = source_frame
        self._bind_responsive_grid(mix_body, [category_frame, source_frame], max_columns=2, min_column_width=460, pad_x=10, pad_y=10)

        overview_right = tk.Frame(self.overview_tab, bg=self.colors["bg"])
        overview_right.grid(row=1, column=1, rowspan=2, sticky="nsew", pady=(14, 0))
        overview_right.grid_rowconfigure(0, weight=1)
        overview_right.grid_rowconfigure(1, weight=1)
        overview_right.grid_rowconfigure(2, weight=1)
        overview_right.grid_columnconfigure(0, weight=1)

        store_card = self._build_tree_card(
            overview_right,
            "store_summary",
            "Store Performance",
            "Quick store-by-store read on sales, order count, and recent activity.",
            [
                ("store_code", "Store", 70),
                ("net_revenue", "Net", 100),
                ("tickets", "Orders", 80),
                ("avg_ticket", "Avg Sale", 100),
                ("units", "Units", 90),
                ("margin_real", "Margin", 90),
                ("vs_yesterday", "Vs Yesterday", 130),
                ("vs_last_week", "Vs Last Week", 130),
                ("minutes_since_ticket", "Last Sale", 110),
            ],
        )
        store_card.grid(row=0, column=0, sticky="nsew", pady=(0, 10))
        self.table_views["store_summary"].bind("<Double-1>", self._on_store_summary_activate)
        self.table_views["store_summary"].bind("<Return>", self._on_store_summary_activate)
        watch_card = self._build_tree_card(
            overview_right,
            "operational_watch",
            "Needs Attention",
            "The few things worth checking first right now.",
            [
                ("priority", "Type", 80),
                ("area", "Area", 90),
                ("detail", "Detail", 250),
                ("impact", "Impact", 130),
            ],
        )
        watch_card.grid(row=1, column=0, sticky="nsew", pady=(0, 10))
        hourly_card = self._build_tree_card(
            overview_right,
            "hourly_all",
            "Sales by Hour",
            "A simple hour-by-hour view across all selected stores.",
            [
                ("hour_label", "Hour", 80),
                ("net_revenue", "Net", 100),
                ("tickets", "Orders", 80),
                ("basket", "Avg Sale", 100),
                ("margin_real", "Margin", 90),
            ],
        )
        hourly_card.grid(row=2, column=0, sticky="nsew")

        def _layout_overview(_event=None) -> None:
            width = self.overview_tab.winfo_width() or self.overview_tab.winfo_reqwidth()
            if width < 1320:
                self.overview_tab.grid_columnconfigure(0, weight=1)
                self.overview_tab.grid_columnconfigure(1, weight=0)
                chart_card.grid_configure(row=1, column=0, padx=(0, 0), pady=(14, 0))
                mix_card.grid_configure(row=2, column=0, padx=(0, 0), pady=(14, 0))
                overview_right.grid_configure(row=3, column=0, rowspan=1, pady=(14, 0))
            else:
                self.overview_tab.grid_columnconfigure(0, weight=3)
                self.overview_tab.grid_columnconfigure(1, weight=2)
                chart_card.grid_configure(row=1, column=0, padx=(0, 10), pady=(14, 0))
                mix_card.grid_configure(row=2, column=0, padx=(0, 10), pady=(14, 0))
                overview_right.grid_configure(row=1, column=1, rowspan=2, pady=(14, 0))

        self.overview_tab.bind("<Configure>", _layout_overview, add="+")
        self.overview_tab.after_idle(_layout_overview)

    def _build_sales_tab(self) -> None:
        sales_cards = [
            self._build_tree_card(
            self.sales_tab,
            "product_net",
            "Top Products by Net Sales",
            "Best same-day sellers by net revenue.",
            [("Product Name", "Product", 320), ("net_revenue", "Net", 110)],
            ),
            self._build_tree_card(
            self.sales_tab,
            "product_units",
            "Top Products by Units",
            "Useful when unit movement matters more than dollars.",
            [("Product Name", "Product", 320), ("units_sold", "Units", 110)],
            ),
            self._build_tree_card(
            self.sales_tab,
            "brand_summary",
            "Brand Summary",
            "Brand revenue and margin using owner_snapshot brand parsing.",
            [("brand", "Brand", 220), ("net_revenue", "Net", 110), ("profit_real", "Profit", 110), ("margin_real", "Margin", 90)],
            ),
            self._build_tree_card(
            self.sales_tab,
            "category_summary",
            "Category Summary",
            "Revenue, profit, units, and discount pressure by category.",
            [("category", "Category", 180), ("net_revenue", "Net", 110), ("profit_real", "Profit", 110), ("margin_real", "Margin", 90), ("items", "Units", 90), ("discount_rate", "Disc Rate", 90)],
            ),
            self._build_tree_card(
            self.sales_tab,
            "vendor_summary",
            "Vendor Leaders",
            "Top vendors by same-day net revenue.",
            [("Vendor Name", "Vendor", 280), ("net_revenue", "Net", 110)],
            ),
            self._build_tree_card(
            self.sales_tab,
            "source_mix",
            "Channel Mix",
            "Order source mix at the transaction level.",
            [("order_source", "Source", 180), ("net_revenue", "Net", 110), ("tickets", "Tickets", 90)],
            ),
            self._build_tree_card(
            self.sales_tab,
            "budtender_summary",
            "Budtender Summary",
            "Net, basket, and discount rate by budtender.",
            [("budtender", "Budtender", 200), ("net_revenue", "Net", 110), ("tickets", "Tickets", 90), ("basket", "Basket", 100), ("discount_rate", "Disc Rate", 90)],
            ),
            self._build_tree_card(
            self.sales_tab,
            "customer_type_summary",
            "Customer Type Summary",
            "How the day is splitting across customer types.",
            [("customer_type", "Customer Type", 180), ("net_revenue", "Net", 110), ("tickets", "Tickets", 90), ("basket", "Basket", 100)],
            ),
            self._build_tree_card(
            self.sales_tab,
            "cart_distribution",
            "Cart Distribution",
            "Ticket counts by net cart-value bucket.",
            [("bucket", "Bucket", 120), ("count", "Count", 90), ("pct", "Share", 90)],
            ),
            self._build_tree_card(
            self.sales_tab,
            "order_type_mix",
            "Order Type Mix",
            "In-store, pickup, and other order types.",
            [("order_type", "Order Type", 180), ("net_revenue", "Net", 110), ("tickets", "Tickets", 90)],
            ),
        ]
        self._bind_responsive_grid(self.sales_tab, sales_cards, max_columns=2, min_column_width=720, pad_x=10, pad_y=10)

    def _build_inventory_tab(self) -> None:
        inventory_cards = [
            self._build_tree_card(
            self.inventory_tab,
            "low_stock",
            "Low Stock Alerts",
            "Items actively selling today that are already getting tight.",
            [("store_code", "Store", 70), ("product_name", "Product", 260), ("brand_name", "Brand", 160), ("available", "Avail", 80), ("units_today", "Sold", 80), ("days_left", "Days Left", 90), ("revenue_today", "Net", 100)],
            ),
            self._build_tree_card(
            self.inventory_tab,
            "inventory_category",
            "Inventory by Category",
            "Current on-hand value and revenue potential by category.",
            [("Category", "Category", 180), ("Available Units", "Avail", 90), ("Inventory Value", "Inv Value", 110), ("Revenue Potential", "Revenue", 120), ("Potential Profit", "Profit", 110), ("SKU Count", "SKUs", 70)],
            ),
            self._build_tree_card(
            self.inventory_tab,
            "inventory_brand",
            "Inventory by Brand",
            "Current inventory value concentration by brand.",
            [("Brand", "Brand", 200), ("Available Units", "Avail", 90), ("Inventory Value", "Inv Value", 110), ("Revenue Potential", "Revenue", 120), ("Potential Profit", "Profit", 110), ("SKU Count", "SKUs", 70)],
            ),
            self._build_tree_card(
            self.inventory_tab,
            "inventory_product",
            "Inventory Product Board",
            "Highest-value live inventory rows across the selected stores.",
            [("Store Code", "Store", 70), ("Product Name", "Product", 280), ("Brand", "Brand", 180), ("Available", "Avail", 80), ("Inventory Value", "Inv Value", 110), ("Revenue Potential", "Revenue", 120), ("Potential Profit", "Profit", 110)],
            ),
        ]
        self._bind_responsive_grid(self.inventory_tab, inventory_cards, max_columns=2, min_column_width=720, pad_x=10, pad_y=10)

    def _build_settings_tab(self) -> None:
        refresh_card, _, refresh_body = self._make_card(
            self.settings_tab,
            "Board Settings",
            "Change refresh behavior and a few display options here.",
        )
        refresh_body.grid_columnconfigure(0, weight=1)
        refresh_card.grid_columnconfigure(0, weight=1)

        tk.Label(refresh_body, text="Refresh every (seconds)", bg=self.colors["card"], fg=self.colors["text"], font=self.fonts["chip"], anchor="w").grid(row=0, column=0, sticky="w")
        ttk.Entry(refresh_body, textvariable=self.refresh_seconds_var, width=10).grid(row=1, column=0, sticky="w", pady=(6, 10))
        ttk.Checkbutton(refresh_body, text="Keep refreshing automatically", variable=self.auto_refresh_var, style="Card.TCheckbutton", command=self._toggle_auto_refresh).grid(row=2, column=0, sticky="w", pady=(0, 6))
        ttk.Checkbutton(refresh_body, text="Hide inventory sections", variable=self.no_inventory_var, style="Card.TCheckbutton").grid(row=3, column=0, sticky="w", pady=(0, 6))
        ttk.Checkbutton(refresh_body, text="Keep window on top", variable=self.pin_window_var, style="Card.TCheckbutton", command=self._apply_window_pin).grid(row=4, column=0, sticky="w", pady=(0, 10))
        ttk.Button(refresh_body, text="Refresh Now", style="Accent.TButton", command=self.refresh_now).grid(row=5, column=0, sticky="w")
        tk.Label(
            refresh_body,
            text="Tip: F5 refreshes the board from anywhere.",
            bg=self.colors["card"],
            fg=self.colors["muted"],
            font=self.fonts["small"],
            anchor="w",
            justify="left",
        ).grid(row=6, column=0, sticky="w", pady=(10, 0))

        locations_card, _, locations_body = self._make_card(
            self.settings_tab,
            "Stores On Screen",
            "Choose which stores appear in the overview and in the store tabs.",
        )
        locations_body.grid_columnconfigure(0, weight=1)
        tk.Label(locations_body, text="Visible locations", bg=self.colors["card"], fg=self.colors["text"], font=self.fonts["chip"], anchor="w").grid(row=0, column=0, sticky="w")
        toggles = tk.Frame(locations_body, bg=self.colors["card"])
        toggles.grid(row=1, column=0, sticky="ew", pady=(8, 0))
        self.store_toggles = toggles
        self.store_buttons = {}
        store_widgets: list[tk.Widget] = []
        for code in STORE_CODES:
            btn = ttk.Checkbutton(
                toggles,
                text=f"{code.upper()}  {STORE_CODES[code]}",
                variable=self.store_vars[code],
                style="Card.TCheckbutton",
                command=self._update_selected_stores_summary,
            )
            if code not in self.configured_store_codes:
                btn.state(["disabled"])
            self.store_buttons[code] = btn
            store_widgets.append(btn)
        self._bind_responsive_grid(toggles, store_widgets, max_columns=3, min_column_width=220, pad_x=18, pad_y=6)

        help_card, _, help_body = self._make_card(
            self.settings_tab,
            "How To Use This Screen",
            "A simple guide for someone checking the day without digging through every tab.",
        )
        help_body.grid_columnconfigure(0, weight=1)
        help_lines = [
            "Overview: best place to start for today's sales, pace, and store performance.",
            "Store tabs: open one store at a time when you want more detail.",
            "Needs Attention: quickest list of what may need action right now.",
        ]
        for idx, line in enumerate(help_lines):
            tk.Label(
                help_body,
                text=line,
                bg=self.colors["card"],
                fg=self.colors["text"],
                font=self.fonts["label"],
                anchor="w",
                justify="left",
                wraplength=520,
            ).grid(row=idx, column=0, sticky="w", pady=(0, 8 if idx < len(help_lines) - 1 else 0))

        settings_cards = [refresh_card, locations_card, help_card]
        self._bind_responsive_grid(self.settings_tab, settings_cards, max_columns=2, min_column_width=460, pad_x=10, pad_y=10)

    def _build_activity_tab(self) -> None:
        self.activity_tab.grid_columnconfigure(0, weight=1)
        self.activity_tab.grid_rowconfigure(0, weight=1)
        log_card, _, log_body = self._make_card(
            self.activity_tab,
            "Activity Feed",
            "Live worker logs, fetch notes, and warnings land here so the dashboard stays understandable while it refreshes.",
        )
        log_card.grid(row=0, column=0, sticky="nsew")
        log_body.grid_rowconfigure(0, weight=1)
        log_body.grid_columnconfigure(0, weight=1)
        self.log_list = tk.Listbox(
            log_body,
            activestyle="none",
            bg=self.colors["log_bg"],
            fg=self.colors["text"],
            font=self.fonts["label"],
            selectbackground=self.colors["accent"],
            selectforeground="#FFFFFF",
            relief="flat",
            highlightthickness=1,
            highlightbackground=self.colors["border"],
        )
        self.log_list.grid(row=0, column=0, sticky="nsew")
        log_scroll = ttk.Scrollbar(log_body, orient="vertical", command=self.log_list.yview)
        log_scroll.grid(row=0, column=1, sticky="ns")
        self.log_list.configure(yscrollcommand=log_scroll.set)

    def _log(self, message: str) -> None:
        timestamp = datetime.now().strftime("%H:%M:%S")
        self.log_queue.put(("log", f"[{timestamp}] {message}"))

    def _append_log(self, message: str) -> None:
        self.log_list.insert(tk.END, message)
        if self.log_list.size() > self.max_log_rows:
            self.log_list.delete(0, self.log_list.size() - self.max_log_rows - 1)
        self.log_list.yview_moveto(1.0)

    def _drain_log_queue(self) -> None:
        try:
            while True:
                kind, payload = self.log_queue.get_nowait()
                if kind == "log":
                    self._append_log(str(payload))
                elif kind == "bundle":
                    self._apply_bundle(payload)
                elif kind == "error":
                    self.status_var.set("Error")
                    self._append_log(str(payload))
                    messagebox.showerror("Live Dashboard Error", str(payload))
                elif kind == "done":
                    self.worker_running = False
                    self.status_var.set("Loaded")
                    self._schedule_auto_refresh_if_needed()
        except queue.Empty:
            pass
        self.root.after(140, self._drain_log_queue)

    def _selected_store_codes(self) -> list[str]:
        return [code for code, var in self.store_vars.items() if var.get()]

    def _refresh_interval_seconds(self) -> int:
        try:
            return max(30, int(self.refresh_seconds_var.get().strip()))
        except Exception:
            return 120

    def _toggle_auto_refresh(self) -> None:
        self.mode_var.set(f"Refresh every {self._refresh_interval_seconds()}s" if self.auto_refresh_var.get() else "Refresh on demand")
        if self.auto_refresh_var.get():
            self._schedule_auto_refresh_if_needed()
        elif self.auto_refresh_job:
            self.root.after_cancel(self.auto_refresh_job)
            self.auto_refresh_job = None

    def _schedule_auto_refresh_if_needed(self) -> None:
        if not self.auto_refresh_var.get():
            return
        if self.worker_running:
            return
        if self.auto_refresh_job:
            try:
                self.root.after_cancel(self.auto_refresh_job)
            except Exception:
                pass
        self.auto_refresh_job = self.root.after(self._refresh_interval_seconds() * 1000, self.refresh_now)

    def refresh_now(self) -> None:
        if self.worker_running:
            return
        self._update_selected_stores_summary()
        selected = self._selected_store_codes()
        if not selected:
            messagebox.showwarning("No Stores Selected", "Choose at least one configured store before refreshing.")
            return
        self.worker_running = True
        self.status_var.set("Refreshing…")
        self.summary_chip_var.set("Loading latest sales")
        self.mode_var.set(f"Refresh every {self._refresh_interval_seconds()}s" if self.auto_refresh_var.get() else "Refresh on demand")
        self._log(f"Starting live refresh for: {', '.join(selected)}")
        include_inventory = not self.no_inventory_var.get()
        threading.Thread(target=self._worker_refresh, args=(selected, include_inventory), daemon=True).start()

    def _worker_refresh(self, selected: list[str], include_inventory: bool) -> None:
        try:
            requested_store_codes, store_keys, integrator_key = _resolve_requested_stores(self.env_file, selected)
            now_local = datetime.now().astimezone()
            snapshots: list[StoreSnapshot] = []
            errors: list[str] = []
            products_cache: dict[str, Any] = {}
            for store_code in requested_store_codes:
                try:
                    self.log_queue.put(("log", f"[FETCH] {store_code} requesting transactions and {'inventory' if include_inventory else 'sales-only'}"))
                    snapshot = _fetch_store_snapshot(
                        store_code=store_code,
                        store_key=store_keys[store_code],
                        integrator_key=integrator_key,
                        now_local=now_local,
                        include_inventory=include_inventory,
                        products_cache=products_cache,
                    )
                    snapshots.append(snapshot)
                    today = _summarize_period(snapshot.transactions_today, snapshot.items_today)
                    self.log_queue.put(
                        ("log", f"[LIVE] {store_code}: {_fmt_money(today['sales_total'])} | {today['tickets']} tickets | last {_fmt_time(today['last_ticket_time'])}")
                    )
                except Exception as exc:
                    errors.append(f"{store_code}: {exc}")
                    self.log_queue.put(("log", f"[WARN] {store_code} failed: {exc}"))

            bundle = build_dashboard_bundle(
                snapshots=snapshots,
                selected_stores=requested_store_codes,
                now_local=now_local,
                errors=errors,
            )
            self.log_queue.put(("bundle", bundle))
            self.log_queue.put(("done", None))
        except Exception as exc:
            self.log_queue.put(("error", f"Live refresh failed: {exc}"))
            self.log_queue.put(("done", None))

    def _apply_bundle(self, bundle: DashboardBundle) -> None:
        self.current_bundle = bundle
        self.last_refresh_var.set("Updated " + bundle.now_local.strftime("%b %-d, %-I:%M:%S %p"))
        self.loaded_store_var.set(f"{len(bundle.snapshots)} stores loaded")
        self.summary_chip_var.set("Sales only" if self.no_inventory_var.get() else "Sales + inventory")
        self.story_var.set(" | ".join(bundle.story_lines[:3]))

        metrics = bundle.overview_metrics
        self.kpi_vars["net_revenue"].set(_fmt_money(metrics.get("net_revenue")))
        self.kpi_vars["gross_sales"].set(_fmt_money(metrics.get("gross_sales")))
        self.kpi_vars["tickets"].set(_fmt_int(metrics.get("tickets")))
        self.kpi_vars["basket"].set(_fmt_money(metrics.get("basket")))
        self.kpi_vars["items"].set(_fmt_decimal(metrics.get("items")))
        self.kpi_vars["profit_real"].set(_fmt_money(metrics.get("profit_real")))
        self.kpi_vars["margin_real"].set(_fmt_pct(metrics.get("margin_real")))
        self.kpi_vars["discount"].set(_fmt_money(metrics.get("discount")))
        self.kpi_vars["discount_rate"].set(_fmt_pct(metrics.get("discount_rate")))
        self.kpi_vars["returns_net"].set(_fmt_money(abs(metrics.get("returns_net", 0.0))))
        self.kpi_vars["last_ticket"].set(_fmt_time(max((snapshot.transactions_today["order_time"].max() for snapshot in bundle.snapshots if not snapshot.transactions_today.empty), default=None)))
        self.kpi_vars["compare_yesterday"].set(_fmt_delta(bundle.compare_yesterday))
        self.kpi_vars["compare_last_week"].set(_fmt_delta(bundle.compare_last_week))

        self._populate_tree(
            "store_summary",
            bundle.store_summary,
            {
                "net_revenue": _fmt_money,
                "tickets": _fmt_int,
                "avg_ticket": _fmt_money,
                "units": _fmt_decimal,
                "margin_real": _fmt_pct,
            },
        )
        self._populate_tree(
            "hourly_all",
            bundle.hourly_all,
            {
                "net_revenue": _fmt_money,
                "tickets": _fmt_int,
                "basket": _fmt_money,
                "margin_real": _fmt_pct,
            },
        )
        self._populate_tree("operational_watch", bundle.operational_watch)
        self._populate_tree("product_net", bundle.product_net, {"net_revenue": _fmt_money})
        self._populate_tree("product_units", bundle.product_units, {"units_sold": _fmt_decimal})
        self._populate_tree("brand_summary", bundle.brand_summary, {"net_revenue": _fmt_money, "profit_real": _fmt_money, "margin_real": _fmt_pct})
        self._populate_tree(
            "category_summary",
            bundle.category_summary,
            {"net_revenue": _fmt_money, "profit_real": _fmt_money, "margin_real": _fmt_pct, "items": _fmt_decimal, "discount_rate": _fmt_pct},
        )
        self._populate_tree("vendor_summary", bundle.vendor_summary, {"net_revenue": _fmt_money})
        self._populate_tree(
            "budtender_summary",
            bundle.budtender_summary,
            {"net_revenue": _fmt_money, "tickets": _fmt_int, "basket": _fmt_money, "discount_rate": _fmt_pct},
        )
        self._populate_tree(
            "customer_type_summary",
            bundle.customer_type_summary,
            {"net_revenue": _fmt_money, "tickets": _fmt_int, "basket": _fmt_money},
        )
        self._populate_tree("cart_distribution", bundle.cart_distribution, {"count": _fmt_int, "pct": _fmt_pct})
        self._populate_tree("source_mix", bundle.source_mix, {"net_revenue": _fmt_money, "tickets": _fmt_int})
        self._populate_tree("order_type_mix", bundle.order_type_mix, {"net_revenue": _fmt_money, "tickets": _fmt_int})
        self._populate_tree(
            "low_stock",
            bundle.low_stock,
            {"available": _fmt_decimal, "units_today": _fmt_decimal, "days_left": lambda value: "n/a" if pd.isna(value) else ("same-day" if _to_float(value) < 1 else f"{_to_float(value):,.1f}"), "revenue_today": _fmt_money},
        )
        self._populate_tree(
            "inventory_category",
            bundle.inventory_category,
            {"Available Units": _fmt_decimal, "Inventory Value": _fmt_money, "Revenue Potential": _fmt_money, "Potential Profit": _fmt_money, "SKU Count": _fmt_int},
        )
        self._populate_tree(
            "inventory_brand",
            bundle.inventory_brand,
            {"Available Units": _fmt_decimal, "Inventory Value": _fmt_money, "Revenue Potential": _fmt_money, "Potential Profit": _fmt_money, "SKU Count": _fmt_int},
        )
        self._populate_tree(
            "inventory_product",
            bundle.inventory_product,
            {"Available": _fmt_decimal, "Inventory Value": _fmt_money, "Revenue Potential": _fmt_money, "Potential Profit": _fmt_money},
        )

        self._draw_line_comparison_chart(
            "pace_all",
            bundle.pace_summary,
            title="Cumulative Net Sales Pace",
            series=[
                ("today_cum", "Today", "#000000"),
                ("yesterday_cum", "Yesterday", "#FFFFFF"),
                ("last_week_cum", "Last Week", "#FFFFFF"),
            ],
            kind="money",
        )
        self._draw_horizontal_bar_chart(
            "store_sales",
            bundle.store_summary,
            label_col="store_code",
            value_col="net_revenue",
            title="Store Net Sales",
            kind="money",
        )
        self._draw_horizontal_bar_chart(
            "category_mix",
            bundle.category_summary,
            label_col="category",
            value_col="net_revenue",
            title="Category Contribution",
            kind="money",
        )
        self._draw_horizontal_bar_chart(
            "source_mix",
            bundle.source_mix,
            label_col="order_source",
            value_col="net_revenue",
            title="Order Source Contribution",
            kind="money",
        )

        self._rebuild_store_tabs(bundle)

        if bundle.errors:
            for err in bundle.errors:
                self._append_log(f"[WARN] {err}")

    def _populate_tree(self, key: str, df: pd.DataFrame, formatters: Optional[dict[str, Callable[[Any], str]]] = None) -> None:
        tree = self.table_views[key]
        columns = self.table_columns[key]
        tree.delete(*tree.get_children())
        if df is None or df.empty:
            return
        working = df.copy()
        for _, row in working.iterrows():
            values: list[str] = []
            for column in columns:
                value = row.get(column, "")
                if formatters and column in formatters:
                    values.append(formatters[column](value))
                else:
                    values.append("" if pd.isna(value) else str(value))
            tree.insert("", tk.END, values=values)

    def _style_chart_axes(self, ax, title: str, *, y_grid: bool = True) -> None:
        ax.set_facecolor(self.colors["canvas"])
        ax.set_title(title, fontsize=12, color=self.colors["text"], loc="left")
        ax.spines["top"].set_visible(False)
        ax.spines["right"].set_visible(False)
        ax.spines["left"].set_color(self.colors["border"])
        ax.spines["bottom"].set_color(self.colors["border"])
        ax.tick_params(axis="x", labelsize=8.5, colors=self.colors["text"])
        ax.tick_params(axis="y", labelsize=8.5, colors=self.colors["text"])
        if y_grid:
            ax.yaxis.grid(True, color=self.colors["border"], alpha=0.35, linewidth=0.8)
        ax.set_axisbelow(True)

    def _apply_value_formatter(self, ax, kind: str) -> None:
        if kind == "money":
            ax.yaxis.set_major_formatter(FuncFormatter(lambda value, _pos: _fmt_money(value)))
        elif kind == "count":
            ax.yaxis.set_major_formatter(FuncFormatter(lambda value, _pos: _fmt_int(value)))
        elif kind == "pct":
            ax.yaxis.set_major_formatter(FuncFormatter(lambda value, _pos: _fmt_pct(value)))

    def _draw_bar_chart(self, frame_key: str, df: pd.DataFrame, label_col: str, value_col: str, title: str, color: str, kind: str = "money") -> None:
        frame = self.chart_frames[frame_key]
        old_canvas = self.chart_canvases.get(frame_key)
        if old_canvas is not None:
            old_canvas.get_tk_widget().destroy()
        for child in frame.winfo_children():
            child.destroy()

        fig = Figure(figsize=(5.4, 3.0), dpi=100, facecolor=self.colors["canvas"])
        ax = fig.add_subplot(111)

        if df is None or df.empty or label_col not in df.columns or value_col not in df.columns:
            ax.text(0.5, 0.5, "No data yet", ha="center", va="center", fontsize=12, color=self.colors["muted"])
            ax.set_axis_off()
        else:
            plot_df = df.copy().head(12)
            labels = [str(value) for value in plot_df[label_col].tolist()]
            values = [_to_float(value) for value in plot_df[value_col].tolist()]
            ax.bar(range(len(labels)), values, color=color)
            ax.set_xticks(range(len(labels)))
            ax.set_xticklabels(labels, rotation=30, ha="right", fontsize=9)
            self._style_chart_axes(ax, title)
            self._apply_value_formatter(ax, kind)

        fig.tight_layout()
        canvas = FigureCanvasTkAgg(fig, master=frame)
        canvas.draw()
        canvas.get_tk_widget().pack(fill="both", expand=True)
        self.chart_canvases[frame_key] = canvas

    def _draw_horizontal_bar_chart(
        self,
        frame_key: str,
        df: pd.DataFrame,
        label_col: str,
        value_col: str,
        title: str,
        kind: str = "money",
        top_n: int = 8,
    ) -> None:
        frame = self.chart_frames[frame_key]
        old_canvas = self.chart_canvases.get(frame_key)
        if old_canvas is not None:
            old_canvas.get_tk_widget().destroy()
        canvas = self._render_horizontal_bar_chart(frame, df, label_col, value_col, title, kind=kind, top_n=top_n)
        self.chart_canvases[frame_key] = canvas

    def _draw_line_comparison_chart(
        self,
        frame_key: str,
        df: pd.DataFrame,
        title: str,
        series: Sequence[tuple[str, str, str]],
        kind: str = "money",
    ) -> None:
        frame = self.chart_frames[frame_key]
        old_canvas = self.chart_canvases.get(frame_key)
        if old_canvas is not None:
            old_canvas.get_tk_widget().destroy()
        canvas = self._render_line_comparison_chart(frame, df, title, series, kind=kind)
        self.chart_canvases[frame_key] = canvas

    def _render_bar_chart(
        self,
        frame: tk.Frame,
        df: pd.DataFrame,
        label_col: str,
        value_col: str,
        title: str,
        color: str,
        kind: str = "money",
    ) -> FigureCanvasTkAgg:
        for child in frame.winfo_children():
            child.destroy()

        fig = Figure(figsize=(5.4, 3.0), dpi=100, facecolor=self.colors["canvas"])
        ax = fig.add_subplot(111)

        if df is None or df.empty or label_col not in df.columns or value_col not in df.columns:
            ax.text(0.5, 0.5, "No data yet", ha="center", va="center", fontsize=12, color=self.colors["muted"])
            ax.set_axis_off()
        else:
            plot_df = df.copy().head(12)
            labels = [str(value) for value in plot_df[label_col].tolist()]
            values = [_to_float(value) for value in plot_df[value_col].tolist()]
            ax.bar(range(len(labels)), values, color=color)
            ax.set_xticks(range(len(labels)))
            ax.set_xticklabels(labels, rotation=30, ha="right", fontsize=9)
            self._style_chart_axes(ax, title)
            self._apply_value_formatter(ax, kind)

        fig.tight_layout()
        canvas = FigureCanvasTkAgg(fig, master=frame)
        canvas.draw()
        canvas.get_tk_widget().pack(fill="both", expand=True)
        return canvas

    def _render_horizontal_bar_chart(
        self,
        frame: tk.Frame,
        df: pd.DataFrame,
        label_col: str,
        value_col: str,
        title: str,
        *,
        kind: str = "money",
        top_n: int = 8,
    ) -> FigureCanvasTkAgg:
        for child in frame.winfo_children():
            child.destroy()

        fig = Figure(figsize=(5.4, 3.0), dpi=100, facecolor=self.colors["canvas"])
        ax = fig.add_subplot(111)

        if df is None or df.empty or label_col not in df.columns or value_col not in df.columns:
            ax.text(0.5, 0.5, "No data yet", ha="center", va="center", fontsize=12, color=self.colors["muted"])
            ax.set_axis_off()
        else:
            plot_df = df.copy().head(top_n).iloc[::-1]
            labels = [_short_label(value, 22) for value in plot_df[label_col].tolist()]
            values = [_to_float(value) for value in plot_df[value_col].tolist()]
            positions = list(range(len(labels)))
            ax.barh(positions, values, color="#000000", height=0.62)
            ax.set_yticks(positions)
            ax.set_yticklabels(labels, fontsize=8.5)
            self._style_chart_axes(ax, title, y_grid=False)
            ax.xaxis.grid(True, color=self.colors["border"], alpha=0.35, linewidth=0.8)
            if kind == "money":
                ax.xaxis.set_major_formatter(FuncFormatter(lambda value, _pos: _fmt_money(value)))
            elif kind == "count":
                ax.xaxis.set_major_formatter(FuncFormatter(lambda value, _pos: _fmt_int(value)))
            elif kind == "pct":
                ax.xaxis.set_major_formatter(FuncFormatter(lambda value, _pos: _fmt_pct(value)))

        fig.tight_layout()
        canvas = FigureCanvasTkAgg(fig, master=frame)
        canvas.draw()
        canvas.get_tk_widget().pack(fill="both", expand=True)
        return canvas

    def _render_line_comparison_chart(
        self,
        frame: tk.Frame,
        df: pd.DataFrame,
        title: str,
        series: Sequence[tuple[str, str, str]],
        *,
        kind: str = "money",
    ) -> FigureCanvasTkAgg:
        for child in frame.winfo_children():
            child.destroy()

        fig = Figure(figsize=(5.4, 3.0), dpi=100, facecolor=self.colors["canvas"])
        ax = fig.add_subplot(111)

        if df is None or df.empty or "hour_label" not in df.columns:
            ax.text(0.5, 0.5, "No data yet", ha="center", va="center", fontsize=12, color=self.colors["muted"])
            ax.set_axis_off()
        else:
            styles = ["solid", "dashed", "dotted"]
            markers = ["o", "s", "^"]
            x_positions = list(range(len(df)))
            for idx, (column, label, marker_fill) in enumerate(series):
                if column not in df.columns:
                    continue
                values = pd.to_numeric(df[column], errors="coerce")
                ax.plot(
                    x_positions,
                    values,
                    color="#000000",
                    linestyle=styles[idx % len(styles)],
                    linewidth=2.0,
                    marker=markers[idx % len(markers)],
                    markersize=3.8,
                    markerfacecolor=marker_fill,
                    markeredgecolor="#000000",
                    label=label,
                )

            tick_idx = list(range(0, len(x_positions), 3))
            if x_positions and tick_idx[-1] != len(x_positions) - 1:
                tick_idx.append(len(x_positions) - 1)
            tick_labels = [str(df.iloc[i]["hour_label"]) for i in tick_idx]
            ax.set_xticks(tick_idx)
            ax.set_xticklabels(tick_labels, rotation=0)
            self._style_chart_axes(ax, title)
            self._apply_value_formatter(ax, kind)
            legend = ax.legend(frameon=False, fontsize=8.2, loc="upper left")
            for text in legend.get_texts():
                text.set_color(self.colors["text"])

        fig.tight_layout()
        canvas = FigureCanvasTkAgg(fig, master=frame)
        canvas.draw()
        canvas.get_tk_widget().pack(fill="both", expand=True)
        return canvas

    def _rebuild_store_tabs(self, bundle: DashboardBundle) -> None:
        current_tab_id = self.notebook.select()
        selected_store_code = next((code for code, page in self.dynamic_store_pages.items() if str(page) == current_tab_id), None)

        for page in list(self.dynamic_store_pages.values()):
            try:
                self.notebook.forget(page)
            except tk.TclError:
                pass
            self.scroll_canvases.pop(str(page), None)
        self.dynamic_store_pages.clear()

        insert_at = 1
        for snapshot in bundle.snapshots:
            page, content = self._make_scrollable_tab(self.notebook)
            self.notebook.insert(insert_at, page, text=snapshot.store_code.upper())
            self.dynamic_store_pages[snapshot.store_code] = page
            self._build_store_tab_content(content, snapshot, bundle.now_local, bundle.per_store_today.get(snapshot.store_code, pd.DataFrame()))
            insert_at += 1

        if selected_store_code and selected_store_code in self.dynamic_store_pages:
            self.notebook.select(self.dynamic_store_pages[selected_store_code])

    def _build_store_tab_content(self, parent: tk.Frame, snapshot: StoreSnapshot, now_local: datetime, df: pd.DataFrame) -> None:
        parent.grid_columnconfigure(0, weight=1)
        parent.grid_columnconfigure(1, weight=1)
        day = now_local.date()
        display_code = snapshot.store_code.upper()
        yesterday_flat = _items_df_to_flat_sales(snapshot.items_yesterday)
        last_week_flat = _items_df_to_flat_sales(snapshot.items_last_week)
        pace_summary = _build_pace_summary(df, yesterday_flat, last_week_flat, now_local)

        if df.empty:
            metrics = _zero_metrics()
            hourly = pd.DataFrame(columns=["hour_label", "net_revenue", "tickets", "basket", "margin_real"])
            top_products = pd.DataFrame(columns=["Product Name", "net_revenue"])
            categories = pd.DataFrame(columns=["category", "net_revenue", "profit_real", "margin_real", "items"])
            budtenders = pd.DataFrame(columns=["budtender", "net_revenue", "tickets", "basket", "discount_rate"])
            customer_types = pd.DataFrame(columns=["customer_type", "net_revenue", "tickets", "basket"])
            vendors = pd.DataFrame(columns=["Vendor Name", "net_revenue"])
            source_mix = pd.DataFrame(columns=["order_source", "net_revenue", "tickets"])
            order_types = pd.DataFrame(columns=["order_type", "net_revenue", "tickets"])
        else:
            daily = osnap.compute_daily_metrics(df)
            metrics = osnap.metrics_for_day(daily, day)
            hourly = _safe_optional_df(
                osnap.compute_hourly_metrics(df, day),
                ["hour", "net_revenue", "profit_real", "tickets", "basket", "margin_real"],
            )
            if not hourly.empty:
                hourly["hour_label"] = hourly["hour"].apply(lambda hour: osnap.fmt_hour_ampm(int(hour)))
            top_products = _safe_optional_df(osnap.compute_breakdown_net(df, ["Product Name"], day, day, top_n=25), ["Product Name", "net_revenue"])
            categories = _safe_optional_df(osnap.compute_category_summary(df, day, day), ["category", "net_revenue", "profit_real", "margin_real", "items"])
            budtenders = _safe_optional_df(
                osnap.compute_budtender_summary(df, day, day),
                ["budtender", "net_revenue", "tickets", "basket", "discount_rate"],
            )
            customer_types = _safe_optional_df(
                osnap.compute_customer_type_summary(df, day, day),
                ["Customer Type", "net_revenue", "tickets", "basket"],
            )
            if "Customer Type" in customer_types.columns:
                customer_types = customer_types.rename(columns={"Customer Type": "customer_type"})
            vendors = _safe_optional_df(osnap.compute_breakdown_net(df, ["Vendor Name"], day, day, top_n=20), ["Vendor Name", "net_revenue"])
            source_mix = _build_mix_summary(snapshot.transactions_today, "order_source", "order_source")
            order_types = _build_mix_summary(snapshot.transactions_today, "order_type", "order_type")

        today = _summarize_period(snapshot.transactions_today, snapshot.items_today)
        yesterday = _summarize_period(snapshot.transactions_yesterday, snapshot.items_yesterday)
        last_week = _summarize_period(snapshot.transactions_last_week, snapshot.items_last_week)

        summary_card, _, summary_body = self._make_card(
            parent,
            f"{display_code} Store Command View",
            f"{snapshot.store_name} stays isolated here so you can leave this tab open and reference it throughout the day.",
        )
        summary_card.grid(row=0, column=0, columnspan=2, sticky="ew")
        summary_tiles: list[tk.Widget] = []
        tiles = [
            ("Net Revenue", _fmt_money(metrics.get("net_revenue")), "Current same-day net sales."),
            ("Tickets", _fmt_int(metrics.get("tickets")), "Distinct ticket count."),
            ("Avg Ticket", _fmt_money(metrics.get("basket")), "Net revenue per ticket."),
            ("Units", _fmt_decimal(metrics.get("items")), "Total units sold."),
            ("Margin", _fmt_pct(metrics.get("margin_real")), "Real margin from line-item profit."),
            ("Discount Rate", _fmt_pct(metrics.get("discount_rate")), "Discount pressure for this store."),
            ("Last Ticket", _fmt_time(today.get("last_ticket_time")), _minutes_since(today.get("last_ticket_time"), now_local)),
            ("Vs Yesterday", _fmt_delta(_compare_metrics(today["sales_total"], yesterday["sales_total"])), "Same-time checkpoint."),
            ("Vs Last Week", _fmt_delta(_compare_metrics(today["sales_total"], last_week["sales_total"])), "Same weekday checkpoint."),
        ]
        for idx, (title, value, note) in enumerate(tiles):
            tile = tk.Frame(summary_body, bg=self.colors["card_alt"], padx=14, pady=12, highlightbackground=self.colors["border"], highlightthickness=1)
            tk.Label(tile, text=title, bg=self.colors["card_alt"], fg=self.colors["muted"], font=self.fonts["small"], anchor="w").pack(anchor="w")
            tk.Label(tile, text=value, bg=self.colors["card_alt"], fg=self.colors["text"], font=self.fonts["metric"], anchor="w").pack(anchor="w", pady=(6, 0))
            tk.Label(tile, text=note, bg=self.colors["card_alt"], fg=self.colors["muted"], font=self.fonts["small"], anchor="w", justify="left", wraplength=220).pack(anchor="w", pady=(4, 0))
            summary_tiles.append(tile)
        self._bind_responsive_grid(summary_body, summary_tiles, max_columns=4, min_column_width=220, pad_x=10, pad_y=10)

        chart_card, _, chart_body = self._make_card(
            parent,
            f"{display_code} Pace vs Prior Days",
            "Cumulative same-time net sales for today against yesterday and last week.",
        )
        chart_card.grid(row=1, column=0, sticky="nsew", padx=(0, 10), pady=(12, 0))
        chart_frame = tk.Frame(chart_body, bg=self.colors["card"], height=280)
        chart_frame.pack(fill="x", expand=True)
        chart_frame.pack_propagate(False)
        self._render_line_comparison_chart(
            chart_frame,
            pace_summary,
            title=f"{display_code} Cumulative Pace",
            series=[
                ("today_cum", "Today", "#000000"),
                ("yesterday_cum", "Yesterday", "#FFFFFF"),
                ("last_week_cum", "Last Week", "#FFFFFF"),
            ],
            kind="money",
        )

        hourly_chart_card, _, hourly_chart_body = self._make_card(
            parent,
            f"{display_code} Hourly Flow",
            "Hourly net sales bars for the current store.",
        )
        hourly_chart_card.grid(row=1, column=1, sticky="nsew", pady=(12, 0))
        hourly_chart_frame = tk.Frame(hourly_chart_body, bg=self.colors["card"], height=280)
        hourly_chart_frame.pack(fill="x", expand=True)
        hourly_chart_frame.pack_propagate(False)
        self._render_bar_chart(
            hourly_chart_frame,
            hourly,
            label_col="hour_label",
            value_col="net_revenue",
            title=f"{display_code} Hourly Net Sales",
            color=self.colors["accent_dark"],
            kind="money",
        )

        top_products_card = self._build_local_tree_card(
            parent,
            "Top Products",
            "Top same-day products in this store.",
            [("Product Name", "Product", 340), ("net_revenue", "Net", 110)],
            top_products,
            {"net_revenue": _fmt_money},
        )
        category_card = self._build_local_tree_card(
            parent,
            "Category Summary",
            "Category-level performance for this store.",
            [("category", "Category", 180), ("net_revenue", "Net", 110), ("profit_real", "Profit", 110), ("margin_real", "Margin", 90), ("items", "Units", 90)],
            categories,
            {"net_revenue": _fmt_money, "profit_real": _fmt_money, "margin_real": _fmt_pct, "items": _fmt_decimal},
        )

        budtenders_card = self._build_local_tree_card(
            parent,
            "Budtenders",
            "Budtender breakdown for this store.",
            [("budtender", "Budtender", 200), ("net_revenue", "Net", 110), ("tickets", "Tickets", 90), ("basket", "Basket", 100), ("discount_rate", "Disc Rate", 90)],
            budtenders,
            {"net_revenue": _fmt_money, "tickets": _fmt_int, "basket": _fmt_money, "discount_rate": _fmt_pct},
        )
        customer_types_card = self._build_local_tree_card(
            parent,
            "Customer Types",
            "Customer type split for this store.",
            [("customer_type", "Customer Type", 180), ("net_revenue", "Net", 110), ("tickets", "Tickets", 90), ("basket", "Basket", 100)],
            customer_types,
            {"net_revenue": _fmt_money, "tickets": _fmt_int, "basket": _fmt_money},
        )

        vendors_card = self._build_local_tree_card(
            parent,
            "Vendor Leaders",
            "Top vendors for this store today.",
            [("Vendor Name", "Vendor", 300), ("net_revenue", "Net", 110)],
            vendors,
            {"net_revenue": _fmt_money},
        )
        source_mix_card = self._build_local_tree_card(
            parent,
            "Order Channels",
            "Order source and type mix for this store.",
            [("order_source", "Source", 180), ("net_revenue", "Net", 110), ("tickets", "Tickets", 90)],
            source_mix,
            {"net_revenue": _fmt_money, "tickets": _fmt_int},
        )

        order_types_card = self._build_local_tree_card(
            parent,
            "Order Types",
            "In-store, pickup, and other order type mix for this store.",
            [("order_type", "Order Type", 180), ("net_revenue", "Net", 110), ("tickets", "Tickets", 90)],
            order_types,
            {"net_revenue": _fmt_money, "tickets": _fmt_int},
        )
        hourly_table_card = self._build_local_tree_card(
            parent,
            "Hourly Table",
            "Hourly net, ticket count, basket, and margin for this store.",
            [("hour_label", "Hour", 80), ("net_revenue", "Net", 100), ("tickets", "Tickets", 80), ("basket", "Basket", 100), ("margin_real", "Margin", 90)],
            hourly,
            {"net_revenue": _fmt_money, "tickets": _fmt_int, "basket": _fmt_money, "margin_real": _fmt_pct},
        )

        detail_cards = [
            chart_card,
            hourly_chart_card,
            top_products_card,
            category_card,
            budtenders_card,
            customer_types_card,
            vendors_card,
            source_mix_card,
            order_types_card,
            hourly_table_card,
        ]
        self._bind_responsive_grid(parent, detail_cards, max_columns=2, min_column_width=780, row_start=1, pad_x=10, pad_y=12)


def build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(description="Native Tkinter command-center screen for same-day Dutchie sales and inventory.")
    parser.add_argument("--env-file", default=DEFAULT_ENV_FILE, help=f"Path to the .env file. Default: {DEFAULT_ENV_FILE}")
    parser.add_argument("--refresh-seconds", type=int, default=120, help="Default auto-refresh interval shown in the GUI.")
    parser.add_argument("--no-inventory", action="store_true", help="Start with inventory fetching disabled.")
    return parser


def main() -> int:
    parser = build_parser()
    args = parser.parse_args()
    root = tk.Tk()
    app = DutchieLiveDashboardGUI(
        root,
        env_file=args.env_file,
        default_refresh_seconds=args.refresh_seconds,
        default_no_inventory=bool(args.no_inventory),
    )
    try:
        root.mainloop()
    except KeyboardInterrupt:
        pass
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
