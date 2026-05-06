#!/usr/bin/env python3
"""Deterministic insights for brand meeting packets."""

from __future__ import annotations

from datetime import date
from pathlib import Path
from typing import Any, Dict, List, Tuple

import numpy as np
import pandas as pd


def money0(value: Any) -> str:
    try:
        return f"${float(value):,.0f}"
    except Exception:
        return "$0"


def pct1(value: Any) -> str:
    try:
        return f"{float(value) * 100.0:.1f}%"
    except Exception:
        return "0.0%"


def _num(value: Any, default: float = 0.0) -> float:
    try:
        if pd.isna(value):
            return default
        return float(value)
    except Exception:
        return default


def generate_credit_action_items(credit_summary: Dict[str, Any]) -> List[Dict[str, Any]]:
    items: List[Dict[str, Any]] = []
    gap = _num(credit_summary.get("credit_gap"))
    target = _num(credit_summary.get("target_margin"), 0.35)
    received_margin = _num(credit_summary.get("received_credit_margin"))
    credit_needed = _num(credit_summary.get("credit_needed_to_hit_target"))
    if gap > 1000:
        items.append({
            "priority": "High",
            "category": "Credit Follow-Up",
            "problem": "Outstanding credit gap",
            "evidence": f"Expected support exceeds received support by {money0(gap)}.",
            "brand_action": "Pay outstanding credit",
            "store": "",
            "product": "",
            "category_name": "",
            "dollar_amount": gap,
        })
    elif gap > 250:
        items.append({
            "priority": "Medium",
            "category": "Credit Follow-Up",
            "problem": "Partial support received",
            "evidence": f"Open credit gap is {money0(gap)}.",
            "brand_action": "Confirm invoice credit",
            "store": "",
            "product": "",
            "category_name": "",
            "dollar_amount": gap,
        })
    if received_margin + 0.05 < target and credit_needed > 0:
        items.append({
            "priority": "High",
            "category": "Margin Support",
            "problem": "Received-support margin is below target",
            "evidence": f"Received credit margin is {pct1(received_margin)} vs target {pct1(target)}.",
            "brand_action": "Fund additional credit",
            "store": "",
            "product": "",
            "category_name": "",
            "dollar_amount": credit_needed,
        })
    return items


def generate_margin_action_items(metrics: Dict[str, Any], targets: Dict[str, float]) -> List[Dict[str, Any]]:
    items: List[Dict[str, Any]] = []
    margin = _num(metrics.get("margin_real"))
    net = _num(metrics.get("net_revenue"))
    profit = _num(metrics.get("profit_real"))
    target = _num(targets.get("target_margin"), 0.35)
    if margin + 0.05 < target and net > 0:
        needed = max(target * net - profit, 0.0)
        items.append({
            "priority": "High",
            "category": "Margin Support",
            "problem": "Real margin is below target",
            "evidence": f"Real margin is {pct1(margin)} on {money0(net)} sales.",
            "brand_action": "Lower invoice cost",
            "store": "",
            "product": "",
            "category_name": "",
            "dollar_amount": needed,
        })
    elif margin + 0.02 < target and net > 0:
        items.append({
            "priority": "Medium",
            "category": "Margin Support",
            "problem": "Margin needs improvement",
            "evidence": f"Real margin is {pct1(margin)} vs target {pct1(target)}.",
            "brand_action": "Increase kickback percentage",
            "store": "",
            "product": "",
            "category_name": "",
            "dollar_amount": 0.0,
        })
    return items


def generate_inventory_action_items(inv_products: pd.DataFrame, targets: Dict[str, float]) -> List[Dict[str, Any]]:
    items: List[Dict[str, Any]] = []
    if inv_products is None or inv_products.empty:
        return items
    tmp = inv_products.copy()
    for col in ["inventory_value", "units_available", "days_of_supply", "trend_units_per_day_30d"]:
        if col not in tmp.columns:
            tmp[col] = 0.0
        tmp[col] = pd.to_numeric(tmp[col], errors="coerce").replace([np.inf, -np.inf], np.nan).fillna(0.0)
    tmp = tmp.sort_values(["days_of_supply", "inventory_value"], ascending=False)
    high = tmp[(tmp["inventory_value"] >= 2500) & ((tmp["days_of_supply"] > _num(targets.get("max_days_supply"), 60)) | (tmp["trend_units_per_day_30d"] <= 0))]
    if not high.empty:
        r = high.iloc[0]
        product = str(r.get("display_product", r.get("product_group_display", "Product")))
        days = _num(r.get("days_of_supply"))
        items.append({
            "priority": "High" if days > 90 or _num(r.get("trend_units_per_day_30d")) <= 0 else "Medium",
            "category": "Slow-Moving Inventory",
            "problem": "Inventory risk needs brand support",
            "evidence": f"{product} has {money0(r.get('inventory_value'))} on hand and {days:.1f} days of supply.",
            "brand_action": "Fund markdown",
            "store": str(r.get("_store_abbr", "")),
            "product": product,
            "category_name": str(r.get("category_normalized", "")),
            "dollar_amount": _num(r.get("inventory_value")),
        })
    return items


def generate_discount_action_items(metrics: Dict[str, Any], targets: Dict[str, float]) -> List[Dict[str, Any]]:
    discount_rate = _num(metrics.get("discount_rate"))
    margin = _num(metrics.get("margin_real"))
    max_discount = _num(targets.get("max_discount_rate"), 0.45)
    target_margin = _num(targets.get("target_margin"), 0.35)
    if discount_rate > max_discount and margin < target_margin:
        return [{
            "priority": "High",
            "category": "Discount Strategy",
            "problem": "High discounts are not protecting margin",
            "evidence": f"Discount rate is {pct1(discount_rate)} and real margin is {pct1(margin)}.",
            "brand_action": "Reduce required discount",
            "store": "",
            "product": "",
            "category_name": "",
            "dollar_amount": _num(metrics.get("discount")),
        }]
    return []


def generate_store_action_items(store_df: pd.DataFrame, targets: Dict[str, float]) -> List[Dict[str, Any]]:
    if store_df is None or store_df.empty:
        return []
    tmp = store_df.copy()
    for col in ["net_revenue", "margin_real", "discount_rate"]:
        if col not in tmp.columns:
            tmp[col] = 0.0
        tmp[col] = pd.to_numeric(tmp[col], errors="coerce").fillna(0.0)
    target_margin = _num(targets.get("target_margin"), 0.35)
    bad = tmp[(tmp["net_revenue"] > 0) & (tmp["margin_real"] + 0.03 < target_margin)].sort_values("net_revenue", ascending=False)
    if bad.empty:
        return []
    r = bad.iloc[0]
    store = str(r.get("_store_abbr", ""))
    return [{
        "priority": "Medium",
        "category": "Store-Level Support",
        "problem": "Store margin trails target",
        "evidence": f"{store} margin is {pct1(r.get('margin_real'))} on {money0(r.get('net_revenue'))} sales.",
        "brand_action": "Send rep for store training",
        "store": store,
        "product": "",
        "category_name": "",
        "dollar_amount": 0.0,
    }]


def generate_brand_action_items(
    metrics: Dict[str, Any],
    credit_summary: Dict[str, Any],
    inv_products: pd.DataFrame,
    store_df: pd.DataFrame,
    targets: Dict[str, float],
    max_items: int = 8,
) -> List[Dict[str, Any]]:
    items: List[Dict[str, Any]] = []
    items.extend(generate_credit_action_items(credit_summary))
    items.extend(generate_margin_action_items(metrics, targets))
    items.extend(generate_inventory_action_items(inv_products, targets))
    items.extend(generate_discount_action_items(metrics, targets))
    items.extend(generate_store_action_items(store_df, targets))
    priority_rank = {"High": 0, "Medium": 1, "Low": 2}
    dedup: List[Dict[str, Any]] = []
    seen = set()
    for item in sorted(items, key=lambda x: (priority_rank.get(x.get("priority", "Low"), 9), -_num(x.get("dollar_amount")))):
        key = (item.get("category"), item.get("problem"), item.get("store"), item.get("product"))
        if key in seen:
            continue
        seen.add(key)
        dedup.append(item)
    return dedup[:max_items]


def generate_brand_health_score(
    metrics: Dict[str, Any],
    credit_summary: Dict[str, Any],
    inv_overview: Dict[str, Any],
    store_df: pd.DataFrame,
    targets: Dict[str, float],
) -> Tuple[int, str, str]:
    target_margin = _num(targets.get("target_margin"), 0.35)
    max_discount = _num(targets.get("max_discount_rate"), 0.45)
    max_days = _num(targets.get("max_days_supply"), 60)
    net = _num(metrics.get("net_revenue"))
    margin = _num(metrics.get("margin_real"))
    received_margin = _num(credit_summary.get("received_credit_margin"), margin)
    discount = _num(metrics.get("discount_rate"))
    days_supply = _num(inv_overview.get("days_of_supply"), 0.0)
    credit_gap = _num(credit_summary.get("credit_gap"))

    score = 0.0
    score += 20.0 if net > 0 else 0.0
    score += min(max(margin / max(target_margin, 0.01), 0.0), 1.2) / 1.2 * 25.0
    score += min(max(received_margin / max(target_margin, 0.01), 0.0), 1.2) / 1.2 * 20.0
    score += 15.0 if days_supply <= max_days or days_supply == 0 else max(0.0, 15.0 * (max_days / max(days_supply, 1.0)))
    score += 10.0 if discount <= max_discount else max(0.0, 10.0 * (max_discount / max(discount, 0.01)))
    if store_df is not None and not store_df.empty and "margin_real" in store_df.columns:
        margins = pd.to_numeric(store_df["margin_real"], errors="coerce").dropna()
        consistency = 1.0 - min(float(margins.std() or 0.0), 0.20) / 0.20 if not margins.empty else 0.5
        score += max(0.0, consistency) * 10.0
    else:
        score += 5.0
    if credit_gap > 1000:
        score -= min(15.0, credit_gap / 1000.0)
    if margin < target_margin and discount > max_discount:
        score = min(score, 84.0)
    if received_margin < target_margin and credit_gap > 1000:
        score = min(score, 84.0)
    if margin + 0.05 < target_margin and discount > max_discount and credit_gap > 1000:
        score = min(score, 69.0)
    if days_supply > max_days * 1.5 and margin < target_margin:
        score = min(score, 69.0)
    final = int(round(max(0.0, min(score, 100.0))))
    if final >= 85:
        status = "Strong"
    elif final >= 70:
        status = "Good"
    elif final >= 55:
        status = "Watch"
    else:
        status = "Needs Support"
    reasons = []
    if margin < target_margin:
        reasons.append("margin below target")
    if credit_gap > 0:
        reasons.append("credit gap open")
    if days_supply > max_days:
        reasons.append("inventory risk building")
    if discount > max_discount:
        reasons.append("discounting elevated")
    reason = ", ".join(reasons[:2]) if reasons else "healthy margin and support profile"
    return final, status, reason


def generate_meeting_ask(credit_summary: Dict[str, Any], action_items: List[Dict[str, Any]]) -> str:
    gap = _num(credit_summary.get("credit_gap"))
    needed = _num(credit_summary.get("credit_needed_to_hit_target"))
    if gap > 0:
        return f"Collect {money0(gap)} outstanding credit/support and payment reference."
    if needed > 0:
        return f"Request {money0(needed)} additional support to reach target margin."
    for item in action_items:
        category = str(item.get("category") or "").lower()
        action = str(item.get("brand_action") or "").strip()
        amount = _num(item.get("dollar_amount"))
        if "inventory" in category and amount > 0:
            return f"Ask brand to fund markdown or buyback for {money0(amount)} at-risk inventory."
        if "discount" in category:
            return "Replace blanket discounting with funded bundle support."
        if "replenishment" in category or "fast" in category:
            return "Prioritize restock of the fastest-moving product groups."
        if action.lower() in {"fund markdown", "buy back", "transfer"} and amount > 0:
            return f"Ask brand to {action.lower()} for {money0(amount)} inventory risk."
    for item in action_items:
        action = str(item.get("brand_action") or "").strip()
        product = str(item.get("product") or "").strip()
        if action and product:
            return f"Ask brand to {action.lower()} for {product}."
        if action:
            return f"Ask brand to {action.lower()}."
    return "Confirm next promo plan, margin support, and replenishment priorities."


def load_monthly_reference(brand: str, start_day: date, end_day: date, reports_root: Path = Path("reports/monthly/data")) -> Dict[str, Any]:
    month_key = start_day.strftime("%Y-%m")
    folder = Path(reports_root) / month_key
    result: Dict[str, Any] = {"available": False, "month": month_key, "folder": str(folder), "brand_rows": pd.DataFrame(), "inventory_rows": pd.DataFrame(), "notes": []}
    if not folder.exists():
        result["notes"].append("Monthly owner data folder not found.")
        return result
    result["available"] = True
    brand_l = str(brand).strip().lower()
    brand_csv = folder / "monthly_brand_summary.csv"
    if brand_csv.exists():
        try:
            df = pd.read_csv(brand_csv)
            brand_cols = [c for c in ["brand", "Brand", "canonical_brand", "brand_name"] if c in df.columns]
            if brand_cols:
                col = brand_cols[0]
                result["brand_rows"] = df[df[col].fillna("").astype(str).str.lower() == brand_l].copy()
        except Exception as exc:
            result["notes"].append(f"Could not read brand summary: {exc}")
    inv_csv = folder / "monthly_inventory_watchlist_products.csv"
    if inv_csv.exists():
        try:
            df = pd.read_csv(inv_csv)
            brand_cols = [c for c in ["Brand", "brand", "canonical_brand"] if c in df.columns]
            if brand_cols:
                col = brand_cols[0]
                result["inventory_rows"] = df[df[col].fillna("").astype(str).str.lower() == brand_l].copy()
        except Exception as exc:
            result["notes"].append(f"Could not read inventory watchlist: {exc}")
    return result


def build_followup_text(
    brand: str,
    start_day: date,
    end_day: date,
    metrics: Dict[str, Any],
    credit_summary: Dict[str, Any],
    action_items: List[Dict[str, Any]],
    meeting_ask: str,
) -> str:
    lines = [
        f"Subject: Follow-up: {brand} performance + support request",
        "",
        "Summary:",
        f"{brand} generated {money0(metrics.get('net_revenue'))} net revenue from {metrics.get('items', 0):,.0f} units sold.",
        f"Real margin was {pct1(metrics.get('margin_real'))}; received-support margin was {pct1(credit_summary.get('received_credit_margin'))}; target margin is {pct1(credit_summary.get('target_margin'))}.",
        "",
        "Open support:",
        f"Expected credit/support: {money0(credit_summary.get('expected_credit_amount'))}",
        f"Received credit/support: {money0(credit_summary.get('received_credit_amount'))}",
        f"Gap: {money0(credit_summary.get('credit_gap'))}",
        "",
        "Requested action:",
        meeting_ask,
        "",
        "Next steps:",
    ]
    for idx, item in enumerate(action_items[:4], start=1):
        lines.append(f"{idx}. {item.get('brand_action', 'Review support')} - {item.get('evidence', '')}")
    if not action_items:
        lines.append("1. Confirm next promo plan and margin support.")
    lines.append("")
    lines.append(f"Window: {start_day.isoformat()} to {end_day.isoformat()}")
    return "\n".join(lines)
