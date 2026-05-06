#!/usr/bin/env python3
"""Manual brand credit ledger utilities for brand meeting packets."""

from __future__ import annotations

import csv
import json
import uuid
from dataclasses import dataclass, field
from datetime import date, datetime
from pathlib import Path
from typing import Any, Dict, Iterable, List, Optional, Tuple

import pandas as pd


CREDIT_TYPES = [
    "Cash rebate",
    "Invoice credit",
    "COGS kickback",
    "Per-unit kickback",
    "Flat promo credit",
    "Markdown support",
    "Buyback credit",
    "Co-op marketing",
    "Demo/event support",
    "Display/sample credit",
    "Other",
]

BASIS_TYPES = [
    "flat_amount",
    "percent_of_cogs",
    "percent_of_sales",
    "per_unit",
    "manual_adjustment",
]

STATUS_TYPES = ["expected", "partial", "received", "disputed", "overdue", "written_off"]

DEFAULT_LEDGER = {
    "version": 1,
    "updated_at": "",
    "credits": [],
}


@dataclass
class BrandCredit:
    id: str
    brand: str
    canonical_brand: str
    store_code: str = ""
    category: str = ""
    product: str = ""
    start_date: str = ""
    end_date: str = ""
    credit_type: str = "Other"
    basis: str = "manual_adjustment"
    expected_amount: float = 0.0
    received_amount: float = 0.0
    expected_percent: Optional[float] = None
    received_percent: Optional[float] = None
    per_unit_amount: Optional[float] = None
    status: str = "expected"
    invoice_reference: str = ""
    payment_reference: str = ""
    notes: str = ""
    apply_to_margin: bool = True
    manual_override_expected: bool = False
    created_at: str = ""
    updated_at: str = ""
    created_by: str = ""
    extra: Dict[str, Any] = field(default_factory=dict)


def _now_iso() -> str:
    return datetime.now().replace(microsecond=0).isoformat()


def _parse_day(value: Any) -> Optional[date]:
    if value is None:
        return None
    text = str(value).strip()
    if not text:
        return None
    try:
        return date.fromisoformat(text[:10])
    except ValueError:
        return None


def _num(value: Any, default: float = 0.0) -> float:
    if value is None:
        return default
    text = str(value).strip()
    if not text:
        return default
    text = text.replace("$", "").replace(",", "").replace("%", "")
    try:
        return float(text)
    except ValueError:
        return default


def _pct(value: Any) -> Optional[float]:
    if value is None or str(value).strip() == "":
        return None
    v = _num(value, 0.0)
    if abs(v) > 1.0:
        v = v / 100.0
    return max(0.0, min(v, 10.0))


def _bool(value: Any, default: bool = True) -> bool:
    if value is None or value == "":
        return default
    if isinstance(value, bool):
        return value
    return str(value).strip().lower() in {"1", "true", "yes", "y", "on"}


def make_credit_id(brand: str, start_date: str = "") -> str:
    slug = "".join(ch for ch in str(brand).upper() if ch.isalnum())[:18] or "BRAND"
    ym = str(start_date).replace("-", "")[:6] or datetime.now().strftime("%Y%m")
    return f"CRD-{ym}-{slug}-{uuid.uuid4().hex[:6].upper()}"


def normalize_credit_record(record: Dict[str, Any]) -> Dict[str, Any]:
    now = _now_iso()
    brand = str(record.get("brand") or record.get("canonical_brand") or "").strip()
    canonical = str(record.get("canonical_brand") or brand).strip()
    start = str(record.get("start_date") or "").strip()
    rec = {
        "id": str(record.get("id") or make_credit_id(canonical or brand, start)).strip(),
        "brand": brand,
        "canonical_brand": canonical,
        "store_code": str(record.get("store_code") or record.get("store") or "").strip().upper(),
        "category": str(record.get("category") or "").strip(),
        "product": str(record.get("product") or "").strip(),
        "start_date": start,
        "end_date": str(record.get("end_date") or "").strip(),
        "credit_type": str(record.get("credit_type") or "Other").strip(),
        "basis": str(record.get("basis") or "manual_adjustment").strip(),
        "expected_amount": _num(record.get("expected_amount"), 0.0),
        "received_amount": _num(record.get("received_amount"), 0.0),
        "expected_percent": _pct(record.get("expected_percent")),
        "received_percent": _pct(record.get("received_percent")),
        "per_unit_amount": _num(record.get("per_unit_amount"), 0.0) if str(record.get("per_unit_amount", "")).strip() else None,
        "status": str(record.get("status") or "expected").strip().lower(),
        "invoice_reference": str(record.get("invoice_reference") or "").strip(),
        "payment_reference": str(record.get("payment_reference") or "").strip(),
        "notes": str(record.get("notes") or "").strip(),
        "apply_to_margin": _bool(record.get("apply_to_margin"), True),
        "manual_override_expected": _bool(record.get("manual_override_expected"), False),
        "source": str(record.get("source") or "manual").strip().lower(),
        "external_id": str(record.get("external_id") or "").strip(),
        "created_at": str(record.get("created_at") or now).strip(),
        "updated_at": str(record.get("updated_at") or now).strip(),
        "created_by": str(record.get("created_by") or "").strip(),
    }
    if rec["credit_type"] not in CREDIT_TYPES:
        rec["credit_type"] = "Other"
    if rec["basis"] not in BASIS_TYPES:
        rec["basis"] = "manual_adjustment"
    if rec["status"] not in STATUS_TYPES:
        rec["status"] = "expected"
    if rec["received_amount"] > 0 and rec["received_amount"] < rec["expected_amount"] and rec["status"] == "expected":
        rec["status"] = "partial"
    if rec["expected_amount"] > 0 and rec["received_amount"] >= rec["expected_amount"] and rec["status"] in {"expected", "partial"}:
        rec["status"] = "received"
    return rec


def ensure_credit_ledger(path: Path) -> None:
    path = Path(path)
    if path.exists():
        return
    payload = dict(DEFAULT_LEDGER)
    payload["updated_at"] = _now_iso()
    path.write_text(json.dumps(payload, indent=2) + "\n", encoding="utf-8")


def load_credit_ledger(path: Path) -> List[Dict[str, Any]]:
    path = Path(path)
    ensure_credit_ledger(path)
    try:
        payload = json.loads(path.read_text(encoding="utf-8"))
    except Exception:
        payload = dict(DEFAULT_LEDGER)
    credits = payload.get("credits", []) if isinstance(payload, dict) else []
    if not isinstance(credits, list):
        credits = []
    return [normalize_credit_record(c) for c in credits if isinstance(c, dict)]


def save_credit_ledger(path: Path, credits: Iterable[Dict[str, Any]]) -> None:
    path = Path(path)
    rows = [normalize_credit_record(c) for c in credits]
    payload = {
        "version": 1,
        "updated_at": _now_iso(),
        "credits": rows,
    }
    path.write_text(json.dumps(payload, indent=2) + "\n", encoding="utf-8")


def ledger_to_dataframe(credits: Iterable[Dict[str, Any]]) -> pd.DataFrame:
    return pd.DataFrame([normalize_credit_record(c) for c in credits])


def import_credit_csv(path: Path) -> List[Dict[str, Any]]:
    with Path(path).open("r", newline="", encoding="utf-8-sig") as f:
        return [normalize_credit_record(row) for row in csv.DictReader(f)]


def export_credit_csv(path: Path, credits: Iterable[Dict[str, Any]]) -> None:
    rows = [normalize_credit_record(c) for c in credits]
    path = Path(path)
    fieldnames = list(normalize_credit_record({}).keys())
    with path.open("w", newline="", encoding="utf-8") as f:
        writer = csv.DictWriter(f, fieldnames=fieldnames, extrasaction="ignore")
        writer.writeheader()
        writer.writerows(rows)


def filter_credits_for_brand_window(
    credits: Iterable[Dict[str, Any]],
    brand: str,
    start_day: date,
    end_day: date,
) -> List[Dict[str, Any]]:
    brand_l = str(brand).strip().lower()
    out: List[Dict[str, Any]] = []
    for row in credits:
        rec = normalize_credit_record(row)
        rec_brand = str(rec.get("canonical_brand") or rec.get("brand") or "").strip().lower()
        if rec_brand != brand_l:
            continue
        cs = _parse_day(rec.get("start_date")) or start_day
        ce = _parse_day(rec.get("end_date")) or end_day
        if ce < start_day or cs > end_day:
            continue
        out.append(rec)
    return out


def _matching_sales(df: pd.DataFrame, credit: Dict[str, Any]) -> pd.DataFrame:
    if df is None or df.empty:
        return pd.DataFrame()
    out = df.copy()
    store = str(credit.get("store_code") or "").strip().upper()
    if store and "_store_abbr" in out.columns:
        out = out[out["_store_abbr"].fillna("").astype(str).str.upper() == store].copy()
    category = str(credit.get("category") or "").strip().lower()
    if category and "category_normalized" in out.columns:
        out = out[out["category_normalized"].fillna("").astype(str).str.lower() == category].copy()
    product = str(credit.get("product") or "").strip().lower()
    if product:
        product_cols = [c for c in ["product_group_display", "display_product", "_product_raw", "Product", "Product Name"] if c in out.columns]
        if product_cols:
            mask = pd.Series(False, index=out.index)
            for col in product_cols:
                mask = mask | out[col].fillna("").astype(str).str.lower().str.contains(product, regex=False)
            out = out[mask].copy()
    return out


def _sales_amounts(df: pd.DataFrame) -> Tuple[float, float, float]:
    if df is None or df.empty:
        return 0.0, 0.0, 0.0
    net = pd.to_numeric(df.get("_net", 0.0), errors="coerce").fillna(0.0).sum()
    cogs = pd.to_numeric(df.get("_cogs_real", 0.0), errors="coerce").fillna(0.0).sum()
    units = pd.to_numeric(df.get("_qty", 0.0), errors="coerce").fillna(0.0).sum()
    return float(net), float(cogs), float(units)


def _allocated_amounts(credit: Dict[str, Any], matched_df: pd.DataFrame) -> Tuple[float, float]:
    basis = str(credit.get("basis") or "manual_adjustment")
    net, cogs, units = _sales_amounts(matched_df)
    expected = float(credit.get("expected_amount") or 0.0)
    received = float(credit.get("received_amount") or 0.0)

    if basis == "percent_of_cogs":
        if credit.get("expected_percent") is not None:
            expected = cogs * float(credit.get("expected_percent") or 0.0)
        if credit.get("received_percent") is not None:
            received = cogs * float(credit.get("received_percent") or 0.0)
    elif basis == "percent_of_sales":
        if credit.get("expected_percent") is not None:
            expected = net * float(credit.get("expected_percent") or 0.0)
        if credit.get("received_percent") is not None:
            received = net * float(credit.get("received_percent") or 0.0)
    elif basis == "per_unit":
        per_unit = float(credit.get("per_unit_amount") or 0.0)
        if per_unit and not expected:
            expected = units * per_unit
        if per_unit and not received and str(credit.get("status")) == "received":
            received = units * per_unit
    return max(expected, 0.0), max(received, 0.0)


def summarize_credit_reconciliation(
    credits: Iterable[Dict[str, Any]],
    sales_df: pd.DataFrame,
    brand: str,
    start_day: date,
    end_day: date,
    target_margin: float = 0.35,
    system_expected_credit: float = 0.0,
) -> Tuple[Dict[str, Any], pd.DataFrame]:
    matched = filter_credits_for_brand_window(credits, brand, start_day, end_day)
    rows: List[Dict[str, Any]] = []
    manual_expected = 0.0
    manual_received = 0.0
    manual_override_expected = 0.0
    has_external_expected_override = False

    for credit in matched:
        sales_match = _matching_sales(sales_df, credit)
        expected, received = _allocated_amounts(credit, sales_match)
        apply_to_margin = bool(credit.get("apply_to_margin", True))
        if not apply_to_margin:
            expected_for_margin = 0.0
            received_for_margin = 0.0
        else:
            expected_for_margin = expected
            received_for_margin = received
        if credit.get("manual_override_expected"):
            manual_override_expected += expected_for_margin
            if expected_for_margin > 0:
                has_external_expected_override = True
        else:
            manual_expected += expected_for_margin
        manual_received += received_for_margin
        gap = max(expected - received, 0.0)
        store_code = str(credit.get("store_code") or "").strip().upper()
        category = str(credit.get("category") or "").strip()
        product = str(credit.get("product") or "").strip()
        scope_bits = []
        if store_code:
            scope_bits.append(store_code)
        if category:
            scope_bits.append(category)
        if product:
            scope_bits.append(product)
        scope = " / ".join(scope_bits) if scope_bits else "Brand"
        rows.append({
            "Type": credit.get("credit_type", "Other"),
            "Scope": scope,
            "Store": store_code,
            "Category": category,
            "Product": product,
            "Expected": expected,
            "Received": received,
            "Gap": gap,
            "Status": credit.get("status", "expected"),
            "Margin Lift Expected": 0.0,
            "Margin Lift Received": 0.0,
            "Applied Expected": expected_for_margin,
            "Applied Received": received_for_margin,
            "Included In Margin": apply_to_margin,
            "Notes": credit.get("notes", ""),
            "Credit ID": credit.get("id", ""),
            "External ID": credit.get("external_id", ""),
            "Invoice Reference": credit.get("invoice_reference", ""),
            "Payment Reference": credit.get("payment_reference", ""),
            "Source": credit.get("source", "manual"),
        })

    if system_expected_credit > 0:
        system_is_reference_only = has_external_expected_override
        rows.insert(0, {
            "Type": "System expected credit" if not system_is_reference_only else "System expected credit (reference)",
            "Scope": "Brand",
            "Store": "",
            "Category": "",
            "Product": "",
            "Expected": float(system_expected_credit),
            "Received": 0.0,
            "Gap": 0.0 if system_is_reference_only else float(system_expected_credit),
            "Status": "reference" if system_is_reference_only else "expected",
            "Margin Lift Expected": 0.0,
            "Margin Lift Received": 0.0,
            "Applied Expected": 0.0 if system_is_reference_only else float(system_expected_credit),
            "Applied Received": 0.0,
            "Included In Margin": not system_is_reference_only,
            "Notes": (
                "Calculated from deals.py rules; shown as reference because ERP/CreditFlow expected credits exist."
                if system_is_reference_only
                else "Calculated from deals.py rules when kickback adjustments are enabled."
            ),
            "Credit ID": "SYSTEM-DEALS",
            "External ID": "",
            "Invoice Reference": "",
            "Payment Reference": "",
            "Source": "system",
        })

    net = float(pd.to_numeric(sales_df.get("_net", pd.Series(dtype=float)), errors="coerce").fillna(0.0).sum()) if sales_df is not None else 0.0
    real_profit = float(pd.to_numeric(sales_df.get("_profit_real", pd.Series(dtype=float)), errors="coerce").fillna(0.0).sum()) if sales_df is not None else 0.0
    expected_support = (manual_override_expected if manual_override_expected > 0 else float(system_expected_credit or 0.0)) + manual_expected
    received_support = manual_received
    credit_gap = max(expected_support - received_support, 0.0)
    expected_profit = real_profit + expected_support
    received_profit = real_profit + received_support
    real_margin = (real_profit / net) if net else 0.0
    expected_margin = (expected_profit / net) if net else 0.0
    received_margin = (received_profit / net) if net else 0.0
    credit_needed = max((float(target_margin) * net) - real_profit - received_support, 0.0)

    reconciliation = pd.DataFrame(rows)
    if not reconciliation.empty and net:
        reconciliation["Margin Lift Expected"] = reconciliation["Applied Expected"].astype(float) / net
        reconciliation["Margin Lift Received"] = reconciliation["Applied Received"].astype(float) / net

    summary = {
        "brand": brand,
        "target_margin": float(target_margin),
        "net_revenue": net,
        "real_profit": real_profit,
        "real_margin": real_margin,
        "system_expected_credit": float(system_expected_credit or 0.0),
        "system_expected_reference_only": bool(has_external_expected_override and system_expected_credit > 0),
        "manual_expected_credit": manual_expected + manual_override_expected,
        "manual_received_credit": manual_received,
        "expected_credit_amount": expected_support,
        "received_credit_amount": received_support,
        "credit_gap": credit_gap,
        "expected_credit_profit": expected_profit,
        "received_credit_profit": received_profit,
        "expected_credit_margin": expected_margin,
        "received_credit_margin": received_margin,
        "credit_needed_to_hit_target": credit_needed,
        "credit_rows": len(rows),
        "creditflow_rows": int((reconciliation.get("Source", pd.Series(dtype=str)).astype(str).str.lower() == "creditflow").sum()) if not reconciliation.empty else 0,
        "manual_rows": int((reconciliation.get("Source", pd.Series(dtype=str)).astype(str).str.lower() == "manual").sum()) if not reconciliation.empty else 0,
        "overdue_rows": int((reconciliation.get("Status", pd.Series(dtype=str)).astype(str).str.lower() == "overdue").sum()) if not reconciliation.empty else 0,
        "disputed_rows": int((reconciliation.get("Status", pd.Series(dtype=str)).astype(str).str.lower() == "disputed").sum()) if not reconciliation.empty else 0,
    }
    return summary, reconciliation
