#!/usr/bin/env python3
"""Target-margin configuration for brand meeting packets."""

from __future__ import annotations

import json
from datetime import datetime
from pathlib import Path
from typing import Any, Dict


DEFAULT_TARGETS = {
    "default_target_margin": 0.35,
    "default_max_discount_rate": 0.45,
    "default_max_days_supply": 60,
    "default_min_sell_through": 0.25,
    "brand_targets": {},
}


def _now_iso() -> str:
    return datetime.now().replace(microsecond=0).isoformat()


def ensure_targets_file(path: Path) -> None:
    path = Path(path)
    if path.exists():
        return
    payload = dict(DEFAULT_TARGETS)
    payload["updated_at"] = _now_iso()
    path.write_text(json.dumps(payload, indent=2) + "\n", encoding="utf-8")


def load_targets(path: Path) -> Dict[str, Any]:
    path = Path(path)
    ensure_targets_file(path)
    try:
        payload = json.loads(path.read_text(encoding="utf-8"))
    except Exception:
        payload = dict(DEFAULT_TARGETS)
    if not isinstance(payload, dict):
        payload = dict(DEFAULT_TARGETS)
    merged = dict(DEFAULT_TARGETS)
    merged.update(payload)
    if not isinstance(merged.get("brand_targets"), dict):
        merged["brand_targets"] = {}
    return merged


def save_targets(path: Path, payload: Dict[str, Any]) -> None:
    out = dict(DEFAULT_TARGETS)
    out.update(payload or {})
    out["updated_at"] = _now_iso()
    Path(path).write_text(json.dumps(out, indent=2) + "\n", encoding="utf-8")


def get_brand_targets(payload: Dict[str, Any], brand: str) -> Dict[str, float]:
    brand_targets = payload.get("brand_targets", {}) if isinstance(payload, dict) else {}
    target = None
    brand_l = str(brand).strip().lower()
    for key, vals in brand_targets.items():
        if str(key).strip().lower() == brand_l and isinstance(vals, dict):
            target = vals
            break
    target = target or {}
    return {
        "target_margin": float(target.get("target_margin", payload.get("default_target_margin", 0.35))),
        "max_discount_rate": float(target.get("max_discount_rate", payload.get("default_max_discount_rate", 0.45))),
        "max_days_supply": float(target.get("max_days_supply", payload.get("default_max_days_supply", 60))),
        "min_sell_through": float(target.get("min_sell_through", payload.get("default_min_sell_through", 0.25))),
    }
