#!/usr/bin/env python3
"""CreditFlow API client and normalization helpers for brand meeting packets."""

from __future__ import annotations

import json
import re
import urllib.error
import urllib.parse
import urllib.request
from datetime import date, datetime, timedelta
from pathlib import Path
from typing import Any, Dict, Iterable, List, Optional, Sequence, Tuple


DEFAULT_BASE_URL = "https://creditflow.replit.app/api/v1"
DEFAULT_ENV_KEYS = ("CREDITFLOW_API_KEY", "creditflow", "CREDITFLOW", "CREDIT_FLOW_API_KEY")

STORE_CODE_BY_NAME = {
    "mission valley": "MV",
    "mv": "MV",
    "la mesa": "LM",
    "lemon grove": "LG",
    "sorrento valley": "SV",
    "sv": "SV",
    "national city": "NC",
    "wildomar": "WP",
    "wp": "WP",
}

COMMON_BRAND_ALIASES = {
    "goodgood": "goodgood",
    "good good": "goodgood",
    "claybourne co": "claybourne",
    "claybourne": "claybourne",
    "cbx": "cannabiotix cbx",
    "cannabiotix": "cannabiotix cbx",
    "cannabiotix cbx": "cannabiotix cbx",
    "710": "710 labs",
    "710 labs": "710 labs",
    "710labs": "710 labs",
    "kiva": "kiva",
    "kanha": "kanha",
    "wyld": "wyld goodtide",
    "goodtide": "wyld goodtide",
    "good tide": "wyld goodtide",
}

CREDITFLOW_VENDOR_CODE_BY_BRAND = {
    "710": "V030",
    "Almora": "V010",
    "American Weed": "V056",
    "Autumn Brands": "V084",
    "Baba Ku": "V080",
    "Ball Family Farms": "V059",
    "BigPetes": "V004",
    "Blem": "V029",
    "Cake": "V021",
    "Cam": "V058",
    "CANN": "V085",
    "Cannabiotix (CBX)": "V064",
    "Claybourne": "V038",
    "CLSICS": "V066",
    "ColdFire": "V079",
    "Cream of the Crop": "V035",
    "Dab Daddy": "V062",
    "Dabwoods": "V006",
    "Decibel": "V042",
    "Dixie": "V028",
    "Dr. Norms": "V067",
    "Drops": "V045",
    "Ember Valley": "V020",
    "Emerald Sky": "V065",
    "EmeraldBay": "V050",
    "Eureka": "V019",
    "Ghost": "V072",
    "Green Dawg": "V022",
    "Happy Fruit": "V043",
    "Hashish": "V001",
    "Heady Heads": "V055",
    "Heavy Hitters": "V009",
    "Heirbloom": "V070",
    "Highatus": "V071",
    "HolySmoke/Water": "V005",
    "Jeeter": "V002",
    "Jetty": "V012",
    "Josh Wax": "V057",
    "Just J": "V069",
    "KANHA": "V053",
    "KEEF": "V046",
    "Kikoko": "V015",
    "Kiva": "V003",
    "Kushy Punch": "V075",
    "LA FARMS": "V023",
    "Level": "V051",
    "Lime": "V076",
    "Lyfe Sauce": "V074",
    "Made": "V017",
    "Made-Eddys": "V077",
    "Mary Medical": "V078",
    "Master Makers": "V024",
    "Maven": "V073",
    "Mountain Man": "V086",
    "Mountain Melts": "V044",
    "Nasha": "V063",
    "P&B": "V032",
    "Pacific Stone": "V008",
    "PBR-NYF-STIDES": "V031",
    "Pearl Pharma": "V039",
    "Planta": "V052",
    "PlugnPlay": "V048",
    "Preferred": "V014",
    "Punch": "V027",
    "Pure Beauty": "V037",
    "Pusha": "V081",
    "Quiet Kings": "V061",
    "Raw Garden": "V025",
    "Royal Blunts": "V054",
    "Sauce": "V034",
    "Seed Junky": "V068",
    "Sluggers": "V047",
    "Smokiez": "V013",
    "Sol Flora": "V083",
    "STIIIZY": "V033",
    "Tags": "V049",
    "Time Machine": "V007",
    "TopShelf": "V040",
    "TreeSap": "V016",
    "Turn": "V018",
    "Turtle Pie Co.": "V060",
    "Uncle Arnies": "V026",
    "Wavvy": "V082",
    "WYLD/GoodTide": "V011",
}


def _clean_base_url(value: str) -> str:
    text = str(value or DEFAULT_BASE_URL).strip().rstrip("/")
    if not text.endswith("/api/v1"):
        text = f"{text}/api/v1"
    return text


def _parse_env_file(path: Path) -> Dict[str, str]:
    out: Dict[str, str] = {}
    path = Path(path)
    if not path.exists():
        return out
    for line in path.read_text(errors="ignore").splitlines():
        text = line.strip()
        if not text or text.startswith("#") or "=" not in text:
            continue
        key, value = text.split("=", 1)
        key = key.strip()
        value = value.strip().strip('"').strip("'")
        out[key] = value
    return out


def load_creditflow_api_key(env_file: Path = Path(".env"), env_keys: Sequence[str] = DEFAULT_ENV_KEYS) -> str:
    env = _parse_env_file(Path(env_file))
    for key in env_keys:
        if env.get(key):
            return env[key]
    return ""


def normalize_name(value: Any) -> str:
    text = str(value or "").strip().lower()
    text = text.replace("&", " and ")
    text = re.sub(r"[^a-z0-9]+", " ", text)
    text = re.sub(r"\b(co|company|inc|llc|cannabis|brands|brand)\b", " ", text)
    text = re.sub(r"\s+", " ", text).strip()
    return COMMON_BRAND_ALIASES.get(text, text)


def vendor_code_for_brand(value: Any) -> str:
    target = normalize_name(value)
    if not target:
        return ""
    for brand_name, vendor_code in CREDITFLOW_VENDOR_CODE_BY_BRAND.items():
        brand_norm = normalize_name(brand_name)
        if target == brand_norm:
            return vendor_code
    return ""


def _nested_text(value: Any, keys: Sequence[str]) -> str:
    if isinstance(value, dict):
        for key in keys:
            if value.get(key) not in (None, ""):
                return str(value.get(key))
    return ""


def _row_brand_id(row: Dict[str, Any]) -> str:
    direct = row.get("brandId") or row.get("brand_id")
    if direct not in (None, ""):
        return str(direct)
    return _nested_text(row.get("brand"), ("id", "_id", "brandId"))


def _row_store_id(row: Dict[str, Any]) -> str:
    direct = row.get("storeId") or row.get("store_id")
    if direct not in (None, ""):
        return str(direct)
    return _nested_text(row.get("store"), ("id", "_id", "storeId"))


def _row_brand_name(row: Dict[str, Any], brand_name_by_id: Optional[Dict[str, str]] = None) -> str:
    brand_name_by_id = brand_name_by_id or {}
    direct = row.get("brandName")
    if direct:
        return str(direct)
    nested = _nested_text(row.get("brand"), ("name", "displayName", "brandName", "title", "label"))
    if nested:
        return nested
    return brand_name_by_id.get(_row_brand_id(row), "")


def _row_store_name(row: Dict[str, Any], store_name_by_id: Optional[Dict[str, str]] = None) -> str:
    store_name_by_id = store_name_by_id or {}
    direct = row.get("storeName")
    if direct:
        return str(direct)
    nested = _nested_text(row.get("store"), ("name", "displayName", "storeName", "title", "label"))
    if nested:
        return nested
    return store_name_by_id.get(_row_store_id(row), "")


def _field_text(row: Dict[str, Any], *keys: str) -> str:
    for key in keys:
        value = row.get(key)
        if isinstance(value, dict):
            nested = _nested_text(value, ("name", "displayName", "title", "label", "number", "id"))
            if nested:
                return nested
        elif value not in (None, ""):
            return str(value)
    return ""


def _item_id(row: Dict[str, Any]) -> str:
    for key in ("id", "_id", "brandId", "storeId", "creditId"):
        if row.get(key) is not None:
            return str(row[key])
    return ""


def _item_name(row: Dict[str, Any]) -> str:
    for key in ("name", "displayName", "brandName", "storeName", "title", "label"):
        if row.get(key):
            return str(row[key])
    return ""


def _num(value: Any, default: float = 0.0) -> float:
    if value is None:
        return default
    text = str(value).strip().replace("$", "").replace(",", "")
    if not text:
        return default
    try:
        return float(text)
    except ValueError:
        return default


def _bool(value: Any) -> bool:
    if isinstance(value, bool):
        return value
    return str(value or "").strip().lower() in {"1", "true", "yes", "y", "paid", "received"}


def _day_text(value: Any) -> str:
    if value is None:
        return ""
    text = str(value).strip()
    if not text:
        return ""
    try:
        return date.fromisoformat(text[:10]).isoformat()
    except ValueError:
        return text[:10]


def _extract_list(payload: Any, keys: Sequence[str]) -> List[Dict[str, Any]]:
    if isinstance(payload, list):
        return [x for x in payload if isinstance(x, dict)]
    if isinstance(payload, dict):
        for key in keys:
            value = payload.get(key)
            if isinstance(value, list):
                return [x for x in value if isinstance(x, dict)]
        data = payload.get("data")
        if isinstance(data, list):
            return [x for x in data if isinstance(x, dict)]
        if isinstance(data, dict):
            for key in keys:
                value = data.get(key)
                if isinstance(value, list):
                    return [x for x in value if isinstance(x, dict)]
    return []


class CreditFlowClient:
    def __init__(self, api_key: str, base_url: str = DEFAULT_BASE_URL, timeout: int = 30) -> None:
        self.api_key = api_key
        self.base_url = _clean_base_url(base_url)
        self.timeout = int(timeout or 30)

    def get(self, path: str, params: Optional[Dict[str, Any]] = None) -> Any:
        if not self.api_key:
            raise RuntimeError("CreditFlow API key is missing.")
        url = f"{self.base_url}/{path.strip('/')}"
        clean_params = {k: v for k, v in (params or {}).items() if v not in (None, "")}
        if clean_params:
            url = f"{url}?{urllib.parse.urlencode(clean_params)}"
        req = urllib.request.Request(url, headers={"X-API-Key": self.api_key, "Accept": "application/json"})
        try:
            with urllib.request.urlopen(req, timeout=self.timeout) as resp:
                raw = resp.read().decode("utf-8")
        except urllib.error.HTTPError as exc:
            raise RuntimeError(f"CreditFlow API HTTP {exc.code} for {path}") from exc
        except urllib.error.URLError as exc:
            raise RuntimeError(f"CreditFlow API connection failed for {path}: {exc.reason}") from exc
        return json.loads(raw) if raw.strip() else {}

    def stores(self) -> List[Dict[str, Any]]:
        return _extract_list(self.get("stores"), ("stores",))

    def brands(self) -> List[Dict[str, Any]]:
        return _extract_list(self.get("brands"), ("brands",))

    def credits(self, week_start: date, week_end: date, brand_id: str = "", store_id: str = "") -> List[Dict[str, Any]]:
        params = {
            "weekStart": week_start.isoformat(),
            "weekEnd": week_end.isoformat(),
            "brandId": brand_id,
            "storeId": store_id,
        }
        return _extract_list(self.get("credits", params), ("credits",))


def _date_chunks(start_day: date, end_day: date, chunk_days: int = 31, overlap_days: int = 7) -> Iterable[Tuple[date, date]]:
    current = start_day
    step = max(1, int(chunk_days or 31))
    overlap = max(0, int(overlap_days or 0))
    while current <= end_day:
        chunk_end = min(end_day, current + timedelta(days=step - 1))
        yield current, chunk_end
        if chunk_end >= end_day:
            break
        next_current = chunk_end - timedelta(days=max(overlap - 1, 0))
        if next_current <= current:
            next_current = chunk_end + timedelta(days=1)
        current = next_current


def _dedupe_credit_rows(rows: Iterable[Dict[str, Any]]) -> List[Dict[str, Any]]:
    out: Dict[str, Dict[str, Any]] = {}
    for row in rows or []:
        credit_id = str(row.get("id") or row.get("_id") or row.get("creditId") or "")
        key = credit_id or json.dumps(row, sort_keys=True, default=str)
        out[key] = row
    return list(out.values())


def build_lookup(rows: Iterable[Dict[str, Any]]) -> Tuple[Dict[str, Dict[str, Any]], Dict[str, str]]:
    by_id: Dict[str, Dict[str, Any]] = {}
    name_by_id: Dict[str, str] = {}
    for row in rows or []:
        rid = _item_id(row)
        name = _item_name(row)
        if rid:
            by_id[rid] = row
            name_by_id[rid] = name
    return by_id, name_by_id


def resolve_creditflow_brand_ids(
    brands: Iterable[Dict[str, Any]],
    target_brand: str,
    aliases: Optional[Iterable[str]] = None,
) -> Tuple[List[str], List[str], Dict[str, str]]:
    alias_list = [str(x) for x in (aliases or []) if str(x or "").strip()]
    target_names = [target_brand] + alias_list
    target_vendor_codes = {
        code
        for code in (vendor_code_for_brand(name) for name in target_names)
        if code
    }
    matched_ids: List[str] = []
    matched_codes: List[str] = []
    matched_names: Dict[str, str] = {}
    for row in brands or []:
        brand_id = _item_id(row)
        brand_name = _item_name(row)
        vendor_code = str(row.get("vendorCode") or row.get("vendor_code") or "").strip()
        name_match = brand_matches_target(brand_name, target_brand, alias_list)
        code_match = vendor_code in target_vendor_codes if vendor_code else False
        if not name_match and not code_match:
            continue
        if brand_id and brand_id not in matched_ids:
            matched_ids.append(brand_id)
            matched_names[brand_id] = brand_name
        if vendor_code and vendor_code not in matched_codes:
            matched_codes.append(vendor_code)
    return matched_ids, matched_codes, matched_names


def resolve_store_code(raw_store: str, row: Optional[Dict[str, Any]] = None) -> str:
    candidates = [raw_store]
    if row:
        candidates.extend(str(row.get(k) or "") for k in ("code", "abbr", "shortName", "name", "displayName"))
    for candidate in candidates:
        text = str(candidate or "").strip()
        if not text:
            continue
        up = text.upper()
        if up in {"MV", "LM", "SV", "LG", "NC", "WP"}:
            return up
        norm = normalize_name(text)
        for key, code in STORE_CODE_BY_NAME.items():
            if key in norm:
                return code
    return ""


def brand_matches_target(raw_brand: str, target_brand: str, aliases: Optional[Iterable[str]] = None) -> bool:
    raw_norm = normalize_name(raw_brand)
    targets = {normalize_name(target_brand)}
    for alias in aliases or []:
        if alias:
            targets.add(normalize_name(alias))
    if not raw_norm or not targets:
        return False
    if raw_norm in targets:
        return True
    return any(raw_norm in t or t in raw_norm for t in targets if len(raw_norm) > 4 and len(t) > 4)


def credit_amounts(row: Dict[str, Any]) -> Tuple[float, float]:
    expected = _num(
        row.get("expectedAmount")
        or row.get("expected_amount")
        or row.get("amount")
        or row.get("creditAmount")
        or row.get("total")
    )
    explicit_received = _num(
        row.get("receivedAmount")
        or row.get("received_amount")
        or row.get("paidAmount")
        or row.get("paid_amount")
        or row.get("paymentAmount")
    )
    allocated = _num(row.get("allocatedAmount") or row.get("allocated_amount"))
    remaining_raw = row.get("remainingBalance")
    remaining = _num(remaining_raw)
    paid_from_remaining = max(expected - remaining, 0.0) if expected > 0 and remaining_raw not in (None, "") else 0.0
    payment_total = 0.0
    payments = row.get("payments")
    if isinstance(payments, list):
        for payment in payments:
            if isinstance(payment, dict):
                payment_total += _num(payment.get("amount"))
    received = max(explicit_received, allocated, paid_from_remaining, payment_total)
    if _bool(row.get("paid")) and received <= 0:
        received = expected
    return max(expected, 0.0), max(received, 0.0)


def normalize_credit_type(value: Any) -> str:
    text = str(value or "").strip().lower()
    if "cogs" in text or "kickback" in text:
        return "COGS kickback"
    if "markdown" in text:
        return "Markdown support"
    if "buyback" in text:
        return "Buyback credit"
    if "cash" in text or "rebate" in text:
        return "Cash rebate"
    if "promo" in text:
        return "Flat promo credit"
    if "marketing" in text or "co-op" in text or "coop" in text:
        return "Co-op marketing"
    if "demo" in text or "event" in text:
        return "Demo/event support"
    if "display" in text or "sample" in text:
        return "Display/sample credit"
    return "Invoice credit"


def normalize_creditflow_credit(
    row: Dict[str, Any],
    target_brand: str,
    brand_name_by_id: Dict[str, str],
    store_name_by_id: Dict[str, str],
    store_by_id: Dict[str, Dict[str, Any]],
) -> Dict[str, Any]:
    brand_id = _row_brand_id(row)
    store_id = _row_store_id(row)
    raw_brand = _row_brand_name(row, brand_name_by_id) or target_brand
    raw_store = _row_store_name(row, store_name_by_id)
    expected, received = credit_amounts(row)
    paid = _bool(row.get("paid")) or (expected > 0 and received >= expected)
    if received > 0 and received < expected:
        status = "partial"
    elif paid:
        status = "received"
    else:
        status = "expected"
    week_start = _day_text(row.get("weekStart") or row.get("startDate") or row.get("date") or row.get("createdAt"))
    week_end = _day_text(row.get("weekEnd") or row.get("endDate") or row.get("date") or row.get("createdAt"))
    credit_id = str(row.get("id") or row.get("_id") or row.get("creditId") or "")
    credit_type = normalize_credit_type(_field_text(row, "type", "creditType", "category", "supportType"))
    invoice_ref = _field_text(row, "invoiceReference", "invoice", "invoiceId", "creditMemoNumber")
    payment_ref = _field_text(row, "paymentReference", "payment", "paymentId")
    payments = row.get("payments")
    if isinstance(payments, list) and payments:
        invoice_numbers = [
            _field_text(payment, "invoiceNumber", "paymentReference", "id")
            for payment in payments
            if isinstance(payment, dict)
        ]
        invoice_numbers = [x for x in invoice_numbers if x]
        if invoice_numbers:
            payment_ref = payment_ref or ", ".join(invoice_numbers[:3])
    notes = str(row.get("notes") or row.get("description") or "")
    source_note = f"CreditFlow brand: {raw_brand}"
    if notes:
        notes = f"{notes} | {source_note}"
    else:
        notes = source_note
    return {
        "id": f"CREDITFLOW-{credit_id}" if credit_id else "",
        "brand": raw_brand,
        "canonical_brand": target_brand,
        "store_code": resolve_store_code(raw_store, store_by_id.get(store_id)),
        "category": _field_text(row, "categoryName", "category"),
        "product": _field_text(row, "productName", "product"),
        "start_date": week_start,
        "end_date": week_end,
        "credit_type": credit_type,
        "basis": "manual_adjustment",
        "expected_amount": expected,
        "received_amount": received,
        "status": status,
        "invoice_reference": invoice_ref,
        "payment_reference": payment_ref,
        "notes": notes,
        "apply_to_margin": True,
        "manual_override_expected": True,
        "source": "creditflow",
        "external_id": credit_id,
    }


def fetch_creditflow_credits_for_brand(
    brand: str,
    start_day: date,
    end_day: date,
    env_file: Path = Path(".env"),
    base_url: str = DEFAULT_BASE_URL,
    aliases: Optional[Iterable[str]] = None,
) -> Tuple[List[Dict[str, Any]], Dict[str, Any]]:
    api_key = load_creditflow_api_key(env_file)
    meta: Dict[str, Any] = {
        "enabled": bool(api_key),
        "base_url": _clean_base_url(base_url),
        "raw_credits": 0,
        "matched_credits": 0,
        "raw_brand_counts": {},
        "matched_raw_brands": {},
        "target_brand_ids": [],
        "target_vendor_codes": [],
        "target_brand_names": {},
        "brand_filter_used": False,
        "date_chunk_days": 31,
        "date_chunk_overlap_days": 7,
        "brands": 0,
        "stores": 0,
        "warning": "",
    }
    if not api_key:
        meta["warning"] = "CreditFlow API key not found."
        return [], meta

    client = CreditFlowClient(api_key=api_key, base_url=base_url)
    try:
        brands = client.brands()
        stores = client.stores()
        store_by_id, store_name_by_id = build_lookup(stores)
        _brand_by_id, brand_name_by_id = build_lookup(brands)
        target_brand_ids, target_vendor_codes, target_brand_names = resolve_creditflow_brand_ids(brands, brand, aliases)
        raw_credits_by_id: Dict[str, Dict[str, Any]] = {}
        if target_brand_ids:
            for target_brand_id in target_brand_ids:
                for chunk_start, chunk_end in _date_chunks(start_day, end_day):
                    for row in client.credits(chunk_start, chunk_end, brand_id=target_brand_id):
                        credit_id = str(row.get("id") or row.get("_id") or row.get("creditId") or "")
                        key = credit_id or json.dumps(row, sort_keys=True, default=str)
                        raw_credits_by_id[key] = row
            raw_credits = list(raw_credits_by_id.values())
        else:
            raw_credits = _dedupe_credit_rows(
                row
                for chunk_start, chunk_end in _date_chunks(start_day, end_day)
                for row in client.credits(chunk_start, chunk_end)
            )
    except Exception as exc:
        meta["warning"] = str(exc)
        return [], meta

    meta["brands"] = len(brands)
    meta["stores"] = len(stores)
    meta["raw_credits"] = len(raw_credits)
    meta["target_brand_ids"] = target_brand_ids
    meta["target_vendor_codes"] = target_vendor_codes
    meta["target_brand_names"] = target_brand_names
    meta["brand_filter_used"] = bool(target_brand_ids)
    raw_brand_counts: Dict[str, int] = {}
    matched_raw_brands: Dict[str, int] = {}
    target_brand_id_set = set(target_brand_ids)
    out: List[Dict[str, Any]] = []
    for row in raw_credits:
        brand_id = _row_brand_id(row)
        raw_brand = _row_brand_name(row, brand_name_by_id)
        raw_brand_label = raw_brand.strip() or "(blank)"
        raw_brand_counts[raw_brand_label] = raw_brand_counts.get(raw_brand_label, 0) + 1
        id_match = bool(brand_id and brand_id in target_brand_id_set)
        if not id_match and not brand_matches_target(raw_brand, brand, aliases):
            continue
        matched_raw_brands[raw_brand_label] = matched_raw_brands.get(raw_brand_label, 0) + 1
        out.append(normalize_creditflow_credit(row, brand, brand_name_by_id, store_name_by_id, store_by_id))
    meta["matched_credits"] = len(out)
    meta["raw_brand_counts"] = dict(sorted(raw_brand_counts.items(), key=lambda item: (-item[1], item[0].lower())))
    meta["matched_raw_brands"] = dict(sorted(matched_raw_brands.items(), key=lambda item: (-item[1], item[0].lower())))
    return out, meta


def write_creditflow_cache(path: Path, rows: List[Dict[str, Any]], meta: Dict[str, Any]) -> None:
    path = Path(path)
    path.parent.mkdir(parents=True, exist_ok=True)
    payload = {
        "fetched_at": datetime.now().replace(microsecond=0).isoformat(),
        "meta": meta,
        "credits": rows,
    }
    path.write_text(json.dumps(payload, indent=2) + "\n", encoding="utf-8")
