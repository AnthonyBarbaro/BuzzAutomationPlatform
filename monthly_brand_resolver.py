import json
import re
from pathlib import Path
from typing import Any, Dict, Iterable, List, Optional

import pandas as pd

try:
    from rapidfuzz import fuzz, process
except Exception:  # pragma: no cover - optional dependency
    fuzz = None
    process = None


DEFAULT_BRAND_ALIASES: Dict[str, List[str]] = {
    "Good Good": ["goodgood", "good good", "good-good", "goodgood cannabis"],
    "Claybourne Co.": ["claybourne", "claybourne co", "claybourne company", "claybourne co."],
    "Cannabiotix (CBX)": ["cbx", "cannabiotix", "cannabiotix cbx", "cannabiotix (cbx)"],
    "710 Labs": ["710", "710 labs", "710labs"],
    "KIVA": ["kiva", "kiva confections"],
    "KANHA": ["kanha", "kanha gummies"],
    "CAM": ["cam", "cam cannabis"],
    "PAX": ["pax"],
    "Puffco": ["puffco"],
    "Sol Flora": ["sol flora", "solflora"],
    "Dab Daddy": ["dab daddy", "dabdaddy"],
    "Raw Garden": ["raw garden", "rawgarden"],
    "Hashish": ["hashish"],
    "THC Design": ["thc design", "thcdesign"],
}

NOISE_TOKENS = {
    "co",
    "company",
    "inc",
    "llc",
    "cannabis",
    "brands",
    "brand",
}

UNKNOWN_VALUES = {"", "unknown", "nan", "none", "null", "n/a", "na"}
SHORT_BRAND_LENGTH = 4
FUZZY_THRESHOLD = 92
SUGGEST_THRESHOLD = 80


def normalize_brand_text(value: Any) -> str:
    text = str(value or "").lower().strip()
    text = text.replace("&", " and ")
    text = re.sub(r"[^a-z0-9]+", " ", text)
    tokens = [token for token in text.split() if token and token not in NOISE_TOKENS]
    return "".join(tokens)


def normalize_product_text(value: Any) -> str:
    text = str(value or "").lower().strip()
    text = text.replace("&", " and ")
    text = re.sub(r"[^a-z0-9]+", " ", text)
    return re.sub(r"\s+", " ", text).strip()


def clean_brand_display(value: Any) -> str:
    text = re.sub(r"\s+", " ", str(value or "").strip())
    return text if text else "Unknown"


def save_default_brand_aliases(path: Path) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(json.dumps(DEFAULT_BRAND_ALIASES, indent=2), encoding="utf-8")


def load_brand_aliases(path: Path) -> Dict[str, List[str]]:
    if not path.exists():
        save_default_brand_aliases(path)
    try:
        payload = json.loads(path.read_text(encoding="utf-8"))
    except Exception:
        payload = DEFAULT_BRAND_ALIASES
    aliases: Dict[str, List[str]] = {}
    for canonical, values in (payload or {}).items():
        if not canonical:
            continue
        if isinstance(values, str):
            values = [values]
        aliases[clean_brand_display(canonical)] = [clean_brand_display(v) for v in (values or []) if clean_brand_display(v)]
    return aliases


def _is_usable_brand(value: Any) -> bool:
    raw = str(value or "").strip()
    return normalize_brand_text(raw) not in UNKNOWN_VALUES


def infer_brand_from_product_name(product_name: Any) -> str:
    text = str(product_name or "").strip()
    if not text:
        return ""
    if "|" in text:
        for part in text.split("|"):
            candidate = part.strip()
            if candidate:
                return candidate
    for delimiter in [" - ", " / "]:
        if delimiter in text:
            candidate = text.split(delimiter, 1)[0].strip()
            if 1 < len(candidate) <= 32:
                return candidate
    return ""


def _alias_lookup(aliases: Dict[str, List[str]]) -> Dict[str, Dict[str, Any]]:
    lookup: Dict[str, Dict[str, Any]] = {}
    for canonical, values in aliases.items():
        canonical_display = clean_brand_display(canonical)
        canonical_key = normalize_brand_text(canonical_display)
        if canonical_key:
            lookup[canonical_key] = {"canonical": canonical_display, "method": "exact", "score": 100}
        for alias in values or []:
            alias_key = normalize_brand_text(alias)
            if alias_key:
                lookup[alias_key] = {
                    "canonical": canonical_display,
                    "method": "alias" if alias_key != canonical_key else "exact",
                    "score": 98 if alias_key != canonical_key else 100,
                }
    return lookup


def _iter_clean(values: Optional[Iterable[Any]]) -> Iterable[str]:
    if values is None:
        return
    for value in values:
        if _is_usable_brand(value):
            yield clean_brand_display(value)


def build_brand_master(
    sales_brands: Optional[Iterable[Any]] = None,
    sales_products: Optional[Iterable[Any]] = None,
    inventory_brands: Optional[Iterable[Any]] = None,
    inventory_products: Optional[Iterable[Any]] = None,
    aliases: Optional[Dict[str, List[str]]] = None,
) -> Dict[str, Any]:
    aliases = aliases or {}
    alias_lookup = _alias_lookup(aliases)
    canonical_by_key = {key: info["canonical"] for key, info in alias_lookup.items()}
    observed: List[str] = []
    observed.extend(_iter_clean(sales_brands))
    observed.extend(_iter_clean(inventory_brands))
    observed.extend(_iter_clean(infer_brand_from_product_name(v) for v in (sales_products if sales_products is not None else [])))
    observed.extend(_iter_clean(infer_brand_from_product_name(v) for v in (inventory_products if inventory_products is not None else [])))
    for brand in observed:
        key = normalize_brand_text(brand)
        if key and key not in canonical_by_key:
            canonical_by_key[key] = clean_brand_display(brand)
    return {
        "alias_lookup": alias_lookup,
        "canonical_by_key": canonical_by_key,
        "canonical_choices": sorted(set(canonical_by_key.values())),
        "rapidfuzz_available": bool(fuzz and process),
    }


def _suggest_alias(candidate_key: str, master: Dict[str, Any]) -> str:
    if not fuzz or not process or not candidate_key or len(candidate_key) <= SHORT_BRAND_LENGTH:
        return ""
    choices = list(master.get("canonical_by_key", {}).keys())
    if not choices:
        return ""
    match = process.extractOne(candidate_key, choices, scorer=fuzz.WRatio)
    if not match:
        return ""
    key, score, _ = match
    if score >= SUGGEST_THRESHOLD:
        return master.get("canonical_by_key", {}).get(key, "")
    return ""


def resolve_brand_name(
    raw_brand: Any,
    product_name: Any = None,
    aliases: Optional[Dict[str, List[str]]] = None,
    master: Optional[Dict[str, Any]] = None,
    source: str = "",
) -> Dict[str, Any]:
    aliases = aliases or {}
    master = master or build_brand_master(aliases=aliases)
    alias_lookup = master.get("alias_lookup", {})
    canonical_by_key = master.get("canonical_by_key", {})

    inferred_brand = infer_brand_from_product_name(product_name)
    raw_candidate = clean_brand_display(raw_brand) if _is_usable_brand(raw_brand) else ""
    candidate = raw_candidate or inferred_brand
    candidate_key = normalize_brand_text(candidate)

    if not candidate_key:
        return {
            "source": source,
            "raw_brand": clean_brand_display(raw_brand),
            "inferred_brand": clean_brand_display(inferred_brand),
            "canonical_brand": "Unknown",
            "canonical_brand_key": "unknown",
            "match_method": "unmatched",
            "match_score": 0,
            "suggested_alias": "",
        }

    if not raw_candidate and inferred_brand and candidate_key in alias_lookup:
        info = alias_lookup[candidate_key]
        canonical = info["canonical"]
        return {
            "source": source,
            "raw_brand": clean_brand_display(raw_brand),
            "inferred_brand": clean_brand_display(inferred_brand),
            "canonical_brand": canonical,
            "canonical_brand_key": normalize_brand_text(canonical),
            "match_method": "product_prefix",
            "match_score": 95,
            "suggested_alias": "",
        }

    if candidate_key in alias_lookup:
        info = alias_lookup[candidate_key]
        canonical = info["canonical"]
        return {
            "source": source,
            "raw_brand": clean_brand_display(raw_brand),
            "inferred_brand": clean_brand_display(inferred_brand),
            "canonical_brand": canonical,
            "canonical_brand_key": normalize_brand_text(canonical),
            "match_method": info["method"],
            "match_score": info["score"],
            "suggested_alias": "",
        }

    if candidate_key in canonical_by_key:
        canonical = canonical_by_key[candidate_key]
        return {
            "source": source,
            "raw_brand": clean_brand_display(raw_brand),
            "inferred_brand": clean_brand_display(inferred_brand),
            "canonical_brand": canonical,
            "canonical_brand_key": normalize_brand_text(canonical),
            "match_method": "product_prefix" if not raw_candidate and inferred_brand else "exact",
            "match_score": 95 if not raw_candidate and inferred_brand else 100,
            "suggested_alias": "",
        }

    if len(candidate_key) > SHORT_BRAND_LENGTH and fuzz and process:
        choices = list(canonical_by_key.keys())
        match = process.extractOne(candidate_key, choices, scorer=fuzz.WRatio) if choices else None
        if match:
            matched_key, score, _ = match
            if score >= FUZZY_THRESHOLD:
                canonical = canonical_by_key[matched_key]
                return {
                    "source": source,
                    "raw_brand": clean_brand_display(raw_brand),
                    "inferred_brand": clean_brand_display(inferred_brand),
                    "canonical_brand": canonical,
                    "canonical_brand_key": normalize_brand_text(canonical),
                    "match_method": "fuzzy_high",
                    "match_score": int(score),
                    "suggested_alias": "",
                }

    display = clean_brand_display(candidate)
    return {
        "source": source,
        "raw_brand": clean_brand_display(raw_brand),
        "inferred_brand": clean_brand_display(inferred_brand),
        "canonical_brand": display,
        "canonical_brand_key": candidate_key,
        "match_method": "unmatched",
        "match_score": 0,
        "suggested_alias": _suggest_alias(candidate_key, master),
    }


def resolve_brand_series(
    brands: Iterable[Any],
    products: Iterable[Any],
    aliases: Optional[Dict[str, List[str]]] = None,
    master: Optional[Dict[str, Any]] = None,
    source: str = "",
) -> pd.DataFrame:
    rows = [
        resolve_brand_name(raw_brand=brand, product_name=product, aliases=aliases, master=master, source=source)
        for brand, product in zip(list(brands), list(products))
    ]
    return pd.DataFrame(rows)


def strip_brand_prefix_from_product(product_name: Any, canonical_brand: Any, aliases: Optional[Dict[str, List[str]]] = None) -> str:
    text = str(product_name or "").strip()
    if not text:
        return ""
    if "|" in text:
        parts = [part.strip() for part in text.split("|")]
        if len(parts) > 1:
            return " | ".join(part for part in parts[1:] if part)
    candidates = [canonical_brand]
    aliases = aliases or {}
    candidates.extend(aliases.get(clean_brand_display(canonical_brand), []))
    lowered = text.lower()
    for candidate in sorted({str(c or "").strip() for c in candidates if str(c or "").strip()}, key=len, reverse=True):
        pattern = candidate.lower()
        if lowered.startswith(pattern + " "):
            return text[len(candidate):].strip(" -/|")
    return text


def canonical_product_key(product_name: Any, canonical_brand_key: Any, canonical_brand: Any, aliases: Optional[Dict[str, List[str]]] = None) -> str:
    core = strip_brand_prefix_from_product(product_name, canonical_brand, aliases=aliases)
    return f"{canonical_brand_key}|{normalize_product_text(core)}"


def apply_brand_resolution_to_sales(
    df: pd.DataFrame,
    aliases: Dict[str, List[str]],
    master: Dict[str, Any],
    brand_col: str = "brand",
    product_col: str = "product",
) -> pd.DataFrame:
    out = df.copy()
    resolution = resolve_brand_series(out.get(brand_col, pd.Series(dtype=object)), out.get(product_col, pd.Series(dtype=object)), aliases, master, source="sales")
    return pd.concat([out.reset_index(drop=True), resolution.reset_index(drop=True)], axis=1)


def apply_brand_resolution_to_inventory(
    df: pd.DataFrame,
    aliases: Dict[str, List[str]],
    master: Dict[str, Any],
    brand_col: str = "brand",
    product_col: str = "product",
) -> pd.DataFrame:
    out = df.copy()
    resolution = resolve_brand_series(out.get(brand_col, pd.Series(dtype=object)), out.get(product_col, pd.Series(dtype=object)), aliases, master, source="inventory")
    return pd.concat([out.reset_index(drop=True), resolution.reset_index(drop=True)], axis=1)


def write_brand_pairing_audit(path: Path, audit: pd.DataFrame) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    (audit if audit is not None else pd.DataFrame()).to_csv(path, index=False)
