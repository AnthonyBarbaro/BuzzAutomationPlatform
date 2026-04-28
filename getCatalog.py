#!/usr/bin/env python3
"""
Fetch current store catalog/inventory exports from the Dutchie POS API.

This script intentionally preserves the old getCatalog.py calling pattern used
throughout the repo:

    python getCatalog.py
    python getCatalog.py /path/to/output_dir

It now writes one CSV per store in the familiar filename format:

    MM-DD-YYYY_<STORE>.csv
"""

from __future__ import annotations

import argparse
from concurrent.futures import ThreadPoolExecutor, as_completed
from datetime import datetime
from pathlib import Path
from threading import Lock
from typing import Any

import pandas as pd

from dutchie_api_reports import (
    DEFAULT_API_WORKERS,
    DutchieAPIError,
    STORE_CODES,
    canonical_env_map,
    create_session,
    discover_configured_store_codes,
    positive_int,
    print_threadsafe,
    request_json,
    resolve_integrator_key,
    resolve_store_keys,
    resolve_worker_count,
)

DEFAULT_OUTPUT_DIR = Path(__file__).resolve().parent / "files"
DEFAULT_ENV_FILE = Path(__file__).resolve().parent / ".env"
EXPORT_COLUMNS = [
    "SKU",
    "Available",
    "Product",
    "Cost",
    "Location price",
    "Price",
    "Category",
    "Brand",
    "Strain",
    "Vendor",
    "Tags",
    "Strain Type",
]


def _to_float(value: Any) -> float:
    try:
        if value is None or value == "":
            return 0.0
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


def _tag_names(tags: Any) -> str:
    if not isinstance(tags, list):
        return ""

    names: list[str] = []
    for item in tags:
        if not isinstance(item, dict):
            continue
        name = str(item.get("tagName") or item.get("name") or "").strip()
        if name:
            names.append(name)
    return ", ".join(names)


def _normalize_inventory_rows(payload: Any) -> pd.DataFrame:
    rows: list[dict[str, Any]] = []

    for item in payload or []:
        if not isinstance(item, dict):
            continue

        product_name = str(item.get("productName") or item.get("alternateName") or "").strip()
        category = _first_nonempty(item.get("category"), item.get("masterCategory"), "Unknown")
        unit_price = _to_float(
            _first_nonempty(item.get("unitPrice"), item.get("recUnitPrice"), item.get("medUnitPrice"))
        )
        brand_name = str(item.get("brandName") or "").strip()

        rows.append(
            {
                "SKU": _first_nonempty(item.get("sku"), ""),
                "Available": _to_float(item.get("quantityAvailable")),
                "Product": product_name,
                "Cost": _to_float(item.get("unitCost")),
                "Location price": unit_price,
                "Price": unit_price,
                "Category": str(category),
                "Brand": brand_name,
                "Strain": _first_nonempty(item.get("strain"), ""),
                "Vendor": _first_nonempty(item.get("vendor"), item.get("producer"), ""),
                "Tags": _tag_names(item.get("tags")),
                "Strain Type": _first_nonempty(item.get("strainType"), ""),
            }
        )

    frame = pd.DataFrame(rows)
    if frame.empty:
        return pd.DataFrame(columns=EXPORT_COLUMNS)

    frame = frame.loc[:, ~frame.columns.astype(str).str.contains("^Unnamed", case=False, regex=True)].copy()
    frame.columns = [str(col).strip() for col in frame.columns]
    for column in EXPORT_COLUMNS:
        if column not in frame.columns:
            frame[column] = ""
    return frame[EXPORT_COLUMNS].copy()


def _resolve_requested_store_codes(args: argparse.Namespace) -> list[str]:
    env_map = canonical_env_map(args.env_file)
    configured = discover_configured_store_codes(env_map)

    if args.stores:
        requested = [str(code).strip().upper() for code in args.stores if str(code).strip()]
    elif configured:
        requested = configured
    else:
        requested = list(STORE_CODES)

    store_keys = resolve_store_keys(env_map, requested)
    missing = [code for code in requested if code not in store_keys]
    if missing:
        missing_text = ", ".join(missing)
        raise SystemExit(
            "Missing Dutchie API location key(s) for: "
            f"{missing_text}. Add them to {args.env_file} using names like DUTCHIE_API_KEY_MV or MV."
        )

    args._env_map = env_map
    args._store_keys = store_keys
    args._integrator_key = resolve_integrator_key(env_map)
    return requested


def _build_output_path(output_dir: Path, store_code: str) -> Path:
    today_str = datetime.now().strftime("%m-%d-%Y")
    return output_dir / f"{today_str}_{store_code}.csv"


def _fetch_inventory_csv_for_store(
    store_code: str,
    location_key: str,
    integrator_key: str,
    output_dir: Path,
    print_lock: Lock | None = None,
) -> Path:
    location_label = STORE_CODES.get(store_code, store_code)
    session = create_session(location_key, integrator_key)

    try:
        print_threadsafe(f"[FETCH] {store_code} ({location_label}) -> /reporting/inventory", print_lock)
        payload = request_json(session, "/reporting/inventory")
        frame = _normalize_inventory_rows(payload)

        output_path = _build_output_path(output_dir, store_code)
        frame.to_csv(output_path, index=False)

        print_threadsafe(f"[SAVED] {store_code}: {len(frame)} row(s) -> {output_path}", print_lock)
        return output_path
    finally:
        session.close()


def fetch_inventory_csvs(args: argparse.Namespace) -> list[Path]:
    output_dir = Path(args.output_dir).expanduser().resolve()
    output_dir.mkdir(parents=True, exist_ok=True)

    store_codes = _resolve_requested_store_codes(args)
    store_keys = args._store_keys
    integrator_key = args._integrator_key
    written_paths: list[Path] = []

    print("[INFO] Using Dutchie API for catalog/inventory file fetch.")
    print(f"[INFO] Output directory: {output_dir}")
    print(f"[INFO] Stores requested: {', '.join(store_codes)}")

    worker_count = resolve_worker_count(args.workers, len(store_codes))
    worker_label = "serial mode" if worker_count == 1 else f"{worker_count} store worker threads"
    print(f"[INFO] Running Dutchie API catalog refresh with {worker_label}.")
    print_lock = Lock()

    if worker_count == 1:
        for store_code in store_codes:
            written_paths.append(
                _fetch_inventory_csv_for_store(
                    store_code=store_code,
                    location_key=store_keys[store_code],
                    integrator_key=integrator_key,
                    output_dir=output_dir,
                    print_lock=print_lock,
                )
            )
    else:
        with ThreadPoolExecutor(max_workers=worker_count) as executor:
            futures = {
                executor.submit(
                    _fetch_inventory_csv_for_store,
                    store_code,
                    store_keys[store_code],
                    integrator_key,
                    output_dir,
                    print_lock,
                ): store_code
                for store_code in store_codes
            }
            failures = []
            for future in as_completed(futures):
                store_code = futures[future]
                try:
                    written_paths.append(future.result())
                except Exception as exc:
                    failures.append(f"{store_code}: {exc}")
            if failures:
                raise DutchieAPIError("Dutchie API catalog refresh failed for: " + "; ".join(failures))

    return written_paths


def build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(
        description="Fetch Dutchie inventory exports and save them as margin-report-ready CSV files.",
    )
    parser.add_argument(
        "output_dir",
        nargs="?",
        default=str(DEFAULT_OUTPUT_DIR),
        help=f"Directory where CSVs will be written. Default: {DEFAULT_OUTPUT_DIR}",
    )
    parser.add_argument(
        "--env-file",
        default=str(DEFAULT_ENV_FILE),
        help=f"Path to the Dutchie API .env file. Default: {DEFAULT_ENV_FILE}",
    )
    parser.add_argument(
        "--stores",
        nargs="*",
        help="Optional store codes to fetch, for example: MV LG LM WP SV NC",
    )
    parser.add_argument(
        "--workers",
        type=positive_int,
        default=DEFAULT_API_WORKERS,
        help=(
            "Number of stores to fetch concurrently. "
            f"Default: {DEFAULT_API_WORKERS}. Use 1 for serial API calls."
        ),
    )
    return parser


def main() -> int:
    parser = build_parser()
    args = parser.parse_args()
    try:
        fetch_inventory_csvs(args)
        return 0
    except (DutchieAPIError, ValueError) as exc:
        parser.error(str(exc))
        return 2


if __name__ == "__main__":
    raise SystemExit(main())
