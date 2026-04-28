#!/usr/bin/env python3
"""
Direct Dutchie POS API exporter for sales, catalog, and inventory reports.

Environment variable support for each store code includes patterns like:
- DUTCHIE_API_KEY_MV
- DUTCHIE_LOCATION_KEY_MV
- MV_DUTCHIE_API_KEY
- MV

Optional integrator key names:
- DUTCHIE_INTEGRATOR_KEY
- DUTCHIE_POS_INTEGRATOR_KEY
- INTEGRATOR_KEY
"""

from __future__ import annotations

import argparse
import json
import os
import time
from collections import OrderedDict
from concurrent.futures import ThreadPoolExecutor, as_completed
from dataclasses import dataclass
from datetime import datetime, time as dt_time, timezone
from pathlib import Path
from threading import Lock
from typing import Any
from zoneinfo import ZoneInfo

import pandas as pd
import requests
from dotenv import dotenv_values

BASE_URL = "https://api.pos.dutchie.com"
DEFAULT_ENV_FILE = ".env"
DEFAULT_OUTPUT_DIR = Path("reports/api_exports")
DEFAULT_TIMEZONE = "America/Los_Angeles"
DEFAULT_FORMATS = ("json", "csv")
DEFAULT_REPORTS = ("sales", "catalog", "inventory")
DEFAULT_API_WORKERS = 4

STORE_CODES = OrderedDict(
    [
        ("MV", "Mission Valley"),
        ("LG", "Lemon Grove"),
        ("LM", "La Mesa"),
        ("WP", "Wildomar Palomar"),
        ("SV", "Sorrento Valley"),
        ("NC", "National City"),
    ]
)

STORE_ENV_PATTERNS = (
    "DUTCHIE_LOCATION_KEY_{code}",
    "DUTCHIE_API_KEY_{code}",
    "DUTCHIE_POS_API_KEY_{code}",
    "{code}_DUTCHIE_LOCATION_KEY",
    "{code}_DUTCHIE_API_KEY",
    "{code}_API_KEY",
    "{code}",
)

INTEGRATOR_ENV_PATTERNS = (
    "DUTCHIE_INTEGRATOR_KEY",
    "DUTCHIE_POS_INTEGRATOR_KEY",
    "INTEGRATOR_KEY",
)


class DutchieAPIError(RuntimeError):
    """Raised when the Dutchie API returns an error response."""


@dataclass(frozen=True)
class ReportSpec:
    name: str
    endpoint: str


REPORT_SPECS = {
    "sales": ReportSpec(name="sales", endpoint="/reporting/transactions"),
    "catalog": ReportSpec(name="catalog", endpoint="/reporting/products"),
    "inventory": ReportSpec(name="inventory", endpoint="/reporting/inventory"),
}


def canonical_env_map(env_file: str | os.PathLike[str]) -> dict[str, str]:
    merged: dict[str, str] = {}

    file_values = dotenv_values(env_file)
    for key, value in file_values.items():
        if key is None or value is None:
            continue
        clean_key = str(key).strip()
        clean_value = str(value).strip()
        if clean_key and clean_value:
            merged[clean_key.upper()] = clean_value

    for key, value in os.environ.items():
        clean_key = str(key).strip()
        clean_value = str(value).strip()
        if clean_key and clean_value:
            merged[clean_key.upper()] = clean_value

    return merged


def parse_multi_values(values: list[str] | None) -> list[str]:
    parsed: list[str] = []
    for value in values or []:
        for piece in str(value).replace(",", " ").split():
            clean_piece = piece.strip()
            if clean_piece:
                parsed.append(clean_piece)
    return parsed


def positive_int(value: str) -> int:
    try:
        parsed = int(value)
    except (TypeError, ValueError) as exc:
        raise argparse.ArgumentTypeError("Expected a whole number greater than zero.") from exc
    if parsed < 1:
        raise argparse.ArgumentTypeError("Expected a whole number greater than zero.")
    return parsed


def resolve_worker_count(requested_workers: int | None, job_count: int) -> int:
    if job_count <= 0:
        return 1
    requested = requested_workers or DEFAULT_API_WORKERS
    return max(1, min(int(requested), int(job_count)))


def normalize_store_code(raw_value: str) -> str:
    value = raw_value.strip().upper()
    if value not in STORE_CODES:
        allowed = ", ".join(STORE_CODES)
        raise ValueError(f"Unknown store code '{raw_value}'. Expected one of: {allowed}")
    return value


def parse_store_codes(values: list[str] | None) -> list[str]:
    seen: list[str] = []
    for item in parse_multi_values(values):
        code = normalize_store_code(item)
        if code not in seen:
            seen.append(code)
    return seen


def parse_report_names(values: list[str] | None) -> list[str]:
    seen: list[str] = []
    valid = set(REPORT_SPECS)
    for item in parse_multi_values(values):
        name = item.strip().lower()
        if name not in valid:
            allowed = ", ".join(sorted(valid))
            raise ValueError(f"Unknown report '{item}'. Expected one of: {allowed}")
        if name not in seen:
            seen.append(name)
    return seen


def parse_format_names(values: list[str] | None) -> list[str]:
    seen: list[str] = []
    valid = {"json", "csv"}
    for item in parse_multi_values(values):
        name = item.strip().lower()
        if name not in valid:
            allowed = ", ".join(sorted(valid))
            raise ValueError(f"Unknown format '{item}'. Expected one of: {allowed}")
        if name not in seen:
            seen.append(name)
    return seen


def resolve_store_keys(env_map: dict[str, str], store_codes: list[str]) -> dict[str, str]:
    resolved: dict[str, str] = {}
    for code in store_codes:
        for pattern in STORE_ENV_PATTERNS:
            env_name = pattern.format(code=code).upper()
            if env_name in env_map:
                resolved[code] = env_map[env_name]
                break
    return resolved


def discover_configured_store_codes(env_map: dict[str, str]) -> list[str]:
    discovered: list[str] = []
    for code in STORE_CODES:
        if resolve_store_keys(env_map, [code]).get(code):
            discovered.append(code)
    return discovered


def resolve_integrator_key(env_map: dict[str, str]) -> str:
    for name in INTEGRATOR_ENV_PATTERNS:
        if name.upper() in env_map:
            return env_map[name.upper()]
    return ""


def isoformat_utc(dt_value: datetime) -> str:
    return dt_value.astimezone(timezone.utc).replace(microsecond=0).isoformat().replace("+00:00", "Z")


def local_date_range_to_utc_strings(
    from_date_text: str,
    to_date_text: str,
    tz_name: str = DEFAULT_TIMEZONE,
) -> tuple[str, str]:
    tz = ZoneInfo(tz_name)
    from_date = datetime.fromisoformat(from_date_text).date()
    to_date = datetime.fromisoformat(to_date_text).date()
    start_local = datetime.combine(from_date, dt_time.min, tzinfo=tz)
    end_local = datetime.combine(to_date, dt_time.max, tzinfo=tz)
    return isoformat_utc(start_local), isoformat_utc(end_local)


def build_sales_params(args: argparse.Namespace) -> dict[str, Any]:
    now_local = datetime.now(ZoneInfo(args.timezone))
    from_date_text = args.from_date or args.to_date or now_local.date().isoformat()
    to_date_text = args.to_date or from_date_text
    from_utc, to_utc = local_date_range_to_utc_strings(from_date_text, to_date_text, args.timezone)

    params: dict[str, Any] = {
        "FromDateUTC": from_utc,
        "ToDateUTC": to_utc,
        "IncludeDetail": True,
        "IncludeTaxes": True,
        "IncludeOrderIds": True,
        "IncludeFeesAndDonations": True,
    }
    if args.transaction_id is not None:
        params["TransactionId"] = args.transaction_id
    return params


def build_catalog_params(args: argparse.Namespace) -> dict[str, Any]:
    params: dict[str, Any] = {}
    if args.catalog_modified_since_utc:
        params["fromLastModifiedDateUTC"] = args.catalog_modified_since_utc
    return params


def build_inventory_params(args: argparse.Namespace) -> dict[str, Any]:
    return {
        "includeLabResults": bool(args.include_lab_results),
        "includeRoomQuantities": bool(args.include_room_quantities),
        "includeAllocated": bool(args.include_allocated),
        "includeLineage": bool(args.include_lineage),
    }


def build_params(report_name: str, args: argparse.Namespace) -> dict[str, Any]:
    if report_name == "sales":
        return build_sales_params(args)
    if report_name == "catalog":
        return build_catalog_params(args)
    if report_name == "inventory":
        return build_inventory_params(args)
    raise ValueError(f"Unhandled report name: {report_name}")


def create_session(location_key: str, integrator_key: str) -> requests.Session:
    session = requests.Session()
    session.auth = (location_key, integrator_key or "")
    session.headers.update({"Accept": "application/json"})
    return session


def format_http_error(response: requests.Response) -> str:
    body_preview = ""
    try:
        payload = response.json()
        body_preview = json.dumps(payload, ensure_ascii=False)
    except ValueError:
        body_preview = response.text.strip()

    if len(body_preview) > 400:
        body_preview = f"{body_preview[:400]}..."

    return f"{response.status_code} {response.reason}: {body_preview or 'No response body'}"


def request_json(
    session: requests.Session,
    endpoint: str,
    params: dict[str, Any] | None = None,
    timeout: int = 120,
    max_attempts: int = 4,
) -> Any:
    url = f"{BASE_URL}{endpoint}"
    last_error: Exception | None = None

    for attempt in range(1, max_attempts + 1):
        try:
            response = session.get(url, params=params, timeout=timeout)
        except requests.RequestException as exc:
            last_error = exc
            if attempt == max_attempts:
                break
            time.sleep(min(2 ** (attempt - 1), 8))
            continue

        if response.status_code in {429, 500, 502, 503, 504} and attempt < max_attempts:
            time.sleep(min(2 ** (attempt - 1), 8))
            continue

        if not response.ok:
            raise DutchieAPIError(f"{endpoint} failed: {format_http_error(response)}")

        try:
            return response.json()
        except ValueError as exc:
            raise DutchieAPIError(f"{endpoint} returned non-JSON data.") from exc

    raise DutchieAPIError(f"{endpoint} request failed after {max_attempts} attempts: {last_error}")


def payload_row_count(payload: Any) -> int:
    if isinstance(payload, list):
        return len(payload)
    if payload is None:
        return 0
    return 1


def csv_safe_value(value: Any) -> Any:
    if isinstance(value, (dict, list)):
        return json.dumps(value, ensure_ascii=False, default=str)
    if isinstance(value, (datetime, Path)):
        return str(value)
    return value


def payload_to_dataframe(payload: Any) -> pd.DataFrame:
    if isinstance(payload, list):
        rows = payload
    elif isinstance(payload, dict):
        rows = [payload]
    else:
        rows = [{"value": payload}]

    if not rows:
        return pd.DataFrame()

    if all(isinstance(row, dict) for row in rows):
        frame = pd.json_normalize(rows, sep=".")
    else:
        frame = pd.DataFrame({"value": [csv_safe_value(row) for row in rows]})

    return frame.apply(lambda column: column.map(csv_safe_value))


def ensure_output_dir(path_value: str | os.PathLike[str]) -> Path:
    output_dir = Path(path_value)
    output_dir.mkdir(parents=True, exist_ok=True)
    return output_dir


def build_output_stem(report_name: str, store_code: str, args: argparse.Namespace) -> str:
    timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
    if report_name == "sales":
        from_date_text = args.from_date or args.to_date or datetime.now(ZoneInfo(args.timezone)).date().isoformat()
        to_date_text = args.to_date or from_date_text
        return f"{report_name}_{store_code.lower()}_{from_date_text}_to_{to_date_text}_{timestamp}"
    return f"{report_name}_{store_code.lower()}_{timestamp}"


def write_payload_files(payload: Any, destination_base: Path, formats: list[str]) -> list[Path]:
    written: list[Path] = []

    if "json" in formats:
        json_path = destination_base.with_suffix(".json")
        json_path.write_text(json.dumps(payload, indent=2, ensure_ascii=False, default=str), encoding="utf-8")
        written.append(json_path)

    if "csv" in formats:
        csv_path = destination_base.with_suffix(".csv")
        frame = payload_to_dataframe(payload)
        frame.to_csv(csv_path, index=False)
        written.append(csv_path)

    return written


def print_discovered_store_summary(configured: list[str], integrator_key: str) -> None:
    print("Configured Dutchie store keys:")
    if not configured:
        print("  - none found")
    else:
        for code in configured:
            print(f"  - {code}: {STORE_CODES[code]}")
    print(f"Integrator key present: {'yes' if integrator_key else 'no'}")


def print_threadsafe(message: str, print_lock: Lock | None = None) -> None:
    if print_lock is None:
        print(message)
        return
    with print_lock:
        print(message)


def export_store_reports(
    store_code: str,
    location_key: str,
    integrator_key: str,
    report_names: list[str],
    formats: list[str],
    output_root: Path,
    args: argparse.Namespace,
    print_lock: Lock | None = None,
) -> None:
    session = create_session(location_key, integrator_key)
    location_label = STORE_CODES.get(store_code, store_code)

    try:
        if not args.skip_verify:
            identity = request_json(session, "/whoami")
            location_name = (
                identity.get("locationName")
                or identity.get("name")
                or identity.get("LocationName")
                or location_label
            )
            print_threadsafe(f"[VERIFY] {store_code}: {location_name}", print_lock)

        if args.verify_only:
            return

        for report_name in report_names:
            spec = REPORT_SPECS[report_name]
            params = build_params(report_name, args)
            report_dir = ensure_output_dir(output_root / report_name)
            output_stem = build_output_stem(report_name, store_code, args)
            output_base = report_dir / output_stem

            print_threadsafe(f"[FETCH] {store_code} {report_name} -> {spec.endpoint}", print_lock)
            payload = request_json(session, spec.endpoint, params=params)
            written_files = write_payload_files(payload, output_base, formats)
            file_list = ", ".join(str(path) for path in written_files)
            print_threadsafe(
                f"[SAVED] {store_code} {report_name}: {payload_row_count(payload)} row(s) -> {file_list}",
                print_lock,
            )
    finally:
        session.close()


def export_reports(args: argparse.Namespace) -> int:
    env_map = canonical_env_map(args.env_file)
    configured_store_codes = discover_configured_store_codes(env_map)
    requested_store_codes = parse_store_codes(args.stores) or configured_store_codes or list(STORE_CODES)
    report_names = parse_report_names(args.reports) or list(DEFAULT_REPORTS)
    formats = parse_format_names(args.format) or list(DEFAULT_FORMATS)
    integrator_key = resolve_integrator_key(env_map)

    if args.list_stores:
        print_discovered_store_summary(configured_store_codes, integrator_key)
        return 0

    store_keys = resolve_store_keys(env_map, requested_store_codes)
    missing_store_codes = [code for code in requested_store_codes if code not in store_keys]
    if missing_store_codes:
        missing = ", ".join(missing_store_codes)
        raise SystemExit(
            "Missing Dutchie location key(s) for: "
            f"{missing}. Add them to {args.env_file} using names like "
            "DUTCHIE_API_KEY_MV or just MV."
        )

    output_root = ensure_output_dir(args.output_dir)
    print_discovered_store_summary(requested_store_codes, integrator_key)

    worker_count = resolve_worker_count(args.workers, len(requested_store_codes))
    worker_label = "serial mode" if worker_count == 1 else f"{worker_count} store worker threads"
    print(f"[INFO] Running Dutchie API exports with {worker_label}.")

    failures: list[str] = []
    print_lock = Lock()

    if worker_count == 1:
        for store_code in requested_store_codes:
            try:
                export_store_reports(
                    store_code=store_code,
                    location_key=store_keys[store_code],
                    integrator_key=integrator_key,
                    report_names=report_names,
                    formats=formats,
                    output_root=output_root,
                    args=args,
                    print_lock=print_lock,
                )
            except Exception as exc:
                failures.append(f"{store_code}: {exc}")
    else:
        with ThreadPoolExecutor(max_workers=worker_count) as executor:
            futures = {
                executor.submit(
                    export_store_reports,
                    store_code,
                    store_keys[store_code],
                    integrator_key,
                    report_names,
                    formats,
                    output_root,
                    args,
                    print_lock,
                ): store_code
                for store_code in requested_store_codes
            }
            for future in as_completed(futures):
                store_code = futures[future]
                try:
                    future.result()
                except Exception as exc:
                    failures.append(f"{store_code}: {exc}")

    if failures:
        raise DutchieAPIError("Dutchie API export failed for: " + "; ".join(failures))

    return 0


def build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(
        description="Export Dutchie POS sales, catalog, and inventory data via the API.",
    )
    parser.add_argument(
        "--env-file",
        default=DEFAULT_ENV_FILE,
        help=f"Path to the .env file. Default: {DEFAULT_ENV_FILE}",
    )
    parser.add_argument(
        "--stores",
        nargs="*",
        help="Store codes to export, for example: mv lg lm wp sv nc",
    )
    parser.add_argument(
        "--reports",
        nargs="*",
        help="Reports to export: sales, catalog, inventory",
    )
    parser.add_argument(
        "--format",
        nargs="*",
        default=list(DEFAULT_FORMATS),
        help="Output formats: json csv",
    )
    parser.add_argument(
        "--output-dir",
        default=str(DEFAULT_OUTPUT_DIR),
        help=f"Directory for exported files. Default: {DEFAULT_OUTPUT_DIR}",
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
    parser.add_argument(
        "--list-stores",
        action="store_true",
        help="Show which store keys were found in the environment and exit.",
    )
    parser.add_argument(
        "--skip-verify",
        action="store_true",
        help="Skip the /whoami identity check before exporting.",
    )
    parser.add_argument(
        "--verify-only",
        action="store_true",
        help="Only call /whoami for each selected store and then exit.",
    )
    parser.add_argument(
        "--timezone",
        default=DEFAULT_TIMEZONE,
        help=f"Timezone used for --from-date/--to-date on sales exports. Default: {DEFAULT_TIMEZONE}",
    )
    parser.add_argument(
        "--from-date",
        help="Sales report start date in local time, YYYY-MM-DD.",
    )
    parser.add_argument(
        "--to-date",
        help="Sales report end date in local time, YYYY-MM-DD.",
    )
    parser.add_argument(
        "--transaction-id",
        type=int,
        help="Optional single transaction ID lookup for the sales endpoint.",
    )
    parser.add_argument(
        "--catalog-modified-since-utc",
        help="Optional UTC timestamp for incremental catalog sync, for example 2026-03-24T00:00:00Z.",
    )
    parser.add_argument(
        "--include-lab-results",
        action="store_true",
        help="Include lab results on the inventory report export.",
    )
    parser.add_argument(
        "--include-room-quantities",
        action="store_true",
        help="Include room-level quantities on the inventory report export.",
    )
    parser.add_argument(
        "--include-allocated",
        action="store_true",
        help="Include allocated quantities on the inventory report export.",
    )
    parser.add_argument(
        "--include-lineage",
        action="store_true",
        help="Include lineage and traceability data on the inventory report export.",
    )
    return parser


def main() -> int:
    parser = build_parser()
    args = parser.parse_args()
    try:
        return export_reports(args)
    except (ValueError, DutchieAPIError, requests.RequestException) as exc:
        parser.error(str(exc))
        return 2


if __name__ == "__main__":
    raise SystemExit(main())
