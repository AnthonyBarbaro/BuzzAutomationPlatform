#!/usr/bin/env python3
"""
Build weekly aged flower inventory reports for multiple brands.

The runner fetches Dutchie inventory once for each configured store, filters the
same source data into separate brand files, uploads each brand folder to Drive,
and can email one digest to the inventory manager.
"""

from __future__ import annotations

import argparse
import base64
import csv
import html
import json
import mimetypes
import re
from concurrent.futures import ThreadPoolExecutor, as_completed
from dataclasses import dataclass, field
from datetime import date, datetime
from email.message import EmailMessage
from pathlib import Path
from typing import Any
from zoneinfo import ZoneInfo

import pandas as pd

from aged_flower_inventory_report import (
    BASE_DIR,
    DEFAULT_AGE_DAYS,
    DEFAULT_DRIVE_PARENT_FOLDER,
    DEFAULT_ENV_FILE,
    DEFAULT_OUTPUT_ROOT,
    DEFAULT_TIMEZONE,
    DEFAULT_WORKERS,
    STORE_CODES,
    canonical_env_map,
    discover_configured_store_codes,
    fetch_inventory_for_store,
    find_or_create_drive_folder,
    is_brand_match,
    is_flower_row,
    is_sample_or_promo,
    make_drive_item_public,
    parse_store_codes,
    report_columns,
    resolve_integrator_key,
    resolve_store_keys,
    row_to_report_record,
    upload_or_update_drive_file,
    write_report,
)


# Same brand source used by BrandINVEmailer.py.
DEFAULT_BRANDS_FILE = BASE_DIR / "brand_config2.json"
DEFAULT_ALIAS_FILE = BASE_DIR / "brand_aliases_monthly.json"
DEFAULT_CREDENTIALS_FILE = BASE_DIR / "credentials.json"
DEFAULT_GMAIL_TOKEN_FILE = BASE_DIR / "token_gmail.json"
GMAIL_SCOPES = ["https://www.googleapis.com/auth/gmail.send"]


@dataclass
class BrandRequest:
    brand: str
    aliases: list[str] = field(default_factory=list)
    recipients: list[str] = field(default_factory=list)
    age_days: int | None = None
    enabled: bool = True


@dataclass
class BatchConfig:
    brands: list[BrandRequest]
    recipients: list[str] = field(default_factory=list)
    age_days: int | None = None
    include_prerolls: bool = False
    include_samples: bool = False


@dataclass
class BrandReportResult:
    brand: str
    aliases: list[str]
    age_days: int
    rows: int
    units: float
    cost_value: float
    retail_value: float
    xlsx_path: str
    csv_path: str
    drive_folder: str = ""
    drive_links: dict[str, str] = field(default_factory=dict)
    error: str = ""


def resolve_repo_path(path_value: str | Path) -> Path:
    path = Path(path_value)
    return path if path.is_absolute() else BASE_DIR / path


def normalize_key(value: Any) -> str:
    return re.sub(r"[^a-z0-9]+", "", str(value or "").lower())


def dedupe_text(values: list[str]) -> list[str]:
    result: list[str] = []
    seen: set[str] = set()
    for value in values:
        text = str(value or "").strip()
        key = text.casefold()
        if text and key not in seen:
            result.append(text)
            seen.add(key)
    return result


def split_multi_value(value: Any) -> list[str]:
    if value is None:
        return []
    if isinstance(value, (list, tuple, set)):
        parts: list[str] = []
        for item in value:
            parts.extend(split_multi_value(item))
        return parts
    return [part.strip() for part in re.split(r"[;,|]", str(value)) if part.strip()]


def coerce_bool(value: Any, default: bool = True) -> bool:
    if value is None:
        return default
    if isinstance(value, bool):
        return value
    return str(value).strip().casefold() not in {"0", "false", "no", "n", "off", "disabled"}


def coerce_age_days(value: Any) -> int | None:
    if value in (None, ""):
        return None
    return int(value)


def brand_request_from_mapping(row: dict[str, Any], default_recipients: list[str]) -> list[BrandRequest]:
    enabled = coerce_bool(row.get("enabled"), default=True)
    recipients = dedupe_text(split_multi_value(row.get("recipients") or row.get("emails") or row.get("to")))
    if not recipients:
        recipients = list(default_recipients)

    explicit_brand = str(row.get("brand") or row.get("name") or "").strip()
    config_synonyms = dedupe_text(split_multi_value(row.get("brand_synonyms")))

    if not explicit_brand and config_synonyms:
        return [
            BrandRequest(
                brand=brand,
                aliases=[],
                recipients=recipients,
                age_days=coerce_age_days(row.get("age_days")),
                enabled=enabled,
            )
            for brand in config_synonyms
        ]

    brand = explicit_brand or str(row.get("folder_name") or "").strip()
    if not brand:
        return []

    aliases = split_multi_value(row.get("aliases") or row.get("brand_aliases"))
    if config_synonyms:
        aliases.extend(config_synonyms)

    return [
        BrandRequest(
            brand=brand,
            aliases=dedupe_text(aliases),
            recipients=recipients,
            age_days=coerce_age_days(row.get("age_days")),
            enabled=enabled,
        )
    ]


def load_json_config(path: Path) -> BatchConfig:
    payload = json.loads(path.read_text(encoding="utf-8"))
    if isinstance(payload, list):
        raw_brands = payload
        recipients: list[str] = []
        age_days = None
        include_prerolls = False
        include_samples = False
    elif isinstance(payload, dict):
        if payload.get("test_mode") and payload.get("test_email"):
            recipients = dedupe_text(split_multi_value(payload.get("test_email")))
        else:
            recipients = dedupe_text(split_multi_value(payload.get("recipients") or payload.get("emails") or payload.get("to")))
        raw_brands = payload.get("brands", [])
        age_days = coerce_age_days(payload.get("age_days"))
        include_prerolls = bool(payload.get("include_prerolls", False))
        include_samples = bool(payload.get("include_samples", False))
    else:
        raise ValueError(f"{path} must contain a JSON object or list.")

    brands: list[BrandRequest] = []
    for raw in raw_brands:
        if isinstance(raw, str):
            brands.append(BrandRequest(brand=raw.strip(), recipients=list(recipients)))
        elif isinstance(raw, dict):
            brands.extend(brand_request_from_mapping(raw, recipients))
        else:
            raise ValueError(f"Unsupported brand entry in {path}: {raw!r}")

    return BatchConfig(
        brands=[brand for brand in brands if brand.brand],
        recipients=recipients,
        age_days=age_days,
        include_prerolls=include_prerolls,
        include_samples=include_samples,
    )


def load_csv_config(path: Path) -> BatchConfig:
    brands: list[BrandRequest] = []
    recipients: list[str] = []
    age_days = None
    with path.open(newline="", encoding="utf-8-sig") as handle:
        reader = csv.DictReader(handle)
        for row in reader:
            if normalize_key(row.get("brand")) == "defaults":
                recipients = dedupe_text(split_multi_value(row.get("recipients") or row.get("emails") or row.get("to")))
                age_days = coerce_age_days(row.get("age_days"))
                continue
            brands.extend(brand_request_from_mapping(row, recipients))
    return BatchConfig(brands=[brand for brand in brands if brand.brand], recipients=recipients, age_days=age_days)


def load_text_config(path: Path) -> BatchConfig:
    brands: list[BrandRequest] = []
    for line in path.read_text(encoding="utf-8").splitlines():
        stripped = line.strip()
        if not stripped or stripped.startswith("#"):
            continue
        parts = [part.strip() for part in stripped.split("|") if part.strip()]
        if parts:
            brands.append(BrandRequest(brand=parts[0], aliases=parts[1:]))
    return BatchConfig(brands=brands)


def load_batch_config(path: Path) -> BatchConfig:
    suffix = path.suffix.casefold()
    if suffix == ".json":
        return load_json_config(path)
    if suffix == ".csv":
        return load_csv_config(path)
    return load_text_config(path)


def load_alias_map(path: Path) -> dict[str, list[str]]:
    if not path.exists():
        return {}
    payload = json.loads(path.read_text(encoding="utf-8"))
    if not isinstance(payload, dict):
        return {}
    return {
        normalize_key(brand): dedupe_text(split_multi_value(aliases))
        for brand, aliases in payload.items()
    }


def aliases_for_brand(request: BrandRequest, alias_map: dict[str, list[str]]) -> list[str]:
    aliases = [request.brand, *request.aliases]
    aliases.extend(alias_map.get(normalize_key(request.brand), []))
    return dedupe_text(aliases)


def fetch_inventory_by_store(
    store_codes: list[str],
    env_file: Path,
    workers: int,
) -> dict[str, list[dict[str, Any]]]:
    env_map = canonical_env_map(env_file)
    store_keys = resolve_store_keys(env_map, store_codes)
    missing = [code for code in store_codes if code not in store_keys]
    if missing:
        raise RuntimeError(f"Missing Dutchie API location key(s) for: {', '.join(missing)}")

    integrator_key = resolve_integrator_key(env_map)
    inventory_by_store: dict[str, list[dict[str, Any]]] = {}
    worker_count = max(1, min(int(workers or 1), len(store_codes)))

    def process_store(code: str) -> tuple[str, list[dict[str, Any]]]:
        return code, fetch_inventory_for_store(code, store_keys[code], integrator_key)

    if worker_count == 1:
        for store_code in store_codes:
            print(f"[FETCH] {store_code} inventory")
            code, rows = process_store(store_code)
            print(f"[FETCH] {code}: {len(rows)} row(s)")
            inventory_by_store[code] = rows
    else:
        with ThreadPoolExecutor(max_workers=worker_count) as executor:
            futures = {executor.submit(process_store, code): code for code in store_codes}
            for future in as_completed(futures):
                code = futures[future]
                store_code, rows = future.result()
                print(f"[FETCH] {code}: {len(rows)} row(s)")
                inventory_by_store[store_code] = rows

    return inventory_by_store


def build_report_frame_from_inventory(
    inventory_by_store: dict[str, list[dict[str, Any]]],
    brand_label: str,
    brand_aliases: list[str],
    age_days: int,
    as_of_day: date,
    include_prerolls: bool,
    include_samples: bool,
) -> pd.DataFrame:
    records: list[dict[str, Any]] = []

    for store_code, rows in inventory_by_store.items():
        store_records: list[dict[str, Any]] = []
        for row in rows:
            if not is_brand_match(row, brand_aliases):
                continue
            if not is_flower_row(row, include_prerolls=include_prerolls):
                continue
            if not include_samples and is_sample_or_promo(row):
                continue
            record = row_to_report_record(row, store_code, brand_label, as_of_day, age_days)
            if record:
                store_records.append(record)
        print(f"[FILTER] {store_code} {brand_label}: {len(store_records)} aged row(s)")
        records.extend(store_records)

    frame = pd.DataFrame(records)
    if frame.empty:
        return pd.DataFrame(columns=report_columns())

    return (
        frame[report_columns()]
        .sort_values(
            by=["Age Days", "Store", "Product", "Package ID"],
            ascending=[False, True, True, True],
        )
        .reset_index(drop=True)
    )


def summarize_frame(frame: pd.DataFrame) -> tuple[int, float, float, float]:
    if frame.empty:
        return 0, 0.0, 0.0, 0.0
    return (
        int(len(frame)),
        float(frame["Available"].sum()),
        float(frame["Cost Value"].sum()),
        float(frame["Retail Value"].sum()),
    )


def build_reports(
    requests: list[BrandRequest],
    inventory_by_store: dict[str, list[dict[str, Any]]],
    output_root: Path,
    as_of_day: date,
    default_age_days: int,
    alias_map: dict[str, list[str]],
    include_prerolls: bool,
    include_samples: bool,
) -> list[BrandReportResult]:
    results: list[BrandReportResult] = []

    for request in requests:
        age_days = request.age_days or default_age_days
        aliases = aliases_for_brand(request, alias_map)
        print(f"[INFO] Building {request.brand}: age_days>={age_days}, aliases={', '.join(aliases)}")

        frame = build_report_frame_from_inventory(
            inventory_by_store=inventory_by_store,
            brand_label=request.brand,
            brand_aliases=aliases,
            age_days=age_days,
            as_of_day=as_of_day,
            include_prerolls=include_prerolls,
            include_samples=include_samples,
        )
        xlsx_path, csv_path = write_report(
            frame=frame,
            output_root=output_root,
            as_of_day=as_of_day,
            brand_label=request.brand,
            age_days=age_days,
        )
        rows, units, cost_value, retail_value = summarize_frame(frame)
        results.append(
            BrandReportResult(
                brand=request.brand,
                aliases=aliases,
                age_days=age_days,
                rows=rows,
                units=units,
                cost_value=cost_value,
                retail_value=retail_value,
                xlsx_path=str(xlsx_path),
                csv_path=str(csv_path),
            )
        )
        print(f"[SAVED] {request.brand}: {xlsx_path}")

    return results


def authenticate_drive_api():
    import google.auth.transport.requests
    from google.oauth2.credentials import Credentials
    from google_auth_oauthlib.flow import InstalledAppFlow
    from googleapiclient.discovery import build

    from aged_flower_inventory_report import CREDENTIALS_FILE, DRIVE_SCOPES, TOKEN_DRIVE_FILE

    creds = None
    if TOKEN_DRIVE_FILE.exists():
        creds = Credentials.from_authorized_user_file(str(TOKEN_DRIVE_FILE), DRIVE_SCOPES)

    if not creds or not creds.valid:
        if creds and creds.expired and creds.refresh_token:
            creds.refresh(google.auth.transport.requests.Request())
        else:
            if not CREDENTIALS_FILE.exists():
                raise RuntimeError(f"{CREDENTIALS_FILE.name} not found. Cannot upload to Google Drive.")
            flow = InstalledAppFlow.from_client_secrets_file(str(CREDENTIALS_FILE), DRIVE_SCOPES)
            creds = flow.run_local_server(port=0)
        TOKEN_DRIVE_FILE.write_text(creds.to_json(), encoding="utf-8")

    return build("drive", "v3", credentials=creds)


def upload_results_to_drive(
    results: list[BrandReportResult],
    drive_parent_folder: str,
    as_of_day: date,
    make_public: bool,
) -> None:
    service = authenticate_drive_api()
    parent_id = find_or_create_drive_folder(service, drive_parent_folder)
    date_id = find_or_create_drive_folder(service, as_of_day.isoformat(), parent_id=parent_id)

    for result in results:
        brand_id = find_or_create_drive_folder(service, result.brand, parent_id=date_id)
        folder_link = f"https://drive.google.com/drive/folders/{brand_id}"
        if make_public:
            folder_link = make_drive_item_public(service, brand_id) or folder_link
        result.drive_folder = folder_link

        for raw_path in (result.xlsx_path, result.csv_path):
            path = Path(raw_path)
            file_id = upload_or_update_drive_file(service, path, brand_id)
            link = make_drive_item_public(service, file_id) if make_public else None
            result.drive_links[path.name] = link or f"https://drive.google.com/file/d/{file_id}/view"
            print(f"[DRIVE] Uploaded {path.name}: {result.drive_links[path.name]}")


def money(value: float) -> str:
    return f"${value:,.2f}"


def units(value: float) -> str:
    return f"{value:,.0f}" if abs(value - round(value)) < 0.0001 else f"{value:,.2f}"


def report_link_for_result(result: BrandReportResult) -> str:
    xlsx_name = Path(result.xlsx_path).name
    return result.drive_links.get(xlsx_name) or result.drive_folder or result.xlsx_path


def build_email_content(results: list[BrandReportResult], as_of_day: date) -> tuple[str, str, str]:
    subject = f"Aged Flower Inventory Reports - {as_of_day.isoformat()}"
    plain_lines = [
        f"Aged flower inventory reports are ready for {as_of_day.isoformat()}.",
        "",
    ]
    for result in results:
        plain_lines.append(
            f"- {result.brand}: {result.rows} row(s), {units(result.units)} unit(s), "
            f"cost value {money(result.cost_value)}"
        )
        plain_lines.append(f"  Report: {report_link_for_result(result)}")
        if result.drive_folder:
            plain_lines.append(f"  Folder: {result.drive_folder}")

    table_rows = []
    for result in results:
        report_link = html.escape(report_link_for_result(result), quote=True)
        folder_link = html.escape(result.drive_folder, quote=True) if result.drive_folder else ""
        folder_cell = f'<a href="{folder_link}">Folder</a>' if folder_link else ""
        table_rows.append(
            "<tr>"
            f"<td>{html.escape(result.brand)}</td>"
            f"<td style=\"text-align:right;\">{result.rows:,}</td>"
            f"<td style=\"text-align:right;\">{html.escape(units(result.units))}</td>"
            f"<td style=\"text-align:right;\">{html.escape(money(result.cost_value))}</td>"
            f"<td><a href=\"{report_link}\">Report</a></td>"
            f"<td>{folder_cell}</td>"
            "</tr>"
        )

    html_body = f"""
    <html>
      <body style="font-family:Arial,sans-serif;color:#111827;">
        <p>Aged flower inventory reports are ready for <strong>{as_of_day.isoformat()}</strong>.</p>
        <table cellspacing="0" cellpadding="6" style="border-collapse:collapse;border:1px solid #d1d5db;">
          <thead>
            <tr style="background:#12302A;color:#ffffff;">
              <th align="left">Brand</th>
              <th align="right">Rows</th>
              <th align="right">Units</th>
              <th align="right">Cost Value</th>
              <th align="left">Report</th>
              <th align="left">Folder</th>
            </tr>
          </thead>
          <tbody>{''.join(table_rows)}</tbody>
        </table>
      </body>
    </html>
    """
    return subject, "\n".join(plain_lines), html_body


def authenticate_gmail(credentials_file: Path, token_file: Path):
    import google.auth.transport.requests
    from google.oauth2.credentials import Credentials
    from google_auth_oauthlib.flow import InstalledAppFlow
    from googleapiclient.discovery import build

    creds = None
    if token_file.exists():
        creds = Credentials.from_authorized_user_file(str(token_file), GMAIL_SCOPES)

    if not creds or not creds.valid:
        if creds and creds.expired and creds.refresh_token:
            creds.refresh(google.auth.transport.requests.Request())
        else:
            if not credentials_file.exists():
                raise RuntimeError(f"{credentials_file} not found. Cannot send Gmail message.")
            flow = InstalledAppFlow.from_client_secrets_file(str(credentials_file), GMAIL_SCOPES)
            creds = flow.run_local_server(port=0)
        token_file.write_text(creds.to_json(), encoding="utf-8")

    return build("gmail", "v1", credentials=creds)


def send_email(
    recipients: list[str],
    subject: str,
    plain_body: str,
    html_body: str,
    attachments: list[Path],
    credentials_file: Path,
    token_file: Path,
) -> None:
    if not recipients:
        raise ValueError("No email recipients configured.")

    service = authenticate_gmail(credentials_file, token_file)
    message = EmailMessage()
    message["To"] = ", ".join(recipients)
    message["From"] = "me"
    message["Subject"] = subject
    message.set_content(plain_body)
    message.add_alternative(html_body, subtype="html")

    for path in attachments:
        if not path.exists():
            continue
        content_type, _encoding = mimetypes.guess_type(str(path))
        maintype, subtype = (content_type or "application/octet-stream").split("/", 1)
        message.add_attachment(path.read_bytes(), maintype=maintype, subtype=subtype, filename=path.name)

    raw = base64.urlsafe_b64encode(message.as_bytes()).decode()
    sent = service.users().messages().send(userId="me", body={"raw": raw}).execute()
    print(f"[GMAIL] Email sent to {', '.join(recipients)} | ID: {sent['id']} | Subject: {subject}")


def write_batch_outputs(
    report_dir: Path,
    results: list[BrandReportResult],
    recipients: list[str],
    as_of_day: date,
) -> tuple[Path, Path]:
    report_dir.mkdir(parents=True, exist_ok=True)
    manifest_path = report_dir / "weekly_aged_flower_inventory_manifest.json"
    links_path = report_dir / "weekly_aged_flower_inventory_links.txt"

    payload = {
        "generated_at": datetime.now().isoformat(timespec="seconds"),
        "as_of_date": as_of_day.isoformat(),
        "recipients": recipients,
        "results": [
            {
                "brand": result.brand,
                "aliases": result.aliases,
                "age_days": result.age_days,
                "rows": result.rows,
                "units": result.units,
                "cost_value": result.cost_value,
                "retail_value": result.retail_value,
                "xlsx_path": result.xlsx_path,
                "csv_path": result.csv_path,
                "drive_folder": result.drive_folder,
                "drive_links": result.drive_links,
            }
            for result in results
        ],
    }
    manifest_path.write_text(json.dumps(payload, indent=2, sort_keys=True), encoding="utf-8")

    with links_path.open("w", encoding="utf-8") as handle:
        for result in results:
            handle.write(f"{result.brand}: {report_link_for_result(result)}\n")
            if result.drive_folder:
                handle.write(f"{result.brand} folder: {result.drive_folder}\n")

    print(f"[SAVED] Manifest: {manifest_path}")
    print(f"[SAVED] Links: {links_path}")
    return manifest_path, links_path


def requested_brand_filter(values: list[str] | None) -> set[str]:
    return {normalize_key(value) for value in values or [] if str(value).strip()}


def filter_requests(requests: list[BrandRequest], requested: set[str]) -> list[BrandRequest]:
    if not requested:
        return requests

    selected: list[BrandRequest] = []
    for request in requests:
        candidates = {normalize_key(request.brand), *[normalize_key(alias) for alias in request.aliases]}
        if not candidates.isdisjoint(requested):
            selected.append(request)
    return selected


def recipients_from_requests(requests: list[BrandRequest]) -> list[str]:
    recipients: list[str] = []
    for request in requests:
        recipients.extend(request.recipients)
    return dedupe_text(recipients)


def build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(description="Build weekly aged flower inventory reports by brand.")
    parser.add_argument(
        "--brands-file",
        default=str(DEFAULT_BRANDS_FILE),
        help="JSON, CSV, or text file containing brands. Defaults to BrandINVEmailer.py's brand_config2.json.",
    )
    parser.add_argument("--alias-file", default=str(DEFAULT_ALIAS_FILE), help="Optional JSON brand alias map.")
    parser.add_argument("--env-file", default=str(DEFAULT_ENV_FILE), help="Path to .env with Dutchie API keys.")
    parser.add_argument("--output-root", default=str(DEFAULT_OUTPUT_ROOT), help="Root folder for report output.")
    parser.add_argument("--stores", nargs="*", help="Store codes to include. Defaults to configured stores.")
    parser.add_argument("--brands", nargs="*", help="Optional subset of brands from the brand file to run.")
    parser.add_argument("--age-days", type=int, help="Minimum package age in days. Overrides the brand file default.")
    parser.add_argument("--as-of-date", help="Age cutoff date in YYYY-MM-DD. Defaults to today in local timezone.")
    parser.add_argument("--timezone", default=DEFAULT_TIMEZONE, help=f"Local timezone. Default: {DEFAULT_TIMEZONE}.")
    parser.add_argument("--include-prerolls", action="store_true", help="Include prerolls whose master category is Flower.")
    parser.add_argument("--include-samples", action="store_true", help="Include sample/promo/tester rows. Excluded by default.")
    parser.add_argument("--workers", type=int, default=DEFAULT_WORKERS, help="Concurrent store fetch workers.")
    parser.add_argument("--drive-folder", default=DEFAULT_DRIVE_PARENT_FOLDER, help="Google Drive parent folder for uploads.")
    parser.add_argument("--no-drive-upload", action="store_true", help="Write local files only; skip Google Drive upload.")
    parser.add_argument("--private-drive", action="store_true", help="Do not make uploaded Drive folder/files public.")
    parser.add_argument("--send-email", action="store_true", help="Email one digest to configured recipients.")
    parser.add_argument("--email-to", action="append", default=[], help="Email recipient override. Repeat or comma-separate.")
    parser.add_argument("--attach-xlsx", action="store_true", help="Attach generated XLSX files to the email.")
    parser.add_argument("--attach-csv", action="store_true", help="Attach generated CSV files to the email.")
    parser.add_argument("--gmail-credentials", default=str(DEFAULT_CREDENTIALS_FILE), help="Google OAuth credentials JSON.")
    parser.add_argument("--gmail-token", default=str(DEFAULT_GMAIL_TOKEN_FILE), help="Gmail OAuth token JSON.")
    return parser


def main(argv: list[str] | None = None) -> int:
    args = build_parser().parse_args(argv)
    brands_file = resolve_repo_path(args.brands_file)
    alias_file = resolve_repo_path(args.alias_file)
    env_file = resolve_repo_path(args.env_file)
    output_root = resolve_repo_path(args.output_root)

    config = load_batch_config(brands_file)
    alias_map = load_alias_map(alias_file)
    brand_requests = [request for request in config.brands if request.enabled]
    brand_requests = filter_requests(brand_requests, requested_brand_filter(args.brands))
    if not brand_requests:
        raise RuntimeError(f"No enabled brands found in {brands_file}.")

    recipients = dedupe_text(split_multi_value(args.email_to) or config.recipients or recipients_from_requests(brand_requests))
    default_age_days = args.age_days or config.age_days or DEFAULT_AGE_DAYS
    include_prerolls = bool(args.include_prerolls or config.include_prerolls)
    include_samples = bool(args.include_samples or config.include_samples)

    env_map = canonical_env_map(env_file)
    requested_stores = parse_store_codes(args.stores)
    store_codes = requested_stores or discover_configured_store_codes(env_map) or list(STORE_CODES)

    if args.as_of_date:
        as_of_day = datetime.fromisoformat(args.as_of_date).date()
    else:
        as_of_day = datetime.now(ZoneInfo(args.timezone)).date()

    print(
        f"[INFO] Weekly aged flower batch: brands={len(brand_requests)}, "
        f"stores={','.join(store_codes)}, age_days>={default_age_days}, as_of={as_of_day}"
    )

    inventory_by_store = fetch_inventory_by_store(
        store_codes=store_codes,
        env_file=env_file,
        workers=args.workers,
    )

    results = build_reports(
        requests=brand_requests,
        inventory_by_store=inventory_by_store,
        output_root=output_root,
        as_of_day=as_of_day,
        default_age_days=default_age_days,
        alias_map=alias_map,
        include_prerolls=include_prerolls,
        include_samples=include_samples,
    )

    if not args.no_drive_upload:
        upload_results_to_drive(
            results=results,
            drive_parent_folder=args.drive_folder,
            as_of_day=as_of_day,
            make_public=not args.private_drive,
        )

    report_dir = output_root / as_of_day.isoformat()
    manifest_path, links_path = write_batch_outputs(report_dir, results, recipients, as_of_day)

    if args.send_email:
        attachments: list[Path] = []
        if args.attach_xlsx:
            attachments.extend(Path(result.xlsx_path) for result in results)
        if args.attach_csv:
            attachments.extend(Path(result.csv_path) for result in results)
        subject, plain_body, html_body = build_email_content(results, as_of_day)
        send_email(
            recipients=recipients,
            subject=subject,
            plain_body=plain_body,
            html_body=html_body,
            attachments=attachments,
            credentials_file=resolve_repo_path(args.gmail_credentials),
            token_file=resolve_repo_path(args.gmail_token),
        )

    total_rows = sum(result.rows for result in results)
    total_units = sum(result.units for result in results)
    print(
        f"[DONE] {len(results)} brand report(s), {total_rows} aged row(s), "
        f"{units(total_units)} unit(s). Manifest: {manifest_path}. Links: {links_path}."
    )
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
