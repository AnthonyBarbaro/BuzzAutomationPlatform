#!/usr/bin/env python3

import argparse
import base64
import html
import os
import re
from dataclasses import dataclass
from datetime import date, datetime as dt, timedelta
from email import encoders
from email.mime.base import MIMEBase
from email.mime.multipart import MIMEMultipart
from email.mime.text import MIMEText
from email.utils import formatdate
from pathlib import Path
from typing import Iterable, Sequence

from autoJob import DEFAULT_API_ENV_FILE, run_sales_report_api, run_sales_report_browser
from brandDEALSEmailer import parse_kickback_summary
from deals import DEFAULT_BRAND_CRITERIA, run_deals_reports

BASE_DIR = Path(__file__).resolve().parent
DEFAULT_OUTPUT_ROOT = BASE_DIR / "reports" / "monthly_brand_reports"
DEFAULT_DRIVE_PARENT_FOLDER = "monthly"
TEST_RECIPIENT = "anthony@buzzcannabis.com"
PRODUCTION_RECIPIENT = "donna@buzzcannabis.com"
DRIVE_TOKEN_FILE = BASE_DIR / "token_drive.json"
LEGACY_DRIVE_TOKEN_FILE = BASE_DIR / "token.json"
GMAIL_TOKEN_FILE = BASE_DIR / "token_gmail.json"
CREDENTIALS_FILE = BASE_DIR / "credentials.json"

DRIVE_SCOPES = ["https://www.googleapis.com/auth/drive.file"]
GMAIL_SCOPES = ["https://www.googleapis.com/auth/gmail.send"]

DEFAULT_MONTHLY_BRANDS = [
    "Lime",
    "KANHA",
    "Raw Garden",
    "Mary Medical",
    "Dixie",
    "TreeSap",
    "LA FARMS",
]

BRAND_ALIASES = {
    "lime": "Lime",
    "kanha": "KANHA",
    "rawgarden": "Raw Garden",
    "marymedical": "Mary Medical",
    "marysmedical": "Mary Medical",
    "marymedicinal": "Mary Medical",
    "marysmedicinal": "Mary Medical",
    "marysmedicinals": "Mary Medical",
    "dixie": "Dixie",
    "treesap": "TreeSap",
    "laff": "LA FARMS",
    "lafarms": "LA FARMS",
}


@dataclass(frozen=True)
class DriveUpload:
    file_name: str
    link: str


@dataclass(frozen=True)
class DriveUploadResult:
    folder_link: str
    file_links: list[DriveUpload]


def parse_date(value: str) -> date:
    return dt.fromisoformat(str(value).strip()).date()


def previous_month_range(as_of: date | None = None) -> tuple[date, date]:
    anchor = as_of or date.today()
    first_this_month = date(anchor.year, anchor.month, 1)
    end_day = first_this_month - timedelta(days=1)
    start_day = date(end_day.year, end_day.month, 1)
    return start_day, end_day


def month_folder_name(start_day: date) -> str:
    return start_day.strftime("%B %Y")


def month_slug(start_day: date) -> str:
    return start_day.strftime("%Y-%m")


def normalize_brand_name(value: str) -> str:
    return re.sub(r"[^a-z0-9]+", "", str(value or "").casefold())


def split_arg_values(values: Sequence[str] | None) -> list[str]:
    out = []
    for value in values or []:
        out.extend(part.strip() for part in str(value).split(",") if part.strip())
    return out


def resolve_monthly_brands(
    requested_brands: Iterable[str] | None = None,
    available_brands: Iterable[str] | None = None,
) -> list[str]:
    available = list(available_brands or DEFAULT_BRAND_CRITERIA.keys())
    available_lookup = {normalize_brand_name(brand): brand for brand in available}
    alias_lookup = {**available_lookup, **BRAND_ALIASES}

    resolved = []
    missing = []
    for brand in requested_brands or DEFAULT_MONTHLY_BRANDS:
        brand_text = str(brand or "").strip()
        if not brand_text:
            continue

        target = alias_lookup.get(normalize_brand_name(brand_text))
        if target is None or normalize_brand_name(target) not in available_lookup:
            missing.append(brand_text)
            continue
        if target not in resolved:
            resolved.append(target)

    if missing:
        raise ValueError(f"Unknown monthly brand(s): {', '.join(missing)}")
    return resolved


def parse_recipients(values: Sequence[str] | None, production: bool) -> list[str]:
    if values:
        recipients = split_arg_values(values)
    elif production:
        recipients = [PRODUCTION_RECIPIENT]
    else:
        recipients = [TEST_RECIPIENT]

    if not recipients:
        raise ValueError("At least one email recipient is required.")
    return recipients


def cleanup_sales_exports(files_dir: Path = BASE_DIR / "files") -> None:
    files_dir.mkdir(parents=True, exist_ok=True)
    for path in files_dir.glob("sales*.xlsx"):
        if path.is_file():
            path.unlink()
            print(f"[CLEANUP] Deleted stale sales export: {path}")


def list_report_files(reports_dir: Path) -> list[Path]:
    if not reports_dir.exists():
        return []
    return sorted(
        path
        for path in reports_dir.glob("*.xlsx")
        if path.is_file() and not path.name.startswith("~$")
    )


def write_links_file(output_dir: Path, uploads: Sequence[DriveUpload]) -> Path:
    links_path = output_dir / "monthly_links.txt"
    with links_path.open("w", encoding="utf-8") as handle:
        for item in uploads:
            handle.write(f"{item.file_name}: {item.link}\n")
    return links_path


def authenticate_drive_api():
    import google.auth.transport.requests
    from google.oauth2.credentials import Credentials
    from google_auth_oauthlib.flow import InstalledAppFlow
    from googleapiclient.discovery import build

    token_file = (
        DRIVE_TOKEN_FILE
        if DRIVE_TOKEN_FILE.exists() or not LEGACY_DRIVE_TOKEN_FILE.exists()
        else LEGACY_DRIVE_TOKEN_FILE
    )
    creds = None
    if token_file.exists():
        creds = Credentials.from_authorized_user_file(str(token_file), DRIVE_SCOPES)

    if not creds or not creds.valid:
        if creds and creds.expired and creds.refresh_token:
            creds.refresh(google.auth.transport.requests.Request())
        else:
            if not CREDENTIALS_FILE.exists():
                raise RuntimeError(f"{CREDENTIALS_FILE.name} not found. Run Google OAuth setup first.")
            flow = InstalledAppFlow.from_client_secrets_file(str(CREDENTIALS_FILE), DRIVE_SCOPES)
            creds = flow.run_local_server(port=0)
        token_file.write_text(creds.to_json(), encoding="utf-8")

    return build("drive", "v3", credentials=creds)


def _escape_drive_query_text(value: str) -> str:
    return str(value).replace("\\", "\\\\").replace("'", "\\'")


def find_or_create_drive_folder(service, folder_name: str, parent_id: str | None = None) -> str:
    escaped_name = _escape_drive_query_text(folder_name)
    query = (
        "mimeType='application/vnd.google-apps.folder' "
        f"and name='{escaped_name}' and trashed=false"
    )
    if parent_id:
        query += f" and '{parent_id}' in parents"

    response = (
        service.files()
        .list(q=query, spaces="drive", fields="files(id,name)", pageSize=10)
        .execute()
    )
    folders = response.get("files", [])
    if folders:
        return folders[0]["id"]

    metadata = {
        "name": folder_name,
        "mimeType": "application/vnd.google-apps.folder",
    }
    if parent_id:
        metadata["parents"] = [parent_id]

    folder = service.files().create(body=metadata, fields="id").execute()
    return folder["id"]


def make_drive_item_public(service, file_id: str) -> str | None:
    try:
        service.permissions().create(
            fileId=file_id,
            body={"type": "anyone", "role": "reader"},
            fields="id",
        ).execute()
    except Exception as exc:
        print(f"[WARN] Could not update Drive permissions for {file_id}: {exc}")

    try:
        info = service.files().get(fileId=file_id, fields="webViewLink").execute()
        return info.get("webViewLink")
    except Exception as exc:
        print(f"[WARN] Could not fetch Drive link for {file_id}: {exc}")
        return None


def upload_or_update_file(service, file_path: Path, folder_id: str) -> str:
    from googleapiclient.http import MediaFileUpload

    escaped_name = _escape_drive_query_text(file_path.name)
    query = f"name='{escaped_name}' and '{folder_id}' in parents and trashed=false"
    response = (
        service.files()
        .list(q=query, spaces="drive", fields="files(id,name)", pageSize=10)
        .execute()
    )
    matches = response.get("files", [])
    media = MediaFileUpload(str(file_path), resumable=True)

    if matches:
        file_id = matches[0]["id"]
        service.files().update(fileId=file_id, media_body=media, fields="id").execute()
        return file_id

    metadata = {"name": file_path.name, "parents": [folder_id]}
    uploaded = (
        service.files()
        .create(body=metadata, media_body=media, fields="id")
        .execute()
    )
    return uploaded["id"]


def upload_reports_to_drive(
    reports: Sequence[Path],
    drive_parent_folder: str,
    month_label: str,
) -> DriveUploadResult:
    service = authenticate_drive_api()
    parent_id = find_or_create_drive_folder(service, drive_parent_folder)
    month_id = find_or_create_drive_folder(service, month_label, parent_id=parent_id)
    make_drive_item_public(service, month_id)
    folder_link = f"https://drive.google.com/drive/folders/{month_id}"

    uploads = []
    for report in reports:
        file_id = upload_or_update_file(service, report, month_id)
        file_link = make_drive_item_public(service, file_id) or folder_link
        uploads.append(DriveUpload(report.name, file_link))
        print(f"[DRIVE] Uploaded {report.name}: {file_link}")

    return DriveUploadResult(folder_link=folder_link, file_links=uploads)


def _format_money(value) -> str:
    try:
        return f"${float(value):,.2f}"
    except Exception:
        return str(value)


def build_report_summary_rows(report_paths: Sequence[Path]) -> list[dict[str, str]]:
    rows = []
    for path in report_paths:
        if path.name.startswith("consolidated_brand_report_"):
            continue
        brand = re.sub(r"_report_.*$", "", path.stem)
        summary_rows = parse_kickback_summary(str(path))
        total = 0.0
        for _store, owed in summary_rows:
            try:
                total += float(owed)
            except Exception:
                pass
        rows.append(
            {
                "brand": brand,
                "store_count": str(len(summary_rows)),
                "total": _format_money(total),
            }
        )
    return sorted(rows, key=lambda row: row["brand"].casefold())


def build_email_html(
    start_day: date,
    end_day: date,
    brands: Sequence[str],
    folder_link: str | None,
    uploads: Sequence[DriveUpload],
    report_paths: Sequence[Path],
    upload_skipped: bool = False,
) -> str:
    brand_items = "".join(f"<li>{html.escape(brand)}</li>" for brand in brands)
    if uploads:
        link_items = "".join(
            f"<li><a href='{html.escape(item.link)}'>{html.escape(item.file_name)}</a></li>"
            for item in sorted(uploads, key=lambda item: item.file_name.casefold())
        )
    elif upload_skipped:
        link_items = "<li>Drive upload was skipped for this run.</li>"
    else:
        link_items = "<li>No Drive links were created.</li>"

    folder_html = (
        f"<p><strong>Drive folder:</strong> <a href='{html.escape(folder_link)}'>{html.escape(folder_link)}</a></p>"
        if folder_link
        else ""
    )

    summary_rows = build_report_summary_rows(report_paths)
    if summary_rows:
        summary_html = """
        <table border="1" cellpadding="5" cellspacing="0">
          <thead><tr><th>Brand</th><th>Stores With Data</th><th>Total Kickback Owed</th></tr></thead>
          <tbody>
        """
        for row in summary_rows:
            summary_html += (
                "<tr>"
                f"<td>{html.escape(row['brand'])}</td>"
                f"<td>{html.escape(row['store_count'])}</td>"
                f"<td>{html.escape(row['total'])}</td>"
                "</tr>"
            )
        summary_html += "</tbody></table>"
    else:
        summary_html = "<p>No brand summary rows were found in the generated reports.</p>"

    return f"""
    <html>
      <body>
        <p>Hello,</p>
        <p>The monthly brand reports are ready for {start_day.isoformat()} through {end_day.isoformat()}.</p>
        {folder_html}
        <h3>Brands</h3>
        <ul>{brand_items}</ul>
        <h3>Report Links</h3>
        <ul>{link_items}</ul>
        <h3>Summary</h3>
        {summary_html}
      </body>
    </html>
    """


def send_email_with_gmail_html(
    subject: str,
    html_body: str,
    recipients: Sequence[str],
    attachments: Sequence[Path] | None = None,
) -> None:
    import google.auth.transport.requests
    from google.oauth2.credentials import Credentials
    from google_auth_oauthlib.flow import InstalledAppFlow
    from googleapiclient.discovery import build

    creds = None
    if GMAIL_TOKEN_FILE.exists():
        creds = Credentials.from_authorized_user_file(str(GMAIL_TOKEN_FILE), GMAIL_SCOPES)

    if not creds or not creds.valid:
        if creds and creds.expired and creds.refresh_token:
            creds.refresh(google.auth.transport.requests.Request())
        else:
            if not CREDENTIALS_FILE.exists():
                raise RuntimeError(f"{CREDENTIALS_FILE.name} not found. Run Gmail OAuth setup first.")
            flow = InstalledAppFlow.from_client_secrets_file(str(CREDENTIALS_FILE), GMAIL_SCOPES)
            creds = flow.run_local_server(port=0)
        GMAIL_TOKEN_FILE.write_text(creds.to_json(), encoding="utf-8")

    service = build("gmail", "v1", credentials=creds)

    message = MIMEMultipart("alternative")
    message["From"] = "me"
    message["To"] = ", ".join(recipients)
    message["Date"] = formatdate(localtime=True)
    message["Subject"] = subject
    message.attach(MIMEText(html_body, "html"))

    for attachment in attachments or []:
        path = Path(attachment)
        if not path.is_file():
            continue
        part = MIMEBase("application", "octet-stream")
        part.set_payload(path.read_bytes())
        encoders.encode_base64(part)
        part.add_header("Content-Disposition", f'attachment; filename="{path.name}"')
        message.attach(part)

    raw_message = base64.urlsafe_b64encode(message.as_bytes()).decode()
    sent = service.users().messages().send(userId="me", body={"raw": raw_message}).execute()
    print(f"[EMAIL] Sent monthly report email to {', '.join(recipients)}. Gmail ID: {sent.get('id')}")


def build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(description="Pull monthly brand reports, upload them to Drive, and email the links.")
    parser.add_argument("--as-of", help="Anchor date for previous-month calculation, YYYY-MM-DD. Defaults to today.")
    parser.add_argument("--start-date", help="Override report start date, YYYY-MM-DD.")
    parser.add_argument("--end-date", help="Override report end date, YYYY-MM-DD.")
    parser.add_argument("--brands", nargs="+", help="Brands to report. Commas are allowed. Defaults to the monthly brand list.")
    parser.add_argument("--output-root", default=str(DEFAULT_OUTPUT_ROOT), help=f"Local output root. Default: {DEFAULT_OUTPUT_ROOT}")
    parser.add_argument("--drive-parent-folder", default=DEFAULT_DRIVE_PARENT_FOLDER, help="Top-level Drive folder name. Default: monthly")
    parser.add_argument("--recipient", action="append", help="Email recipient. Repeat or comma-separate for multiple recipients.")
    parser.add_argument("--production", action="store_true", help=f"Send to {PRODUCTION_RECIPIENT} and remove the TEST subject prefix.")
    parser.add_argument("--sales-source", choices=("api", "browser"), default="api", help="Sales source. Default: api")
    parser.add_argument("--env-file", default=str(DEFAULT_API_ENV_FILE), help="Dutchie API .env file for --sales-source api.")
    parser.add_argument("--skip-sales", action="store_true", help="Use existing files/sales*.xlsx instead of pulling new sales exports.")
    parser.add_argument("--skip-upload", action="store_true", help="Generate reports but do not upload to Drive.")
    parser.add_argument("--skip-email", action="store_true", help="Generate/upload reports but do not send Gmail.")
    parser.add_argument("--sync-brand-config", action="store_true", help="Also sync the deals brand config reference before report generation.")
    parser.add_argument("--dry-run", action="store_true", help="Print the planned run without pulling sales, writing reports, uploading, or emailing.")
    return parser


def determine_report_range(args: argparse.Namespace) -> tuple[date, date]:
    if args.start_date or args.end_date:
        if not args.start_date or not args.end_date:
            raise ValueError("--start-date and --end-date must be provided together.")
        start_day = parse_date(args.start_date)
        end_day = parse_date(args.end_date)
    else:
        as_of = parse_date(args.as_of) if args.as_of else None
        start_day, end_day = previous_month_range(as_of)

    if end_day < start_day:
        raise ValueError("end date cannot be earlier than start date.")
    return start_day, end_day


def main(argv: list[str] | None = None) -> int:
    args = build_parser().parse_args(argv)
    os.chdir(BASE_DIR)

    start_day, end_day = determine_report_range(args)
    requested_brands = split_arg_values(args.brands) or DEFAULT_MONTHLY_BRANDS
    brands = resolve_monthly_brands(requested_brands)
    recipients = parse_recipients(args.recipient, production=args.production)
    month_label = month_folder_name(start_day)
    output_dir = Path(args.output_root).expanduser().resolve() / month_slug(start_day)

    subject_prefix = "" if args.production else "[TEST] "
    subject = f"{subject_prefix}Monthly Brand Reports - {month_label}"

    print("===== Starting monthly_brand_reports_job.py =====")
    print(f"[RANGE] {start_day.isoformat()} -> {end_day.isoformat()}")
    print(f"[BRANDS] {', '.join(brands)}")
    print(f"[OUTPUT] {output_dir}")
    print(f"[DRIVE] {args.drive_parent_folder}/{month_label}")
    print(f"[EMAIL] {', '.join(recipients)}")

    if args.dry_run:
        print("[DRY-RUN] No sales pull, report generation, Drive upload, or email was performed.")
        return 0

    if not args.skip_sales:
        cleanup_sales_exports()
        if args.sales_source == "api":
            run_sales_report_api(start_day, end_day, env_file=args.env_file)
        else:
            run_sales_report_browser(start_day, end_day)
    else:
        print("[SALES] Skipping sales pull and using existing files/sales*.xlsx")

    output_dir.mkdir(parents=True, exist_ok=True)
    run_deals_reports(
        selected_brands=brands,
        output_dir=output_dir,
        old_dir=output_dir / "old",
        archive_existing=True,
        sync_reference=args.sync_brand_config,
        sync_sheet=args.sync_brand_config,
    )

    reports = list_report_files(output_dir)
    if not reports:
        raise RuntimeError(f"No monthly report files were generated in {output_dir}")
    print(f"[REPORTS] Generated {len(reports)} workbook(s).")

    upload_result = DriveUploadResult(folder_link="", file_links=[])
    if args.skip_upload:
        print("[DRIVE] Upload skipped.")
    else:
        upload_result = upload_reports_to_drive(reports, args.drive_parent_folder, month_label)
        links_path = write_links_file(output_dir, upload_result.file_links)
        print(f"[DRIVE] Links written to {links_path}")

    if args.skip_email:
        print("[EMAIL] Email skipped.")
    else:
        html_body = build_email_html(
            start_day=start_day,
            end_day=end_day,
            brands=brands,
            folder_link=upload_result.folder_link,
            uploads=upload_result.file_links,
            report_paths=reports,
            upload_skipped=args.skip_upload,
        )
        send_email_with_gmail_html(subject=subject, html_body=html_body, recipients=recipients)

    print("===== monthly_brand_reports_job.py completed successfully. =====")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
