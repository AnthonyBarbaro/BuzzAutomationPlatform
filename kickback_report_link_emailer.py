#!/usr/bin/env python3
import argparse
import base64
import html
import re
from collections import deque
from dataclasses import dataclass
from datetime import date, datetime, timedelta
from email.message import EmailMessage
from pathlib import Path
from typing import Iterable, Sequence

from google.auth.transport.requests import Request
from google.oauth2.credentials import Credentials
from google_auth_oauthlib.flow import InstalledAppFlow
from googleapiclient.discovery import build


THIS_DIR = Path(__file__).resolve().parent
CREDENTIALS_FILE = THIS_DIR / "credentials.json"
GMAIL_TOKEN_FILE = THIS_DIR / "token_gmail.json"
DRIVE_TOKEN_FILE = THIS_DIR / "token_drive.json"
READONLY_DRIVE_TOKEN_FILE = THIS_DIR / "token_drive_readonly.json"
GMAIL_SCOPES = ["https://www.googleapis.com/auth/gmail.send"]
DRIVE_SCOPES = ["https://www.googleapis.com/auth/drive.file"]
FOLDER_MIME_TYPE = "application/vnd.google-apps.folder"
ANTHONY_EMAIL = "anthony@buzzcannabis.com"
DEFAULT_RECIPIENTS = [ANTHONY_EMAIL]
DEFAULT_SUPPORT_LINE = (
    "Please include anthony@buzzcannabis.com "
    "in all emails regarding these credits."
)
KICKBACK_ROOTS = {
    "2026": {
        "label": "2026_Kickback",
        "folder_id": "1DeUaZcMM3cE5L0seh0QzfdpA1rvfCov3",
    },
    "2025": {
        "label": "2025_Kickback",
        "folder_id": "1NgVdfjgdmhpt1j39Cabawg5CJyFDGOLf",
    },
}


@dataclass(frozen=True)
class ReportMatch:
    root_key: str
    root_label: str
    folder_path: tuple[str, ...]
    file_name: str
    file_id: str
    web_view_link: str
    start_date: datetime | None
    end_date: datetime | None


def _extract_folder_id(folder_ref: str) -> str:
    text = str(folder_ref or "").strip()
    if not text:
        raise ValueError("Folder reference cannot be empty.")

    match = re.search(r"/folders/([a-zA-Z0-9_-]+)", text)
    if match:
        return match.group(1)

    if re.fullmatch(r"[a-zA-Z0-9_-]{10,}", text):
        return text

    raise ValueError(f"Could not extract a Google Drive folder id from: {folder_ref!r}")


def _normalize_for_match(text: str) -> str:
    return re.sub(r"[^a-z0-9]+", "", str(text or "").lower())


def _file_matches_brand(file_name: str, brand_query: str) -> bool:
    normalized_brand = _normalize_for_match(brand_query)
    if not normalized_brand:
        return False
    normalized_name = _normalize_for_match(file_name)
    return normalized_brand in normalized_name


def _parse_report_dates(file_name: str) -> tuple[datetime | None, datetime | None]:
    match = re.search(r"_(\d{4}-\d{2}-\d{2})_to_(\d{4}-\d{2}-\d{2})", file_name)
    if not match:
        return None, None

    start_text, end_text = match.groups()
    try:
        return (
            datetime.fromisoformat(start_text),
            datetime.fromisoformat(end_text),
        )
    except ValueError:
        return None, None


def _parse_date_token(text: str, default_year: int) -> date:
    value = str(text or "").strip()
    if re.fullmatch(r"\d{4}-\d{1,2}-\d{1,2}", value):
        year_text, month_text, day_text = value.split("-")
        return date(int(year_text), int(month_text), int(day_text))

    match = re.fullmatch(r"(\d{1,2})[/-](\d{1,2})", value)
    if match:
        month_text, day_text = match.groups()
        return date(default_year, int(month_text), int(day_text))

    raise ValueError(f"Could not parse date token: {text!r}")


def parse_date_range_text(text: str, default_year: int) -> tuple[date, date]:
    tokens = re.findall(r"\d{4}-\d{1,2}-\d{1,2}|\d{1,2}[/-]\d{1,2}", str(text or ""))
    if len(tokens) < 2:
        raise ValueError(f"Date range must include a start and end date: {text!r}")

    start_day = _parse_date_token(tokens[0], default_year)
    end_day = _parse_date_token(tokens[1], default_year)
    if end_day < start_day:
        end_day = end_day.replace(year=end_day.year + 1)
    return start_day, end_day


def _format_date_range(window: tuple[date, date]) -> str:
    return f"{window[0].isoformat()} to {window[1].isoformat()}"


def _parse_folder_date_range(match: ReportMatch) -> tuple[date, date] | None:
    for folder_name in reversed(match.folder_path):
        tokens = re.findall(r"\d{1,2}[/-]\d{1,2}", folder_name)
        if len(tokens) < 2:
            continue
        try:
            start_day = _parse_date_token(tokens[0], int(match.root_key))
            end_day = _parse_date_token(tokens[1], int(match.root_key))
        except ValueError:
            continue
        if end_day < start_day:
            if start_day.month == 12 and end_day.month == 1:
                start_day = start_day.replace(year=start_day.year - 1)
            else:
                end_day = end_day.replace(year=end_day.year + 1)
        return start_day, end_day
    return None


def _report_date_windows(match: ReportMatch) -> list[tuple[date, date]]:
    windows: list[tuple[date, date]] = []
    if match.start_date and match.end_date:
        windows.append((match.start_date.date(), match.end_date.date()))

    folder_window = _parse_folder_date_range(match)
    if folder_window:
        windows.append(folder_window)
        windows.append((folder_window[0] - timedelta(days=1), folder_window[1] - timedelta(days=1)))

    deduped = []
    seen = set()
    for window in windows:
        if window in seen:
            continue
        seen.add(window)
        deduped.append(window)
    return deduped


def filter_reports_by_date_ranges(
    matches: Sequence[ReportMatch],
    date_ranges: Sequence[tuple[date, date]],
) -> tuple[list[ReportMatch], list[tuple[date, date]]]:
    selected: list[ReportMatch] = []
    missing: list[tuple[date, date]] = []
    seen_file_ids = set()

    for target_range in date_ranges:
        target_matches = [
            match
            for match in matches
            if target_range in _report_date_windows(match)
        ]
        if not target_matches:
            missing.append(target_range)
            continue

        for match in sorted(target_matches, key=_report_sort_key):
            if match.file_id in seen_file_ids:
                continue
            seen_file_ids.add(match.file_id)
            selected.append(match)

    return selected, missing


def _report_sort_key(match: ReportMatch) -> tuple:
    start_ord = match.start_date.toordinal() if match.start_date else -1
    end_ord = match.end_date.toordinal() if match.end_date else -1
    return (
        match.root_key,
        -end_ord,
        -start_ord,
        tuple(part.lower() for part in match.folder_path),
        match.file_name.lower(),
    )


def _build_drive_service():
    creds = None
    for token_path in (DRIVE_TOKEN_FILE, READONLY_DRIVE_TOKEN_FILE):
        if not token_path.exists():
            continue
        try:
            creds = Credentials.from_authorized_user_file(str(token_path), DRIVE_SCOPES)
            break
        except Exception:
            continue

    if not creds or not creds.valid:
        if creds and creds.expired and creds.refresh_token:
            creds.refresh(Request())
        else:
            if not CREDENTIALS_FILE.exists():
                raise RuntimeError("credentials.json not found. Run Google auth setup first.")
            flow = InstalledAppFlow.from_client_secrets_file(str(CREDENTIALS_FILE), DRIVE_SCOPES)
            creds = flow.run_local_server(port=0)
        DRIVE_TOKEN_FILE.write_text(creds.to_json(), encoding="utf-8")

    return build("drive", "v3", credentials=creds)


def _build_gmail_service():
    if not GMAIL_TOKEN_FILE.exists():
        raise RuntimeError("token_gmail.json not found. Run Gmail auth first.")

    creds = Credentials.from_authorized_user_file(str(GMAIL_TOKEN_FILE), GMAIL_SCOPES)
    if not creds.valid:
        if creds.expired and creds.refresh_token:
            creds.refresh(Request())
            GMAIL_TOKEN_FILE.write_text(creds.to_json(), encoding="utf-8")
        else:
            if not CREDENTIALS_FILE.exists():
                raise RuntimeError("credentials.json not found. Run Google auth setup first.")
            flow = InstalledAppFlow.from_client_secrets_file(str(CREDENTIALS_FILE), GMAIL_SCOPES)
            creds = flow.run_local_server(port=0)
            GMAIL_TOKEN_FILE.write_text(creds.to_json(), encoding="utf-8")

    return build("gmail", "v1", credentials=creds)


def _iter_drive_children(service, parent_id: str) -> Iterable[dict]:
    query = f"'{parent_id}' in parents and trashed = false"
    page_token = None

    while True:
        response = service.files().list(
            q=query,
            spaces="drive",
            fields="nextPageToken, files(id, name, mimeType, webViewLink)",
            pageSize=1000,
            pageToken=page_token,
        ).execute()

        for item in response.get("files", []):
            yield item

        page_token = response.get("nextPageToken")
        if not page_token:
            break


def _walk_reports(service, root_key: str, root_label: str, root_folder_id: str) -> Iterable[ReportMatch]:
    queue = deque([(root_folder_id, tuple())])

    while queue:
        folder_id, folder_path = queue.popleft()
        for item in _iter_drive_children(service, folder_id):
            name = str(item.get("name") or "").strip()
            mime_type = item.get("mimeType") or ""
            item_id = str(item.get("id") or "").strip()

            if not name or not item_id:
                continue

            if mime_type == FOLDER_MIME_TYPE:
                queue.append((item_id, folder_path + (name,)))
                continue

            if not name.lower().endswith((".xlsx", ".xls")):
                continue

            start_date, end_date = _parse_report_dates(name)
            web_view_link = item.get("webViewLink") or f"https://drive.google.com/file/d/{item_id}/view"

            yield ReportMatch(
                root_key=root_key,
                root_label=root_label,
                folder_path=folder_path,
                file_name=name,
                file_id=item_id,
                web_view_link=web_view_link,
                start_date=start_date,
                end_date=end_date,
            )


def find_brand_reports(service, brand_query: str, years: Sequence[str]) -> list[ReportMatch]:
    matches: list[ReportMatch] = []

    for year in years:
        cfg = KICKBACK_ROOTS[year]
        for report in _walk_reports(service, year, cfg["label"], cfg["folder_id"]):
            if _file_matches_brand(report.file_name, brand_query):
                matches.append(report)

    return sorted(matches, key=_report_sort_key)


def build_email_bodies(
    brand_query: str,
    matches: Sequence[ReportMatch],
    support_line: str,
    requested_date_ranges: Sequence[tuple[date, date]] | None = None,
) -> tuple[str, str]:
    safe_brand = html.escape(brand_query)
    count = len(matches)
    date_note_html = ""
    date_note_text = ""
    if requested_date_ranges:
        ranges_text = ", ".join(_format_date_range(window) for window in requested_date_ranges)
        date_note_html = f"<p>Requested date windows: {html.escape(ranges_text)}.</p>"
        date_note_text = f"Requested date windows: {ranges_text}.\n"

    intro_html = (
        f"<p>Hello,</p>"
        f"<p>I searched the 2026 and 2025 kickback Drive folders for "
        f"<strong>{safe_brand}</strong> and found <strong>{count}</strong> matching report(s).</p>"
        f"{date_note_html}"
    )
    intro_text = (
        f"Hello,\n\n"
        f"I searched the 2026 and 2025 kickback Drive folders for {brand_query} "
        f"and found {count} matching report(s).\n"
        f"{date_note_text}"
    )

    if not matches:
        no_match_html = (
            intro_html
            + f"<p><strong>Support</strong><br>{html.escape(support_line)}</p>"
        )
        no_match_text = intro_text + f"\nSupport\n{support_line}\n"
        return no_match_text, no_match_html

    html_sections = [intro_html]
    text_sections = [intro_text]
    current_root = None

    for match in matches:
        if match.root_label != current_root:
            current_root = match.root_label
            html_sections.append(
                f"<h3 style='margin-bottom:8px;'>{html.escape(match.root_label)}</h3>"
            )
            text_sections.append(f"\n{match.root_label}\n")

        folder_label = " / ".join(match.folder_path) if match.folder_path else match.root_label
        html_sections.append(
            "".join(
                [
                    "<div style='margin:0 0 16px 0;padding:12px 14px;border:1px solid #E5E7EB;border-radius:8px;'>",
                    f"<div style='color:#6B7280;font-size:12px;margin-bottom:6px;'>Folder: {html.escape(folder_label)}</div>",
                    f"<div style='font-weight:700;margin-bottom:6px;'>Share &quot;{html.escape(match.file_name)}&quot;</div>",
                    f"<div><a href='{html.escape(match.web_view_link, quote=True)}'>{html.escape(match.web_view_link)}</a></div>",
                    "</div>",
                ]
            )
        )
        text_sections.append(
            "\n".join(
                [
                    f"Folder: {folder_label}",
                    f'Share "{match.file_name}"',
                    match.web_view_link,
                    "",
                ]
            )
        )

    html_sections.append(
        f"<p><strong>Support</strong><br>{html.escape(support_line)}</p>"
    )
    text_sections.append(f"\nSupport\n{support_line}\n")
    return "\n".join(text_sections).strip() + "\n", "\n".join(html_sections)


def anthony_only_recipients(recipients: Sequence[str] | None) -> list[str]:
    return [ANTHONY_EMAIL]


def send_email(subject: str, text_body: str, html_body: str, recipients: Sequence[str]) -> str:
    recipients = anthony_only_recipients(recipients)
    service = _build_gmail_service()
    message = EmailMessage()
    message["From"] = "me"
    message["To"] = ", ".join(recipients)
    message["Subject"] = subject
    message.set_content(text_body)
    message.add_alternative(html_body, subtype="html")

    payload = {"raw": base64.urlsafe_b64encode(message.as_bytes()).decode("utf-8")}
    sent = service.users().messages().send(userId="me", body=payload).execute()
    return str(sent.get("id") or "")


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        description="Search kickback Drive folders for a brand report and email the share links."
    )
    parser.add_argument("--brand", help="Brand/report name to search for, e.g. Pusha")
    parser.add_argument(
        "--year",
        choices=["2025", "2026", "all"],
        default="all",
        help="Which kickback root to search (default: all)",
    )
    parser.add_argument(
        "--to",
        nargs="+",
        default=DEFAULT_RECIPIENTS,
        help="Recipient email address(es). Kickback sends are forced to anthony@buzzcannabis.com.",
    )
    parser.add_argument(
        "--dry-run",
        action="store_true",
        help="Preview the email body in the terminal without sending.",
    )
    parser.add_argument(
        "--date-range",
        action="append",
        default=[],
        help=(
            "Only include reports matching this date window. Accepts MM/DD-MM/DD "
            "or YYYY-MM-DD to YYYY-MM-DD. Repeat for multiple windows."
        ),
    )
    parser.add_argument(
        "--default-year",
        type=int,
        default=datetime.now().year,
        help="Year used for MM/DD date ranges (default: current year).",
    )
    parser.add_argument(
        "--support-line",
        default=DEFAULT_SUPPORT_LINE,
        help="Support/contact text appended at the end of the email.",
    )
    parser.add_argument(
        "--subject",
        help="Override the email subject. Default: Kickback Drive Links - <brand>",
    )
    parser.add_argument(
        "--folder-2026",
        default=KICKBACK_ROOTS["2026"]["folder_id"],
        help="Override the 2026 kickback root folder id or full Drive URL.",
    )
    parser.add_argument(
        "--folder-2025",
        default=KICKBACK_ROOTS["2025"]["folder_id"],
        help="Override the 2025 kickback root folder id or full Drive URL.",
    )
    return parser.parse_args()


def main() -> int:
    args = parse_args()
    brand = (args.brand or input("Which brand/report name should I search for? ")).strip()
    if not brand:
        raise SystemExit("Brand/report name is required.")

    KICKBACK_ROOTS["2026"]["folder_id"] = _extract_folder_id(args.folder_2026)
    KICKBACK_ROOTS["2025"]["folder_id"] = _extract_folder_id(args.folder_2025)

    years = ["2026", "2025"] if args.year == "all" else [args.year]
    subject = args.subject or f"Kickback Drive Links - {brand}"

    drive_service = _build_drive_service()
    matches = find_brand_reports(drive_service, brand, years)
    requested_date_ranges = [
        parse_date_range_text(value, args.default_year)
        for value in args.date_range
    ]
    missing_date_ranges = []
    if requested_date_ranges:
        matches, missing_date_ranges = filter_reports_by_date_ranges(matches, requested_date_ranges)

    text_body, html_body = build_email_bodies(
        brand,
        matches,
        args.support_line,
        requested_date_ranges=requested_date_ranges,
    )

    print(f"[INFO] Found {len(matches)} matching report(s) for {brand}.")
    if requested_date_ranges:
        print("[INFO] Requested date range filter:")
        for window in requested_date_ranges:
            print(f"- {_format_date_range(window)}")
    if missing_date_ranges:
        print("[ERROR] No report matched these requested date range(s):")
        for window in missing_date_ranges:
            print(f"- {_format_date_range(window)}")
        return 1

    for match in matches:
        folder_label = " / ".join(match.folder_path) if match.folder_path else match.root_label
        print(f"- {match.root_label} | {folder_label} | {match.file_name}")

    if args.dry_run:
        print("\n===== EMAIL PREVIEW =====\n")
        print(text_body)
        return 0

    recipients = anthony_only_recipients(args.to)
    blocked_recipients = [
        recipient
        for recipient in args.to
        if str(recipient).strip().lower() != ANTHONY_EMAIL
    ]
    if blocked_recipients:
        print(f"[WARN] Recipient policy forced send to {ANTHONY_EMAIL} only.")

    message_id = send_email(subject, text_body, html_body, recipients)
    print(f"[GMAIL] Email sent to {', '.join(recipients)} | ID: {message_id} | Subject: {subject}")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
