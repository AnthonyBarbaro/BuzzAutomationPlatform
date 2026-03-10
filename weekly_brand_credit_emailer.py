#!/usr/bin/env python3
"""
Send weekly brand credit emails for the brands that need both deal report links
and inventory folder links in the same message.

Expected workflow:
1. Run BrandINVEmailer.py to generate inventory folders and inventory_links/latest.json
2. Run deals.py to generate brand_reports/*.xlsx
3. Run uploadDrive.py (or equivalent) to refresh links.txt with deal report links
4. Run this script to email the external brand contacts and CC Buzz finance contacts
"""

import argparse
import base64
import datetime
import json
import os
import re
import sys
from email.mime.base import MIMEBase
from email.mime.multipart import MIMEMultipart
from email.mime.text import MIMEText
from email import encoders
from email.utils import formatdate

import openpyxl


CREDENTIALS_FILE = "credentials.json"
TOKEN_GMAIL_FILE = "token_gmail.json"
DEFAULT_REPORTS_DIR = "brand_reports"
DEFAULT_LINKS_FILE = "links.txt"
DEFAULT_INVENTORY_LINKS_FILE = os.path.join("inventory_links", "latest.json")
BUZZ_CC = ["joseph@buzzcannabis.com", "donna@buzzcannabis.com"]

WEEKLY_BRAND_EMAILS = [
    {
        "brand": "Hashish",
        "report_aliases": ["Hashish"],
        "inventory_folder": "Hashish",
        "to": ["ryanbtcventures@gmail.com"],
    },
    {
        "brand": "TreeSap",
        "report_aliases": ["TreeSap", "Treesap"],
        "inventory_folder": "Treesap",
        "to": ["sales@treesapsyrup.com"],
    },
]


def normalize_key(value):
    return re.sub(r"[^a-z0-9]+", "", str(value or "").lower())


def emit_status(message, status_callback=None):
    if status_callback:
        status_callback(message)
    else:
        print(message)


def gmail_authenticate():
    import google.auth.transport.requests
    from google_auth_oauthlib.flow import InstalledAppFlow
    from google.oauth2.credentials import Credentials
    from googleapiclient.discovery import build

    scopes = ["https://www.googleapis.com/auth/gmail.send"]
    creds = None
    if os.path.exists(TOKEN_GMAIL_FILE):
        creds = Credentials.from_authorized_user_file(TOKEN_GMAIL_FILE, scopes)
    if not creds or not creds.valid:
        if creds and creds.expired and creds.refresh_token:
            creds.refresh(google.auth.transport.requests.Request())
        else:
            flow = InstalledAppFlow.from_client_secrets_file(CREDENTIALS_FILE, scopes)
            creds = flow.run_local_server(port=0)
        with open(TOKEN_GMAIL_FILE, "w", encoding="utf-8") as f:
            f.write(creds.to_json())

    return build("gmail", "v1", credentials=creds)


def send_email_with_gmail_html(
    subject,
    html_body,
    recipients,
    cc_recipients=None,
    attachments=None,
    dry_run=False,
    status_callback=None,
):
    if isinstance(recipients, str):
        recipients = [recipients]
    cc_recipients = list(cc_recipients or [])
    attachments = list(attachments or [])

    if dry_run:
        emit_status(f"[DRY RUN] Subject: {subject}", status_callback)
        emit_status(f"[DRY RUN] To: {', '.join(recipients)}", status_callback)
        if cc_recipients:
            emit_status(f"[DRY RUN] Cc: {', '.join(cc_recipients)}", status_callback)
        if attachments:
            emit_status(f"[DRY RUN] Attachments: {attachments}", status_callback)
        return

    service = gmail_authenticate()

    msg = MIMEMultipart("alternative")
    msg["From"] = "me"
    msg["To"] = ", ".join(recipients)
    if cc_recipients:
        msg["Cc"] = ", ".join(cc_recipients)
    msg["Date"] = formatdate(localtime=True)
    msg["Subject"] = subject
    msg.attach(MIMEText(html_body, "html"))

    for file_path in attachments:
        if not os.path.isfile(file_path):
            continue
        with open(file_path, "rb") as fp:
            file_data = fp.read()
        part = MIMEBase("application", "octet-stream")
        part.set_payload(file_data)
        encoders.encode_base64(part)
        part.add_header(
            "Content-Disposition",
            f'attachment; filename="{os.path.basename(file_path)}"',
        )
        msg.attach(part)

    raw_message = base64.urlsafe_b64encode(msg.as_bytes()).decode()
    body = {"raw": raw_message}
    all_recipients = recipients + cc_recipients

    sent = service.users().messages().send(userId="me", body=body).execute()
    emit_status(
        f"[GMAIL] Email sent to {all_recipients} | ID: {sent['id']} | Subject: {subject}",
        status_callback,
    )


def parse_kickback_summary(report_path):
    results = []
    wb = openpyxl.load_workbook(report_path, data_only=True)
    try:
        if "Summary" not in wb.sheetnames:
            return results

        sheet = wb["Summary"]
        for row_idx in range(2, sheet.max_row + 1):
            store_val = sheet.cell(row=row_idx, column=1).value
            owed_val = sheet.cell(row=row_idx, column=2).value
            if store_val is None or owed_val is None:
                continue

            store_str = str(store_val).strip().lower()
            owed_str = str(owed_val).strip().lower()
            if store_str in {"", "store"} or owed_str in {"", "kickback owed"}:
                continue

            results.append((store_val, owed_val))
    finally:
        wb.close()

    return results


def build_kickback_table(rows):
    if not rows:
        return "<p>(No kickback summary data found.)</p>"

    html = [
        "<table border='1' cellpadding='6' cellspacing='0'>",
        "<thead><tr><th>Store</th><th>Kickback Owed</th></tr></thead>",
        "<tbody>",
    ]
    for store, owed in rows:
        try:
            owed_text = f"${float(owed):,.2f}"
        except (TypeError, ValueError):
            owed_text = str(owed)
        html.append(f"<tr><td>{store}</td><td>{owed_text}</td></tr>")
    html.append("</tbody></table>")
    return "\n".join(html)


def parse_report_link_line(line):
    if ":" not in line:
        return None

    filename, url = line.split(":", 1)
    filename = filename.strip()
    url = url.strip()
    if not url.startswith("http"):
        return None

    match = re.match(
        r"^(?P<brand>.+?)_report_(?P<start>\d{4}-\d{2}-\d{2})_to_(?P<end>\d{4}-\d{2}-\d{2})\.xlsx$",
        filename,
        re.IGNORECASE,
    )
    if match:
        return {
            "brand": match.group("brand").strip(),
            "filename": filename,
            "url": url,
            "start_date": match.group("start"),
            "end_date": match.group("end"),
        }

    return None


def load_report_links(links_file):
    entries = []
    if not os.path.isfile(links_file):
        return entries

    with open(links_file, "r", encoding="utf-8") as f:
        for raw_line in f:
            line = raw_line.strip()
            if not line:
                continue
            parsed = parse_report_link_line(line)
            if parsed:
                entries.append(parsed)

    return entries


def build_report_link_list(entries):
    if not entries:
        return "<p>(No deal report links found.)</p>"

    html = ["<ul>"]
    for entry in entries:
        html.append(
            "<li><strong>{filename}</strong>: <a href='{url}'>{url}</a></li>".format(
                filename=entry["filename"],
                url=entry["url"],
            )
        )
    html.append("</ul>")
    return "\n".join(html)


def load_inventory_links(manifest_path):
    payload = load_inventory_manifest(manifest_path)
    folders = payload.get("folders", {})
    out = {}
    for folder_name, info in folders.items():
        if isinstance(info, str):
            info = {"link": info, "emails": []}
        out[normalize_key(folder_name)] = {
            "folder_name": folder_name,
            "link": info.get("link", ""),
            "emails": info.get("emails", []),
        }
    return out


def default_inventory_manifest():
    now = datetime.datetime.now()
    return {
        "date": now.strftime("%Y-%m-%d"),
        "day": now.strftime("%A"),
        "generated_at": now.isoformat(timespec="seconds"),
        "folders": {},
    }


def load_inventory_manifest(manifest_path):
    if not os.path.isfile(manifest_path):
        return default_inventory_manifest()

    with open(manifest_path, "r", encoding="utf-8") as f:
        payload = json.load(f)

    if not isinstance(payload, dict):
        return default_inventory_manifest()

    payload.setdefault("date", datetime.datetime.now().strftime("%Y-%m-%d"))
    payload.setdefault("day", datetime.datetime.now().strftime("%A"))
    payload.setdefault("generated_at", datetime.datetime.now().isoformat(timespec="seconds"))
    payload.setdefault("folders", {})
    return payload


def save_inventory_manifest(manifest_path, payload, status_callback=None):
    parent_dir = os.path.dirname(manifest_path)
    if parent_dir:
        os.makedirs(parent_dir, exist_ok=True)

    payload["generated_at"] = datetime.datetime.now().isoformat(timespec="seconds")
    payload.setdefault("date", datetime.datetime.now().strftime("%Y-%m-%d"))
    payload.setdefault("day", datetime.datetime.now().strftime("%A"))
    payload.setdefault("folders", {})

    with open(manifest_path, "w", encoding="utf-8") as f:
        json.dump(payload, f, indent=2, sort_keys=True)

    emit_status(f"[INFO] Saved inventory links manifest: {manifest_path}", status_callback)


def set_inventory_link(payload, folder_name, link, emails=None):
    folders = payload.setdefault("folders", {})
    existing = folders.get(folder_name, {})
    existing_emails = []
    if isinstance(existing, dict):
        existing_emails = existing.get("emails", []) or []

    merged_emails = list(dict.fromkeys(list(existing_emails) + list(emails or [])))
    folders[folder_name] = {
        "link": link,
        "emails": merged_emails,
    }

    return {
        "folder_name": folder_name,
        "link": link,
        "emails": merged_emails,
    }


def find_latest_report(reports_dir, aliases):
    pattern = re.compile(
        r"^(?P<brand>.+?)_report_(?P<start>\d{4}-\d{2}-\d{2})_to_(?P<end>\d{4}-\d{2}-\d{2})\.xlsx$",
        re.IGNORECASE,
    )
    alias_keys = {normalize_key(alias) for alias in aliases}
    candidates = []

    if not os.path.isdir(reports_dir):
        return None

    for filename in os.listdir(reports_dir):
        match = pattern.match(filename)
        if not match:
            continue
        if normalize_key(match.group("brand")) not in alias_keys:
            continue

        candidates.append(
            {
                "brand": match.group("brand").strip(),
                "filename": filename,
                "path": os.path.join(reports_dir, filename),
                "start_date": match.group("start"),
                "end_date": match.group("end"),
            }
        )

    if not candidates:
        return None

    candidates.sort(key=lambda item: (item["end_date"], item["start_date"], item["filename"]))
    return candidates[-1]


def select_report_links(report_info, link_entries, aliases):
    alias_keys = {normalize_key(alias) for alias in aliases}
    matching = [
        entry for entry in link_entries
        if entry["filename"] == report_info["filename"]
    ]
    if matching:
        return matching

    return [
        entry for entry in link_entries
        if normalize_key(entry["brand"]) in alias_keys
    ]


def build_email_body(brand_label, report_info, inventory_info, report_links, kickback_rows):
    inventory_folder_name = inventory_info["folder_name"]
    inventory_link = inventory_info["link"]
    report_link_html = build_report_link_list(report_links)
    kickback_html = build_kickback_table(kickback_rows)

    return f"""
    <html>
      <body>
        <p>Hello,</p>
        <p>Please see below the {brand_label} brand deals for <strong>{report_info['start_date']} to {report_info['end_date']}</strong>, along with the inventory folder link.</p>
        <h3>Folder: {inventory_folder_name}</h3>
        <p>Link: <a href="{inventory_link}">{inventory_link}</a></p>
        <h3>{brand_label}</h3>
        <p><strong>Links:</strong></p>
        {report_link_html}
        <h3>Kickback Summary:</h3>
        {kickback_html}
        <p>Joseph and Donna are copied on this credit email.</p>
        <p>Regards,<br>Buzz Cannabis</p>
      </body>
    </html>
    """


def parse_args():
    parser = argparse.ArgumentParser(description="Send weekly deal + inventory credit emails.")
    parser.add_argument(
        "--brands",
        nargs="*",
        help="Optional subset to send, e.g. --brands Hashish TreeSap",
    )
    parser.add_argument(
        "--reports-dir",
        default=DEFAULT_REPORTS_DIR,
        help=f"Directory containing brand deal reports (default: {DEFAULT_REPORTS_DIR})",
    )
    parser.add_argument(
        "--links-file",
        default=DEFAULT_LINKS_FILE,
        help=f"links.txt path containing Drive URLs for uploaded deal reports (default: {DEFAULT_LINKS_FILE})",
    )
    parser.add_argument(
        "--inventory-links-file",
        default=DEFAULT_INVENTORY_LINKS_FILE,
        help=f"Inventory link manifest written by BrandINVEmailer.py (default: {DEFAULT_INVENTORY_LINKS_FILE})",
    )
    parser.add_argument(
        "--inventory-link",
        action="append",
        default=[],
        metavar="BRAND=URL",
        help="Provide or override an inventory folder link and save it to the manifest. Repeat as needed.",
    )
    parser.add_argument(
        "--dry-run",
        action="store_true",
        help="Preview the sends without actually emailing Gmail recipients.",
    )
    parser.add_argument(
        "--test-email",
        help="Override all external recipients with one test email address while still CCing Buzz contacts.",
    )
    parser.add_argument(
        "--no-attachments",
        action="store_true",
        help="Do not attach the local XLSX deal report to the email.",
    )
    return parser.parse_args()


def should_include_brand(brand_cfg, requested_brands):
    if not requested_brands:
        return True

    requested = {normalize_key(value) for value in requested_brands}
    candidates = {normalize_key(brand_cfg["brand"]), normalize_key(brand_cfg["inventory_folder"])}
    candidates.update(normalize_key(alias) for alias in brand_cfg["report_aliases"])
    return not candidates.isdisjoint(requested)


def parse_inventory_link_overrides(raw_values):
    overrides = {}
    invalid = []

    for raw in raw_values:
        if "=" not in raw:
            invalid.append(raw)
            continue
        brand_key, link = raw.split("=", 1)
        brand_key = brand_key.strip()
        link = link.strip()
        if not brand_key or not link:
            invalid.append(raw)
            continue
        overrides[normalize_key(brand_key)] = link

    if invalid:
        raise ValueError(
            "Invalid --inventory-link value(s): "
            + ", ".join(invalid)
            + ". Use BRAND=URL."
        )

    return overrides


def get_inventory_override_for_brand(brand_cfg, overrides):
    candidates = [
        brand_cfg["brand"],
        brand_cfg["inventory_folder"],
        *brand_cfg["report_aliases"],
    ]
    for candidate in candidates:
        link = overrides.get(normalize_key(candidate))
        if link:
            return link
    return None


def prompt_for_inventory_link(brand_cfg):
    if not sys.stdin.isatty():
        return None

    prompt = (
        f"Enter inventory folder link for {brand_cfg['brand']} "
        f"({brand_cfg['inventory_folder']}) and press Enter "
        "or leave blank to skip: "
    )
    try:
        entered = input(prompt).strip()
    except EOFError:
        return None

    return entered or None


def normalize_inventory_overrides(inventory_overrides):
    if not inventory_overrides:
        return {}

    if isinstance(inventory_overrides, dict):
        return {
            normalize_key(key): str(value).strip()
            for key, value in inventory_overrides.items()
            if str(value).strip()
        }

    return parse_inventory_link_overrides(inventory_overrides)


def run_weekly_brand_credit_emailer(
    selected_brands=None,
    reports_dir=DEFAULT_REPORTS_DIR,
    links_file=DEFAULT_LINKS_FILE,
    inventory_links_file=DEFAULT_INVENTORY_LINKS_FILE,
    inventory_overrides=None,
    dry_run=False,
    test_email=None,
    no_attachments=False,
    prompt_for_missing=False,
    status_callback=None,
):
    inventory_overrides = normalize_inventory_overrides(inventory_overrides)
    report_links = load_report_links(links_file)
    inventory_manifest = load_inventory_manifest(inventory_links_file)
    inventory_links = load_inventory_links(inventory_links_file)

    failures = []
    sends = 0

    for brand_cfg in WEEKLY_BRAND_EMAILS:
        if not should_include_brand(brand_cfg, selected_brands):
            continue

        report_info = find_latest_report(reports_dir, brand_cfg["report_aliases"])
        if not report_info:
            failures.append(f"{brand_cfg['brand']}: no report found in {reports_dir}")
            continue

        selected_links = select_report_links(report_info, report_links, brand_cfg["report_aliases"])
        if not selected_links:
            failures.append(f"{brand_cfg['brand']}: no Drive report link found in {links_file}")
            continue

        inventory_info = inventory_links.get(normalize_key(brand_cfg["inventory_folder"]))
        override_link = get_inventory_override_for_brand(brand_cfg, inventory_overrides)
        if override_link:
            inventory_info = set_inventory_link(
                inventory_manifest,
                folder_name=brand_cfg["inventory_folder"],
                link=override_link,
                emails=brand_cfg["to"] + BUZZ_CC,
            )
            save_inventory_manifest(inventory_links_file, inventory_manifest, status_callback=status_callback)
            inventory_links = load_inventory_links(inventory_links_file)

        if (not inventory_info or not inventory_info.get("link")) and prompt_for_missing:
            prompted_link = prompt_for_inventory_link(brand_cfg)
            if prompted_link:
                inventory_info = set_inventory_link(
                    inventory_manifest,
                    folder_name=brand_cfg["inventory_folder"],
                    link=prompted_link,
                    emails=brand_cfg["to"] + BUZZ_CC,
                )
                save_inventory_manifest(inventory_links_file, inventory_manifest, status_callback=status_callback)
                inventory_links = load_inventory_links(inventory_links_file)

        if not inventory_info or not inventory_info.get("link"):
            failures.append(
                f"{brand_cfg['brand']}: no inventory folder link found for '{brand_cfg['inventory_folder']}' in {inventory_links_file}"
            )
            continue

        kickback_rows = parse_kickback_summary(report_info["path"])
        html_body = build_email_body(
            brand_label=brand_cfg["brand"],
            report_info=report_info,
            inventory_info=inventory_info,
            report_links=selected_links,
            kickback_rows=kickback_rows,
        )
        subject = (
            f"{brand_cfg['brand']} Brand Deals for {report_info['start_date']} to "
            f"{report_info['end_date']} and Inventory"
        )
        attachments = [] if no_attachments else [report_info["path"]]
        recipients = [test_email] if test_email else brand_cfg["to"]

        send_email_with_gmail_html(
            subject=subject,
            html_body=html_body,
            recipients=recipients,
            cc_recipients=BUZZ_CC,
            attachments=attachments,
            dry_run=dry_run,
            status_callback=status_callback,
        )
        sends += 1

    if failures:
        emit_status("[WARN] Some brand emails were skipped:", status_callback)
        for failure in failures:
            emit_status(f"  - {failure}", status_callback)

    return {
        "sends": sends,
        "failures": failures,
    }


def main():
    args = parse_args()
    try:
        inventory_overrides = parse_inventory_link_overrides(args.inventory_link)
    except ValueError as exc:
        print(f"[ERROR] {exc}")
        sys.exit(2)

    result = run_weekly_brand_credit_emailer(
        selected_brands=args.brands,
        reports_dir=args.reports_dir,
        links_file=args.links_file,
        inventory_links_file=args.inventory_links_file,
        inventory_overrides=inventory_overrides,
        dry_run=args.dry_run,
        test_email=args.test_email,
        no_attachments=args.no_attachments,
        prompt_for_missing=True,
    )

    if result["sends"] == 0 and not args.dry_run:
        sys.exit(1)


if __name__ == "__main__":
    main()
