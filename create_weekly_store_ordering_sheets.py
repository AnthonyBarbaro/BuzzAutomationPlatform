#!/usr/bin/env python3
from __future__ import annotations

import argparse
from pathlib import Path

from deals_brand_config_sync import authenticate_sheets
from dutchie_api_reports import STORE_CODES
from weekly_store_ordering_sheets import upsert_readme_tab


BASE_DIR = Path(__file__).resolve().parent
DEFAULT_OUTPUT_PATH = BASE_DIR / "weekly_store_ordering_sheet_url.txt"
DEFAULT_TITLE_PREFIX = "Buzz Weekly Store Ordering"


def load_existing_targets(path: Path) -> dict[str, str]:
    targets: dict[str, str] = {}
    if not path.exists():
        return targets

    for raw_line in path.read_text(encoding="utf-8").splitlines():
        line = raw_line.strip()
        if not line or line.startswith("#") or "=" not in line:
            continue
        raw_key, raw_value = line.split("=", 1)
        key = str(raw_key or "").strip().upper()
        value = str(raw_value or "").strip()
        if key in STORE_CODES and value:
            targets[key] = value
    return targets


def write_targets(path: Path, targets: dict[str, str]) -> None:
    lines = [
        "# Store-specific weekly ordering spreadsheet targets.",
        "# Format: STORE_CODE=https://docs.google.com/spreadsheets/d/.../edit",
        "# The weekly ordering job will use the matching store URL for each run.",
        "",
    ]
    for code in STORE_CODES:
        lines.append(f"{code}={targets.get(code, '')}")
    path.write_text("\n".join(lines) + "\n", encoding="utf-8")


def create_spreadsheet(service, store_code: str, store_name: str, title_prefix: str) -> tuple[str, str]:
    title = f"{title_prefix} - {store_code} - {store_name}"
    body = {
        "properties": {"title": title},
        "sheets": [{"properties": {"title": "README"}}],
    }
    result = service.spreadsheets().create(
        body=body,
        fields="spreadsheetId,spreadsheetUrl,properties.title",
    ).execute()
    spreadsheet_id = str(result.get("spreadsheetId", "")).strip()
    spreadsheet_url = f"https://docs.google.com/spreadsheets/d/{spreadsheet_id}/edit" if spreadsheet_id else ""
    if spreadsheet_id:
        upsert_readme_tab(
            service=service,
            spreadsheet_id=spreadsheet_id,
            store_code=store_code,
            store_name=store_name,
        )
    return spreadsheet_id, spreadsheet_url


def build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(
        description="Create one Google Spreadsheet per store for the weekly ordering flow and write their URLs to weekly_store_ordering_sheet_url.txt.",
    )
    parser.add_argument(
        "--output-file",
        default=str(DEFAULT_OUTPUT_PATH),
        help=f"Where to write the store spreadsheet URL mapping. Default: {DEFAULT_OUTPUT_PATH}",
    )
    parser.add_argument(
        "--title-prefix",
        default=DEFAULT_TITLE_PREFIX,
        help=f"Spreadsheet title prefix. Default: {DEFAULT_TITLE_PREFIX}",
    )
    parser.add_argument(
        "--replace-existing",
        action="store_true",
        help="Create new spreadsheets even for stores that already have URLs in the output file.",
    )
    return parser


def main() -> int:
    args = build_parser().parse_args()
    output_path = Path(args.output_file).resolve()
    output_path.parent.mkdir(parents=True, exist_ok=True)

    targets = {} if args.replace_existing else load_existing_targets(output_path)
    service = authenticate_sheets()

    for code, name in STORE_CODES.items():
        if not args.replace_existing and targets.get(code):
            print(f"[SKIP] {code}: {targets[code]}")
            continue

        spreadsheet_id, spreadsheet_url = create_spreadsheet(service, code, name, args.title_prefix)
        targets[code] = spreadsheet_url
        print(f"[CREATE] {code}: {spreadsheet_id} -> {spreadsheet_url}")

    write_targets(output_path, targets)
    print(f"[DONE] Wrote store spreadsheet URLs to {output_path}")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
