#!/usr/bin/env python3
"""
Build and upload full brand inventory workbooks for one requested brand.

This is the CLI/server-friendly version of the BrandInventoryGUIemailer flow:
refresh source exports, generate the selected brand's inventory workbooks, upload
them to Google Drive, and write a local link manifest. It does not send email.
"""

from __future__ import annotations

import argparse
import json
import os
import re
import subprocess
import sys
from datetime import datetime
from pathlib import Path
from typing import Any

from BrandInventoryGUIemailer import (
    DUTCHIE_API_WORKERS,
    CATALOG_API_SCRIPT,
    ORDER_REPORT_API_SCRIPT,
    clear_old_input_exports,
    generate_brand_reports,
    list_catalog_csv_files,
    summarize_order_report_files,
    upload_brand_reports_to_drive,
)


BASE_DIR = Path(__file__).resolve().parent
DEFAULT_INPUT_DIR = BASE_DIR / "files"
DEFAULT_OUTPUT_ROOT = BASE_DIR / "reports" / "brand_inventory_requests"


def safe_slug(value: str) -> str:
    slug = re.sub(r"[^A-Za-z0-9]+", "_", str(value or "").strip()).strip("_").lower()
    return slug or "brand"


def run_script(script_name: str, *args: Any) -> None:
    script_path = BASE_DIR / script_name
    if not script_path.exists():
        raise FileNotFoundError(f"{script_name} not found at {script_path}")
    cmd = [sys.executable, str(script_path), *[str(arg) for arg in args]]
    print(f"[RUN] {' '.join(cmd)}")
    subprocess.check_call(cmd, cwd=BASE_DIR)


def refresh_sources(input_dir: Path, include_order_reports: bool = True) -> dict[str, Any]:
    input_dir.mkdir(parents=True, exist_ok=True)
    deleted = clear_old_input_exports(str(input_dir), clear_order_reports=include_order_reports)
    result: dict[str, Any] = {"deleted": len(deleted), "catalog_mode": "api", "order_mode": None}

    run_script(CATALOG_API_SCRIPT, input_dir, "--workers", DUTCHIE_API_WORKERS)
    if include_order_reports:
        run_script(ORDER_REPORT_API_SCRIPT, input_dir, "--workers", DUTCHIE_API_WORKERS)
        result["order_mode"] = "api"

    result["catalog_files"] = list_catalog_csv_files(str(input_dir))
    result["order_summary"] = summarize_order_report_files(str(input_dir))
    return result


def merge_brand_maps(target: dict[str, list[str]], source: dict[str, list[str]]) -> None:
    for brand, files in source.items():
        target.setdefault(brand, []).extend(files)


def build_brand_inventory_reports(
    input_dir: Path,
    output_dir: Path,
    selected_brands: list[str],
    include_cost: bool,
) -> dict[str, list[str]]:
    catalog_files = list_catalog_csv_files(str(input_dir))
    if not catalog_files:
        raise RuntimeError(f"No catalog CSV files found in {input_dir}.")

    all_brand_map: dict[str, list[str]] = {}
    for filename in catalog_files:
        csv_path = input_dir / filename
        brand_map = generate_brand_reports(
            str(csv_path),
            str(output_dir),
            selected_brands,
            include_cost=include_cost,
            order_reports_dir=str(input_dir),
        )
        merge_brand_maps(all_brand_map, brand_map)

    if not all_brand_map:
        raise RuntimeError(
            "No matching inventory workbooks were generated for: "
            + ", ".join(selected_brands)
        )

    return all_brand_map


def write_manifest(
    manifest_path: Path,
    brand_label: str,
    aliases: list[str],
    brand_map: dict[str, list[str]],
    links: dict[str, str],
    refresh_result: dict[str, Any],
) -> None:
    manifest_path.parent.mkdir(parents=True, exist_ok=True)
    payload = {
        "generated_at": datetime.now().isoformat(timespec="seconds"),
        "brand": brand_label,
        "aliases": aliases,
        "refresh": refresh_result,
        "workbooks": brand_map,
        "links": links,
    }
    manifest_path.write_text(json.dumps(payload, indent=2, sort_keys=True), encoding="utf-8")

    links_path = manifest_path.with_name("drive_links.txt")
    with links_path.open("w", encoding="utf-8") as handle:
        for brand, link in sorted(links.items()):
            handle.write(f"{brand}: {link}\n")


def build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(description="Build/upload a full brand inventory report without sending email.")
    parser.add_argument("--brand", required=True, help="Brand to match, e.g. Hashish, Jeeter, 710 Labs.")
    parser.add_argument("--brand-alias", action="append", default=[], help="Extra exact brand alias. Repeatable.")
    parser.add_argument("--input-dir", default=str(DEFAULT_INPUT_DIR), help="Source CSV/order-report folder.")
    parser.add_argument("--output-root", default=str(DEFAULT_OUTPUT_ROOT), help="Local output root.")
    parser.add_argument("--no-refresh", action="store_true", help="Use existing source files; do not refresh Dutchie exports.")
    parser.add_argument("--no-order-reports", action="store_true", help="Skip inventory order report refresh and Order tabs.")
    parser.add_argument("--no-cost", action="store_true", help="Hide cost columns in generated workbooks.")
    parser.add_argument("--no-drive-upload", action="store_true", help="Create local workbooks only.")
    return parser


def main(argv: list[str] | None = None) -> int:
    args = build_parser().parse_args(argv)
    input_dir = Path(args.input_dir)
    output_root = Path(args.output_root)
    if not input_dir.is_absolute():
        input_dir = BASE_DIR / input_dir
    if not output_root.is_absolute():
        output_root = BASE_DIR / output_root

    brand_label = str(args.brand).strip()
    aliases = [brand_label, *[str(alias).strip() for alias in args.brand_alias if str(alias).strip()]]
    selected_brands = list(dict.fromkeys(aliases))
    today = datetime.now().strftime("%Y-%m-%d")
    output_dir = output_root / today / safe_slug(brand_label)
    output_dir.mkdir(parents=True, exist_ok=True)

    print(f"[INFO] Building inventory report for {brand_label} ({', '.join(selected_brands)})")
    refresh_result = {"skipped": True}
    if not args.no_refresh:
        refresh_result = refresh_sources(input_dir, include_order_reports=not args.no_order_reports)

    brand_map = build_brand_inventory_reports(
        input_dir=input_dir,
        output_dir=output_dir,
        selected_brands=selected_brands,
        include_cost=not args.no_cost,
    )

    links: dict[str, str] = {}
    if not args.no_drive_upload:
        links = upload_brand_reports_to_drive(brand_map)

    manifest_path = output_dir / "brand_inventory_manifest.json"
    write_manifest(
        manifest_path=manifest_path,
        brand_label=brand_label,
        aliases=selected_brands,
        brand_map=brand_map,
        links=links,
        refresh_result=refresh_result,
    )

    workbook_count = sum(len(files) for files in brand_map.values())
    print(f"[SAVED] Manifest: {manifest_path}")
    print(f"[DONE] {workbook_count} workbook(s), {len(links)} Drive folder link(s).")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
