#!/usr/bin/env python3
"""Refresh catalog exports, then sync the location-priced product list."""

from __future__ import annotations

import os
import runpy
import sys
from pathlib import Path

from getCatalog_browser import run_catalog_browser_export


REPO_ROOT = Path(__file__).resolve().parent
FILES_DIR = REPO_ROOT / "files"
LOCATION_PRICE_SCRIPT = REPO_ROOT / "other-scripts" / "location_priced_product_list.py"
CATALOG_EXPORT_SUFFIXES = {".csv", ".xls", ".xlsx"}


def _catalog_stores_from_env() -> list[str] | None:
    raw = os.getenv("CATALOG_STORES", "").strip()
    if not raw:
        return None
    return [store for store in raw.split() if store]


def clear_catalog_output_dir(output_dir: Path) -> int:
    output_dir.mkdir(parents=True, exist_ok=True)
    deleted = 0
    for path in output_dir.iterdir():
        if not path.is_file() or path.suffix.lower() not in CATALOG_EXPORT_SUFFIXES:
            continue
        path.unlink()
        deleted += 1
    return deleted


if __name__ == "__main__":
    os.chdir(REPO_ROOT)

    if any(arg in {"-h", "--help"} for arg in sys.argv[1:]):
        sys.argv[0] = str(LOCATION_PRICE_SCRIPT)
        runpy.run_path(str(LOCATION_PRICE_SCRIPT), run_name="__main__")

    output_dir = Path(os.getenv("CATALOG_OUTPUT_DIR", str(FILES_DIR))).expanduser()
    if not output_dir.is_absolute():
        output_dir = REPO_ROOT / output_dir

    deleted = clear_catalog_output_dir(output_dir)
    print(f"[STEP] Cleared {deleted} old catalog export file(s) from {output_dir}")

    print("[STEP] Refreshing Dutchie catalog CSVs")
    run_catalog_browser_export(output_dir=str(output_dir), stores=_catalog_stores_from_env())

    print("[STEP] Building and syncing location price sheet")
    sys.argv[0] = str(LOCATION_PRICE_SCRIPT)
    if output_dir != FILES_DIR and "--input-dir" not in sys.argv:
        sys.argv.extend(["--input-dir", str(output_dir)])
    runpy.run_path(str(LOCATION_PRICE_SCRIPT), run_name="__main__")
