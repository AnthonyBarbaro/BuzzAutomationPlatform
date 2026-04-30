# Project Reorganization Plan

This is the cleanup blueprint from the April 2026 reorganization pass. It is meant to be kept as a future plan if the current file moves are reverted.

## Goal

Make the project easier to scan by grouping files by what they do, and grouping loose config files by file type.

The intended end state is:

- Root folder stays small.
- Python programs live in purpose-based folders.
- JSON, CSV, and TXT config files live under `config/`.
- Generated data and reports stay in their existing working folders so day-to-day output paths remain predictable.

## Proposed Top-Level Layout

```text
.
├── brands/
├── config/
│   ├── csv/
│   ├── json/
│   └── txt/
├── core/
├── delivery/
├── docs/
├── getters/
├── inventory/
├── jobs/
├── margins/
├── other-scripts/
│   └── random_brands/
├── reporting/
├── tests/
└── utilities/
```

Keep these existing generated/output folders as-is:

- `files/`
- `reports/`
- `brand_reports/`
- `done/`
- `inventory_links/`
- old/archive folders such as `old/`, `old_api/`, `brand_reports_api/`

## Python File Move Map

### Getters

Move Dutchie/API/browser data pullers into `getters/`:

- `dutchie_api_reports.py` -> `getters/dutchie_api_reports.py`
- `getCatalog.py` -> `getters/getCatalog.py`
- `getCatalog_browser.py` -> `getters/getCatalog_browser.py`
- `getClosingReport.py` -> `getters/getClosingReport.py`
- `getInventoryOrderReport.py` -> `getters/getInventoryOrderReport.py`
- `getInventoryOrderReport_api.py` -> `getters/getInventoryOrderReport_api.py`
- `getSalesReport.py` -> `getters/getSalesReport.py`

### Inventory

Move inventory workflows into `inventory/`:

- `BrandINVEmailer.py` -> `inventory/BrandINVEmailer.py`
- `BrandInventoryGUIemailer.py` -> `inventory/BrandInventoryGUIemailer.py`
- `inventory_order_reports.py` -> `inventory/inventory_order_reports.py`

### Brands

Move brand/deal/kickback tools into `brands/`:

- `brandDEALSEmailer.py` -> `brands/brandDEALSEmailer.py`
- `brand_meeting_gui.py` -> `brands/brand_meeting_gui.py`
- `brand_meeting_packet.py` -> `brands/brand_meeting_packet.py`
- `deals.py` -> `brands/deals.py`
- `deals_brand_config_sync.py` -> `brands/deals_brand_config_sync.py`
- `kickback_report_link_emailer.py` -> `brands/kickback_report_link_emailer.py`
- `weekly_brand_credit_emailer.py` -> `brands/weekly_brand_credit_emailer.py`
- `weekly_brand_credit_emailer_gui.py` -> `brands/weekly_brand_credit_emailer_gui.py`

### Margins

Move margin/pricing tools into `margins/`:

- `discount.py` -> `margins/discount.py`
- `marginCalc.py` -> `margins/marginCalc.py`
- `margin_floor_report.py` -> `margins/margin_floor_report.py`
- `margin_report.py` -> `margins/margin_report.py`
- `store_discount_roundup_sheet.py` -> `margins/store_discount_roundup_sheet.py`

### Reporting

Move reports, dashboards, owner snapshot, and weekly ordering tools into `reporting/`:

- `create_weekly_store_ordering_sheets.py` -> `reporting/create_weekly_store_ordering_sheets.py`
- `dutchie_live_dashboard_gui.py` -> `reporting/dutchie_live_dashboard_gui.py`
- `dutchie_today_dashboard.py` -> `reporting/dutchie_today_dashboard.py`
- `owner_emailer.py` -> `reporting/owner_emailer.py`
- `owner_snapshot.py` -> `reporting/owner_snapshot.py`
- `weekly_store_ordering_sheet.py` -> `reporting/weekly_store_ordering_sheet.py`
- `weekly_store_ordering_sheets.py` -> `reporting/weekly_store_ordering_sheets.py`

### Delivery

Move Google Drive/upload helpers into `delivery/`:

- `googleDrive.py` -> `delivery/googleDrive.py`
- `uploadDrive.py` -> `delivery/uploadDrive.py`

### Jobs

Move scheduled/orchestration jobs into `jobs/`:

- `autoJob.py` -> `jobs/autoJob.py`

### Utilities

Move small helpers into `utilities/`:

- `listBrands.py` -> `utilities/listBrands.py`

### Random Brand One-Offs

Move brand-specific older scripts into `other-scripts/random_brands/`:

- `other-scripts/kushy_bogo.py`
- `other-scripts/stiiizy.py`
- `other-scripts/stiiizyMarginTest.py`
- `other-scripts/stiiizyStartingInv.py`
- `other-scripts/turn.py`

## Config File Move Map

### JSON

Move JSON config/token files into `config/json/`:

- `brand_config.json`
- `brand_config2.json`
- `brand_meeting_gui_custom_brands.json`
- `weekly_store_ordering_config.json`
- `credentials.json`
- `token.json`
- `token_drive.json`
- `token_gmail.json`
- `token_sheets.json`

### CSV

Move CSV config snapshots into `config/csv/`:

- `deals_brand_config.csv`

### TXT

Move text config/output-link files into `config/txt/`:

- `config.txt`
- `links.txt`
- `deals_brand_config_sheet_url.txt`
- `deals_brand_config_url.txt`
- `weekly_store_ordering_sheet_url.txt`

Keep `.env` and `login.py` at the root unless you also update all secret-loading paths.

## Shared Path Helper

Add `core/paths.py` so moved scripts can still find the repo root and grouped config folders:

```python
from pathlib import Path
import sys

PACKAGE_ROOT = Path(__file__).resolve().parent
PROJECT_ROOT = PACKAGE_ROOT.parent
CONFIG_ROOT = PROJECT_ROOT / "config"
CONFIG_JSON_DIR = CONFIG_ROOT / "json"
CONFIG_CSV_DIR = CONFIG_ROOT / "csv"
CONFIG_TXT_DIR = CONFIG_ROOT / "txt"

def module_command(module_name: str, *args: object) -> list[str]:
    return [sys.executable, "-m", module_name, *[str(arg) for arg in args]]
```

Add `__init__.py` files to every runnable folder:

- `brands/__init__.py`
- `core/__init__.py`
- `delivery/__init__.py`
- `getters/__init__.py`
- `inventory/__init__.py`
- `jobs/__init__.py`
- `margins/__init__.py`
- `reporting/__init__.py`
- `utilities/__init__.py`

## Import Updates

Update imports from old root-module names to folder modules.

Examples:

```python
from dutchie_api_reports import STORE_CODES
```

becomes:

```python
from getters.dutchie_api_reports import STORE_CODES
```

```python
import owner_snapshot as osnap
```

becomes:

```python
import reporting.owner_snapshot as osnap
```

Common replacements:

- `dutchie_api_reports` -> `getters.dutchie_api_reports`
- `getSalesReport` -> `getters.getSalesReport`
- `getCatalog` -> `getters.getCatalog`
- `getInventoryOrderReport_api` -> `getters.getInventoryOrderReport_api`
- `inventory_order_reports` -> `inventory.inventory_order_reports`
- `deals` -> `brands.deals`
- `deals_brand_config_sync` -> `brands.deals_brand_config_sync`
- `brand_meeting_packet` -> `brands.brand_meeting_packet`
- `owner_snapshot` -> `reporting.owner_snapshot`
- `owner_emailer` -> `reporting.owner_emailer`
- `weekly_store_ordering_sheet` -> `reporting.weekly_store_ordering_sheet`
- `weekly_store_ordering_sheets` -> `reporting.weekly_store_ordering_sheets`

## Command Updates

Run scripts as modules from the repo root.

Examples:

```bash
.venv/bin/python -m jobs.autoJob
.venv/bin/python -m getters.getCatalog
.venv/bin/python -m getters.getSalesReport
.venv/bin/python -m reporting.owner_snapshot
.venv/bin/python -m reporting.weekly_store_ordering_sheet --all-stores
.venv/bin/python -m brands.deals
.venv/bin/python -m margins.margin_report
```

Subprocess calls should also use module form:

```python
subprocess.run([sys.executable, "-m", "getters.getCatalog"], cwd=PROJECT_ROOT)
```

## Path Updates Needed

Any script that used root-relative files needs to point to the new config folders:

- `credentials.json` -> `config/json/credentials.json`
- `token*.json` -> `config/json/token*.json`
- `brand_config*.json` -> `config/json/brand_config*.json`
- `weekly_store_ordering_config.json` -> `config/json/weekly_store_ordering_config.json`
- `deals_brand_config.csv` -> `config/csv/deals_brand_config.csv`
- `links.txt` -> `config/txt/links.txt`
- `config.txt` -> `config/txt/config.txt`
- `deals_brand_config_url.txt` -> `config/txt/deals_brand_config_url.txt`
- `deals_brand_config_sheet_url.txt` -> `config/txt/deals_brand_config_sheet_url.txt`
- `weekly_store_ordering_sheet_url.txt` -> `config/txt/weekly_store_ordering_sheet_url.txt`

Do not move generated working folders unless you are ready to update more paths:

- `files/`
- `reports/`
- `brand_reports/`
- `done/`
- `inventory_links/`

## Tests

Move root tests into `tests/`:

- `test_auto_job.py` -> `tests/test_auto_job.py`
- `test_dutchie_api_reports.py` -> `tests/test_dutchie_api_reports.py`
- `test_get_catalog.py` -> `tests/test_get_catalog.py`
- `test_get_sales_report.py` -> `tests/test_get_sales_report.py`
- `test_inventory_order_report_api.py` -> `tests/test_inventory_order_report_api.py`
- `test_inventory_order_reports.py` -> `tests/test_inventory_order_reports.py`
- `test_kickback_report_link_emailer.py` -> `tests/test_kickback_report_link_emailer.py`
- `test_weekly_store_ordering.py` -> `tests/test_weekly_store_ordering.py`

Fixture path updates:

```python
Path(__file__).resolve().parent / "fixtures" / "weekly_store_ordering"
```

instead of:

```python
Path(__file__).resolve().parent / "tests" / "fixtures" / "weekly_store_ordering"
```

## Docs To Update

Update these docs to use the new module commands and config paths:

- `README.md`
- `docs/weekly_store_ordering_setup.md`
- `docs/weekly_google_sheets_ordering_plan.md`
- `docs/codex_weekly_google_sheets_ordering_prompt.md`

Important examples:

```bash
.venv/bin/python -m reporting.weekly_store_ordering_sheet --all-stores
.venv/bin/python -m reporting.create_weekly_store_ordering_sheets
```

Config references should point to:

- `config/json/weekly_store_ordering_config.json`
- `config/txt/weekly_store_ordering_sheet_url.txt`
- `config/json/credentials.json`
- `config/json/token_sheets.json`

## Verification Commands

After reorganizing, run:

```bash
.venv/bin/python -m compileall -q core getters inventory brands margins reporting delivery jobs utilities other-scripts tests
.venv/bin/python -m unittest discover -s tests
```

During the tested pass, both commands passed with 54 tests.

## Notes And Cautions

- `brands.deals` appears to run real report logic when invoked directly; do not use `python -m brands.deals --help` as a harmless smoke test.
- Moving ignored local token/config files cannot be done with `git mv`; use regular `mv` for ignored files.
- If moving `.env` or `login.py`, update every script that reads Dutchie credentials first. The tested plan kept them at the root.
- Keep `files/`, `reports/`, `brand_reports/`, `done/`, and `inventory_links/` stable unless you want a larger output-path migration.
- After moving config files, test any Gmail/Drive flow carefully because OAuth paths are easy to miss.

## Suggested Future Workflow

1. Create a fresh branch.
2. Move Python files by category.
3. Add `__init__.py` files and `core/paths.py`.
4. Update imports.
5. Move tracked config files into `config/`.
6. Move ignored local config/token files into `config/`.
7. Update path constants.
8. Update README and setup docs.
9. Run compile and unit tests.
10. Run one safe `--dry-run` workflow before any live email/Drive/Sheets command.
