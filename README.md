# BuzzAutomationPlatform

Internal reporting and automation toolkit for Buzz Cannabis.

This repository is a Python automation stack (not a web backend) that:
- Pulls sales/catalog/closing data from Dutchie Backoffice (Selenium)
- Processes exports into Excel/PDF reports (Pandas, OpenPyXL, ReportLab)
- Applies deal/kickback and margin logic
- Uploads reports to Google Drive and emails results via Gmail API

## What This Project Is

- **Primary style:** batch scripts + desktop GUI tools (Tkinter)
- **Data sources:** Dutchie Backoffice exports (sales, catalog, closing reports)
- **Outputs:** `.xlsx` and `.pdf` reports in `reports/`, `done/`, and `brand_reports/`
- **Delivery:** Gmail API + Drive links

## Repository Structure

- `files/` - raw downloaded exports (sales/catalog)
- `reports/raw_sales/` - archived sales export windows
- `reports/pdf/` - generated owner snapshot PDFs
- `brand_reports/` - brand deal/inventory report outputs
- `done/` - processed margin and other generated outputs

## Key Scripts

### Core export/reporting
- `getSalesReport.py` - Dutchie sales export automation for all stores/date range
- `getInventoryOrderReport.py` - Dutchie inventory order report export automation for 7d/14d/30d windows
- `getCatalog.py` - Dutchie catalog export automation
- `dutchie_api_reports.py` - direct Dutchie POS API exporter for sales, catalog, inventory, and key verification
- `weekly_store_ordering_sheet.py` - store-first weekly reorder workbook builder for one Google Spreadsheet with `AUTO` + `REVIEW` tabs per store/week
- `weekly_store_ordering_sheets.py` - Google Sheets helper/upsert layer for weekly ordering tabs
- `store_discount_roundup_sheet.py` - Google Sheets discount round-up pricing planner with store tabs, cost price reference, margins, kickbacks, and an `All Pricing` shared-price rollup
- `dutchie_today_dashboard.py` - live same-day Dutchie API HTML dashboard with store pace, hourly flow, top products, and low-stock flags
- `dutchie_live_dashboard_gui.py` - native Tkinter live dashboard with same-day KPIs, store focus, sales mix tables, and inventory alerts
- `getClosingReport.py` - closing report by day/store (GUI)
- `owner_snapshot.py` - builds owner snapshot PDFs + summary email; can run fresh exports

### Deal and margin analysis
- `deals.py` - applies brand discount/kickback rules and writes weekly brand reports
- `margin_report.py` - GUI margin reporting with scenario analysis
- `margin_floor_report.py` - CLI margin floor report by brand
- `marginCalc.py` - quick GUI margin calculator
- `discount.py` - one-off discount anomaly report

### Email and delivery
- `owner_emailer.py` - owner snapshot email formatting/sending
- `brandDEALSEmailer.py` - brand kickback email sender
- `BrandINVEmailer.py` - scheduled brand inventory generation + Drive + email
- `BrandInventoryGUIemailer.py` - GUI version of inventory report workflow
- `googleDrive.py`, `uploadDrive.py` - Drive upload helpers

### Orchestration and utilities
- `autoJob.py` - weekly orchestration (Dutchie API sales export -> deals -> upload -> email)
- `listBrands.py` - lists unique brands from CSV files

## Weekly Ordering Resources

- Setup guide: `docs/weekly_store_ordering_setup.md`
- Training video: https://youtu.be/ri9VkqPGAUQ

## Discount Round-Up Pricing Resources

- Training video: https://youtu.be/La_JT4Pir0I

## Prerequisites

- Python 3.10+
- Google Chrome installed
- Internet access (Dutchie + Google APIs)
- OAuth client file: `credentials.json`
- Dutchie login credentials available to scripts

## Setup

```bash
python3 -m venv .venv
source .venv/bin/activate
pip install -r requirements.txt
```

## Config and Secrets

This project uses local config/token files such as:
- `login.py` (Dutchie credentials)
- `brand_config.json` (brand schedules/recipients)
- `config.txt` (GUI path memory)
- `credentials.json` (Google OAuth client)
- `token*.json` (Google OAuth tokens generated after first auth)

For direct Dutchie POS API exports, add your location keys to `.env` using either
explicit names like `DUTCHIE_API_KEY_MV` or plain store-code keys like `mv`.

Example:

```bash
DUTCHIE_INTEGRATOR_KEY=
mv=your_mission_valley_location_key
lg=your_lemon_grove_location_key
lm=your_la_mesa_location_key
wp=your_wildomar_palomar_location_key
sv=your_sorrento_valley_location_key
nc=your_national_city_location_key
```

Recommended operational practice:
- Keep credential/token files out of git and backed up securely.
- Rotate secrets if shared accidentally.

## Deals Brand Config Sync

`deals.py` can load deal rules from a flat CSV/JSON config and keep a CSV snapshot in sync for sharing.

Files:
- `deals_brand_config_url.txt` - published Google Sheets CSV URL used as the live source
- `deals_brand_config_sheet_url.txt` - editable Google Sheets tab to sync back to
- `deals_brand_config.csv` - local flat export snapshot written on each run
- `token_sheets.json` - OAuth token for Google Sheets sync

Normal workflow:
1. Edit the Google Sheet tab from `deals_brand_config_sheet_url.txt`.
2. Make sure that tab is published as CSV and matches `deals_brand_config_url.txt`.
3. Run `deals.py`.
4. `deals.py` reloads the published CSV, writes `deals_brand_config.csv`, and syncs the flattened rows back to the editable sheet tab.

Useful commands:
```bash
.venv/bin/python deals.py
.venv/bin/python deals.py --sync-brand-config-only
.venv/bin/python deals.py --seed-brand-config-sheet
```

`--seed-brand-config-sheet` overwrites the target sheet tab with the full built-in config from `deals.py`. Use that once if the shared sheet needs to be bootstrapped from the hardcoded rules.

## Common Runs

### 1) Sales exports (all stores)
```bash
.venv/bin/python getSalesReport.py
```

### 1a) Weekly automation job
Uses Dutchie API sales pulls by default:
```bash
.venv/bin/python autoJob.py
```

Force the legacy browser export path if needed:
```bash
.venv/bin/python autoJob.py --sales-source browser
```

### 2) Owner snapshot (default window)
```bash
.venv/bin/python owner_snapshot.py
```

### 2a) Verify Dutchie API keys only
```bash
.venv/bin/python dutchie_api_reports.py --verify-only --stores mv lg lm wp sv nc
```

Dutchie API scripts fetch stores concurrently with 6 workers by default. Tune that with `--workers`; use `--workers 1` if you need serial requests because of rate limits.

### 2b) Inventory order reports (7d / 14d / 30d)
```bash
.venv/bin/python getInventoryOrderReport_api.py --workers 6
```

### 2c) Pull API sales, catalog, and inventory exports
```bash
.venv/bin/python dutchie_api_reports.py --stores mv lg --reports sales catalog inventory --from-date 2026-03-01 --to-date 2026-03-24 --workers 2
```

Optional inventory detail flags:
```bash
.venv/bin/python dutchie_api_reports.py --stores mv --reports inventory --include-lab-results --include-room-quantities --include-allocated --include-lineage
```

### 2d) Live same-day Dutchie dashboard
One-shot HTML snapshot:
```bash
.venv/bin/python dutchie_today_dashboard.py --stores mv lg lm wp sv nc --open
```

Keep refreshing a live dashboard in the browser:
```bash
.venv/bin/python dutchie_today_dashboard.py --stores mv lg lm wp sv nc --watch --refresh-seconds 90 --open
```

Skip inventory if you only want sales-side pace metrics:
```bash
.venv/bin/python dutchie_today_dashboard.py --stores mv lg --no-inventory
```

### 2e) Native live dashboard GUI
Launch the desktop dashboard:
```bash
.venv/bin/python dutchie_live_dashboard_gui.py
```

Start with inventory calls disabled:
```bash
.venv/bin/python dutchie_live_dashboard_gui.py --no-inventory
```

### 2f) Weekly store ordering sheets
What this flow does:
- Builds weekly reorder tabs from live Dutchie API data
- Uses the store list in `weekly_store_ordering_config.json` when you run `--all-stores`
- Defaults `--as-of-date` to today in `America/Los_Angeles`
- Defaults `--week` to the Monday of the chosen `--as-of-date`
- Updates the same store/week tabs on rerun instead of creating duplicates

Before the first live run, make sure you have:
- Dutchie API keys in `.env`
- `credentials.json` and `token_sheets.json` for Google Sheets access
- `weekly_store_ordering_sheet_url.txt` or `WEEKLY_STORE_ORDERING_SHEET_URL` pointing to the target spreadsheet(s)

Dry-run with the bundled fixture:
```bash
.venv/bin/python weekly_store_ordering_sheet.py --store MV --week 2026-03-30 --as-of-date 2026-04-03 --fixture-root tests/fixtures/weekly_store_ordering --dry-run
```

Dry-run against the live store list without writing to Google Sheets:
```bash
.venv/bin/python weekly_store_ordering_sheet.py --all-stores --dry-run --week 2026-04-13 --as-of-date 2026-04-14
```

Live single-store write:
```bash
.venv/bin/python weekly_store_ordering_sheet.py --store MV --week 2026-04-13 --as-of-date 2026-04-14
```

Live all-store write:
```bash
.venv/bin/python weekly_store_ordering_sheet.py --all-stores
```

Outputs to check after a run:
- `reports/store_weekly_ordering/<week_of>/run_summary.json`
- `reports/store_weekly_ordering/<week_of>/<STORE>/review_preview.csv`
- `reports/store_weekly_ordering/<week_of>/<STORE>/sheet_payload.json`

Current repo config writes the `Review` tab to Google Sheets. Local proof artifacts are still written for every run.

Setup and cron details:
- [`docs/weekly_store_ordering_setup.md`](/home/anthony/projects/BuzzPythonGUI/docs/weekly_store_ordering_setup.md)

### 3) Owner snapshot for one specific report day (example: Jan 31, 2026)
```bash
.venv/bin/python owner_snapshot.py --report-day 2026-01-31 --run-export
```

Skip email for a dry run:
```bash
.venv/bin/python owner_snapshot.py --report-day 2026-01-31 --run-export --no-email
```

### 4) Deal/kickback report generation
```bash
.venv/bin/python deals.py
```

### 5) Closing report GUI
```bash
.venv/bin/python getClosingReport.py
```

## WSL Notes

For WSL environments, browser rendering can be unstable in visible mode.
If a script supports it, run headless (default in updated scripts) or set:

```bash
BUZZ_HEADLESS=1
```

Use visible mode only for debugging:

```bash
BUZZ_HEADLESS=0
```

## Typical End-to-End Flow

1. Export sales data from Dutchie (`getSalesReport.py` or via `owner_snapshot.py --run-export`)
2. Archive exports to date-window folders under `reports/raw_sales/`
3. Compute KPIs/deal adjustments/margins (`owner_snapshot.py`, `deals.py`, margin scripts)
4. Generate PDFs/XLSX outputs
5. Email stakeholders and/or upload to Drive

## Troubleshooting

- Selenium login/dropdown errors:
  - Re-run with visible browser (`BUZZ_HEADLESS=0`) for selector diagnostics
  - Confirm Dutchie credentials in `login.py`
- Missing exports for a store:
  - Check `files/` and log warnings for failed store selection
- Gmail/Drive failures:
  - Re-authenticate by removing stale token files and rerunning
- Empty report windows:
  - Verify date window and source export coverage in `reports/raw_sales/`

## Maintenance Tips

- Keep `requirements.txt` versions synced with working runtime.
- Update `store_abbr_map` consistently across scripts when stores change.
- Keep report naming conventions stable (several scripts parse filenames).
- Prefer `owner_snapshot.py` CLI flags for one-off historical runs instead of hardcoding dates.
