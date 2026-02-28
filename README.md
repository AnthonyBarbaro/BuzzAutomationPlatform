# BuzzPythonGUI

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
- `getCatalog.py` - Dutchie catalog export automation
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
- `autoJob.py` - weekly orchestration (sales export -> deals -> upload -> email)
- `listBrands.py` - lists unique brands from CSV files

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

Recommended operational practice:
- Keep credential/token files out of git and backed up securely.
- Rotate secrets if shared accidentally.

## Common Runs

### 1) Sales exports (all stores)
```bash
.venv/bin/python getSalesReport.py
```

### 2) Owner snapshot (default window)
```bash
.venv/bin/python owner_snapshot.py
```

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
