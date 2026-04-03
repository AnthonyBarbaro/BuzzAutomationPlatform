# Weekly Store Ordering Setup

## What This Flow Does

`weekly_store_ordering_sheet.py` builds a store-first, Dutchie API-only weekly reorder workbook and writes two tabs per store into one Google Spreadsheet:

- `AUTO`: script-owned reorder output
- `REVIEW`: same metrics plus preserved staff review columns

The flow is idempotent by store/week. A rerun updates the same:

- `MV 2026-03-30 Auto`
- `MV 2026-03-30 Review`

It does not create duplicate weekly tabs for the same store/week.

## Required Inputs

1. Dutchie API keys in `.env`
2. `credentials.json` for Google OAuth
3. `token_sheets.json` generated after the first successful Sheets auth
4. `weekly_store_ordering_config.json`
5. A Google Sheets target supplied by one of:
   - `--sheet-url`
   - `WEEKLY_STORE_ORDERING_SHEET_URL`
   - `weekly_store_ordering_sheet_url.txt`

## Config

Main config file: [`weekly_store_ordering_config.json`](/home/anthony/projects/BuzzPythonGUI/weekly_store_ordering_config.json)

Important settings:

- `stores`: store list used by `--all-stores`
- `timezone`: default `America/Los_Angeles`
- `sheet_names.auto_suffix` / `sheet_names.review_suffix`
- `eligibility.mode`: default `brand_or_vendor`
- `eligibility.include_sales_only_rows`: include recent sold items even if current inventory is zero/missing
- `exclusions.pattern`: default sample/promo regex aligned to the existing reorder workflow
- `exclusions.exclude_low_cost_rows`: optional low-cost suppression toggle
- `reorder.velocity_window_days`: default `30`
- `reorder.target_cover_days`: default `14`
- `reorder.days_of_supply_urgent`: default `3`
- `reorder.days_of_supply_low`: default `7`
- `reorder.high_sell_through_30d`: default `0.6`

## Commands

Dry run with local fixtures:

```bash
.venv/bin/python weekly_store_ordering_sheet.py \
  --store MV \
  --week 2026-03-30 \
  --as-of-date 2026-04-03 \
  --fixture-root tests/fixtures/weekly_store_ordering \
  --dry-run
```

Live single-store write:

```bash
.venv/bin/python weekly_store_ordering_sheet.py \
  --store MV \
  --sheet-url "https://docs.google.com/spreadsheets/d/..." \
  --week 2026-03-30
```

Live all-store write:

```bash
.venv/bin/python weekly_store_ordering_sheet.py --all-stores
```

## Artifacts And Proof

Each run writes proof artifacts under:

- `reports/store_weekly_ordering/<week_of>/run_summary.json`
- `reports/store_weekly_ordering/<week_of>/<STORE>/normalized_inventory.csv`
- `reports/store_weekly_ordering/<week_of>/<STORE>/normalized_sales.csv`
- `reports/store_weekly_ordering/<week_of>/<STORE>/sku_metrics.csv`
- `reports/store_weekly_ordering/<week_of>/<STORE>/auto_preview.csv`
- `reports/store_weekly_ordering/<week_of>/<STORE>/review_preview.csv`
- `reports/store_weekly_ordering/<week_of>/<STORE>/sheet_payload.json`

These are written for dry runs and live runs.

## Review Preservation

The `REVIEW` tab preserves manual columns on rerun by `Row Key`.

Current preserved columns:

- `Shelf Count Checked`
- `Proposed Order Qty`
- `Final Approved Qty`
- `Ordered?`
- `Buyer Initials`
- `Reviewer Initials`
- `Cross-Check Status`
- `Notes`
- `PO / Vendor Ref`

Default row key strategy:

- `store_code + SKU` when SKU exists
- fallback to `store_code + brand_product_key`
- fallback to normalized product identity

## Logging

Typical log lines:

```text
[MV] Building weekly ordering bundle for week 2026-03-30 as of 2026-04-03
[MV] rows=4 needs_order=3 tabs=MV 2026-03-30 Auto / MV 2026-03-30 Review
[MV] excluded inventory={'pattern:product': 1} sales={'pattern:product': 1} tx={'status:cancelled': 1}
Run summary saved to .../reports/store_weekly_ordering/2026-03-30/run_summary.json
```

## Cron Example

Monday 7:05 AM Pacific:

```cron
5 7 * * MON cd /home/anthony/projects/BuzzPythonGUI && /home/anthony/projects/BuzzPythonGUI/.venv/bin/python weekly_store_ordering_sheet.py --all-stores >> /home/anthony/projects/BuzzPythonGUI/reports/store_weekly_ordering/weekly_store_ordering.log 2>&1
```

Recommended first run:

1. Run once manually with `--dry-run`
2. Inspect `auto_preview.csv`, `review_preview.csv`, and `sheet_payload.json`
3. Run a live single-store write
4. Confirm review-column preservation by editing the `REVIEW` tab and rerunning the same week/store
