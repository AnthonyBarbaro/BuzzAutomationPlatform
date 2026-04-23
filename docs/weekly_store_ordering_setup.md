# Weekly Store Ordering Setup

## What This Flow Does

Training video: https://youtu.be/ri9VkqPGAUQ

`weekly_store_ordering_sheet.py` builds a store-first, Dutchie API-only weekly reorder workbook and can write up to two tabs per store into one Google Spreadsheet:

- `AUTO`: script-owned reorder output
- `REVIEW`: same metrics plus preserved staff review columns

Current repo config writes only the `REVIEW` tab to Google Sheets. The `AUTO` tab can be re-enabled later through config without changing code.

The flow is idempotent by store/week. A rerun updates the same:

- `MV 2026-03-30 Auto`
- `MV 2026-03-30 Review`

It does not create duplicate weekly tabs for the same store/week.
If one store fails during a multi-store run, the remaining stores still continue and the failure is recorded in `run_summary.json`.

The workflow supports two reorder modes:

- `exact_sku`: reorder against the exact item row
- `family_par`: reorder against a configurable replacement family / par bucket, with family summary rows followed by detail rows for strain choice and cross-check

Current row ordering in the exported `AUTO` and `REVIEW` tabs is:

- `Brand`
- `Category`
- `Cost`
- `Price`
- `Product`
- `Reorder Priority`

This keeps like items together for faster buying review inside each store tab.

Default row filters also exclude:

- items with `Cost < $1.00`
- items with fewer than `3` units sold in the last `30` days

The sheet summary area is intentionally minimal:

- `Store`
- `Week Of`
- `Snapshot Generated At`
- `Total Inventory Value`

`Total Inventory Value` is calculated from the full normalized inventory snapshot before ordering filters are applied, so it reflects all inventory rather than only the rows that remain in the ordering tab.

## Required Inputs

1. Dutchie API keys in `.env`
2. `credentials.json` for Google OAuth
3. `token_sheets.json` generated after the first successful Sheets auth
4. `weekly_store_ordering_config.json`
5. A Google Sheets target supplied by one of:
   - `--sheet-url`
   - `WEEKLY_STORE_ORDERING_SHEET_URL`
   - `weekly_store_ordering_sheet_url.txt`

`weekly_store_ordering_sheet_url.txt` supports either:

- one shared spreadsheet URL/ID for all stores
- store-specific mappings like `MV=https://...` and `LG=https://...`
- an optional `DEFAULT=https://...` fallback for stores without an explicit mapping

## Quick Start

Use this section if you just want to know how to run the weekly ordering flow safely.

Default behavior:

- `--all-stores` uses the `stores` list from `weekly_store_ordering_config.json`
- if you omit `--as-of-date`, the script uses today in `America/Los_Angeles`
- if you omit `--week`, the script uses the Monday of the chosen `--as-of-date`
- rerunning the same store/week updates the same tabs instead of creating duplicates
- current repo config writes only the `REVIEW` tab to Google Sheets

Recommended operator flow:

1. Confirm `.env`, `credentials.json`, `token_sheets.json`, and `weekly_store_ordering_sheet_url.txt` are present.
2. Run a dry run first so you can inspect the generated proof files without touching Google Sheets.
3. If the output looks right, run one store live.
4. Then run all stores live.

Safe all-store dry run:

```bash
.venv/bin/python weekly_store_ordering_sheet.py \
  --all-stores \
  --dry-run \
  --week 2026-04-13 \
  --as-of-date 2026-04-14
```

Live single-store validation run:

```bash
.venv/bin/python weekly_store_ordering_sheet.py \
  --store MV \
  --week 2026-04-13 \
  --as-of-date 2026-04-14
```

Live all-store run:

```bash
.venv/bin/python weekly_store_ordering_sheet.py --all-stores
```

Where to look after a run:

- `reports/store_weekly_ordering/<week_of>/run_summary.json`
- `reports/store_weekly_ordering/<week_of>/<STORE>/review_preview.csv`
- `reports/store_weekly_ordering/<week_of>/<STORE>/sheet_payload.json`

## Config

Main config file: [`weekly_store_ordering_config.json`](/home/anthony/projects/BuzzPythonGUI/weekly_store_ordering_config.json)

Important settings:

- `stores`: store list used by `--all-stores`
- `timezone`: default `America/Los_Angeles`
- `sheet_names.auto_suffix` / `sheet_names.review_suffix`
- `sheet_outputs.write_auto_tab`: currently `false`; set to `true` to re-enable the Google `AUTO` tab
- `sheet_outputs.write_review_tab`: currently `true`
- `eligibility.mode`: default `brand_or_vendor`
- `eligibility.include_sales_only_rows`: include recent sold items even if current inventory is zero/missing
- `eligibility.min_units_sold_30d`: default `3`; rows under this 30-day sales floor are excluded from the final ordering tabs
- `exclusions.pattern`: default sample/promo regex aligned to the existing reorder workflow
- `exclusions.exclude_low_cost_rows`: default `true`; excludes rows where cost is below the configured threshold
- `exclusions.low_cost_threshold`: default `1.0`
- `reorder.velocity_window_days`: default `14`
- `reorder.target_cover_days`: default `14`
- `reorder.days_of_supply_urgent`: default `3`
- `reorder.days_of_supply_low`: default `7`
- `reorder.high_sell_through_30d`: default `0.6`
- `family_reorder.default_mode`: default `exact_sku`
- `family_reorder.mode_rules`: switch matched products into `family_par`
- `family_reorder.par_targets`: configurable family-level par rules by brand/product type/size/pack/strain type
- `family_reorder.fallback_target_mode`: fallback behavior when a family has no explicit par rule
- `family_reorder.include_cost_in_family_key`: when true, family buckets are split by same-cost item within the brand
- `family_reorder.cost_field`: default `cost`
- `family_reorder.cost_precision`: default `2`

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

Create one Google Spreadsheet per store and write their URLs into `weekly_store_ordering_sheet_url.txt`:

```bash
.venv/bin/python create_weekly_store_ordering_sheets.py
```

## Artifacts And Proof

Each run writes proof artifacts under:

- `reports/store_weekly_ordering/<week_of>/run_summary.json`
- `reports/store_weekly_ordering/<week_of>/<STORE>/normalized_inventory.csv`
- `reports/store_weekly_ordering/<week_of>/<STORE>/normalized_sales.csv`
- `reports/store_weekly_ordering/<week_of>/<STORE>/sku_metrics.csv`
- `reports/store_weekly_ordering/<week_of>/<STORE>/family_metrics.csv`
- `reports/store_weekly_ordering/<week_of>/<STORE>/ordering_rows.csv`
- `reports/store_weekly_ordering/<week_of>/<STORE>/auto_preview.csv`
- `reports/store_weekly_ordering/<week_of>/<STORE>/review_preview.csv`
- `reports/store_weekly_ordering/<week_of>/<STORE>/sheet_payload.json`

These are written for dry runs and live runs.

## Review Preservation

The `REVIEW` tab preserves manual columns on rerun by `Row Key`.

Even when the Google `AUTO` tab is disabled, the local `auto_preview.csv` artifact is still written for proof/debugging.

Current preserved columns:

- `Chosen Replacement Strain`
- `Units To Order`
- `Reviewer Cross-Check`
- `Approved By`
- `Order Notes`
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
- family summary rows use `family|<reorder_family_key>`
- family detail rows use `detail|<exact_row_key>`

For `family_par` rows, the current default family bucket is:

- `store + vendor + brand + product type + size / pack + I/H/S bucket + same-cost item`

## Logging

Typical log lines:

```text
[MV] Building weekly ordering bundle for week 2026-03-30 as of 2026-04-03
[MV] rows=14 needs_order=7 tabs=MV 2026-03-30 Auto / MV 2026-03-30 Review
[MV] row_types={'Family Detail': 6, 'Exact SKU': 4, 'Family Summary': 4} reorder_modes={'family_par': 6, 'exact_sku': 4} parser_conflicts={'inventory_strain_type_conflicts': 0, 'sales_strain_type_conflicts': 0}
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

## Metric Notes

`Sell-Through 7D/14D/30D` is a combined display column that shows all three sell-through windows in one field:

- display format: `7d% / 14d% / 30d%`
- sell-through formula for each window: `units sold in the window / (units sold in the window + current available units)`
- example: `67% / 80% / 89%`

Why this matters:

- it keeps the short-, mid-, and month-view movement together in one easy scan line
- it highlights products that are moving quickly relative to what is still on the shelf
- it gives the reorder logic and the buyer a fast-turn signal without needing a separate beginning-inventory snapshot

`Avg Daily Sold 14d` is:

- formula: `Units Sold 14d / 14`

Why this matters:

- it uses a more recent demand window than the old 30-day display
- it smooths day-to-day noise into a usable daily demand estimate
- it gives buyers the same 14-day pace the reorder math now uses for `Days of Supply`, `Target Qty`, and `Suggested Order Qty`
