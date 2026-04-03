# Weekly Google Sheets Ordering Plan

Status: planning only. No implementation is included in this document.

## Goal

Design a new store-first, Dutchie API-only weekly Google Sheets reorder workflow that writes into one existing Google Spreadsheet and creates two tabs per store per week:

- `AUTO`: fully script-owned
- `REVIEW`: staff-editable for count, approval, and cross-check

The intended result is a cron-safe, idempotent, professional weekly ordering workbook that replaces the current browser-export-driven reorder path for this use case.

## Workstreams

1. Repo and data-flow discovery
2. Reorder math and sell-through logic
3. Google Sheets tab writer and formatting
4. Tests, logs, and proof artifacts

## Verified Repo Map

### 1) Dutchie API auth, store config, and date helpers

- `dutchie_api_reports.py`
  - Store codes and labels are defined in `STORE_CODES` for `MV`, `LG`, `LM`, `WP`, `SV`, `NC`.
  - Store credential environment name resolution happens in `resolve_store_keys()`.
  - Integrator key resolution happens in `resolve_integrator_key()`.
  - Request sessions are created in `create_session()`.
  - Canonical timezone is `America/Los_Angeles` in `DEFAULT_TIMEZONE`.
  - Report endpoints are:
    - `sales -> /reporting/transactions`
    - `catalog -> /reporting/products`
    - `inventory -> /reporting/inventory`
- `brand_meeting_packet.py`
  - Packet-side API auth wrapper lives in `_api_auth_bundle()` and `_api_store_keys()`.
  - Packet-side canonical timezone is also `America/Los_Angeles` via `REPORT_TZ`.

Verified references:

- `dutchie_api_reports.py:34-39`
- `dutchie_api_reports.py:41-66`
- `dutchie_api_reports.py:160-183`
- `dutchie_api_reports.py:190-252`
- `brand_meeting_packet.py:61`
- `brand_meeting_packet.py:377-401`

### 2) Raw Dutchie API export flow

- `dutchie_api_reports.py` is the raw exporter.
- It supports per-store export of `sales`, `catalog`, and `inventory` reports.
- `sales` requests include detail, taxes, order IDs, and fees/donations.
- `catalog` supports incremental export via `fromLastModifiedDateUTC`.
- `inventory` supports extra detail flags like lab results, room quantities, allocated, and lineage.
- `export_reports()` loops store by store, verifies `/whoami`, fetches each report, and writes JSON/CSV files.

Verified references:

- `dutchie_api_reports.py:79-83`
- `dutchie_api_reports.py:203-245`
- `dutchie_api_reports.py:269-301`
- `dutchie_api_reports.py:381-436`

### 3) API-native normalized sales and inventory/catalog layer

- `brand_meeting_packet.py` already has an API-native normalized layer that is much closer to what the new weekly ordering flow needs than the current order-report path.
- Sales API flow:
  - `_fetch_sales_exports_via_api()` fetches `/reporting/products` plus chunked `/reporting/transactions`.
  - `_normalize_transactions_api_sales_rows()` flattens transaction items into export-like sales rows.
  - `_prepare_sales_df_for_brand()` and `_prepare_sales_df_all_brands()` normalize those rows into analysis fields like `_store_abbr`, `_net`, `_qty`, `_tx_key`, `_is_return`, merge keys, and product grouping fields.
- Inventory/catalog API flow:
  - `_fetch_catalog_exports_via_api()` fetches `/reporting/inventory`.
  - `_normalize_inventory_api_catalog_rows()` converts inventory rows into catalog-like rows containing:
    - `SKU`
    - `Available`
    - `Product`
    - `Cost`
    - `Location price`
    - `Price`
    - `Category`
    - `Brand`
    - `Strain`
    - `Vendor`
    - `Tags`
    - `Strain Type`
    - `Store`
    - `Store Code`
  - `_load_catalog_exports()` restores `_store_abbr` from filename or `Store`.

Verified references:

- `brand_meeting_packet.py:461-492`
- `brand_meeting_packet.py:495-579`
- `brand_meeting_packet.py:622-701`
- `brand_meeting_packet.py:1153-1197`
- `brand_meeting_packet.py:1669-1842`
- `brand_meeting_packet.py:2575-2617`

### 4) Current brand workbook and email flow

- `BrandInventoryGUIemailer.py` is the current GUI workbook/email layer.
- It is brand-first, not store-first.
- It keeps only:
  - required columns `Available`, `Product`, `Brand`
  - optional columns `Category`, `Cost`
- Because of that keep-list, it drops `Store` and `Store Code` before workbook generation.
- It filters samples/promos using product-name regex.
- It splits each catalog CSV into:
  - `Available` sheet for `Available > 2`
  - `Unavailable` sheet for `Available <= 2`
- It groups output by brand, not by store.
- It infers store from the source filename using `extract_store_code_from_filename()`.
- It then attaches `Order_7d`, `Order_14d`, `Order_30d` sections using `inventory_order_reports.build_brand_order_sections()`.
- Google integrations already present here are Drive upload and Gmail send, not Google Sheets writing.
- `BrandINVEmailer.py` is a legacy sibling with the same overall brand-first pattern.

Verified references:

- `BrandInventoryGUIemailer.py:6-20`
- `BrandInventoryGUIemailer.py:39-45`
- `BrandInventoryGUIemailer.py:72-93`
- `BrandInventoryGUIemailer.py:330-455`
- `BrandInventoryGUIemailer.py:471-600`
- `BrandInventoryGUIemailer.py:602-639`
- `BrandINVEmailer.py:1-45`

### 5) Current reorder logic and why it is not directly API-native

- `getInventoryOrderReport.py` is Selenium/browser automation against the Dutchie inventory order report page.
- It downloads `inventory_order_7d_*`, `inventory_order_14d_*`, `inventory_order_30d_*` files.
- `inventory_order_reports.py` consumes those exported files and contains the current reorder-sheet math and styling.
- `prepare_reorder_sheet()` expects browser-export-specific columns such as:
  - `Quantity on Hand`
  - `Quantity Sold`
  - `Sold Per Day`
  - `Days Remaining`
  - `Days Since Last Received`
  - `Last Ordered Quantity`
- Its reorder quantity logic is:
  - `Target Qty = ceil(Sold Per Day * window_days)`
  - `Suggested Order Qty = max(0, Target Qty - Quantity on Hand)`
- Its priority labels also depend on fields that do not currently exist in the normalized API-only row model, especially `Days Remaining` and `Days Since Last Received`.
- Conclusion: current reorder logic is reusable conceptually, but not directly as an API-native dependency.

Verified references:

- `getInventoryOrderReport.py:19-188`
- `inventory_order_reports.py:85-89`
- `inventory_order_reports.py:131-146`
- `inventory_order_reports.py:398-420`
- `inventory_order_reports.py:518-654`
- `inventory_order_reports.py:657-742`
- `inventory_order_reports.py:961-1013`

### 6) Existing store-first metrics logic already in the repo

- `brand_meeting_packet.py` already contains store-first assortment analysis that is much closer to the target weekly ordering workbook than `inventory_order_reports.py`.
- `build_store_level_assortment_views()` merges store inventory and store sales by product grouping, then computes:
  - `units_sold_window`
  - `units_per_day`
  - `sell_through_ratio`
  - `days_of_supply`
  - `last_sale_date`
  - `days_since_last_sale`
  - action labels like `Cut`, `Review`, `Healthy`
- This logic is not the final desired reorder workflow, but it proves the repo already has API-native sell-through and days-of-supply formulas.

Verified references:

- `brand_meeting_packet.py:3424-3475`
- `brand_meeting_packet.py:3853-3886`
- `brand_meeting_packet.py:3997-4299`
- `brand_meeting_packet.py:4047-4107`

### 7) Existing Google Sheets support

- The repo already has real Google Sheets support in `deals_brand_config_sync.py`.
- Reusable pieces already exist for:
  - OAuth auth (`authenticate_sheets()`)
  - parsing spreadsheet IDs from editable sheet URLs (`_parse_sheet_target()`)
  - finding existing tab metadata (`_find_sheet_info()`)
  - formatting tabs with frozen rows, banding, widths, filters (`_format_synced_sheet()`)
  - preserving manual columns across reruns (`_preserve_existing_columns()`)
- No existing module currently writes weekly store ordering tabs.

Verified references:

- `deals_brand_config_sync.py:396-417`
- `deals_brand_config_sync.py:435-458`
- `deals_brand_config_sync.py:461-485`
- `deals_brand_config_sync.py:538-695`
- `deals_brand_config_sync.py:698-755`
- `deals_brand_config_sync.py:758-798`

## Gaps and Root Causes

### 1) Browser-export dependency gap

- The current reorder workbook logic depends on Selenium and Dutchie’s browser order report.
- Root cause: `inventory_order_reports.py` expects browser-export-only columns and `getInventoryOrderReport.py` is the producer for those files.

### 2) Brand-first vs store-first storage gap

- Current workbook output is grouped by brand and uploaded to brand folders.
- Root cause: `BrandInventoryGUIemailer.py` iterates `available_df.groupby("Brand")`, not by store.

### 3) Store identity persistence gap

- Store identity exists in normalized API rows, but the current GUI flow drops it by selecting only `Available`, `Product`, `Brand`, `Category`, and `Cost`.
- The current flow then tries to recover store identity from the filename.
- Root cause: store is not treated as a first-class key through workbook generation.

### 4) API-native reorder metric gap

- Current canonical reorder quantity logic depends on order-report-only fields like `Sold Per Day`, `Days Remaining`, and `Days Since Last Received`.
- API-native normalized rows already support sold quantities, sell-through, and days of supply, but not those exact browser fields.
- Root cause: there is no current store-first API-native reorder-sheet builder.

### 5) Google Sheets weekly-tab workflow gap

- Existing Google integrations cover Drive, Gmail, and a separate config sync tab.
- There is no weekly Monday tab creation workflow for a single ordering spreadsheet.

### 6) Manual reviewer overwrite risk

- `AUTO` can be safely rewritten on rerun, but `REVIEW` cannot be blindly cleared without preserving staff-entered fields.
- Root cause: no current row-keyed sheet-preservation design exists for ordering data.

## Proposed Design

### 1) Overview

Build a new store-first weekly ordering flow that:

1. Pulls inventory and sales directly from Dutchie API per store
2. Normalizes inventory and sales using existing packet helpers where practical
3. Builds one store-week source-of-truth table keyed by store and row key
4. Writes two tabs into one existing Google Spreadsheet:
   - `AUTO` tab: script-owned summary + table
   - `REVIEW` tab: mirrored metrics + preserved manual review columns
5. Updates the same week/store tabs idempotently on rerun

### 2) Source-of-truth row model

Proposed canonical row grain: one row per store, per week, per reorderable inventory item.

Proposed columns:

- `week_of`
- `snapshot_generated_at`
- `store_code`
- `store_name`
- `row_key`
- `vendor`
- `brand`
- `category`
- `product`
- `sku`
- `available`
- `cost`
- `price`
- `inventory_value`
- `units_sold_7d`
- `units_sold_14d`
- `units_sold_30d`
- `sell_through_7d`
- `sell_through_14d`
- `sell_through_30d`
- `avg_daily_sold_7d`
- `avg_daily_sold_14d`
- `avg_daily_sold_30d`
- `days_of_supply`
- `last_sale_date`
- `needs_order`
- `target_qty`
- `suggested_order_qty`
- `reorder_priority`
- `reorder_reason`
- `eligible_brand_30d`
- `eligible_vendor_30d`
- `excluded_flag`
- `excluded_reason`
- `source_inventory_present`
- `source_sales_present`

Proposed `row_key` strategy:

1. `store_code + "|" + sku` when `sku` exists
2. fallback to `store_code + "|" + brand_product_key`
3. fallback to `store_code + "|" + normalized vendor + "|" + normalized brand + "|" + normalized product`

Rationale:

- `REVIEW` preservation needs a stable join key across reruns.
- The repo already keeps `SKU` on normalized inventory rows and also keeps product grouping keys in packet logic.

### 3) Raw and derived cache layout by store

Proposed deterministic weekly cache layout:

```text
reports/store_weekly_ordering/
  2026-04-06/
    manifest.json
    MV/
      raw_inventory.json
      raw_products.json
      raw_transactions_30d.json
      normalized_inventory.csv
      normalized_sales.csv
      eligibility_brand_30d.csv
      eligibility_vendor_30d.csv
      sku_metrics.csv
      auto_preview.csv
      review_preview.csv
      sheet_requests.json
    LM/
      ...
```

Why deterministic instead of timestamped inside the week folder:

- reruns should update the same week/store artifacts
- local dry-run diffs become easier to inspect
- cron behavior is simpler and more idempotent

### 4) Data flow for the new weekly builder

For each store:

1. Resolve store credentials using the same store-key logic already used by the API exporter and packet flow.
2. Pull `/reporting/inventory` for the live inventory snapshot.
3. Pull `/reporting/products` plus 30-day `/reporting/transactions` for sales enrichment and SKU/product metadata.
4. Normalize inventory rows using `_normalize_inventory_api_catalog_rows()`.
5. Normalize sales rows using `_normalize_transactions_api_sales_rows()` plus packet-side sales preparation logic.
6. Build 7d, 14d, and 30d sales windows from normalized sales rows.
7. Build vendor/brand 30d eligibility sets from non-return sales rows.
8. Build the final store-week row table by:
   - starting from inventory rows
   - optionally unioning sold-in-window rows that have no current inventory row, if they can be keyed reliably
9. Drop excluded sample/promo rows
10. Compute sell-through, days of supply, suggested order quantity, and needs-order flags
11. Sort vendor-first, then brand, then needs-order first, then urgency, then product
12. Write/update the two Google Sheet tabs

### 5) Reuse vs replace

#### Reuse directly

- `dutchie_api_reports.py`
  - store env/key resolution
  - session creation
  - request JSON helper
  - local date to UTC helper
- `brand_meeting_packet.py`
  - inventory API normalization
  - transaction API normalization
  - packet-style normalized sales fields like `_store_abbr`, `_qty`, `_is_return`
  - store-first sell-through and days-of-supply formulas as the best existing API-native precedent
- `deals_brand_config_sync.py`
  - Google Sheets OAuth
  - spreadsheet/tab lookup
  - formatting request patterns
  - preserve-existing-columns pattern

#### Reuse conceptually only

- `inventory_order_reports.py`
  - reorder-sheet labels
  - general idea of `target_qty` and `suggested_order_qty`
  - order-sheet styling ideas

Reason it cannot be reused directly:

- it depends on browser-export-only columns not present in the API-native normalized model

#### Replace / add

- new weekly store ordering module
- new weekly store ordering config
- new Google Sheets tab writer specialized for:
  - AUTO full rewrite
  - REVIEW merge-preserve rewrite

### 6) Configuration strategy

Proposed config shape:

- `weekly_store_ordering_config.json`
- `weekly_store_ordering_sheet_url.txt`
- optional env overrides

Proposed config fields:

- `spreadsheet_url` or `spreadsheet_id`
- `store_codes`
- `timezone`
- `default_week_mode`
- `reorder_target_days`
- `ordering_velocity_window_days`
- `include_only_brands_with_30d_sales`
- `include_only_vendors_with_30d_sales`
- `sample_promo_product_regexes`
- `sample_promo_tag_values`
- `sample_promo_category_regexes`
- `low_cost_exclusion_threshold`
- `protect_auto_tab`
- `protect_review_script_columns`
- `review_editable_columns`

Defaults proposed from verified repo behavior:

- timezone: `America/Los_Angeles`
- store list: `MV, LM, SV, LG, NC, WP`
- sample/promo product regex:
  - `sample`
  - `samples`
  - `promo`
  - `promos`
  - `promotional`
  - `display`
  - `tester`

## Proposed Weekly Tab Naming

Stable naming convention:

- `MV 2026-04-06 Auto`
- `MV 2026-04-06 Review`

Rules:

- `2026-04-06` is the Monday `week_of` date in `America/Los_Angeles`
- the script computes the same `week_of` for all reruns in that week unless `--week` overrides it
- reruns search for tabs by exact title
- if the tab exists:
  - `AUTO`: clear and rewrite script-owned content
  - `REVIEW`: load existing values, preserve editable columns by `row_key`, then rewrite
- if the tab does not exist:
  - create it once

Idempotency outcome:

- rerunning for the same store and week updates the same two tabs instead of creating duplicates

## Workbook / Worksheet Mock

### AUTO tab

Tab title example: `MV 2026-04-06 Auto`

Summary block mock:

| Field | Value |
| --- | --- |
| Store | MV |
| Week Of | 2026-04-06 |
| Snapshot Generated At | 2026-04-06 08:05 PT |
| Total Inventory Value | $42,315.00 |
| Total SKUs Considered | 184 |
| Total SKUs Needing Order | 37 |
| Total 7d Units Sold | 412 |
| Total 14d Units Sold | 801 |
| Total 30d Units Sold | 1,655 |
| Brands Included | 22 |
| Vendors Included | 11 |

Main table mock:

| Row Key | Reorder Priority | Needs Order | Vendor | Brand | Category | Product | SKU | Available | Cost | Price | Inventory Value | Units Sold 7d | Units Sold 14d | Units Sold 30d | Sell-Through 7d | Sell-Through 14d | Sell-Through 30d | Avg Daily Sold 30d | Days of Supply | Suggested Order Qty | Reorder Notes / Reason | Last Sale Date |
| --- | --- | --- | --- | --- | --- | --- | --- | ---: | ---: | ---: | ---: | ---: | ---: | ---: | ---: | ---: | ---: | ---: | ---: | ---: | --- | --- |
| `MV|SKU123` | Urgent | Y | Vendor A | Brand X | Eighths | Brand X 3.5g Flower | SKU123 | 2 | 14.00 | 28.00 | 28.00 | 11 | 19 | 41 | 84.6% | 90.5% | 95.3% | 1.37 | 1.5 | 18 | Below target cover; active 30d seller | 2026-04-05 |

Sorting target:

1. `Vendor`
2. `Brand`
3. `Needs Order` descending
4. `Reorder Priority` rank
5. `Suggested Order Qty` descending
6. `Product`

Notes:

- `Row Key` should be present but hidden in the final sheet.
- The AUTO tab is fully script-owned and safe to fully rewrite.

### REVIEW tab

Tab title example: `MV 2026-04-06 Review`

Summary block mock:

| Field | Value |
| --- | --- |
| Store | MV |
| Week Of | 2026-04-06 |
| Snapshot Generated At | 2026-04-06 08:05 PT |
| Review Rows | 37 |
| Rows Already Checked | 12 |
| Rows Already Ordered | 4 |

Main table mock:

| Row Key | Reorder Priority | Needs Order | Vendor | Brand | Category | Product | SKU | Available | Cost | Price | Inventory Value | Units Sold 7d | Units Sold 14d | Units Sold 30d | Sell-Through 7d | Sell-Through 14d | Sell-Through 30d | Avg Daily Sold 30d | Days of Supply | Suggested Order Qty | Last Sale Date | Shelf Count Checked | Proposed Order Qty | Final Approved Qty | Ordered? | Buyer Initials | Reviewer Initials | Cross-Check Status | Notes | PO / Vendor Ref |
| --- | --- | --- | --- | --- | --- | --- | --- | ---: | ---: | ---: | ---: | ---: | ---: | ---: | ---: | ---: | ---: | ---: | ---: | ---: | --- | ---: | ---: | ---: | --- | --- | --- | --- | --- | --- |
| `MV|SKU123` | Urgent | Y | Vendor A | Brand X | Eighths | Brand X 3.5g Flower | SKU123 | 2 | 14.00 | 28.00 | 28.00 | 11 | 19 | 41 | 84.6% | 90.5% | 95.3% | 1.37 | 1.5 | 18 | 2026-04-05 | 3 | 18 | 16 | Y | AB | CD | Count matched | Reduce by 2 after backstock check | PO-10482 |

REVIEW preservation rule:

- Script-owned columns are overwritten on rerun.
- Editable review columns are preserved by `row_key`.
- Editable columns:
  - `Shelf Count Checked`
  - `Proposed Order Qty`
  - `Final Approved Qty`
  - `Ordered?`
  - `Buyer Initials`
  - `Reviewer Initials`
  - `Cross-Check Status`
  - `Notes`
  - `PO / Vendor Ref`

## Reorder Math and Sell-Through Math

### Verified current math we can reuse

#### Sold quantities (7d / 14d / 30d)

Proposed source:

- normalized transaction item rows from the API flow

Verified current behavior:

- packet summaries use `_qty` from normalized sales rows
- packet summaries exclude rows where `_is_return` is true

Proposed weekly implementation:

- `units_sold_7d = sum(_qty for non-return rows in trailing 7 days)`
- `units_sold_14d = sum(_qty for non-return rows in trailing 14 days)`
- `units_sold_30d = sum(_qty for non-return rows in trailing 30 days)`

#### Returns

Verified current behavior:

- `_normalize_transactions_api_sales_rows()` identifies `isReturn` / `isReturned`
- packet-prepared sales mark `_is_return`
- many summaries explicitly exclude `_is_return`

Proposed weekly implementation:

- sold-quantity windows should follow the same current repo convention and exclude return rows
- return counts and return units can be logged separately for proof

#### Sell-through

Verified existing API-native formula in store assortment logic:

- `sell_through_ratio = units_sold_window / (units_sold_window + units_available)`

This formula already exists in the repo and is the best verified API-native precedent for weekly store ordering.

Proposed weekly implementation:

- `sell_through_7d = units_sold_7d / (units_sold_7d + available)`
- `sell_through_14d = units_sold_14d / (units_sold_14d + available)`
- `sell_through_30d = units_sold_30d / (units_sold_30d + available)`

Important note:

- This is a snapshot-based sell-through proxy, not a historical opening-inventory sell-through calculation.
- It should be called out in the implementation docs and kept configurable if the business later wants a different denominator.

#### Days of supply

Verified existing API-native formula:

- `units_per_day = units_sold_window / report_days`
- `days_of_supply = available / units_per_day`, with safe zero handling

Proposed weekly implementation:

- use 30-day velocity by default:
  - `avg_daily_sold_30d = units_sold_30d / 30`
  - `days_of_supply = available / avg_daily_sold_30d`

### Where current repo math is not directly reusable

#### Suggested order quantity

Verified current browser-export logic:

- `Target Qty = ceil(Sold Per Day * window_days)`
- `Suggested Order Qty = max(0, Target Qty - Quantity on Hand)`

Why it cannot be reused directly:

- API-normalized rows do not currently include Dutchie order-report fields like `Days Remaining`, `Days Since Last Received`, or `Last Ordered Quantity`.

Proposed fallback formula for the new API-only flow:

- `target_qty = ceil(avg_daily_sold_<ordering_velocity_window> * reorder_target_days)`
- `suggested_order_qty = max(0, target_qty - available)`

Proposed defaults:

- `ordering_velocity_window = 30d`
- `reorder_target_days = 14`

This should be marked explicitly as:

- provisional
- configurable
- chosen because it is the closest API-native equivalent to the current order-report target-quantity logic

#### Needs order

Proposed default:

- `needs_order = suggested_order_qty > 0`

Additional gates:

- vendor eligible in store 30d sales
- brand eligible in store 30d sales
- not excluded as sample/promo

#### Reorder priority

Verified current priority labels:

- `Urgent`
- `Reorder Now`
- `Reorder Soon`
- `Check PO`
- `Watch`
- `Healthy`
- `No Recent Sales`

Verified limitation:

- current label assignment relies on fields not currently present in API-native normalized rows

Proposed provisional API-native priority mapping:

- `Urgent`
  - `days_of_supply <= 3`, or `available <= 0 and units_sold_30d > 0`
- `Reorder Now`
  - `days_of_supply <= 7`
- `Reorder Soon`
  - `days_of_supply <= 14`
- `Watch`
  - `suggested_order_qty > 0`
- `Healthy`
  - otherwise

This should be configurable because the repo does not yet contain a verified API-native canonical replacement for the browser-order-report priority logic.

### Filter scope decision

Question from the prompt:

- sold SKUs only, or all SKUs under brands/vendors with 30d sales?

Proposed answer:

- include all rows for current store inventory items under brands and vendors that had non-return sales in the last 30 days in that store
- also include sold-in-window rows with no current inventory row when they can be keyed reliably
- do not limit the sheet to sold-SKU-only rows

Reason:

- buyers need to see low-stock and zero-stock items for active vendor/brand assortments
- this matches the business rule that brands/vendors must be active in the last 30 days, while still keeping the sheet store-first and order-focused

### Cancelled / voided transactions

Not verified from current code:

- I did not find a verified current filter for cancelled or voided transaction states in the API-native normalization path.

Plan position:

- this remains unresolved until real transaction payload samples confirm whether Dutchie exposes stable cancel/void markers in the response used here
- implementation should log the presence of any candidate cancel/void status fields before relying on them

## Exclusion Logic

### Verified current rules

- Product-name sample/promo regex exists in `inventory_order_reports.py`:
  - `sample`
  - `samples`
  - `promo`
  - `promos`
  - `promotional`
  - `display`
  - `tester`
- `BrandInventoryGUIemailer.py` also drops `sample|promo` from product names.
- `inventory_order_reports.py` additionally excludes low-cost rows from order-summary aggregation when wholesale cost is `<= 1.01`.

### Proposed new-store-ordering exclusion strategy

Use config-based exclusions with drop-count logging.

Proposed layers:

1. Product-name regex exclusions
2. Tag-based exclusions using normalized `Tags`
3. Optional category regex exclusions
4. Optional low-cost threshold exclusion

Default product-name regex list:

- `sample`
- `samples`
- `promo`
- `promos`
- `promotional`
- `display`
- `tester`

Default tag exclusion list:

- start empty unless the live payload proves there are stable sample/promo tags

Required logging:

- dropped rows by product-name rule
- dropped rows by tag rule
- dropped rows by category rule
- dropped rows by low-cost rule
- dropped rows by ineligible vendor/brand rule

## Google Sheets Formatting and Professional Standard

### AUTO tab

Required formatting:

- freeze through the table header row
- summary block at top with label/value formatting
- bold headers
- dark header fill with white text
- alternating row banding
- filter on the main table
- currency formatting for:
  - `Cost`
  - `Price`
  - `Inventory Value`
- percent formatting for sell-through columns
- conditional formatting for:
  - `Needs Order = Y`
  - `Reorder Priority`
  - high `Suggested Order Qty`
- hidden `Row Key` column
- wrap product and note columns

### REVIEW tab

Required formatting:

- same top summary style
- mirrored metric columns visually distinct from editable review columns
- editable columns lightly shaded
- filters and frozen rows
- data validation on fields like:
  - `Ordered?`
  - `Cross-Check Status`
- conditional formatting for incomplete reviews and approved orders

### Range protection

Practical proposal:

- `AUTO` tab can be fully protected except for the script owner
- `REVIEW` tab can keep script-owned columns protected and leave only editable review columns unlocked

Caveat:

- this is practical with Google Sheets batch updates, but should be optional in case the authenticated Google user lacks the needed permissions or the team prefers not to use protections

## CLI and Cron Behavior

Proposed entry point:

- `weekly_store_ordering_sheet.py`

Example commands:

```bash
.venv/bin/python weekly_store_ordering_sheet.py --store MV --week 2026-04-06 --dry-run
.venv/bin/python weekly_store_ordering_sheet.py --all-stores
.venv/bin/python weekly_store_ordering_sheet.py --all-stores --week 2026-04-06
```

Proposed arguments:

- `--store <CODE>` repeatable
- `--all-stores`
- `--week YYYY-MM-DD`
- `--dry-run`
- `--env-file`
- `--spreadsheet-url`
- `--spreadsheet-id`
- `--timezone`
- `--output-dir`
- `--verbose`

Proposed default week behavior:

- compute Monday of the current local week in `America/Los_Angeles`
- require `--week` to also be a Monday date when explicitly passed

Proposed exit codes:

- `0` all requested stores succeeded
- `2` config/auth/validation error
- `3` one or more stores failed after the run started

Required cron-safe behavior:

- safe rerun for same week and same stores
- no duplicate tabs
- same local cache paths for same week
- explicit logs for create vs update behavior

## Test and Proof Plan

### Unit tests

1. sample/promo exclusion from product names
2. tag-based exclusion
3. 30-day brand eligibility by store
4. 30-day vendor eligibility by store
5. 7d/14d/30d sold aggregation
6. return-row exclusion from sold aggregation
7. sell-through calculation
8. days-of-supply calculation
9. suggested order quantity calculation
10. needs-order flag
11. vendor-first then brand sorting
12. needs-order-first sorting inside vendor/brand groups
13. Monday week normalization and tab naming
14. row-key generation and fallback behavior
15. review-column preservation on rerun
16. store identity preserved end to end

### Integration / dry-run proof

Per-store dry-run outputs should include:

- `normalized_inventory.csv`
- `normalized_sales.csv`
- `sku_metrics.csv`
- `auto_preview.csv`
- `review_preview.csv`
- `sheet_requests.json`

Required log lines:

- store start and finish
- inventory rows fetched
- sales rows fetched
- excluded rows by reason
- eligible brands count
- eligible vendors count
- final rows considered
- rows needing order
- tabs created vs updated
- preserved review rows count
- rows with unmatched previous review data

### Manual proof checklist

For one pilot store:

1. run `--dry-run`
2. inspect local CSV previews
3. run live write to the spreadsheet
4. hand-edit REVIEW columns
5. rerun same week/store
6. verify:
   - same tab names reused
   - AUTO rewritten
   - REVIEW editable fields preserved
   - mirrored script metrics refreshed

## Assumptions and Open Questions

### Resolved from code

- `dutchie_api_reports.py` is the raw Dutchie exporter.
- `brand_meeting_packet.py` uses `/reporting/inventory` for API-native catalog-style inventory rows.
- normalized inventory rows already contain store, brand, vendor, tags, cost, price, and available quantity.
- current brand workbook flow drops store identity too early and later infers it from filenames.
- current reorder workbook logic depends on browser-export order reports.
- existing Google Sheets OAuth and sheet-formatting code already exists elsewhere in the repo.
- canonical timezone in relevant Dutchie/reporting flows is `America/Los_Angeles`.

### Still unresolved

- Which exact Google Spreadsheet URL or ID should be the target for the new workflow
- Whether `/reporting/inventory` reliably includes zero-on-hand active items for all stores
- Whether the Dutchie transaction payload includes stable cancel/void fields worth filtering
- Whether the business wants reorder suggestions based on 14-day or 30-day velocity by default
- Whether any sample/promo tags are reliably present in real Dutchie inventory data
- Whether the team wants range protections enabled by default in Google Sheets

## Top Risks

1. API payload gaps around cancelled/voided sales could affect reorder counts if those rows are present and not separately flagged.
2. Missing or inconsistent `SKU` values could weaken review-row preservation unless fallback row keys are designed carefully.
3. If `/reporting/inventory` omits zero-on-hand active items, sold-but-out-of-stock products may need a union-from-sales strategy to stay visible.
4. Vendor/brand naming mismatches across stores could create false eligibility splits without normalization rules.
5. Large multi-store weekly tabs may need careful batch formatting and range sizing to keep Google Sheets writes fast and reliable.

## Proposed Next-Step Implementation Scope

After approval, the likely implementation would include:

- a new weekly store ordering builder module
- a config file for spreadsheet and reorder rules
- a Google Sheets writer module that reuses the existing sheet auth/formatting foundation
- tests and dry-run fixtures
- setup notes for cron and Google auth

No code changes are included in this phase.
