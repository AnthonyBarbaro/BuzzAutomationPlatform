# Brand Meeting And Brand Tools Context

This context is for future work on `brand_meeting_gui.py`, `brand_meeting_packet.py`, and the surrounding brand-reporting tools. The brand meeting packet workflow is the newest, most complete brand surface: it pulls or reuses Dutchie sales/catalog data, summarizes brand performance, reconciles brand support credits, builds PDFs/XLSX files, and can email the result.

## Primary Entry Points

### `brand_meeting_gui.py`

Tkinter app for running brand meeting packets without the CLI.

Main class:

- `BrandMeetingPacketGUI`

What it owns:

- Brand selection and search.
- Store selection.
- Date presets and custom windows.
- Packet options: API/browser data mode, force refresh, email, XLSX, prior-window comparison, charts, store sections, appendix, packet style, target margin, CreditFlow, monthly reference, follow-up notes, compact PDF.
- Manual credit ledger editing/import/export.
- Background worker thread and UI-safe queue for status/log updates.

Important behavior:

- Brand options come from three places:
  - `brand_config.json`, if present.
  - `deals.brand_criteria`, if `deals.py` imports successfully.
  - `brand_meeting_gui_custom_brands.json`.
- The repo currently has `brand_config2.json`, not `brand_config.json`. Unless a local `brand_config.json` exists, the GUI will not load scheduled brand metadata from that config. It will still load deals brands and custom brands.
- All long-running work must stay off the Tkinter main thread. Existing code uses `_run_background`, `_queue_log`, `_queue_activity`, and `_drain_log_queue`.

GUI run modes:

- Prepare sales data: `_on_download_sales`
- Build packet from saved inputs: `_on_build_pdf`
- Build and email from saved inputs: `_on_build_email_no_download`
- Full run with refresh/build/email: `_on_full_run`
- Store-by-store SKU cut / all-store slow-mover report: `_on_all_store_slow_movers`
- Owner top-brands review PDF: `_on_owner_rollup`

Run it with:

```bash
.venv/bin/python brand_meeting_gui.py
```

### `brand_meeting_packet.py`

Core packet engine plus CLI.

Main dataclasses:

- `PacketOptions`
- `RunPaths`
- `PacketArtifacts`
- `AllStoreSlowMoverArtifacts`
- `OwnerBrandRollupArtifacts`

Main packet function:

- `generate_brand_meeting_packet(...)`
- `generate_owner_brand_rollup_packet(...)`

What it does:

1. Resolves report windows: report, last 7/14/30, month-to-date, prior comparable window.
2. Prepares sales exports via cached files, browser export, or Dutchie API.
3. Prepares catalog/inventory exports via cached files, browser export, or Dutchie API.
4. Builds brand aliases from catalog data.
5. Filters and normalizes sales rows for the selected brand.
6. Summarizes sales by store, category, product group, day, week, and window.
7. Summarizes inventory by overview, product, category, and store.
8. Computes days of supply, slow movers, fast movers, inventory risk, movers, and deal/margin scenarios.
9. Reconciles expected/received credits from the manual ledger and CreditFlow.
10. Generates action items, brand health score, meeting ask, and optional follow-up notes.
11. Writes QA/cache CSVs.
12. Builds either the classic packet or the landscape Dashboard / Easy Read packet, optional XLSX, and optional Gmail email.

CLI examples:

```bash
.venv/bin/python brand_meeting_packet.py --brand "Hashish" --use-api --stores MV,LM,SV --no-email --xlsx
```

```bash
.venv/bin/python brand_meeting_packet.py --brand "Raw Garden" --stores MV,LM,SV,LG,NC,WP --no-export --no-catalog-export --no-email
```

Dashboard / Easy Read single-brand packet:

```bash
.venv/bin/python brand_meeting_packet.py --brand "Sol Flora" --stores MV,LM,SV,LG,NC,WP --no-email --packet-layout dashboard --no-appendix
```

Owner-facing top-brands review:

```bash
.venv/bin/python brand_meeting_packet.py --owner-rollup --top-brands 20 --stores MV,LM,SV,LG,NC,WP
```

By default the CLI emails brand packets and owner top-brands reviews unless `--no-email` is passed. Owner top-brands reviews email only `anthony@buzzcannabis.com`.

## Data And Artifact Flow

Default output root:

```text
reports/brand_packets/
```

Typical run folder shape:

```text
reports/brand_packets/<brand>/<YYYY-MM-DD_to_YYYY-MM-DD>/
|-- raw_sales/
|-- raw_catalog/
|-- cache/
`-- pdf/
```

Owner rollup runs use:

```text
reports/brand_packets/owner_rollups/<YYYY-MM-DD_to_YYYY-MM-DD>/
|-- raw_sales/
|-- raw_catalog/
|-- cache/
`-- pdf/
```

Useful cache outputs include:

- `sales_brand_rows.csv`
- `sales_brand_rows_14d.csv`
- `sales_brand_rows_30d.csv`
- `product_groups_60d.csv`
- `product_groups_14d.csv`
- `product_groups_30d.csv`
- `inventory_products.csv`
- `credit_reconciliation.csv`
- `credit_source_summary.csv`
- `brand_action_items.csv`
- `store_credit_scorecards.csv`
- `dos_trend_key_audit_30d.csv`
- `creditflow_credits_cache.json`
- `dashboard_brand_snapshot.csv`
- `dashboard_product_decision_board.csv`
- `dashboard_fast_movers.csv`
- `dashboard_slow_movers.csv`
- `dashboard_store_matrix.csv`
- `dashboard_category_mix.csv`
- `dashboard_credit_margin_summary.csv`
- `owner_top_brands_scorecard.csv`
- `owner_top_brands_summary.csv`

These cache files are useful when the PDF looks wrong. They show the exact rows and grouped metrics used to build the packet.

## Important Config And Data Files

- `brand_meeting_targets.json`
  - Default target margin, max discount rate, max days supply, min sell-through, and optional per-brand overrides.
- `brand_meeting_gui_custom_brands.json`
  - GUI-managed custom brand list.
- `brand_credit_ledger.json`
  - Manual support/credit ledger used by brand meeting packets.
- `brand_aliases_monthly.json`
  - Alias support for owner monthly reference matching.
- `deals_brand_config.csv`
  - CSV source for deal/kickback rules.
- `deals_brand_config_sync.py`
  - Sync/helper for deal config.
- `brand_config2.json`
  - Daily brand inventory email configuration used by `BrandINVEmailer.py`.
- `.env`
  - Dutchie API keys and CreditFlow API key.
- `token_gmail.json`
  - Gmail send token used by packet email.

CreditFlow keys checked by `creditflow_api.py`:

- `CREDITFLOW_API_KEY`
- `creditflow`
- `CREDITFLOW`
- `CREDIT_FLOW_API_KEY`

## Supporting Brand Modules

Brand meeting helpers:

- `brand_meeting_insights.py`
  - Deterministic action items, brand health score, meeting ask, follow-up text, and monthly reference loading.
- `brand_meeting_targets.py`
  - Loads/saves target-margin settings and resolves brand-specific targets.
- `brand_credit_ledger.py`
  - Manual credit ledger normalization, CSV import/export, filtering, and credit reconciliation.
- `creditflow_api.py`
  - CreditFlow API client, brand/vendor-code matching, store normalization, and credit normalization.

Deal and kickback tools:

- `deals.py`
  - Weekly brand deal/kickback logic and `brand_criteria` used by the GUI as one brand source.
- `brandDEALSEmailer.py`
  - Sends brand deal/kickback emails.
- `kickback_report_link_emailer.py`
  - Builds/sends kickback report link emails.
- `deals_brand_config_sync.py`
  - Syncs deal config data.

Brand inventory tools:

- `BrandINVEmailer.py`
  - Scheduled brand inventory generation, Drive upload, and email.
- `BrandInventoryGUIemailer.py`
  - GUI version of brand inventory report workflow.
- `brand_inventory_report_job.py`
  - CLI/server brand inventory report job.
- `brand_inventory_rows.py`
  - Shared inventory row normalization/grouping helpers.
- `inventory_order_reports.py`
  - Weekly order report summaries reused by inventory email workflows.
- `aged_flower_inventory_report.py`
  - Aged flower inventory report by brand.

Recurring brand reporting:

- `weekly_brand_credit_emailer.py`
- `weekly_brand_credit_emailer_gui.py`
- `monthly_brand_reports_job.py`
- `monthly_brand_resolver.py`

Small utilities:

- `listBrands.py`
- `other-scripts/generate_brand_criteria_from_menu.py`
- `other-scripts/brand_inventory.py`

## Development Gotchas

- `brand_meeting_packet.py` is large and mixes data loading, normalization, analytics, PDF layout, XLSX output, and email delivery. Prefer small, focused changes.
- `include_kickback_adjustments` defaults off. Margin should be sales-only unless the caller explicitly enables kickbacks.
- `target_margin` accepts either decimal or percent in user-facing paths. The GUI stores `35`, then normalizes to `0.35`.
- Dutchie API sales are chunked by `SALES_API_MAX_WINDOW_DAYS = 30`.
- Store order should use `order_store_codes`; display order is `MV`, `LM`, `SV`, `LG`, `NC`, `WP`.
- GUI logs intentionally hide noisy QA/archive messages. If debugging, check terminal output and the run `cache/` folder.
- Do not update Tk widgets directly from worker threads. Push events through the queue.
- Email is on by default in the CLI. Use `--no-email` for test runs.
- Dashboard packets use `--packet-layout dashboard` or `--dashboard`. Dashboard appendix tables default off unless `--include-appendix` is passed.
- `brand_config.json` vs `brand_config2.json` is a current mismatch to resolve if scheduled brand metadata should appear in the meeting GUI.

## Testing

Full current test suite:

```bash
.venv/bin/python -m unittest discover -s tests
```

Useful focused tests around adjacent brand systems:

```bash
.venv/bin/python -m unittest \
  tests/test_brand_packet_dashboard.py \
  tests/test_brand_inventory_rows.py \
  tests/test_brand_inv_other_folder.py \
  tests/test_brand_inv_other_drive.py \
  tests/test_brand_inventory_gui_drive.py \
  tests/test_kickback_report_link_emailer.py \
  tests/test_weekly_brand_credit_emailer.py \
  tests/test_monthly_brand_reports_job.py
```

Owner rollup coverage:

```bash
.venv/bin/python -m unittest tests/test_owner_brand_rollup.py
```

Current coverage gap: there are no direct unit tests for `brand_meeting_gui.py` or the full `generate_brand_meeting_packet` path. For packet changes, use a no-email CLI smoke test with a narrow brand/store window when practical, then inspect `reports/brand_packets/.../cache/`.

## Safe Change Checklist

1. Decide whether the change belongs in the GUI, packet engine, config, or one of the helper modules.
2. If touching data matching, inspect the relevant cache CSVs before and after.
3. If touching credits, test both manual ledger-only and CreditFlow-enabled paths.
4. If touching PDF layout, generate at least one no-email packet and open the output PDF.
5. Run `unittest discover -s tests`.
6. Keep generated report/cache files out of commits unless they are intentional fixtures.
