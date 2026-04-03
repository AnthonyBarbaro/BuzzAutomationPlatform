# Codex Prompt: Weekly Google Sheets Reorder Workbook from Dutchie API

Use this in two phases.

---

## Prompt A — Plan / mock / approval only

You are working inside a repo that already contains the Dutchie exporter, normalized inventory/reporting logic, and current brand workbook generation.

Your job is to design — but **not implement yet** — a new **store-first, API-only weekly Google Sheets reorder workbook** that writes into an existing Google Spreadsheet in Drive and creates **two tabs per store per week**:

- one **AUTO** tab that the script fully writes
- one **REVIEW** tab that staff use to count, cross-check, and approve orders

### Non-negotiable rules

1. **Do not touch code until you finish the plan.**
2. **Do not guess.** Verify everything against the repo.
3. **Break the work into separate workstreams** and keep context clean:
   - repo/data-flow discovery
   - reorder math + sell-through logic
   - Google Sheets tab writer / formatting
   - tests, logs, proof
4. **No Selenium/browser export dependency** for this new flow. It must be Dutchie API based.
5. **Every assumption must be called out explicitly.**
6. **Stop after the plan/mock** and wait for approval. Do not implement in this phase.

### Business goal

Build the equivalent of the current brand inventory workbook flow, but as **one professional Google Spreadsheet workflow** that is easy for employees to use for ordering every week.

### Required outcome

The final implementation must support:

- one existing Google Spreadsheet in Drive
- weekly Monday tab creation automatically
- two tabs per store per week
- store-specific ordering data
- 7d / 14d / 30d sales metrics
- 7d / 14d / 30d sell-through
- suggested order quantity / how many to order
- vendor-first then brand organization
- products/brands that need orders surfaced first
- include only brands/vendors that had sales in the last 30 days
- exclude samples and promos
- top-of-sheet total inventory value summary
- a review workflow so one person prepares and another checks/cross-references
- professional formatting
- cron-safe / idempotent reruns

### Known repo context to verify first

The user believes the repo currently works roughly like this. **Verify each point before relying on it.**

- `dutchie_api_reports.py` is the raw Dutchie exporter.
- It authenticates per store code (`MV`, `LG`, `LM`, `WP`, `SV`, `NC`) and pulls:
  - sales from `/reporting/transactions`
  - product/inventory-style data
  - inventory snapshots
- `brand_meeting_packet.py` is the normalized analytics layer.
- Its “catalog” path appears to come from `/reporting/inventory` because stock-on-hand is needed.
- Inventory rows are normalized into catalog-like rows containing fields like SKU, Available, Product, Cost, Location price, Price, Category, Brand, Vendor, Strain, Tags, Store, Store Code.
- `BrandInventoryGUIemailer.py` (or possibly similarly named `BrandINVEmailer.py`) is the current workbook/email layer.
- The current workbook flow drops store identity too early and groups output by brand rather than by store.
- `getInventoryOrderReport.py` and/or `inventory_order_reports.py` contain current reorder-sheet logic, but it likely depends on browser-exported order reports rather than an API-native source.
- If true, the new work will need to recreate reorder math from inventory + sales data.

### Deliverables for this phase only

Create a planning document in the repo, for example:

- `docs/weekly_google_sheets_ordering_plan.md`

The plan document must include all of the following:

#### 1) Verified repo map

List the real files, functions, classes, and data flow that matter.

At minimum verify:

- where store authentication happens
- where Dutchie transaction sales are pulled
- where inventory/catalog rows are normalized
- where store identity is lost or inferred from filenames
- where current reorder math lives
- whether current reorder logic can be reused directly or only conceptually
- where Google Drive / workbook writing already exists, if anywhere

#### 2) Gaps and root causes

Explain the exact gaps between current behavior and desired behavior.

At minimum cover:

- browser-export dependency gap
- brand-first vs store-first storage gap
- store identity persistence gap
- inability to produce reorder metrics directly from current normalized rows
- lack of Google Sheets weekly tab workflow
- risk of overwriting reviewer-entered values on rerun

#### 3) Proposed design

Design a clean implementation that makes **store a first-class key**.

The design must define:

- source-of-truth data model
- raw/derived cache layout by store
- where the weekly sheet builder will read from
- how current repo modules will be reused vs replaced
- configuration strategy for:
  - spreadsheet ID
  - store list
  - timezone (`America/Los_Angeles` unless repo already defines another canonical value)
  - sheet/tab naming convention
  - exclusion rules for samples/promos
  - reorder target days / thresholds
  - sell-through formula if no canonical existing formula exists in repo

#### 4) Proposed weekly tab naming

Propose a stable tab naming scheme that is idempotent and easy for staff.

Example shape:

- `MV 2026-04-06 Auto`
- `MV 2026-04-06 Review`

Explain how reruns update the same week’s tabs instead of creating duplicates.

#### 5) Workbook/worksheet mock

Provide a concrete mock of both sheets.

##### AUTO tab requirements

This is fully script-owned.

It should have a summary block at the top with fields like:

- Store
- Week Of (Monday date)
- Snapshot Generated At
- Total Inventory Value
- Total SKUs Considered
- Total SKUs Needing Order
- Total 7d Units Sold
- Total 14d Units Sold
- Total 30d Units Sold
- Brands Included (count)
- Vendors Included (count)

And a main table with columns close to:

- Reorder Priority
- Needs Order (Y/N)
- Vendor
- Brand
- Category
- Product
- SKU
- Available
- Cost
- Price
- Inventory Value
- Units Sold 7d
- Units Sold 14d
- Units Sold 30d
- Sell-Through 7d
- Sell-Through 14d
- Sell-Through 30d
- Avg Daily Sold 30d
- Days of Supply
- Suggested Order Qty
- Reorder Notes / Reason
- Last Sale Date (if available from source logic)

Sorting target:

- group by Vendor
- then Brand
- within each group show items that need orders first
- then sort by highest reorder urgency / suggested order qty
- then Product

##### REVIEW tab requirements

This is the staff-editable workflow sheet.

It must be designed so staff can count, cross-reference, and approve orders.

Include at minimum:

- core identifying/product columns mirrored from AUTO
- script metrics mirrored from AUTO
- editable columns such as:
  - Shelf Count Checked
  - Proposed Order Qty
  - Final Approved Qty
  - Ordered? (Y/N)
  - Buyer Initials
  - Reviewer Initials
  - Cross-Check Status
  - Notes
  - PO / Vendor Ref

Important: the plan must explicitly say how manual review columns will be preserved across reruns for the same week/store.

#### 6) Reorder math and sell-through math

Do **not** invent formulas silently.

You must:

- inspect existing reorder logic in the repo
- identify what is already canonical
- identify what is unavailable from Dutchie API-only data
- propose a replacement only where necessary
- make replacements configurable

Specifically answer:

- how 7d / 14d / 30d sold quantities will be computed from transactions
- how returns/voids/cancelled transactions will be handled
- how sell-through will be computed
- how suggested order quantity will be computed
- how “needs order” will be determined
- whether filters are by sold SKUs only or by all SKUs under brands/vendors with 30d sales

If the repo already has canonical reorder math, reuse it.
If not, propose a fallback formula and mark it as provisional/configurable.

#### 7) Exclusion logic

Define exact exclusion rules for samples/promos.

The plan must identify whether these are currently detectable through:

- product name
- category
- tags
- vendor/brand naming
- any existing repo-specific rule set

If no rule set exists, define a config-based exclusion list and require drop-count logging.

#### 8) Google Sheets formatting / professionalism

Define the visual standard.

At minimum include:

- frozen header rows
- filters
- bold headers
- alternating row banding
- currency formatting for cost/value
- percentage formatting for sell-through
- conditional formatting for urgency / needs-order rows
- clean summary block at top
- no decorative clutter

If protecting formula/script-owned ranges is practical, propose it.

#### 9) CLI / cron behavior

Design a runnable entry point with examples.

Need:

- dry-run mode
- single-store mode
- all-store mode
- optional explicit week override
- predictable exit codes
- logging

Example shape only (do not implement yet unless repo conventions require a different style):

- `python weekly_store_ordering_sheet.py --store MV --week 2026-04-06 --dry-run`
- `python weekly_store_ordering_sheet.py --all-stores`

#### 10) Test and proof plan

This is mandatory.

Design tests for at least:

- filtering out samples/promos
- 30-day brand/vendor eligibility
- 7d/14d/30d sold aggregation
- sell-through calculation
- suggested order qty calculation
- needs-order ranking/sorting
- stable tab naming and idempotent Monday reruns
- preservation of reviewer columns on rerun
- store identity preserved end-to-end

Also define logging/proof outputs such as:

- per-store row counts in/out
- excluded-row counts by reason
- brands/vendors included count
- SKUs needing order count
- written tab names
- local dry-run artifact output for inspection

### Important implementation expectations for later phase

Your future implementation should likely include some combination of:

- a new store-first weekly sheet builder module
- a config file for spreadsheet settings and reorder rules
- test fixtures / sample data
- documentation for setup and cron

But for this phase, do not code yet.

### Final response format for this phase

Reply with:

1. a concise verified repo map
2. a list of assumptions / open questions resolved from code vs still unresolved
3. the exact plan document path you created
4. the sheet mock in markdown table form
5. the top risks
6. a clear line saying you are waiting for approval before editing code

Stop there.

---

## Prompt B — After approval, implement

Approval granted. Implement the planned store-first weekly Google Sheets reorder workbook exactly as verified in the plan.

### Hard constraints

1. Reuse existing repo logic where correct; do not duplicate business logic unnecessarily.
2. Keep store identity explicit from raw pull through final sheet write.
3. Do not rely on filename inference for store identity if it can be fixed properly in data.
4. Do not use Selenium/browser exports in the new path.
5. Make all fragile business rules configurable.
6. Preserve reviewer-entered columns on rerun for the same store/week.
7. Make Monday tab creation idempotent.
8. Add tests, logs, and proof artifacts.
9. Do not break existing scripts unless the change is intentional and explained.

### Functional requirements

Implement an end-to-end flow that:

- pulls or reads Dutchie-derived data per store
- computes 7d / 14d / 30d sold metrics
- computes sell-through and suggested order quantity
- filters out samples/promos
- limits scope to brands/vendors sold in the last 30 days
- ranks/surfaces items needing order first
- writes to one Google Spreadsheet with weekly store tabs
- creates exactly two tabs per store/week:
  - AUTO
  - REVIEW
- formats both tabs professionally
- includes total inventory value at the top
- supports cron execution on a separate machine

### Strong preference on architecture

Unless the repo shows a better canonical pattern, prefer:

- one new module for weekly sheet orchestration
- one small module for Google Sheets read/write/upsert helpers
- config-driven store/spreadsheet/rule settings
- test fixtures for transactions + inventory snapshots

### Implementation details to honor

#### Data scope

Use the existing store codes already present in repo config if verified (`MV`, `LG`, `LM`, `WP`, `SV`, `NC`).

#### Product universe

Use the exact rule from the plan for:

- products eligible to appear
- brands/vendors with 30d sales
- exclusion of samples/promos

#### Sorting

The final sheet must be easy for buyers to work from:

- Vendor first
- Brand second
- within each Vendor/Brand block, items that need order first
- then urgency / suggested quantity descending
- then Product

#### Review workflow

The REVIEW tab must allow humans to:

- count shelf quantity
- cross-check script output
- propose and approve order quantities
- add notes and vendor references

If rerun occurs for the same week/store, do not wipe these human-entered review fields.
Use a stable key such as store + SKU (or a stronger verified business key if available) to rehydrate manual fields.

#### Professional sheet formatting

Apply:

- summary block at top
- frozen panes
- filters
- readable column widths
- currency/percent/integer formats
- alternating row banding
- conditional formatting for needs-order / high urgency

### Testing and proof

You must run and show:

- unit tests
- any integration-style dry-run you can run locally
- a sample artifact or console proof showing produced tab names / row counts

If Google write cannot run in the local environment due to credentials, provide a dry-run artifact and prove the writer payload structure.

### Required code/documentation outputs

At minimum deliver:

- implementation code
- tests
- setup docs
- cron example
- log examples or documented log structure

### Final response format for implementation phase

Reply with:

1. what files changed and why
2. the exact reorder / sell-through formulas used
3. test results
4. dry-run or live-write proof
5. known limitations
6. next steps only if truly necessary

