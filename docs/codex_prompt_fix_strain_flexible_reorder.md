# Codex prompt: fix strain-flex reorder logic in weekly Google Sheets ordering flow

Paste this into Codex inside the repo.

---

You are patching an existing implementation that already added a weekly Google Sheets ordering workflow, but there is a business-logic flaw: some brands should **not** reorder by exact SKU / exact strain. They should reorder by a **replacement family / par bucket**, where different strains are interchangeable as long as the brand + product type + size/pack + strain type match.

Do **not** start coding immediately.

## Working rules

1. **Plan first.** Inspect the repo and the current implementation before changing code.
2. **No guessing.** If repo reality differs from anything below, say so and replan.
3. Break the work into focused passes and keep the context clean. At minimum, do these passes:
   - parsing / family-key design
   - reorder math / config design
   - sheet UX / Google Sheets write-preservation
   - tests / fixtures / proof
4. **Stop after the design pass and wait for approval** before writing code.
5. When you do implement, do it like a real engineer: root-cause the flaw, patch the minimum correct surface area, add tests, logs, and dry-run proof.
6. Preserve current working behavior unless this new requirement explicitly changes it.

---

## Context from the current repo state

The current weekly ordering implementation already exists and reportedly changed these files:

- `weekly_store_ordering_sheet.py` — end-to-end store-first workflow, Dutchie API/fixture input, normalization, 7d/14d/30d metrics, eligibility filtering, reorder math, artifact writing, CLI.
- `weekly_store_ordering_sheets.py` — Google Sheets upsert/helper layer, stable tab targeting, formatting, hidden Row Key, review-column preservation on rerun.
- `weekly_store_ordering_config.json` — business-rule config for stores, targets, exclusions, eligibility mode, reorder thresholds, review columns.
- `test_weekly_store_ordering.py` — tests for exclusions, eligibility, aggregation, sell-through, suggested qty, sorting, tab naming, review preservation, store identity.
- fixture data under `tests/fixtures/weekly_store_ordering/...`

The current reorder logic reportedly uses:

- 7d / 14d / 30d sold = sum of positive sold qty from Dutchie transactions in window
- eligibility = products whose brand or vendor sold in last 30d
- sell-through = `units_sold_window / (units_sold_window + available)`
- velocity basis = sold / velocity_window_days
- target qty = `ceil(avg_daily_sold * target_cover_days)`
- suggested order qty = `ceil(max(target_qty - available, 0))`
- `needs_order` and priority labels derived from suggested qty, sell-through, days of supply

The issue is **not** that the math is broken for exact-SKU brands.
The issue is that **the grouping level is wrong** for certain brands.

---

## The flaw to fix

Some brands are strain-flexible. We do **not** want to reorder the exact same strain every time.

Examples:

- `Pacific Stone | Flower 7g | H | 805 Glue`
- `Pacific Stone | Flower 7g | H | Blue Z`

These should be treated as the **same reorder family** for ordering purposes.
They are both:

- Brand: `Pacific Stone`
- Product type: `Flower`
- Size: `7g`
- Strain type: `Hybrid` / `H`

But this is a **different family**:

- `Pacific Stone | Flower 3.5G | S | Blue Z`

because size and strain type differ.

Another example:

- `Pacific Stone | Pre-Rolls .35g (20pk) | H | Hybrid Blend`

This should be recognized as **Hybrid** because the product name contains `| H |`.

Operationally, the team wants bucket-level ordering like:

- at least 10 **Indica eighths**
- at least 10 **Hybrid eighths**
- at least 9 **Sativa eighths**

That means if multiple strains are out, a new replacement strain can satisfy the family par. The sheet should make this easy instead of pushing exact old strains.

---

## Business outcome required

Keep the weekly Google Sheets workflow, but add support for **configurable family-par ordering** so the sheet answers this question:

> “For this store, by vendor and brand, which interchangeable product families are below par, how far below par are they, what sold in the last 7d / 14d / 30d, and which strains within that family are currently on hand / sold recently / out of stock so a human can choose what to reorder?”

The sheet must still be easy for employees to use and for a second person to review.

---

## Required design change

Introduce a configurable concept like one of these names:

- `replacement_family_key`
- `reorder_family_key`
- `par_bucket_key`

Use the best name, but the meaning must be:

> a normalized key that groups interchangeable strains together while keeping non-interchangeable variants separate.

For strain-flexible families, the key must be based on the normalized combination of:

- store
- vendor
- brand
- product type
- size / weight
- pack count if applicable
- strain type (`Indica` / `Hybrid` / `Sativa`)

The key must **not** include the specific strain / cultivar / flavor name.

### Important

This must be **configurable**.
Do **not** force every brand into family-par mode.

We need at least two reorder modes:

1. `exact_sku` (current behavior)
2. `family_par` (new behavior for strain-flexible brands / families)

Default should stay safe and explicit. Do not silently change every brand.

Allow config at least at these levels if practical:

- brand
- vendor
- maybe brand + product type
- maybe brand + product type + size

Use the least brittle config shape that is still understandable by staff.

---

## Parsing requirements

Before coding, inspect what fields already exist from Dutchie normalization. Prefer structured fields if they are already present.

If the normalized data does **not** already provide all needed structure, add a parser that can reliably extract:

- product type
- size / weight
- pack count
- strain type
- strain / cultivar name

Use product name parsing only where needed, and make it deterministic and testable.

At minimum, support strain type detection from:

- explicit fields if present
- tokens like `| I |`, `| H |`, `| S |`
- words like `Indica`, `Hybrid`, `Sativa`

Case-insensitive.

Examples that must work:

- `Pacific Stone | Flower 7g | H | 805 Glue` -> family: `Pacific Stone + Flower + 7g + Hybrid`
- `Pacific Stone | Flower 7g | H | Blue Z` -> same family as above
- `Pacific Stone | Flower 3.5G | S | Blue Z` -> different family
- `Pacific Stone | Pre-Rolls .35g (20pk) | H | Hybrid Blend` -> `Pre-Rolls + .35g + 20pk + Hybrid`

---

## Reorder math change required

For `family_par` mode, do **not** compute reorder need only from an individual SKU.

Instead:

1. Aggregate current available units across all current SKUs in the family.
2. Aggregate 7d / 14d / 30d sold across all SKUs in the family.
3. Compute sell-through and velocity at the family level.
4. Support **par-based targets** for families.

We need two concepts:

### A. Family-level current state
For each family, compute at least:

- family available units
- family inventory value
- 7d sold
- 14d sold
- 30d sold
- avg daily sold
- family sell-through
- days of supply if possible
- suggested order qty
- priority

### B. Family-level target / par
Support a configurable target model so we can express rules like:

- `Pacific Stone` + `Flower` + `3.5g` + `Indica` => minimum par 10 units
- `Pacific Stone` + `Flower` + `3.5g` + `Hybrid` => minimum par 10 units
- `Pacific Stone` + `Flower` + `3.5g` + `Sativa` => minimum par 9 units

If there is no explicit par configured for a family, define and document the fallback.
A sensible fallback may be the existing velocity-based cover-days logic, but this must be explicit and reviewable.

Do not bury this in code. Put the business rule in config.

---

## Sheet UX requirements

Do not redesign the whole workflow. Patch it so it becomes easier to order strain-flex brands.

### Keep

- weekly Monday tabs
- two tabs per store/week (`Auto` and `Review`)
- total inventory value at the top
- no samples / promos
- human review workflow
- Google Sheets rerun safety

### Change the layout so it is useful for family-par ordering

Within each store’s weekly tabs, organize output like this:

- **Vendor** section
- within vendor, **Brand** section
- within brand, **family summary rows** first
- under each family summary row, **detail rows** for specific current/recent strains

For each **family summary row**, include enough columns for easy reorder decisions, at minimum:

- Vendor
- Brand
- Reorder Mode
- Product Type
- Size / Pack
- Strain Type
- Family Key
- Family Par Target
- Current Family Units
- Gap To Par
- 7d Sold
- 14d Sold
- 30d Sold
- Sell-Through
- Suggested Order Qty
- Priority
- Inventory Value
- Notes / Guidance

For each **detail row** under the family summary, include at minimum:

- SKU if known
- Product name
- Specific strain / cultivar
- current available units
- 30d sold
- status like `On Hand`, `Out`, `Recent Seller`, `Replacement Candidate`

The sheet should make it obvious that:

- the summary row is the **thing to order against**
- the detail rows are for **cross-reference / strain choice**

### Review sheet

The review tab must preserve human-editable columns across reruns. Add/keep columns such as:

- `Chosen Replacement Strain`
- `Units To Order`
- `Reviewer Cross-Check`
- `Approved By`
- `Order Notes`

Preserve these on rerun by a stable key appropriate for the row type.

For family summary rows, preserve by family key.
For detail rows, preserve by detail row key.
Do not let summary/detail key collisions overwrite review edits.

---

## Sorting requirements

Sort so the most important ordering work appears first.

At minimum:

1. vendors / brands with items needing order first
2. within each brand, family summaries with the biggest gap to par or strongest need first
3. detail rows directly beneath their family summary

Keep the sheet easy to scan.

---

## Exclusions and edge cases

Keep excluding:

- samples
- promos
- promotional/display/tester rows

Also handle these cases explicitly:

1. Family sold in 30d but current specific strains are gone — still show family summary with gap and reorder need.
2. New strain appears in inventory but has no sales yet — it still counts toward current family units.
3. Multiple current strains in same family — aggregate them correctly.
4. Same brand but different size — do not merge.
5. Same size but different strain type — do not merge.
6. If structured strain type conflicts with parsed name token, log it and define precedence.

---

## What I want from you in Phase 1 only

Inspect the repo and return a concise engineering design review with these sections:

1. **Current behavior verified from code**
   - exact files / functions controlling current grouping and reorder math
   - exact row key behavior in Google Sheets preservation
   - what already exists that can be reused

2. **Root cause of the flaw**
   - where the current implementation stays at SKU level
   - why that breaks strain-flex ordering

3. **Patch plan**
   - which files will change
   - what new config shape you propose
   - what new parser / family-key function you propose
   - what sheet layout changes you propose
   - how rerun-safe review preservation will work

4. **Acceptance criteria**
   - clear bullet list of behaviors that will prove the fix works

5. **Risk list**
   - parsing ambiguity
   - config migration risk
   - summary/detail key collisions
   - any other implementation traps you see

Then **stop and wait for approval**.
Do not write code in Phase 1.

---

## After approval: implementation requirements for Phase 2

After approval, implement the patch.

### Code requirements

- patch existing files rather than rebuilding from scratch unless there is a proven reason not to
- keep business rules configurable
- add logs that prove how families were built and why rows were included/excluded in dry-run mode
- do not break current `exact_sku` mode

### Tests required

Add or update tests to cover at minimum:

1. `Pacific Stone | Flower 7g | H | 805 Glue` and `... | Blue Z` collapse into the same family
2. `Pacific Stone | Flower 3.5G | S | Blue Z` remains separate
3. `Pacific Stone | Pre-Rolls .35g (20pk) | H | Hybrid Blend` parses as Hybrid with correct family dimensions
4. family-level par gap math works
5. family-level sales aggregation works across multiple strains
6. `exact_sku` mode still behaves as before
7. review-column preservation works for family summary rows and detail rows without collision
8. summary rows sort ahead of detail rows and needs-order items surface first
9. sample/promo exclusions still hold

### Dry-run proof required

Run a deterministic fixture-based dry run and provide:

- command used
- produced tabs
- row counts by type (`summary` vs `detail`)
- example family summaries
- example detail rows beneath them
- proof that at least one family summary shows gap-to-par based ordering rather than exact-strain reordering

If a live spreadsheet target is available, do a single-store live write after dry-run proof. If not, say so plainly.

---

## Non-negotiable acceptance examples

The implementation is not done unless the resulting sheet behavior supports this real-world use case:

- I do **not** care whether `Pacific Stone | Flower 7g | H | 805 Glue` is specifically reordered.
- I **do** care that the store has enough `Pacific Stone` `Flower` `7g` `Hybrid` units overall.
- If `805 Glue` is out but another `Pacific Stone Flower 7g Hybrid` strain is available to order, the sheet should still guide the employee to fill the **family gap**.
- The sheet must let one person choose a replacement strain and a second person cross-check it.

That is the point of this patch.

