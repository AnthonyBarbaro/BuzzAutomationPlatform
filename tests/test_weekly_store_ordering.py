import json
import logging
import tempfile
import unittest
from datetime import date, datetime
from pathlib import Path
from unittest import mock
from zoneinfo import ZoneInfo

import pandas as pd
from googleapiclient.errors import HttpError

import weekly_store_ordering_sheet as weekly_sheet
import weekly_store_ordering_sheets as weekly_sheet_sheets
from weekly_store_ordering_sheet import (
    _format_sell_through_triplet,
    apply_exclusion_rules,
    build_ordering_bundle,
    build_tab_title,
    load_ordering_config,
    parse_spreadsheet_targets_text,
    resolve_store_spreadsheet_target,
    resolve_week_of,
    sheet_output_flags,
    sort_ordering_rows,
)
from weekly_store_ordering_sheets import build_readme_rows, build_sheet_matrix, build_summary_rows, merge_preserved_review_columns, move_latest_tabs_next_to_readme


FIXTURE_ROOT = Path(__file__).resolve().parent / "fixtures" / "weekly_store_ordering"


class _FakeRequest:
    def __init__(self, callback):
        self._callback = callback

    def execute(self):
        return self._callback()


class _FakeResp:
    def __init__(self, status):
        self.status = status
        self.reason = str(status)


def _raise(error):
    raise error


class _FakeValuesResource:
    def __init__(self, service):
        self.service = service
        self.calls = []

    def clear(self, **kwargs):
        self.calls.append(("clear", kwargs))
        return _FakeRequest(lambda: {})

    def update(self, **kwargs):
        self.calls.append(("update", kwargs))
        def execute():
            title, start_row = _parse_fake_update_range(kwargs.get("range", ""))
            sheet = self.service.find_sheet(title)
            if sheet and start_row > int(sheet.get("row_count", 1000)):
                raise RuntimeError(f"range exceeds fake grid rows: {start_row}")
            return {}

        return _FakeRequest(execute)


class _FakeSheetsResource:
    def __init__(self, service):
        self.service = service
        self.values_resource = _FakeValuesResource(service)

    def values(self):
        return self.values_resource

    def get(self, **kwargs):
        return _FakeRequest(lambda: {"sheets": self.service.metadata_sheets()})

    def batchUpdate(self, **kwargs):
        body = kwargs.get("body", {})
        self.service.batch_update_bodies.append(body)

        def execute():
            self.service.apply_batch_update(body)
            if self.service.fail_next_batch_after_apply:
                self.service.fail_next_batch_after_apply = False
                raise TimeoutError("promote timed out after apply")
            return {"replies": []}

        return _FakeRequest(execute)


class _FakeSheetsService:
    def __init__(self, sheets):
        self.sheets = [dict(sheet) for sheet in sheets]
        self.batch_update_bodies = []
        self.fail_next_batch_after_apply = False
        self._spreadsheets = _FakeSheetsResource(self)

    def spreadsheets(self):
        return self._spreadsheets

    def metadata_sheets(self):
        return [
            {
                "properties": {
                    "sheetId": sheet["sheet_id"],
                    "title": sheet["title"],
                    "index": sheet.get("index", index),
                    "hidden": sheet.get("hidden", False),
                    "gridProperties": {
                        "rowCount": sheet.get("row_count", 1000),
                        "columnCount": sheet.get("column_count", 26),
                    },
                },
                "bandedRanges": sheet.get("banded_ranges", []),
                "conditionalFormats": sheet.get("conditional_rules", []),
            }
            for index, sheet in enumerate(self.sheets)
        ]

    def apply_batch_update(self, body):
        for request in body.get("requests", []):
            if "deleteSheet" in request:
                sheet_id = request["deleteSheet"]["sheetId"]
                self.sheets = [sheet for sheet in self.sheets if sheet["sheet_id"] != sheet_id]
                continue
            add_sheet = request.get("addSheet")
            if add_sheet:
                properties = add_sheet.get("properties", {})
                next_id = max([sheet["sheet_id"] for sheet in self.sheets], default=0) + 1
                self.sheets.append(
                    {
                        "sheet_id": properties.get("sheetId", next_id),
                        "title": properties.get("title", f"Sheet{next_id}"),
                        "index": properties.get("index", len(self.sheets)),
                        "hidden": properties.get("hidden", False),
                        "row_count": properties.get("gridProperties", {}).get("rowCount", 1000),
                        "column_count": properties.get("gridProperties", {}).get("columnCount", 26),
                    }
                )
                continue
            update = request.get("updateSheetProperties")
            if update:
                properties = update.get("properties", {})
                sheet_id = properties.get("sheetId")
                for sheet in self.sheets:
                    if sheet["sheet_id"] != sheet_id:
                        continue
                    if "title" in properties:
                        sheet["title"] = properties["title"]
                    if "hidden" in properties:
                        sheet["hidden"] = properties["hidden"]
                    if "index" in properties:
                        sheet["index"] = properties["index"]
                    grid_properties = properties.get("gridProperties", {})
                    if "rowCount" in grid_properties:
                        sheet["row_count"] = grid_properties["rowCount"]
                    if "columnCount" in grid_properties:
                        sheet["column_count"] = grid_properties["columnCount"]

    def find_sheet(self, title):
        for sheet in self.sheets:
            if sheet["title"] == title:
                return sheet
        return None


def _parse_fake_update_range(range_name):
    title_part, _, cell = str(range_name).partition("!")
    title = title_part.strip("'").replace("''", "'")
    row_digits = "".join(ch for ch in cell if ch.isdigit())
    return title, int(row_digits or 1)


class WeeklyStoreOrderingTests(unittest.TestCase):
    @classmethod
    def setUpClass(cls):
        cls.config = load_ordering_config()
        cls.logger = logging.getLogger("weekly_store_ordering_test")
        cls.as_of_day = date(2026, 4, 3)
        cls.week_of = resolve_week_of("2026-04-03", cls.as_of_day)
        cls.snapshot_generated_at = datetime(2026, 4, 3, 8, 5, tzinfo=ZoneInfo("America/Los_Angeles"))
        store_root = FIXTURE_ROOT / "MV"
        cls.payloads = {
            "inventory": json.loads((store_root / "inventory.json").read_text(encoding="utf-8")),
            "products": json.loads((store_root / "products.json").read_text(encoding="utf-8")),
            "transactions": json.loads((store_root / "transactions.json").read_text(encoding="utf-8")),
        }

    def build_bundle(self):
        return build_ordering_bundle(
            store_code="MV",
            week_of=self.week_of,
            as_of_day=self.as_of_day,
            payloads=self.payloads,
            config=self.config,
            snapshot_generated_at=self.snapshot_generated_at,
            logger=self.logger,
        )

    def test_filters_samples_non_eligible_vendor_rows_and_low_velocity_skus(self):
        bundle = self.build_bundle()
        auto_df = bundle["auto_df"]
        metric_skus = bundle["sku_metrics"]["SKU"].tolist()

        self.assertEqual(len(auto_df), 3)
        self.assertNotIn("SKU", auto_df.columns)
        self.assertEqual(
            list(auto_df.columns),
            [
                "Row Key",
                "Brand",
                "Category",
                "Product",
                "Available",
                "Par Level",
                "Cost",
                "Price",
                "Units Sold 7d",
                "Units Sold 14d",
                "Units Sold 30d",
            ],
        )
        self.assertNotIn("SKU-SAMPLE", metric_skus)
        self.assertNotIn("SKU-DRINK", metric_skus)
        self.assertNotIn("SKU-NOSALES", metric_skus)
        self.assertIn("SKU-ZEROINV", metric_skus)
        self.assertEqual(bundle["logs"]["inventory_exclusion_counts"], {"pattern:product": 1})
        self.assertEqual(bundle["logs"]["metric_filter_counts"], {"min_units_sold_30d": 1})
        self.assertEqual(
            list(bundle["summary"].keys()),
            [
                "Store",
                "Week Of",
                "Snapshot Generated At",
                "Total Inventory Value",
                "Total Cannabis Inventory Value",
                "Total Accessories Inventory Value",
            ],
        )
        self.assertAlmostEqual(bundle["summary"]["Total Inventory Value"], 133.0)
        self.assertAlmostEqual(bundle["summary"]["Total Cannabis Inventory Value"], 133.0)
        self.assertAlmostEqual(bundle["summary"]["Total Accessories Inventory Value"], 0.0)

    def test_santee_keeps_in_stock_skus_with_no_sales_and_repairs_sample_cost(self):
        payloads = {
            "inventory": [
                {
                    "sku": "40918812",
                    "productName": "Emerald Sky | Gummies 100mg (10pk) | S | California Orange",
                    "category": "Edibles",
                    "masterCategory": "Edibles",
                    "quantityAvailable": 18,
                    "unitCost": 0.01,
                    "unitPrice": 10.0,
                    "brandName": "Emerald Sky",
                    "vendor": "Vino & Cigarro, LLC",
                    "tags": [],
                },
                {
                    "sku": "00775052",
                    "productName": "Emerald Sky | Gummies 100mg (10pk) | S | California Orange (SAMPLE)",
                    "category": "Edibles",
                    "masterCategory": "Edibles",
                    "quantityAvailable": 5,
                    "unitCost": 0.01,
                    "unitPrice": 0.01,
                    "brandName": "Emerald Sky",
                    "vendor": "Vino & Cigarro, LLC",
                    "tags": [],
                },
            ],
            "products": [
                {
                    "productId": 40918812,
                    "productName": "Emerald Sky | Gummies 100mg (10pk) | S | California Orange",
                    "category": "Edibles",
                    "masterCategory": "Edibles",
                    "vendorName": "Vino & Cigarro, LLC",
                    "producerName": "Vino & Cigarro, LLC",
                    "sku": "40918812",
                    "unitCost": 4.0,
                    "price": 10.0,
                },
                {
                    "productId": 775052,
                    "productName": "Emerald Sky | Gummies 100mg (10pk) | S | California Orange (SAMPLE)",
                    "category": "Edibles",
                    "masterCategory": "Edibles",
                    "vendorName": "Vino & Cigarro, LLC",
                    "producerName": "Vino & Cigarro, LLC",
                    "sku": "00775052",
                    "unitCost": 0.01,
                    "price": 0.01,
                },
            ],
            "transactions": [],
        }

        bundle = build_ordering_bundle(
            store_code="SE",
            week_of=self.week_of,
            as_of_day=self.as_of_day,
            payloads=payloads,
            config=self.config,
            snapshot_generated_at=self.snapshot_generated_at,
            logger=self.logger,
        )

        metrics = bundle["sku_metrics"].set_index("SKU")
        self.assertIn("40918812", metrics.index)
        self.assertNotIn("00775052", metrics.index)

        california_orange = metrics.loc["40918812"]
        self.assertEqual(int(california_orange["Available"]), 18)
        self.assertEqual(float(california_orange["Cost"]), 4.0)
        self.assertEqual(int(california_orange["Units Sold 30d"]), 0)
        self.assertEqual(bundle["logs"]["inventory_catalog_cost_overrides"], 1)
        self.assertAlmostEqual(bundle["summary"]["Total Inventory Value"], 72.05)

    def test_inventory_value_breakdown_splits_accessories_from_cannabis(self):
        inventory_df = pd.DataFrame(
            [
                {"Category": "Flower", "Available": 2, "Cost": 10.0},
                {"Category": "Accessories", "Available": 3, "Cost": 4.0},
                {"category_normalized": "Smoking Accessories", "Available": 1, "Cost": 5.0},
            ]
        )

        breakdown = weekly_sheet.compute_inventory_value_breakdown(inventory_df)

        self.assertAlmostEqual(breakdown["Total Inventory Value"], 37.0)
        self.assertAlmostEqual(breakdown["Total Cannabis Inventory Value"], 20.0)
        self.assertAlmostEqual(breakdown["Total Accessories Inventory Value"], 17.0)

    def test_extra_keyword_vendor_exclusion_is_supported(self):
        config = json.loads(json.dumps(self.config))
        config["exclusions"]["extra_keywords"] = ["vendor beta"]

        inventory_filtered, counts = apply_exclusion_rules(
            self.build_bundle()["normalized_inventory"],
            "inventory",
            config,
        )

        self.assertNotIn("Vendor Beta", inventory_filtered["Vendor"].tolist())
        self.assertEqual(counts["keyword:vendor"], 1)

    def test_spreadsheet_target_text_supports_default_and_store_specific_entries(self):
        targets = parse_spreadsheet_targets_text(
            """
            # Weekly ordering targets
            DEFAULT=https://docs.google.com/spreadsheets/d/default-sheet-id/edit
            MV=https://docs.google.com/spreadsheets/d/mv-sheet-id/edit
            lg=https://docs.google.com/spreadsheets/d/lg-sheet-id/edit
            """
        )

        self.assertEqual(
            resolve_store_spreadsheet_target(targets, "MV"),
            "https://docs.google.com/spreadsheets/d/mv-sheet-id/edit",
        )
        self.assertEqual(
            resolve_store_spreadsheet_target(targets, "LG"),
            "https://docs.google.com/spreadsheets/d/lg-sheet-id/edit",
        )
        self.assertEqual(
            resolve_store_spreadsheet_target(targets, "NC"),
            "https://docs.google.com/spreadsheets/d/default-sheet-id/edit",
        )

    def test_resolve_week_of_uses_next_monday_when_week_is_omitted_midweek(self):
        self.assertEqual(resolve_week_of(None, date(2026, 4, 22)).isoformat(), "2026-04-27")
        self.assertEqual(resolve_week_of(None, date(2026, 4, 20)).isoformat(), "2026-04-20")

    def test_resolve_week_of_still_normalizes_explicit_dates_to_their_monday(self):
        self.assertEqual(resolve_week_of("2026-04-22", date(2026, 4, 22)).isoformat(), "2026-04-20")
        self.assertEqual(resolve_week_of("2026-04-27", date(2026, 4, 22)).isoformat(), "2026-04-27")

    def test_sheet_output_flags_can_disable_auto_tab_while_keeping_review_enabled(self):
        config = json.loads(json.dumps(self.config))
        config["sheet_outputs"] = {
            "write_auto_tab": False,
            "write_review_tab": True,
        }

        self.assertEqual(sheet_output_flags(config), {"auto": False, "review": True})

    def test_sheet_formatting_defaults_enable_cost_price_separator_line(self):
        self.assertTrue(self.config["sheet_formatting"]["show_cost_price_separator_line"])

    def test_build_readme_rows_includes_latest_review_tab_and_reflects_script_owned_layout(self):
        rows = build_readme_rows(
            store_code="NC",
            store_name="National City",
            output_flags={"auto": False, "review": True},
            week_of="2026-04-13",
            tab_titles={"review": "NC 2026-04-13 Review"},
            manual_columns=[],
            snapshot_generated_at="2026-04-14T08:05:00-07:00",
        )

        self.assertEqual(rows[0][0], "Buzz Weekly Store Ordering")
        self.assertIn(["Store", "NC - National City"], rows)
        self.assertIn(["Latest Week Generated", "2026-04-13"], rows)
        self.assertIn(["Latest Review Tab", "NC 2026-04-13 Review"], rows)
        self.assertIn(["Training Video", "https://youtu.be/ri9VkqPGAUQ"], rows)
        self.assertIn(["Current Google Sheet Output", "This repo currently writes the REVIEW tab to Google Sheets."], rows)
        vendor_row = next(row for row in rows if row[0] == "Pick A Vendor Or Brand")
        self.assertIn("Filter Brand", vendor_row[1])
        filter_row = next(row for row in rows if row[0] == "Recommended Filters")
        self.assertIn("Brand -> Category", filter_row[1])
        manual_row = next(row for row in rows if row[0] == "Manual Columns Preserved On Rerun")
        self.assertEqual(manual_row[1], "None. This sheet is script-owned and safe to rerun.")
        defaults_row = next(row for row in rows if row[0] == "Run Defaults")
        self.assertIn("next Monday", defaults_row[1])

    def test_move_latest_tabs_next_to_readme_keeps_readme_first_and_dedupes_titles(self):
        service = object()
        with mock.patch("weekly_store_ordering_sheets.move_sheet_to_index") as mock_move:
            ordered_titles = move_latest_tabs_next_to_readme(
                service,
                "spreadsheet-123",
                ["NC 2026-04-13 Review", "NC 2026-04-13 Auto", "NC 2026-04-13 Review"],
            )

        self.assertEqual(ordered_titles, ["NC 2026-04-13 Review", "NC 2026-04-13 Auto"])
        self.assertEqual(
            mock_move.call_args_list,
            [
                mock.call(service, "spreadsheet-123", "README", 0),
                mock.call(service, "spreadsheet-123", "NC 2026-04-13 Review", 1),
                mock.call(service, "spreadsheet-123", "NC 2026-04-13 Auto", 2),
            ],
        )

    def test_sheet_execute_retries_timeout_and_logs_context(self):
        outcomes = [TimeoutError("write operation timed out"), {"ok": True}]

        def request_factory():
            return _FakeRequest(lambda: outcomes.pop(0) if not isinstance(outcomes[0], Exception) else (_raise(outcomes.pop(0))))

        with mock.patch("weekly_store_ordering_sheets.time.sleep") as mock_sleep:
            with self.assertLogs("weekly_store_ordering.sheets", level="WARNING") as logs:
                result = weekly_sheet_sheets._execute_sheet_request(
                    request_factory,
                    store_name="MV - Buzz Cannabis - Mission Valley",
                    tab_name="MV 2026-06-22 Review",
                    operation="write rows 1-500",
                )

        self.assertEqual(result, {"ok": True})
        mock_sleep.assert_called_once_with(2.0)
        self.assertIn("MV - Buzz Cannabis - Mission Valley", logs.output[0])
        self.assertIn("MV 2026-06-22 Review", logs.output[0])
        self.assertIn("attempt=1/5", logs.output[0])

    def test_sheet_execute_retries_transient_google_status(self):
        outcomes = [HttpError(_FakeResp(503), b"backend error"), {"ok": True}]

        def request_factory():
            return _FakeRequest(lambda: outcomes.pop(0) if not isinstance(outcomes[0], Exception) else (_raise(outcomes.pop(0))))

        with mock.patch("weekly_store_ordering_sheets.time.sleep"):
            result = weekly_sheet_sheets._execute_sheet_request(
                request_factory,
                store_name="MV",
                tab_name="MV 2026-06-22 Review",
                operation="format ordering tab",
            )

        self.assertEqual(result, {"ok": True})

    def test_write_sheet_values_chunks_large_payloads(self):
        service = _FakeSheetsService([])
        values = [[row] for row in range(1201)]

        written = weekly_sheet_sheets._write_sheet_values(
            service,
            "spreadsheet-123",
            "MV 2026-06-22 Review",
            values,
            store_name="MV",
            chunk_rows=500,
        )

        self.assertEqual(written, 1201)
        value_calls = service.spreadsheets().values_resource.calls
        self.assertEqual(value_calls[0][0], "clear")
        update_calls = [call for call in value_calls if call[0] == "update"]
        self.assertEqual([call[1]["range"] for call in update_calls], [
            "'MV 2026-06-22 Review'!A1",
            "'MV 2026-06-22 Review'!A501",
            "'MV 2026-06-22 Review'!A1001",
        ])
        self.assertEqual([len(call[1]["body"]["values"]) for call in update_calls], [500, 500, 201])

    def test_upsert_ordering_tab_resizes_staging_grid_before_chunk_writes(self):
        title = "MV 2026-06-22 Review"
        staging_title = weekly_sheet_sheets._staging_tab_title(title)
        service = _FakeSheetsService(
            [
                {
                    "sheet_id": 20,
                    "title": staging_title,
                    "index": 1,
                    "hidden": True,
                    "row_count": 1000,
                    "column_count": 26,
                },
            ]
        )
        df = pd.DataFrame({"Row Key": [f"row-{index}" for index in range(1200)], "Product": ["Item"] * 1200})

        result = weekly_sheet_sheets.upsert_ordering_tab(
            service=service,
            spreadsheet_id="spreadsheet-123",
            title=title,
            summary_rows=[],
            df=df,
            sheet_kind="review",
            hidden_headers={"Row Key"},
            show_cost_price_separator=False,
            store_name="MV",
        )

        self.assertEqual(result["rows_written"], 1200)
        final_sheet = service.find_sheet(title)
        self.assertIsNotNone(final_sheet)
        self.assertEqual(final_sheet["row_count"], 1201)
        self.assertEqual(final_sheet["column_count"], 2)
        update_ranges = [
            call[1]["range"]
            for call in service.spreadsheets().values_resource.calls
            if call[0] == "update"
        ]
        self.assertIn(f"'{staging_title}'!A1001", update_ranges)

    def test_promote_staging_sheet_recovers_when_timeout_happened_after_swap(self):
        staging_title = weekly_sheet_sheets._staging_tab_title("MV 2026-06-22 Review")
        service = _FakeSheetsService(
            [
                {"sheet_id": 10, "title": "MV 2026-06-22 Review", "index": 2, "hidden": False},
                {"sheet_id": 20, "title": staging_title, "index": 7, "hidden": True},
            ]
        )
        service.fail_next_batch_after_apply = True

        with mock.patch("weekly_store_ordering_sheets.time.sleep"):
            promoted = weekly_sheet_sheets.promote_staging_sheet(
                service,
                "spreadsheet-123",
                staging_title=staging_title,
                final_title="MV 2026-06-22 Review",
                staging_sheet_id=20,
                sheet_kind="review",
                store_name="MV",
            )

        self.assertEqual(promoted["sheet_id"], 20)
        final_sheets = [sheet for sheet in service.sheets if sheet["title"] == "MV 2026-06-22 Review"]
        self.assertEqual(len(final_sheets), 1)
        self.assertFalse(final_sheets[0]["hidden"])
        self.assertNotIn(10, [sheet["sheet_id"] for sheet in service.sheets])

    def test_ensure_sheet_recovers_when_timeout_happened_after_add(self):
        service = _FakeSheetsService([])
        service.fail_next_batch_after_apply = True

        with mock.patch("weekly_store_ordering_sheets.time.sleep"):
            info = weekly_sheet_sheets.ensure_sheet(
                service,
                "spreadsheet-123",
                "MV 2026-06-22 Review",
                "review",
                hidden=True,
                store_name="MV",
            )

        self.assertEqual(info["title"], "MV 2026-06-22 Review")
        self.assertTrue(info["hidden"])
        self.assertEqual(len(service.sheets), 1)

    def test_cost_price_separator_positions_mark_breaks_when_price_or_category_changes(self):
        df = pd.DataFrame(
            [
                {"Brand": "Ball Family Farms", "Category": "Flower", "Product": "Gelonade", "Cost": 12.0, "Price": 35.0},
                {"Brand": "Ball Family Farms", "Category": "Flower", "Product": "Pookie", "Cost": 12.0, "Price": 35.0},
                {"Brand": "Ball Family Farms", "Category": "Quarters", "Product": "A-Boogie", "Cost": 16.0, "Price": 45.0},
                {"Brand": "Ball Family Farms", "Category": "Quarters", "Product": "Bishop", "Cost": 16.0, "Price": 45.0},
                {"Brand": "Ball Family Farms", "Category": "Quarters", "Product": "Candy Gas", "Cost": 16.0, "Price": 50.0},
            ]
        )

        self.assertEqual(weekly_sheet_sheets._cost_price_separator_positions(df), [2, 4])

    def test_cost_price_separator_positions_ignore_cost_only_changes(self):
        df = pd.DataFrame(
            [
                {"Brand": "Cannabiotix (CBX)", "Category": "Eighths", "Product": "CBX | Flower 3.5g | H | Cereal Milk", "Cost": 17.92, "Price": 52.0},
                {"Brand": "Cannabiotix (CBX)", "Category": "Eighths", "Product": "CBX | Flower 3.5g | I | 98 Octane", "Cost": 18.67, "Price": 52.0},
                {"Brand": "Cannabiotix (CBX)", "Category": "Eighths", "Product": "CBX | Flower 3.5g | H | Kush Mountains", "Cost": 19.60, "Price": 52.0},
            ]
        )

        self.assertEqual(weekly_sheet_sheets._cost_price_separator_positions(df), [])

    def test_cost_price_separator_positions_ignore_float_noise_that_displays_as_same_currency(self):
        df = pd.DataFrame(
            [
                {"Brand": "710 Labs", "Category": "Eighths", "Product": "Cold Creek Kush", "Cost": 23.399999999999995, "Price": 65.0},
                {"Brand": "710 Labs", "Category": "Eighths", "Product": "Rainbow Belts", "Cost": 23.4, "Price": 65.0},
                {"Brand": "710 Labs", "Category": "Eighths", "Product": "Donny Burger", "Cost": 23.400000000000002, "Price": 65.0},
            ]
        )

        self.assertEqual(weekly_sheet_sheets._cost_price_separator_positions(df), [])

    def test_cost_price_separator_positions_mark_break_when_category_changes_with_same_cost_and_price(self):
        df = pd.DataFrame(
            [
                {"Brand": "710 Labs", "Category": "Disposables", "Product": "710 | LRO AIO 1G | Upside Down Frown #15 + Gorilla Dosha #3", "Cost": 36.0, "Price": 90.0},
                {"Brand": "710 Labs", "Category": "Concentrate", "Product": "710 | Persy Badder 1g | Grapefruit OG", "Cost": 36.0, "Price": 90.0},
                {"Brand": "710 Labs", "Category": "Concentrate", "Product": "710 | Persy Badder 1g | Guava", "Cost": 36.0, "Price": 90.0},
            ]
        )

        self.assertEqual(weekly_sheet_sheets._cost_price_separator_positions(df), [1])

    def test_low_cost_exclusion_uses_cost_only(self):
        config = json.loads(json.dumps(self.config))
        config["exclusions"]["exclude_low_cost_rows"] = True
        config["exclusions"]["low_cost_threshold"] = 1.0
        inventory_df = pd.DataFrame(
            [
                {"Product": "Pocket Lighter", "Category": "Accessories", "Tags": "", "Brand": "Brand A", "Vendor": "Vendor A", "Cost": 0.75, "Price": 12.0},
                {"Product": "Regular Item", "Category": "Flower", "Tags": "", "Brand": "Brand A", "Vendor": "Vendor A", "Cost": 1.0, "Price": 12.0},
            ]
        )

        inventory_filtered, counts = apply_exclusion_rules(inventory_df, "inventory", config)

        self.assertEqual(inventory_filtered["Product"].tolist(), ["Regular Item"])
        self.assertEqual(counts["low_cost"], 1)

    def test_computes_sold_windows_sell_through_and_suggested_qty(self):
        bundle = self.build_bundle()
        metrics = bundle["sku_metrics"].set_index("SKU")

        flower = metrics.loc["SKU-FLOWER"]
        self.assertEqual(int(flower["Units Sold 7d"]), 2)
        self.assertEqual(int(flower["Units Sold 14d"]), 4)
        self.assertEqual(int(flower["Units Sold 30d"]), 8)
        self.assertEqual(int(flower["Par Level"]), 5)
        self.assertEqual(int(flower["Suggested Order Qty"]), 4)
        self.assertEqual(flower["Needs Order"], "Y")
        self.assertEqual(str(flower["Last Sale Date"]), "2026-04-02")
        self.assertAlmostEqual(float(flower["Sell-Through 30d"]), 8.0 / 9.0, places=4)
        self.assertAlmostEqual(float(flower["Avg Daily Sold 14d"]), 4.0 / 14.0, places=4)
        self.assertAlmostEqual(float(flower["Days of Supply"]), 3.5, places=2)
        self.assertIn("3.5 days of supply", flower["Reorder Notes / Reason"])
        self.assertIn("4/14d", flower["Reorder Notes / Reason"])

        gummy = metrics.loc["SKU-GUMMY"]
        self.assertEqual(int(gummy["Units Sold 7d"]), 5)
        self.assertEqual(int(gummy["Units Sold 14d"]), 8)
        self.assertEqual(int(gummy["Units Sold 30d"]), 12)
        self.assertEqual(int(gummy["Par Level"]), 10)
        self.assertEqual(int(gummy["Suggested Order Qty"]), 6)
        self.assertEqual(gummy["Needs Order"], "Y")
        self.assertEqual(gummy["Reorder Priority"], "Low Cover")
        self.assertIn("7.0 days of supply", gummy["Reorder Notes / Reason"])
        self.assertIn("suggest 6", gummy["Reorder Notes / Reason"])

        self.assertNotIn("SKU-NOSALES", metrics.index)

        zero_inventory = metrics.loc["SKU-ZEROINV"]
        self.assertEqual(int(zero_inventory["Available"]), 0)
        self.assertEqual(int(zero_inventory["Units Sold 30d"]), 3)
        self.assertEqual(int(zero_inventory["Par Level"]), 4)
        self.assertEqual(int(zero_inventory["Suggested Order Qty"]), 4)
        self.assertEqual(zero_inventory["Reorder Priority"], "Urgent")
        self.assertIn("Out of stock", zero_inventory["Reorder Notes / Reason"])

    def test_sorting_groups_rows_by_brand_category_and_priority_with_slimmer_columns(self):
        bundle = self.build_bundle()
        auto_df = bundle["auto_df"]

        self.assertEqual(
            auto_df["Row Key"].tolist(),
            ["MV|sku:SKU-ZEROINV", "MV|sku:SKU-GUMMY", "MV|sku:SKU-FLOWER"],
        )
        self.assertNotIn("Vendor", auto_df.columns)
        self.assertNotIn("Reorder Priority", auto_df.columns)
        self.assertNotIn("Inventory Value", auto_df.columns)
        self.assertEqual(auto_df["Category"].tolist(), ["Edibles", "Edibles", "Flower"])
        self.assertEqual(
            auto_df["Product"].tolist(),
            [
                "Brand A | Gummies 10pk | Tropical",
                "Brand A | Gummies 100mg | Mixed Berry",
                "Brand A | Flower 3.5g | Blue Dream",
            ],
        )
        self.assertEqual(auto_df["Par Level"].tolist(), ["", "", ""])
        self.assertEqual(auto_df["Cost"].tolist(), [7.0, 8.0, 12.0])

    def test_recent_velocity_and_low_stock_can_push_par_above_14d_sales(self):
        par_level = weekly_sheet._estimate_par_level(
            {
                "units_sold_7d": 10.0,
                "units_sold_14d": 11.0,
                "units_sold_30d": 22.0,
                "available": 4.0,
                "sell_through_7d": 10.0 / 14.0,
                "sell_through_14d": 11.0 / 15.0,
                "sell_through_30d": 22.0 / 26.0,
            },
            self.config,
            14,
        )

        self.assertEqual(par_level, 18)

    def test_catalog_cost_and_price_are_preferred_over_sales_and_inventory_basis(self):
        inventory_agg = pd.DataFrame(
            [
                {
                    "row_key": "MV|sku:40905401",
                    "store_code": "MV",
                    "store_name": "Morena Vista",
                    "sku": "40905401",
                    "vendor": "Vino & Cigarro, LLC",
                    "vendor_key": "vinoycigarro",
                    "brand": "CLSICS",
                    "brand_key": "clsics",
                    "category": "Prerolls",
                    "product": "Hash IN Pre-roll 1g | Blue Crack",
                    "available": 4.0,
                    "cost": 13.75,
                    "price": 12.0,
                    "inventory_value": 55.0,
                    "inventory_row_count": 2,
                }
            ]
        )
        sales_agg = pd.DataFrame(
            [
                {
                    "row_key": "MV|sku:40905401",
                    "store_code": "MV",
                    "store_name": "Morena Vista",
                    "sku": "40905401",
                    "vendor": "Vino & Cigarro, LLC",
                    "vendor_key": "vinoycigarro",
                    "brand": "CLSICS",
                    "brand_key": "clsics",
                    "category": "Prerolls",
                    "product": "Hash IN Pre-roll 1g | Blue Crack",
                    "units_sold_7d": 10.0,
                    "units_sold_14d": 11.0,
                    "units_sold_30d": 22.0,
                    "units_sold_velocity": 11.0,
                    "cost": 4.0,
                    "price": 12.0,
                    "last_sale_date": date(2026, 4, 21),
                    "sales_row_count": 22,
                }
            ]
        )
        catalog_agg = pd.DataFrame(
            [
                {
                    "row_key": "MV|sku:40905401",
                    "cost_catalog": 4.0,
                    "price_catalog": 13.0,
                }
            ]
        )

        merged = weekly_sheet.merge_inventory_sales(inventory_agg, sales_agg, catalog_agg=catalog_agg)
        self.assertEqual(float(merged.loc[0, "cost"]), 4.0)
        self.assertEqual(float(merged.loc[0, "price"]), 13.0)

    def test_sell_through_display_rounds_to_whole_percent_and_collapses_matching_values(self):
        self.assertEqual(_format_sell_through_triplet(1.0, 1.0, 1.0), "100%")
        self.assertEqual(_format_sell_through_triplet(0.375, 0.565, 0.60), "38% / 57% / 60%")

    def test_sorting_uses_product_before_reorder_priority_within_same_group(self):
        metrics_df = pd.DataFrame(
            [
                {
                    "Vendor": "Vendor Alpha",
                    "Brand": "Brand A",
                    "Category": "Flower",
                    "Cost": 12.0,
                    "Price": 35.0,
                    "Reorder Priority": "Reorder",
                    "Priority Rank": 1,
                    "Product": "Item A",
                    "SKU": "SKU-A",
                },
                {
                    "Vendor": "Vendor Alpha",
                    "Brand": "Brand A",
                    "Category": "Flower",
                    "Cost": 12.0,
                    "Price": 35.0,
                    "Reorder Priority": "Urgent",
                    "Priority Rank": 3,
                    "Product": "Item B",
                    "SKU": "SKU-B",
                },
                {
                    "Vendor": "Vendor Alpha",
                    "Brand": "Brand A",
                    "Category": "Flower",
                    "Cost": 12.0,
                    "Price": 35.0,
                    "Reorder Priority": "Low Cover",
                    "Priority Rank": 2,
                    "Product": "Item C",
                    "SKU": "SKU-C",
                },
            ]
        )

        sorted_df = sort_ordering_rows(metrics_df)

        self.assertEqual(sorted_df["Product"].tolist(), ["Item A", "Item B", "Item C"])
        self.assertEqual(sorted_df["Reorder Priority"].tolist(), ["Reorder", "Urgent", "Low Cover"])
        self.assertEqual(sorted_df["SKU"].tolist(), ["SKU-A", "SKU-B", "SKU-C"])

    def test_sorting_uses_category_before_product_within_same_brand_price_block(self):
        metrics_df = pd.DataFrame(
            [
                {
                    "Brand": "710 Labs",
                    "Category": "Disposables",
                    "Cost": 36.0,
                    "Price": 90.0,
                    "Priority Rank": 1,
                    "Product": "710 | LRO AIO 1G | Apple",
                    "SKU": "SKU-B",
                },
                {
                    "Brand": "710 Labs",
                    "Category": "Concentrate",
                    "Cost": 36.0,
                    "Price": 90.0,
                    "Priority Rank": 1,
                    "Product": "710 | Rosin 1G | Banana",
                    "SKU": "SKU-C",
                },
                {
                    "Brand": "710 Labs",
                    "Category": "Eighths",
                    "Cost": 36.0,
                    "Price": 90.0,
                    "Priority Rank": 1,
                    "Product": "710 | Flower 3.5g | Cherry",
                    "SKU": "SKU-A",
                },
            ]
        )

        sorted_df = sort_ordering_rows(metrics_df)

        self.assertEqual(
            sorted_df["Product"].tolist(),
            [
                "710 | Rosin 1G | Banana",
                "710 | LRO AIO 1G | Apple",
                "710 | Flower 3.5g | Cherry",
            ],
        )
        self.assertEqual(sorted_df["Category"].tolist(), ["Concentrate", "Disposables", "Eighths"])

    def test_sorting_uses_price_before_cost_within_same_brand_category_block(self):
        metrics_df = pd.DataFrame(
            [
                {
                    "Brand": "Cannabiotix (CBX)",
                    "Category": "Eighths",
                    "Cost": 17.92,
                    "Price": 52.0,
                    "Priority Rank": 1,
                    "Product": "CBX | Flower 3.5g | H | Cereal Milk",
                    "SKU": "SKU-A",
                },
                {
                    "Brand": "Cannabiotix (CBX)",
                    "Category": "Eighths",
                    "Cost": 22.40,
                    "Price": 52.0,
                    "Priority Rank": 1,
                    "Product": "CBX | Flower 3.5g | I | Jet Lag OG",
                    "SKU": "SKU-B",
                },
                {
                    "Brand": "Cannabiotix (CBX)",
                    "Category": "Eighths",
                    "Cost": 18.67,
                    "Price": 65.0,
                    "Priority Rank": 1,
                    "Product": "CBX | Flower 3.5g | I | Red Eye OG",
                    "SKU": "SKU-C",
                },
            ]
        )

        sorted_df = sort_ordering_rows(metrics_df)

        self.assertEqual(
            sorted_df["Product"].tolist(),
            [
                "CBX | Flower 3.5g | H | Cereal Milk",
                "CBX | Flower 3.5g | I | Jet Lag OG",
                "CBX | Flower 3.5g | I | Red Eye OG",
            ],
        )
        self.assertEqual(sorted_df["Price"].tolist(), [52.0, 52.0, 65.0])

    def test_line_items_stay_separate_even_when_strain_family_matches(self):
        merged_df = pd.DataFrame(
            [
                {
                    "row_key": "NC|sku:1",
                    "store_code": "NC",
                    "store_name": "National City",
                    "sku": "1",
                    "vendor": "Vendor Alpha",
                    "vendor_key": "vendoralpha",
                    "brand": "CAM",
                    "brand_key": "cam",
                    "category": "Eighths",
                    "product": "CAM | Flower 3.5G | H | Coconut Milk",
                    "available": 2.0,
                    "cost": 25.0,
                    "price": 58.0,
                    "inventory_value": 50.0,
                    "inventory_row_count": 1,
                    "units_sold_7d": 14.0,
                    "units_sold_14d": 14.0,
                    "units_sold_30d": 14.0,
                    "units_sold_velocity": 14.0,
                    "last_sale_date": date(2026, 4, 13),
                    "sales_row_count": 1,
                    "has_inventory": True,
                    "eligible_brand_30d": True,
                    "eligible_vendor_30d": True,
                },
                {
                    "row_key": "NC|sku:2",
                    "store_code": "NC",
                    "store_name": "National City",
                    "sku": "2",
                    "vendor": "Vendor Alpha",
                    "vendor_key": "vendoralpha",
                    "brand": "CAM",
                    "brand_key": "cam",
                    "category": "Eighths",
                    "product": "CAM | Flower 3.5G | H | GG #4",
                    "available": 4.0,
                    "cost": 25.0,
                    "price": 58.0,
                    "inventory_value": 100.0,
                    "inventory_row_count": 1,
                    "units_sold_7d": 12.0,
                    "units_sold_14d": 12.0,
                    "units_sold_30d": 12.0,
                    "units_sold_velocity": 12.0,
                    "last_sale_date": None,
                    "sales_row_count": 1,
                    "has_inventory": True,
                    "eligible_brand_30d": True,
                    "eligible_vendor_30d": True,
                },
                {
                    "row_key": "NC|sku:3",
                    "store_code": "NC",
                    "store_name": "National City",
                    "sku": "3",
                    "vendor": "Vendor Alpha",
                    "vendor_key": "vendoralpha",
                    "brand": "CAM",
                    "brand_key": "cam",
                    "category": "Eighths",
                    "product": "CAM | Flower 3.5G | I | Bubba's Girl",
                    "available": 5.0,
                    "cost": 25.0,
                    "price": 58.0,
                    "inventory_value": 125.0,
                    "inventory_row_count": 1,
                    "units_sold_7d": 11.0,
                    "units_sold_14d": 11.0,
                    "units_sold_30d": 27.0,
                    "units_sold_velocity": 11.0,
                    "last_sale_date": date(2026, 4, 13),
                    "sales_row_count": 1,
                    "has_inventory": True,
                    "eligible_brand_30d": True,
                    "eligible_vendor_30d": True,
                },
            ]
        )

        metrics = weekly_sheet.compute_ordering_metrics(merged_df, self.config).set_index("Product")
        self.assertEqual(
            metrics.index.tolist(),
            [
                "CAM | Flower 3.5G | H | Coconut Milk",
                "CAM | Flower 3.5G | H | GG #4",
                "CAM | Flower 3.5G | I | Bubba's Girl",
            ],
        )

        coconut = metrics.loc["CAM | Flower 3.5G | H | Coconut Milk"]
        self.assertEqual(int(coconut["Available"]), 2)
        self.assertEqual(int(coconut["Par Level"]), 23)
        self.assertEqual(int(coconut["Suggested Order Qty"]), 21)
        self.assertEqual(coconut["Reorder Priority"], "Urgent")
        self.assertEqual(str(coconut["Row Key"]), "NC|sku:1")

        gg4 = metrics.loc["CAM | Flower 3.5G | H | GG #4"]
        self.assertEqual(int(gg4["Available"]), 4)
        self.assertEqual(int(gg4["Par Level"]), 19)
        self.assertEqual(int(gg4["Suggested Order Qty"]), 15)
        self.assertEqual(gg4["Reorder Priority"], "Low Cover")
        self.assertEqual(str(gg4["Row Key"]), "NC|sku:2")

    def test_store_identity_and_tab_naming_are_stable(self):
        bundle = self.build_bundle()
        metrics = bundle["sku_metrics"]

        self.assertTrue(all(str(value).startswith("MV|") for value in metrics["Row Key"].tolist()))
        self.assertTrue(all(str(value) == "MV" for value in metrics["store_code"].tolist()))
        self.assertEqual(self.week_of.isoformat(), "2026-03-30")
        self.assertEqual(build_tab_title("MV", self.week_of, "Auto"), "MV 2026-03-30 Auto")
        self.assertEqual(build_tab_title("MV", self.week_of, "Review"), "MV 2026-03-30 Review")

    def test_review_tab_matches_auto_columns_when_manual_columns_are_removed(self):
        bundle = self.build_bundle()
        review_df = bundle["review_df"]

        self.assertEqual(list(review_df.columns), list(bundle["auto_df"].columns))
        self.assertEqual(review_df["Par Level"].tolist(), ["", "", ""])
        merged = merge_preserved_review_columns(review_df, [list(review_df.columns)], manual_columns=self.config["review_manual_columns"])
        pd.testing.assert_frame_equal(merged, review_df)

    def test_sheet_payload_is_json_serializable(self):
        bundle = self.build_bundle()
        summary_rows = build_summary_rows(bundle["summary"])
        values, header_row_number = build_sheet_matrix(summary_rows, bundle["auto_df"])

        self.assertEqual(len(summary_rows), 2)
        self.assertEqual(header_row_number, 3)
        self.assertEqual(summary_rows[1][2], "Total Cannabis Inventory Value")
        self.assertEqual(summary_rows[1][4], "Total Accessories Inventory Value")
        json.dumps({"values": values})

    def test_main_continues_after_one_store_failure_and_records_it(self):
        with tempfile.TemporaryDirectory() as tmpdir:
            config = json.loads(json.dumps(self.config))
            config["stores"] = ["MV", "SV", "NC"]
            config["output_root"] = tmpdir
            config["sheet_outputs"] = {
                "write_auto_tab": False,
                "write_review_tab": True,
            }

            def fake_load_store_payloads(store_code, *_args, **_kwargs):
                if store_code == "SV":
                    raise RuntimeError("SV timed out")
                return {"store_code": store_code}

            def fake_build_ordering_bundle(store_code, week_of, *_args, **_kwargs):
                return {
                    "store_code": store_code,
                    "auto_df": pd.DataFrame([{"Row Key": f"{store_code}|sku:1"}]),
                    "review_df": pd.DataFrame([{"Row Key": f"{store_code}|sku:1"}]),
                    "logs": {
                        "needs_order_rows": 1,
                        "inventory_exclusion_counts": {},
                        "sales_exclusion_counts": {},
                        "transaction_drop_counts": {},
                    },
                    "tab_titles": {
                        "auto": build_tab_title(store_code, week_of, "Auto"),
                        "review": build_tab_title(store_code, week_of, "Review"),
                    },
                }

            def fake_write_store_artifacts(bundle, output_root):
                payload_path = Path(output_root) / f"{bundle['store_code']}_payload.json"
                payload_path.parent.mkdir(parents=True, exist_ok=True)
                payload_path.write_text("{}", encoding="utf-8")
                return {"sheet_payload": str(payload_path)}

            with mock.patch.object(weekly_sheet, "load_ordering_config", return_value=config), \
                 mock.patch.object(weekly_sheet, "load_store_payloads", side_effect=fake_load_store_payloads), \
                 mock.patch.object(weekly_sheet, "build_ordering_bundle", side_effect=fake_build_ordering_bundle), \
                 mock.patch.object(weekly_sheet, "write_store_artifacts", side_effect=fake_write_store_artifacts):
                exit_code = weekly_sheet.main(["--dry-run", "--as-of-date", "2026-04-10"])

            self.assertEqual(exit_code, 1)

            summary_path = Path(tmpdir) / "2026-04-13" / "run_summary.json"
            summary_rows = json.loads(summary_path.read_text(encoding="utf-8"))

            self.assertEqual([row["store_code"] for row in summary_rows], ["MV", "SV", "NC"])

            mv_row = summary_rows[0]
            self.assertEqual(mv_row["status"], "ok")
            self.assertEqual(mv_row["rows"], 1)

            sv_row = summary_rows[1]
            self.assertEqual(sv_row["status"], "failed")
            self.assertEqual(sv_row["error_type"], "RuntimeError")
            self.assertIn("SV timed out", sv_row["error"])

            nc_row = summary_rows[2]
            self.assertEqual(nc_row["status"], "ok")
            self.assertEqual(nc_row["rows"], 1)


if __name__ == "__main__":
    unittest.main()
