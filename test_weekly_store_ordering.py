import json
import logging
import unittest
from datetime import date, datetime
from pathlib import Path
from zoneinfo import ZoneInfo

from weekly_store_ordering_sheet import (
    apply_exclusion_rules,
    build_ordering_bundle,
    build_tab_title,
    load_ordering_config,
    resolve_week_of,
)
from weekly_store_ordering_sheets import build_sheet_matrix, build_summary_rows, merge_preserved_review_columns


FIXTURE_ROOT = Path(__file__).resolve().parent / "tests" / "fixtures" / "weekly_store_ordering"


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

    def test_filters_samples_and_non_eligible_vendor_rows(self):
        bundle = self.build_bundle()
        auto_df = bundle["auto_df"]

        self.assertEqual(len(auto_df), 4)
        self.assertNotIn("SKU-SAMPLE", auto_df["SKU"].tolist())
        self.assertNotIn("SKU-DRINK", auto_df["SKU"].tolist())
        self.assertEqual(bundle["logs"]["inventory_exclusion_counts"], {"pattern:product": 1})
        self.assertEqual(bundle["summary"]["Brands Included (count)"], 1)
        self.assertEqual(bundle["summary"]["Vendors Included (count)"], 1)
        self.assertAlmostEqual(bundle["summary"]["Total Inventory Value"], 104.0)

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

    def test_computes_sold_windows_sell_through_and_suggested_qty(self):
        bundle = self.build_bundle()
        metrics = bundle["sku_metrics"].set_index("SKU")

        flower = metrics.loc["SKU-FLOWER"]
        self.assertEqual(int(flower["Units Sold 7d"]), 2)
        self.assertEqual(int(flower["Units Sold 14d"]), 4)
        self.assertEqual(int(flower["Units Sold 30d"]), 8)
        self.assertEqual(int(flower["Suggested Order Qty"]), 3)
        self.assertEqual(flower["Needs Order"], "Y")
        self.assertEqual(str(flower["Last Sale Date"]), "2026-04-02")
        self.assertAlmostEqual(float(flower["Sell-Through 30d"]), 8.0 / 9.0, places=4)
        self.assertAlmostEqual(float(flower["Avg Daily Sold 30d"]), 8.0 / 30.0, places=4)
        self.assertAlmostEqual(float(flower["Days of Supply"]), 3.75, places=2)

        gummy = metrics.loc["SKU-GUMMY"]
        self.assertEqual(int(gummy["Units Sold 7d"]), 5)
        self.assertEqual(int(gummy["Units Sold 14d"]), 8)
        self.assertEqual(int(gummy["Units Sold 30d"]), 12)
        self.assertEqual(int(gummy["Suggested Order Qty"]), 2)
        self.assertEqual(gummy["Needs Order"], "Y")

        no_sales = metrics.loc["SKU-NOSALES"]
        self.assertEqual(int(no_sales["Units Sold 30d"]), 0)
        self.assertEqual(int(no_sales["Suggested Order Qty"]), 0)
        self.assertEqual(no_sales["Needs Order"], "N")

        zero_inventory = metrics.loc["SKU-ZEROINV"]
        self.assertEqual(int(zero_inventory["Available"]), 0)
        self.assertEqual(int(zero_inventory["Units Sold 30d"]), 3)
        self.assertEqual(int(zero_inventory["Suggested Order Qty"]), 2)
        self.assertEqual(zero_inventory["Reorder Priority"], "Urgent")

    def test_sorting_surfaces_urgent_and_needs_order_rows_first(self):
        bundle = self.build_bundle()
        auto_df = bundle["auto_df"]

        self.assertEqual(auto_df["SKU"].tolist(), ["SKU-ZEROINV", "SKU-FLOWER", "SKU-GUMMY", "SKU-NOSALES"])
        self.assertEqual(auto_df.iloc[0]["Reorder Priority"], "Urgent")
        self.assertEqual(auto_df.iloc[-1]["Needs Order"], "N")

    def test_store_identity_and_tab_naming_are_stable(self):
        bundle = self.build_bundle()
        metrics = bundle["sku_metrics"]

        self.assertTrue(all(str(value).startswith("MV|") for value in metrics["Row Key"].tolist()))
        self.assertTrue(all(str(value) == "MV" for value in metrics["store_code"].tolist()))
        self.assertEqual(self.week_of.isoformat(), "2026-03-30")
        self.assertEqual(build_tab_title("MV", self.week_of, "Auto"), "MV 2026-03-30 Auto")
        self.assertEqual(build_tab_title("MV", self.week_of, "Review"), "MV 2026-03-30 Review")

    def test_review_columns_are_preserved_on_rerun(self):
        bundle = self.build_bundle()
        review_df = bundle["review_df"]
        existing_values = [
            list(review_df.columns),
            [
                "MV|sku:SKU-FLOWER",
                "Urgent",
                "Y",
                "Vendor Alpha",
                "Brand A",
                "Flower",
                "Brand A | Flower 3.5g | Blue Dream",
                "SKU-FLOWER",
                1,
                12,
                35,
                12,
                2,
                4,
                8,
                0.6667,
                0.8,
                0.8889,
                0.2667,
                3.75,
                3,
                "Below target stock cover based on recent demand.",
                "2026-04-02",
                "2",
                "3",
                "2",
                "Y",
                "AB",
                "CD",
                "Matched",
                "Checked backstock",
                "PO-55",
            ],
        ]

        merged = merge_preserved_review_columns(
            review_df,
            existing_values,
            manual_columns=self.config["review_manual_columns"],
        )
        flower = merged.set_index("SKU").loc["SKU-FLOWER"]
        self.assertEqual(flower["Shelf Count Checked"], "2")
        self.assertEqual(flower["Final Approved Qty"], "2")
        self.assertEqual(flower["Buyer Initials"], "AB")
        self.assertEqual(flower["PO / Vendor Ref"], "PO-55")

    def test_sheet_payload_is_json_serializable(self):
        bundle = self.build_bundle()
        summary_rows = build_summary_rows(bundle["summary"])
        values, header_row_number = build_sheet_matrix(summary_rows, bundle["auto_df"])

        self.assertEqual(len(summary_rows), 3)
        self.assertEqual(header_row_number, 4)
        json.dumps({"values": values})


if __name__ == "__main__":
    unittest.main()
