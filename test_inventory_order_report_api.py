import json
import tempfile
import unittest
from datetime import date
from pathlib import Path

from getInventoryOrderReport_api import build_inventory_order_report_frame
from inventory_order_reports import build_brand_order_sections, order_report_filename


FIXTURE_ROOT = Path(__file__).resolve().parent / "tests" / "fixtures" / "weekly_store_ordering" / "MV"


class InventoryOrderReportAPITests(unittest.TestCase):
    def load_payloads(self):
        return {
            "inventory": json.loads((FIXTURE_ROOT / "inventory.json").read_text(encoding="utf-8")),
            "products": json.loads((FIXTURE_ROOT / "products.json").read_text(encoding="utf-8")),
            "transactions": json.loads((FIXTURE_ROOT / "transactions.json").read_text(encoding="utf-8")),
        }

    def test_build_inventory_order_report_frame_includes_reorder_metrics(self):
        payloads = self.load_payloads()

        frame = build_inventory_order_report_frame(
            inventory_payload=payloads["inventory"],
            products_payload=payloads["products"],
            transactions_payload=payloads["transactions"],
            store_code="MV",
            window_days=14,
            start_day=date(2026, 3, 23),
            end_day=date(2026, 4, 6),
        )

        self.assertIn("Quantity on Hand", frame.columns)
        self.assertIn("Quantity Sold", frame.columns)
        self.assertIn("Sold Per Day", frame.columns)
        self.assertIn("Days Remaining", frame.columns)
        self.assertIn("Last Wholesale Cost", frame.columns)
        self.assertIn("Price", frame.columns)
        self.assertGreater(len(frame), 0)

        gummy_row = frame.loc[frame["Product Name"] == "Brand A | Gummies 100mg | Mixed Berry"].iloc[0]
        self.assertEqual(float(gummy_row["Quantity on Hand"]), 4.0)
        self.assertEqual(float(gummy_row["Quantity Sold"]), 8.0)
        self.assertAlmostEqual(float(gummy_row["Sold Per Day"]), 8.0 / 14.0, places=6)
        self.assertAlmostEqual(float(gummy_row["Days Remaining"]), 7.0, places=6)
        self.assertEqual(float(gummy_row["Last Wholesale Cost"]), 8.0)

    def test_api_export_files_feed_existing_order_sheet_builder(self):
        payloads = self.load_payloads()

        frame = build_inventory_order_report_frame(
            inventory_payload=payloads["inventory"],
            products_payload=payloads["products"],
            transactions_payload=payloads["transactions"],
            store_code="MV",
            window_days=14,
            start_day=date(2026, 3, 23),
            end_day=date(2026, 4, 6),
        )

        with tempfile.TemporaryDirectory() as tmp:
            report_path = Path(tmp) / order_report_filename("MV", 14, extension=".csv")
            frame.to_csv(report_path, index=False)

            sections = build_brand_order_sections(tmp, ["Brand A"], store_code="MV")

        self.assertIn("Order_14d", sections)
        detail_df = sections["Order_14d"]["detail"]
        self.assertGreater(len(detail_df), 0)
        self.assertIn("Suggested Order Qty (14d)", detail_df.columns)
        self.assertIn("Reorder Priority", detail_df.columns)

        tropical_row = detail_df.loc[detail_df["Product Name"] == "Brand A | Gummies 10pk | Tropical"].iloc[0]
        self.assertEqual(int(tropical_row["Suggested Order Qty (14d)"]), 2)


if __name__ == "__main__":
    unittest.main()
