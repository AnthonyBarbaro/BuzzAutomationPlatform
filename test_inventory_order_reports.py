import os
import tempfile
import unittest

import pandas as pd

from inventory_order_reports import (
    build_brand_order_sections,
    build_grouped_order_summary,
    order_report_filename,
)


class BuildGroupedOrderSummaryTests(unittest.TestCase):
    def make_detail_df(self):
        return pd.DataFrame(
            [
                {
                    "Category": "Eighths",
                    "Vendor": "Vendor A",
                    "Last Wholesale Cost": 20,
                    "Quantity Sold": 14,
                    "Product Name": "Brand | Flower 3.5g | I | Product 1",
                    "Suggested Order Qty (14d)": 3,
                },
                {
                    "Category": "Eighths",
                    "Vendor": "Vendor B",
                    "Last Wholesale Cost": 20.0,
                    "Quantity Sold": 21,
                    "Product Name": "Brand | Flower 3.5g | S | Product 2",
                    "Suggested Order Qty (14d)": 1,
                },
                {
                    "Category": "Eighths",
                    "Vendor": "Vendor B",
                    "Last Wholesale Cost": 64,
                    "Quantity Sold": 8,
                    "Product Name": "Brand | Flower 3.5g | S | Product 3",
                    "Suggested Order Qty (14d)": 2,
                },
                {
                    "Category": "Eighths",
                    "Vendor": "Vendor A",
                    "Last Wholesale Cost": 1.0,
                    "Quantity Sold": 7,
                    "Product Name": "Brand | Flower 3.5g | H | Promo Product",
                    "Suggested Order Qty (14d)": 4,
                },
                {
                    "Category": "Halves",
                    "Vendor": "Vendor A",
                    "Last Wholesale Cost": 25,
                    "Quantity Sold": 5,
                    "Product Name": "Brand | Flower 14g | H | Half Product",
                    "Suggested Order Qty (14d)": 6,
                },
            ]
        )

    def test_same_category_and_cost_merge_into_one_row(self):
        summary = build_grouped_order_summary(self.make_detail_df(), 14)

        merged = summary[
            (summary["Category"] == "Eighths")
            & (summary["Unit Cost"] == 20)
        ]

        self.assertEqual(len(merged), 1)
        self.assertEqual(int(merged.iloc[0]["Units Needed"]), 4)
        self.assertEqual(int(merged.iloc[0]["Total Quantity Sold"]), 35)

    def test_different_cost_stays_separate(self):
        summary = build_grouped_order_summary(self.make_detail_df(), 14)
        eighth_rows = summary[summary["Category"] == "Eighths"]

        self.assertEqual(len(eighth_rows), 2)
        self.assertEqual(sorted(eighth_rows["Unit Cost"].tolist()), [20, 64])

    def test_low_cost_rows_are_excluded(self):
        summary = build_grouped_order_summary(self.make_detail_df(), 14)

        self.assertNotIn(1.0, summary["Unit Cost"].tolist())
        self.assertEqual(list(summary.columns), ["Category", "Unit Cost", "Units Needed", "Total Quantity Sold"])

    def test_only_actionable_rows_are_counted(self):
        summary = build_grouped_order_summary(
            pd.DataFrame(
                [
                    {
                        "Category": "Prerolls",
                        "Last Wholesale Cost": 12,
                        "Quantity Sold": 6,
                        "Product Name": "Brand | Pre-Rolls 1g (4PK) | Product A",
                        "Suggested Order Qty (7d)": 0,
                    },
                    {
                        "Category": "Prerolls",
                        "Last Wholesale Cost": 12,
                        "Quantity Sold": 3,
                        "Product Name": "Brand | Pre-Rolls 1g (4PK) | Product B",
                        "Suggested Order Qty (7d)": 2,
                    },
                ]
            ),
            7,
        )

        self.assertEqual(len(summary), 1)
        self.assertEqual(int(summary.iloc[0]["Units Needed"]), 2)
        self.assertEqual(int(summary.iloc[0]["Total Quantity Sold"]), 3)


class BuildBrandOrderSectionsTests(unittest.TestCase):
    def write_window(self, directory, days, rows):
        path = os.path.join(directory, order_report_filename("MV", days, extension=".csv"))
        pd.DataFrame(rows).to_csv(path, index=False)

    def test_builds_one_simple_order_sheet_from_all_windows(self):
        base_rows = [
            {
                "Brand": "710 Labs",
                "Category": "Concentrate",
                "Product Name": "710 | LRO Badder 1g | Banana Punch",
                "SKU": "SKU-1",
                "Quantity on Hand": 0,
                "Quantity Sold": 0,
                "Last Wholesale Cost": 22.5,
                "Price": 57,
            },
            {
                "Brand": "710 Labs",
                "Category": "Concentrate",
                "Product Name": "710 | LRO Badder 1g | Do Lato",
                "SKU": "SKU-2",
                "Quantity on Hand": 4,
                "Quantity Sold": 2,
                "Last Wholesale Cost": 22.5,
                "Price": 57,
            },
        ]

        with tempfile.TemporaryDirectory() as tmpdir:
            self.write_window(tmpdir, 7, base_rows)
            rows_14d = [dict(row, **{"Quantity Sold": qty}) for row, qty in zip(base_rows, [2, 2])]
            rows_30d = [dict(row, **{"Quantity Sold": qty}) for row, qty in zip(base_rows, [4, 5])]
            self.write_window(tmpdir, 14, rows_14d)
            self.write_window(tmpdir, 30, rows_30d)

            sections = build_brand_order_sections(tmpdir, ["710 Labs"], store_code="MV")

        self.assertEqual(list(sections), ["Order"])
        order_table = sections["Order"]["table"]
        self.assertEqual(
            list(order_table.columns),
            [
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
        self.assertEqual(len(order_table), 2)
        first_row = order_table.iloc[0]
        self.assertEqual(first_row["Available"], 0)
        self.assertEqual(first_row["Par Level"], 2)
        self.assertEqual(first_row["Units Sold 7d"], 0)
        self.assertEqual(first_row["Units Sold 14d"], 2)
        self.assertEqual(first_row["Units Sold 30d"], 4)


if __name__ == "__main__":
    unittest.main()
