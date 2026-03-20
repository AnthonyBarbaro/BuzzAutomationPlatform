import unittest

import pandas as pd

from inventory_order_reports import build_grouped_order_summary


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


if __name__ == "__main__":
    unittest.main()
