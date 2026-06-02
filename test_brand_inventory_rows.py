import unittest

import pandas as pd

from brand_inventory_rows import (
    consolidate_duplicate_inventory_rows,
    sort_inventory_rows,
)


class BrandInventoryRowTests(unittest.TestCase):
    def test_duplicate_products_are_consolidated_with_summed_available(self):
        rows = pd.DataFrame(
            [
                {
                    "Available": 16,
                    "Product": "Hashish | CC Live Rosin 1g | Margaritaville (W)",
                    "Category": "Concentrate",
                    "Brand": "Hashish",
                    "Cost": 10,
                },
                {
                    "Available": 24,
                    "Product": "Hashish | CC Live Rosin 1g | Margaritaville (W)",
                    "Category": "Concentrate",
                    "Brand": "Hashish",
                    "Cost": 12,
                },
                {
                    "Available": 18,
                    "Product": "Hashish | CC Live Rosin 1g | Ghost Guru (B)",
                    "Category": "Concentrate",
                    "Brand": "Hashish",
                    "Cost": 11,
                },
            ]
        )

        consolidated = consolidate_duplicate_inventory_rows(rows)

        self.assertEqual(len(consolidated), 2)
        margarita = consolidated[
            consolidated["Product"] == "Hashish | CC Live Rosin 1g | Margaritaville (W)"
        ].iloc[0]
        self.assertEqual(margarita["Available"], 40)
        self.assertAlmostEqual(float(margarita["Cost"]), 11.2)

    def test_product_family_sort_keeps_similar_products_together_before_cost(self):
        rows = pd.DataFrame(
            [
                {
                    "Available": 45,
                    "Product": "Hashish | Live Temple Ball 2g | GMO (B)",
                    "Category": "Concentrate",
                    "Brand": "Hashish",
                    "Cost": 1,
                },
                {
                    "Available": 34,
                    "Product": "Hashish | CC Live Rosin 2g | Diamond District (B)",
                    "Category": "Concentrate",
                    "Brand": "Hashish",
                    "Cost": 80,
                },
                {
                    "Available": 26,
                    "Product": "Hashish | CC Live Rosin 1g | Banana Meltshake (B)",
                    "Category": "Concentrate",
                    "Brand": "Hashish",
                    "Cost": 50,
                },
                {
                    "Available": 12,
                    "Product": "Hashish | CC Live Rosin 2g | Garlic Juice (W)",
                    "Category": "Concentrate",
                    "Brand": "Hashish",
                    "Cost": 99,
                },
            ]
        )

        sorted_rows = sort_inventory_rows(rows, include_cost_as_tiebreaker=True)

        self.assertEqual(
            sorted_rows["Product"].tolist(),
            [
                "Hashish | CC Live Rosin 1g | Banana Meltshake (B)",
                "Hashish | CC Live Rosin 2g | Diamond District (B)",
                "Hashish | CC Live Rosin 2g | Garlic Juice (W)",
                "Hashish | Live Temple Ball 2g | GMO (B)",
            ],
        )


if __name__ == "__main__":
    unittest.main()
