import unittest
from argparse import Namespace
from pathlib import Path

from getCatalog import _build_output_path, _normalize_inventory_rows


class GetCatalogTests(unittest.TestCase):
    def test_normalize_inventory_rows_matches_expected_export_schema(self):
        payload = [
            {
                "sku": "ABC-123",
                "quantityAvailable": 14,
                "productName": "Blue Dream 3.5g",
                "unitCost": 11.5,
                "unitPrice": 24.0,
                "category": "Flower",
                "brandName": "Buzz Brand",
                "strain": "Blue Dream",
                "vendor": "Buzz Vendor",
                "tags": [{"tagName": "Indoor"}, {"name": "Top Shelf"}],
                "strainType": "Sativa",
            }
        ]

        frame = _normalize_inventory_rows(payload)

        self.assertEqual(
            list(frame.columns),
            [
                "SKU",
                "Available",
                "Product",
                "Cost",
                "Location price",
                "Price",
                "Category",
                "Brand",
                "Strain",
                "Vendor",
                "Tags",
                "Strain Type",
            ],
        )
        self.assertEqual(frame.loc[0, "SKU"], "ABC-123")
        self.assertEqual(frame.loc[0, "Available"], 14.0)
        self.assertEqual(frame.loc[0, "Location price"], 24.0)
        self.assertEqual(frame.loc[0, "Price"], 24.0)
        self.assertEqual(frame.loc[0, "Tags"], "Indoor, Top Shelf")

    def test_build_output_path_uses_store_code_suffix(self):
        path = _build_output_path(Path("/tmp/catalog"), "MV")
        self.assertTrue(str(path).endswith("_MV.csv"))


if __name__ == "__main__":
    unittest.main()
