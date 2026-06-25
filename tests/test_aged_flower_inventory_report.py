import unittest

from aged_flower_inventory_report import is_brand_match


class AgedFlowerInventoryReportTests(unittest.TestCase):
    def test_short_brand_alias_does_not_match_longer_brand_prefix(self):
        row = {
            "brandName": "Cannabis Brothers",
            "productName": "Cannabis Brothers | Flower 3.5G | Cookies N Chem",
        }

        self.assertFalse(is_brand_match(row, ["CANN"]))

    def test_short_brand_alias_matches_exact_leading_product_token(self):
        row = {
            "brandName": "",
            "productName": "CANN | Lemon Lavender Social Tonic",
        }

        self.assertTrue(is_brand_match(row, ["CANN"]))

    def test_compacted_alias_matches_exact_leading_product_segment(self):
        row = {
            "brandName": "",
            "productName": "710 Labs | Flower 3.5G | Garlic Cocktail",
        }

        self.assertTrue(is_brand_match(row, ["710labs"]))

    def test_numeric_alias_matches_product_prefix_with_boundary(self):
        row = {
            "brandName": "",
            "productName": "710 | LRO AIO 1G | Upside Down Frown",
        }

        self.assertTrue(is_brand_match(row, ["710"]))


if __name__ == "__main__":
    unittest.main()
