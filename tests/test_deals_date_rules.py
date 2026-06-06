import textwrap
import unittest

import pandas as pd

from deals import filter_by_rule
from deals_brand_config_sync import _load_brand_criteria_from_csv_text, flatten_brand_criteria


def _sales_rows():
    rows = [
        ("2026-04-13 10:00", "Monday", "The Clear Group Inc.", "Dabwoods Disposable", "Disposables"),
        ("2026-04-17 18:30", "Friday", "The Clear Group Inc.", "Dabwoods Cartridge", "Cartridges"),
        ("2026-04-18 12:15", "Saturday", "The Clear Group Inc.", "Dabwoods Flower", "Flower"),
        ("2026-04-20 15:45", "Monday", "The Clear Group Inc.", "Dabwoods Edible", "Edibles"),
        ("2026-04-20 16:45", "Monday", "The Clear Group Inc.", "Dabwoods Promo Edible", "Edibles"),
    ]
    return pd.DataFrame(
        rows,
        columns=["order time", "day of week", "vendor name", "product name", "category"],
    ).assign(**{"order time": lambda frame: pd.to_datetime(frame["order time"])})


class DealsDateRulesTest(unittest.TestCase):
    def test_filter_by_rule_honors_inclusive_start_and_end_dates(self):
        rule = {
            "vendors": ["The Clear Group Inc."],
            "brands": ["Dabwoods"],
            "days": ["Monday", "Tuesday", "Wednesday", "Thursday", "Friday"],
            "start_date": "2026-04-13",
            "end_date": "2026-04-17",
            "categories": ["Disposables", "Cartridges"],
            "excluded_phrases": ["Promo", "Promos", "Promotional", "Sample"],
        }

        matched = filter_by_rule(_sales_rows(), rule)

        self.assertEqual(matched["product name"].tolist(), ["Dabwoods Disposable", "Dabwoods Cartridge"])

    def test_filter_by_rule_all_products_when_categories_are_omitted(self):
        rule = {
            "vendors": ["The Clear Group Inc."],
            "brands": ["Dabwoods"],
            "days": ["Saturday", "Sunday", "Monday"],
            "start_date": "2026-04-18",
            "end_date": "2026-04-20",
            "excluded_phrases": ["Promo", "Promos", "Promotional", "Sample"],
        }

        matched = filter_by_rule(_sales_rows(), rule)

        self.assertEqual(matched["product name"].tolist(), ["Dabwoods Flower", "Dabwoods Edible"])

    def test_csv_config_round_trips_rule_dates(self):
        config_text = textwrap.dedent(
            """\
            brand,rule_name,vendors,brands,days,start_date,end_date,discount,kickback,categories,stores,include_phrases,excluded_phrases,include_units,enabled,notes
            Dabwoods,Dabwoods 4/13-4/17,The Clear Group Inc.,Dabwoods,Monday;Tuesday;Wednesday;Thursday;Friday,2026-04-13,2026-04-17,50%,30%,Disposables;Cartridges,,,Promo;Promos,False,True,
            Dabwoods,Dabwoods 4/18-4/20,The Clear Group Inc.,Dabwoods,Saturday;Sunday;Monday,2026-04-18,2026-04-20,50%,30%,,,,Promo;Promos,False,True,
            """
        )

        criteria = _load_brand_criteria_from_csv_text(config_text, "test csv")

        self.assertEqual(criteria["Dabwoods"][0]["start_date"], "2026-04-13")
        self.assertEqual(criteria["Dabwoods"][1]["end_date"], "2026-04-20")

        flattened = flatten_brand_criteria(criteria)

        self.assertEqual(flattened.loc[0, "start_date"], "2026-04-13")
        self.assertEqual(flattened.loc[1, "end_date"], "2026-04-20")


if __name__ == "__main__":
    unittest.main()
