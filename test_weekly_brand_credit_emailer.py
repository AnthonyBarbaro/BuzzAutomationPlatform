import tempfile
import unittest
from datetime import date
from pathlib import Path

from weekly_brand_credit_emailer import (
    WEEKLY_BRAND_EMAILS,
    find_latest_report,
    get_previous_monday_sunday,
    inventory_files_for_brand,
    should_include_brand,
    week_key_for_range,
)


class WeeklyBrandCreditEmailerTests(unittest.TestCase):
    def test_previous_week_range_from_monday(self):
        start_day, end_day = get_previous_monday_sunday(date(2026, 5, 18))

        self.assertEqual(start_day, date(2026, 5, 11))
        self.assertEqual(end_day, date(2026, 5, 17))
        self.assertEqual(week_key_for_range(start_day, end_day), "2026-05-11_to_2026-05-17")

    def test_find_latest_report_uses_brand_aliases(self):
        with tempfile.TemporaryDirectory() as tmp_dir:
            root = Path(tmp_dir)
            (root / "Treesap_report_2026-05-04_to_2026-05-10.xlsx").write_text("", encoding="utf-8")
            (root / "TreeSap_report_2026-05-11_to_2026-05-17.xlsx").write_text("", encoding="utf-8")

            report = find_latest_report(str(root), ["TreeSap", "Treesap"])

        self.assertIsNotNone(report)
        self.assertEqual(report["filename"], "TreeSap_report_2026-05-11_to_2026-05-17.xlsx")
        self.assertEqual(report["start_date"], "2026-05-11")
        self.assertEqual(report["end_date"], "2026-05-17")

    def test_treesap_selection_and_inventory_aliases(self):
        treesap_cfg = next(cfg for cfg in WEEKLY_BRAND_EMAILS if cfg["brand"] == "TreeSap")
        brand_map = {
            "treesap": ["/tmp/treesap_mv.xlsx", "/tmp/treesap_lm.xlsx"],
            "hashish": ["/tmp/hashish_mv.xlsx"],
        }

        self.assertTrue(should_include_brand(treesap_cfg, ["Treesap"]))
        self.assertTrue(should_include_brand(treesap_cfg, ["TreeSap"]))
        self.assertEqual(
            inventory_files_for_brand(treesap_cfg, brand_map),
            ["/tmp/treesap_lm.xlsx", "/tmp/treesap_mv.xlsx"],
        )


if __name__ == "__main__":
    unittest.main()
