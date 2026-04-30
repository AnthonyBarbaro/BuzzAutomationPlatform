import io
import unittest
from contextlib import redirect_stdout
from datetime import date

import monthly_brand_reports_job as monthly


class MonthlyBrandReportsJobTests(unittest.TestCase):
    def test_previous_month_range_regular_month(self):
        start_day, end_day = monthly.previous_month_range(date(2026, 4, 30))

        self.assertEqual(start_day, date(2026, 3, 1))
        self.assertEqual(end_day, date(2026, 3, 31))

    def test_previous_month_range_crosses_year(self):
        start_day, end_day = monthly.previous_month_range(date(2026, 1, 1))

        self.assertEqual(start_day, date(2025, 12, 1))
        self.assertEqual(end_day, date(2025, 12, 31))

    def test_resolve_monthly_brand_aliases(self):
        brands = monthly.resolve_monthly_brands(
            ["Lime", "kanha", "Raw Garden", "Mary's Medicinals", "dixie", "treesap", "laff"]
        )

        self.assertEqual(
            brands,
            ["Lime", "KANHA", "Raw Garden", "Mary Medical", "Dixie", "TreeSap", "LA FARMS"],
        )

    def test_resolve_monthly_brand_aliases_deduplicates(self):
        brands = monthly.resolve_monthly_brands(["laff", "LA FARMS", "L.A.FF"])

        self.assertEqual(brands, ["LA FARMS"])

    def test_parse_recipients_defaults_to_test_recipient(self):
        self.assertEqual(monthly.parse_recipients(None, production=False), ["anthony@buzzcannabis.com"])

    def test_parse_recipients_defaults_to_donna_in_production(self):
        self.assertEqual(monthly.parse_recipients(None, production=True), ["donna@buzzcannabis.com"])

    def test_main_dry_run_prints_plan_without_side_effects(self):
        output = io.StringIO()
        with redirect_stdout(output):
            exit_code = monthly.main(["--dry-run", "--as-of", "2026-04-30"])

        text = output.getvalue()
        self.assertEqual(exit_code, 0)
        self.assertIn("[RANGE] 2026-03-01 -> 2026-03-31", text)
        self.assertIn("[BRANDS] Lime, KANHA, Raw Garden, Mary Medical, Dixie, TreeSap, LA FARMS", text)
        self.assertIn("[DRY-RUN]", text)


if __name__ == "__main__":
    unittest.main()
