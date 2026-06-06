import sys
import tempfile
import unittest
from datetime import date
from pathlib import Path

import pandas as pd

import brand_meeting_packet as bmp


def _product_row(key, name, category, net, units, profit, margin, discount):
    return {
        "product_group_key": key,
        "product_group_display": name,
        "category_normalized": category,
        "size_normalized": "1g",
        "variant_type": "Hybrid",
        "net_revenue": float(net),
        "gross_sales": float(net) + 100.0,
        "units": float(units),
        "profit_real": float(profit),
        "margin_real": float(margin),
        "discount_rate": float(discount),
    }


def _inventory_row(key, name, category, units, value, days=0):
    return {
        "merge_key": key,
        "display_product": name,
        "category_normalized": category,
        "units_available": float(units),
        "inventory_value": float(value),
        "trend_units_per_day_30d": 0.0 if not days else float(units) / float(days),
        "days_of_supply": float(days),
    }


class BrandPacketDashboardTests(unittest.TestCase):
    def test_dashboard_math_helpers_handle_zero_and_missing_values(self):
        self.assertEqual(bmp.safe_div(10, 0, default=-1), -1)
        self.assertIsNone(bmp.pct_change(10, 0))
        self.assertEqual(bmp.pct_change(0, 0), 0.0)
        self.assertAlmostEqual(bmp.pp_change(0.42, 0.35), 0.07)
        self.assertAlmostEqual(bmp.dashboard_days_supply(100, 2), 50.0)
        self.assertTrue(pd.isna(bmp.dashboard_days_supply(100, 0)))
        self.assertAlmostEqual(bmp.dashboard_sell_through(20, 80), 0.20)
        self.assertTrue(pd.isna(bmp.dashboard_sell_through(0, 0)))
        self.assertAlmostEqual(bmp.dashboard_credit_gap_pct_sales(250, 1000), 0.25)
        self.assertEqual(bmp._format_delta(float("nan"), current=10, prior=0), "New")
        self.assertEqual(bmp._format_delta(float("nan"), current=10, prior=float("nan")), "n/a")

    def test_product_and_store_action_classification(self):
        action, risk, _recommendation = bmp._classify_product_action(
            {
                "units_per_day": 2.0,
                "margin_pct": 0.42,
                "discount_pct": 0.10,
                "inventory_units": 30,
                "inventory_value": 600,
                "days_supply": 15,
                "sell_through_pct": 0.60,
                "stores_selling": 5,
            },
            target_margin=0.35,
            selected_store_count=6,
        )
        self.assertEqual((action, risk), ("Reorder / Expand", "Low"))

        action, risk, _recommendation = bmp._classify_product_action(
            {
                "units_per_day": 0.01,
                "sales_vs_prior_pct": -0.50,
                "margin_pct": 0.18,
                "discount_pct": 0.55,
                "inventory_units": 80,
                "inventory_value": 1600,
                "days_supply": 240,
                "sell_through_pct": 0.04,
                "stores_selling": 1,
            },
            target_margin=0.35,
            selected_store_count=6,
        )
        self.assertEqual((action, risk), ("Cut / Buyback", "Critical"))

        self.assertEqual(
            bmp._classify_store_action(
                {
                    "margin_pct": 0.20,
                    "discount_pct": 0.25,
                    "days_supply": 20,
                    "sell_through_pct": 0.40,
                    "sales_vs_prior_pct": 0.10,
                },
                target_margin=0.35,
            ),
            "Fix Margin",
        )
        self.assertEqual(
            bmp._classify_store_action(
                {
                    "margin_pct": 0.40,
                    "discount_pct": 0.10,
                    "days_supply": 15,
                    "sell_through_pct": 0.55,
                    "sales_vs_prior_pct": 0.25,
                    "units_vs_prior_pct": 0.20,
                },
                target_margin=0.35,
            ),
            "Grow",
        )

    def test_decision_board_sorts_priority_and_survives_missing_inventory_and_prior(self):
        products = pd.DataFrame(
            [
                _product_row("fast", "Fast Vape | Blue Dream", "VAPE", 1200, 80, 520, 0.43, 0.12),
                _product_row("stuck", "Old Flower | Kush", "FLOWER", 50, 1, 5, 0.10, 0.55),
                _product_row("missing-inv", "Edible Single", "EDIBLE", 0, 0, 0, 0.0, 0.0),
            ]
        )
        inventory = pd.DataFrame(
            [
                _inventory_row("fast", "Fast Vape | Blue Dream", "VAPE", 20, 400, days=10),
                _inventory_row("stuck", "Old Flower | Kush", "FLOWER", 100, 2000, days=250),
            ]
        )

        board = bmp.build_dashboard_product_decision_board(
            products,
            inventory,
            prior_product=pd.DataFrame(),
            report_days=14,
            selected_store_count=6,
            store_count_map={"fast": 4, "stuck": 1},
            max_products=10,
            target_margin=0.35,
            include_prior_data=False,
        )

        self.assertEqual(board.iloc[0]["action"], "Cut / Buyback")
        self.assertIn("missing-inv", board["product_key"].tolist())
        self.assertTrue(pd.isna(board.loc[board["product_key"] == "fast", "sales_vs_prior_pct"].iloc[0]))
        self.assertLessEqual(len(board), 10)
        self.assertLessEqual(len(bmp._shorten_product_name("Very Long Product Name " * 5, 24)), 24)

    def test_dashboard_pdf_and_cache_build_from_synthetic_payload(self):
        products = pd.DataFrame(
            [
                _product_row("fast", "Fast Vape | Blue Dream", "VAPE", 1200, 80, 520, 0.43, 0.12),
                _product_row("stuck", "Old Flower | Kush", "FLOWER", 50, 1, 5, 0.10, 0.55),
            ]
        )
        inventory = pd.DataFrame(
            [
                _inventory_row("fast", "Fast Vape | Blue Dream", "VAPE", 20, 400, days=10),
                _inventory_row("stuck", "Old Flower | Kush", "FLOWER", 100, 2000, days=250),
            ]
        )
        window_metrics = {
            "report": {"net_revenue": 1250, "items": 81, "profit_real": 525, "margin_real": 0.42, "discount_rate": 0.16},
            "prior_report": {"net_revenue": 1000, "items": 70, "profit_real": 400, "margin_real": 0.40},
        }
        dashboard = bmp.build_dashboard_packet_data(
            product_60=products,
            inv_products=inventory,
            prior_product=pd.DataFrame([_product_row("fast", "Fast Vape | Blue Dream", "VAPE", 1000, 70, 400, 0.40, 0.10)]),
            report_df=pd.DataFrame(),
            report_days=14,
            selected_store_codes=["MV", "LM"],
            store_60=pd.DataFrame(),
            store_sales_packets={},
            inv_store=pd.DataFrame(),
            category_60=pd.DataFrame([{"category_normalized": "VAPE", "net_revenue": 1200, "items": 80, "margin_real": 0.43, "discount_rate": 0.12}]),
            inv_category=pd.DataFrame([{"category_normalized": "VAPE", "inventory_value": 400, "units_available": 20}]),
            window_metrics=window_metrics,
            inv_overview={"inventory_value": 2400, "units": 120, "days_of_supply": 21},
            credit_summary={"credit_gap": 300, "manual_expected_credit": 500, "manual_received_credit": 200},
            target_margin=0.35,
            max_products=10,
            include_prior_data=True,
            meeting_ask="Ask for $300 credit support.",
        )

        with tempfile.TemporaryDirectory() as tmpdir:
            tmp = Path(tmpdir)
            bmp.write_dashboard_cache(tmp, dashboard)
            self.assertTrue((tmp / "dashboard_product_decision_board.csv").exists())

            pdf_path = tmp / "dashboard.pdf"
            bmp.build_brand_packet_dashboard_pdf(
                out_pdf=pdf_path,
                brand="Synthetic Brand",
                start_day=date(2026, 5, 1),
                end_day=date(2026, 5, 14),
                selected_store_codes=["MV", "LM"],
                options=bmp.PacketOptions(packet_layout="dashboard", include_product_appendix=False, max_products=10),
                dashboard_data=dashboard,
                credit_reconciliation=pd.DataFrame(),
            )
            self.assertTrue(pdf_path.exists())
            self.assertGreater(pdf_path.stat().st_size, 1000)

    def test_cli_dashboard_options_are_passed_to_generator(self):
        old_argv = sys.argv[:]
        old_generate = bmp.generate_brand_meeting_packet
        captured = {}
        try:
            def fake_generate_brand_meeting_packet(*, brand, start_day, end_day, selected_store_codes, output_root, options, logger=None):
                captured["brand"] = brand
                captured["stores"] = list(selected_store_codes)
                captured["options"] = options
                return None

            bmp.generate_brand_meeting_packet = fake_generate_brand_meeting_packet
            sys.argv = [
                "brand_meeting_packet.py",
                "--brand", "Synthetic Brand",
                "--start-date", "2026-05-01",
                "--end-date", "2026-05-14",
                "--stores", "MV,LM",
                "--no-email",
                "--dashboard",
                "--no-appendix",
                "--max-products", "7",
                "--max-store-products", "3",
            ]
            bmp.main()

            options = captured["options"]
            self.assertEqual(captured["brand"], "Synthetic Brand")
            self.assertEqual(captured["stores"], ["MV", "LM"])
            self.assertEqual(options.packet_layout, "dashboard")
            self.assertFalse(options.include_product_appendix)
            self.assertFalse(options.email_results)
            self.assertEqual(options.max_products, 7)
            self.assertEqual(options.max_store_products, 3)
        finally:
            bmp.generate_brand_meeting_packet = old_generate
            sys.argv = old_argv


if __name__ == "__main__":
    unittest.main()
