import tempfile
import unittest
from datetime import date
from pathlib import Path

import pandas as pd

import brand_meeting_packet as bmp


def _sales_row(day, brand_key, brand_name, net, gross, qty, profit, discount, store="MV"):
    return {
        "_date": day,
        "_product_raw": f"{brand_name} | Test Product",
        "_store_abbr": store,
        "_store_name": store,
        "_net": float(net),
        "_gross": float(gross),
        "_qty": float(qty),
        "_disc_main": float(discount),
        "_disc_loyal": 0.0,
        "_disc_total": float(discount),
        "_tx_key": f"{store}-{brand_key}-{day.isoformat()}-{net}",
        "_kickback_amt": 0.0,
        "_cogs_real": float(net) - float(profit),
        "_cogs_kb": float(net) - float(profit),
        "_profit_real": float(profit),
        "_profit_kb": float(profit),
        "_weight": 0.0,
        "_is_return": False,
        "brand_key": brand_key,
        "brand_name": brand_name,
        "category_normalized": "FLOWER",
        "product_group_display": f"{brand_name} Product",
        "brand_category_key": f"{brand_key}|FLOWER",
        "brand_product_key": f"{brand_key}|product",
    }


def _catalog_row(brand_key, brand_name, units, inventory_value):
    return {
        "brand_key": brand_key,
        "brand_name": brand_name,
        "Available": float(units),
        "Inventory_Value": float(inventory_value),
        "Potential_Revenue": float(inventory_value) * 2.0,
        "Potential_Profit": float(inventory_value) * 0.7,
        "Effective_Price": 20.0,
        "brand_product_key": f"{brand_key}|product",
    }


class OwnerBrandRollupTests(unittest.TestCase):
    def test_safe_math_handles_zero_and_new_prior(self):
        self.assertEqual(bmp.safe_div(10, 0, default=-1), -1)
        self.assertAlmostEqual(bmp.safe_div(10, 4), 2.5)
        self.assertIsNone(bmp.pct_change(10, 0))
        self.assertEqual(bmp.pct_change(0, 0), 0.0)
        self.assertAlmostEqual(bmp.pct_change(125, 100), 0.25)
        self.assertAlmostEqual(bmp.pp_change(0.42, 0.35), 0.07)

    def test_status_action_prioritizes_credit_margin_inventory_and_growth(self):
        targets = {
            "target_margin": 0.35,
            "max_discount_rate": 0.45,
            "max_days_supply": 60,
            "min_sell_through": 0.25,
        }

        status, action = bmp.owner_brand_status_action(
            {
                "net_revenue": 1000,
                "margin_real": 0.40,
                "discount_rate": 0.20,
                "days_supply": 10,
                "sell_through_pct": 0.50,
                "credit_gap": 150,
                "credit_gap_pct_sales": 0.15,
            },
            targets,
        )
        self.assertEqual((status, action), ("Fix", "Collect support"))

        status, action = bmp.owner_brand_status_action(
            {
                "net_revenue": 100,
                "sales_share_pct": 0.005,
                "margin_real": 0.20,
                "discount_rate": 0.50,
                "days_supply": 120,
                "sell_through_pct": 0.05,
                "credit_gap": 0,
                "credit_gap_pct_sales": 0,
            },
            targets,
        )
        self.assertEqual((status, action), ("Exit / Reduce", "Exit or buyback"))

        status, action = bmp.owner_brand_status_action(
            {
                "net_revenue": 5000,
                "sales_vs_prior_pct": 0.20,
                "margin_real": 0.40,
                "discount_rate": 0.25,
                "days_supply": 20,
                "sell_through_pct": 0.50,
                "credit_gap": 0,
                "credit_gap_pct_sales": 0,
            },
            targets,
        )
        self.assertEqual((status, action), ("Grow", "Grow"))

    def test_scorecard_ranks_brands_and_computes_owner_metrics(self):
        start_day = date(2026, 5, 8)
        end_day = date(2026, 5, 14)
        prior_start, _prior_end = bmp.compute_prior_report_window(start_day, end_day)
        sales = pd.DataFrame(
            [
                _sales_row(start_day, "alpha", "Alpha", 1000, 1200, 50, 420, 200),
                _sales_row(start_day, "beta", "Beta", 600, 1000, 30, 120, 400, store="SV"),
                _sales_row(start_day, "gamma", "Gamma", 200, 240, 10, 90, 40, store="LM"),
                _sales_row(prior_start, "alpha", "Alpha", 800, 1000, 40, 320, 200),
                _sales_row(prior_start, "beta", "Beta", 1000, 1200, 50, 350, 200, store="SV"),
            ]
        )
        catalog = pd.DataFrame(
            [
                _catalog_row("alpha", "Alpha", 50, 500),
                _catalog_row("beta", "Beta", 120, 2400),
                _catalog_row("gamma", "Gamma", 2, 80),
            ]
        )
        credit_rows = [
            {
                "brand": "Alpha",
                "canonical_brand": "Alpha",
                "start_date": start_day.isoformat(),
                "end_date": end_day.isoformat(),
                "credit_type": "Invoice credit",
                "basis": "manual_adjustment",
                "expected_amount": 200,
                "received_amount": 0,
                "status": "expected",
                "apply_to_margin": True,
            }
        ]

        scorecard = bmp.build_owner_brand_rollup_scorecard(
            sales,
            catalog,
            start_day,
            end_day,
            top_n=3,
            targets_payload={
                "default_target_margin": 0.35,
                "default_max_discount_rate": 0.45,
                "default_max_days_supply": 60,
                "default_min_sell_through": 0.25,
                "brand_targets": {},
            },
            credit_rows=credit_rows,
        )

        self.assertEqual(scorecard["brand_name"].tolist(), ["Alpha", "Beta", "Gamma"])
        alpha = scorecard.iloc[0]
        self.assertAlmostEqual(alpha["sales_share_pct"], 1000 / 1800)
        self.assertAlmostEqual(alpha["sales_vs_prior_pct"], 0.25)
        self.assertAlmostEqual(alpha["units_vs_prior_pct"], 0.25)
        self.assertAlmostEqual(alpha["margin_gap_pp"], 0.07)
        self.assertAlmostEqual(alpha["credit_gap_pct_sales"], 0.20)
        self.assertEqual(alpha["recommended_action"], "Collect support")
        self.assertEqual(alpha["top_store"], "MV")
        gamma = scorecard.iloc[2]
        self.assertTrue(pd.isna(gamma["sales_vs_prior_pct"]))
        self.assertEqual(bmp._owner_pct_change_label(gamma["sales_vs_prior_pct"], gamma["net_revenue"], gamma["prior_net_revenue"]), "New")
        self.assertEqual(gamma["units_available"], 2)

        no_prior = bmp.build_owner_brand_rollup_scorecard(
            sales,
            catalog,
            start_day,
            end_day,
            top_n=1,
            targets_payload={
                "default_target_margin": 0.35,
                "default_max_discount_rate": 0.45,
                "default_max_days_supply": 60,
                "default_min_sell_through": 0.25,
                "brand_targets": {},
            },
            include_prior_data=False,
        )
        self.assertTrue(pd.isna(no_prior.iloc[0]["prior_net_revenue"]))
        self.assertTrue(pd.isna(no_prior.iloc[0]["sales_vs_prior_pct"]))
        self.assertEqual(
            bmp._owner_pct_change_label(
                no_prior.iloc[0]["sales_vs_prior_pct"],
                no_prior.iloc[0]["net_revenue"],
                no_prior.iloc[0]["prior_net_revenue"],
            ),
            "n/a",
        )

    def test_owner_rollup_pdf_builds_from_scorecard(self):
        start_day = date(2026, 5, 8)
        end_day = date(2026, 5, 14)
        scorecard = pd.DataFrame(
            [
                {
                    "rank": 1,
                    "brand_name": "Alpha",
                    "net_revenue": 1000,
                    "sales_share_pct": 0.625,
                    "sales_vs_prior_pct": 0.25,
                    "prior_net_revenue": 800,
                    "units": 50,
                    "units_vs_prior_pct": 0.25,
                    "prior_units": 40,
                    "margin_real": 0.42,
                    "target_margin": 0.35,
                    "margin_gap_pp": 0.07,
                    "discount_rate": 0.166,
                    "inventory_value": 500,
                    "days_supply": 7,
                    "sell_through_pct": 0.50,
                    "credit_gap": 200,
                    "credit_gap_pct_sales": 0.20,
                    "status": "Fix",
                    "recommended_action": "Collect support",
                    "top_store": "MV",
                    "top_category": "FLOWER",
                    "top_product": "Alpha Product",
                },
                {
                    "rank": 2,
                    "brand_name": "Beta",
                    "net_revenue": 600,
                    "sales_share_pct": 0.375,
                    "sales_vs_prior_pct": -0.40,
                    "prior_net_revenue": 1000,
                    "units": 30,
                    "units_vs_prior_pct": -0.40,
                    "prior_units": 50,
                    "margin_real": 0.20,
                    "target_margin": 0.35,
                    "margin_gap_pp": -0.15,
                    "discount_rate": 0.40,
                    "inventory_value": 2400,
                    "days_supply": 28,
                    "sell_through_pct": 0.20,
                    "credit_gap": 0,
                    "credit_gap_pct_sales": 0,
                    "status": "Fix",
                    "recommended_action": "Fix margin",
                    "top_store": "SV",
                    "top_category": "FLOWER",
                    "top_product": "Beta Product",
                },
            ]
        )
        summary = {
            "total_net_sales": 1600,
            "total_units_sold": 80,
            "average_real_margin": 0.3375,
            "average_discount_rate": 0.25,
            "inventory_value": 2900,
            "days_supply": 25.4,
            "total_credit_gap": 200,
            "brands_reviewed": 2,
            "missing_sales_stores": "None",
            "missing_catalog_stores": "None",
        }

        with tempfile.TemporaryDirectory() as tmpdir:
            out_pdf = Path(tmpdir) / "owner_rollup.pdf"
            bmp.build_owner_brand_rollup_pdf(
                out_pdf,
                start_day,
                end_day,
                ["MV", "SV"],
                scorecard,
                summary,
                include_brand_cards=True,
            )

            self.assertTrue(out_pdf.exists())
            self.assertGreater(out_pdf.stat().st_size, 1000)

    def test_public_generator_writes_owner_rollup_artifacts_without_external_services(self):
        start_day = date(2026, 5, 8)
        end_day = date(2026, 5, 14)
        prior_start, _prior_end = bmp.compute_prior_report_window(start_day, end_day)
        prepared_sales = pd.DataFrame(
            [
                _sales_row(start_day, "alpha", "Alpha", 1000, 1200, 50, 420, 200),
                _sales_row(start_day, "beta", "Beta", 600, 1000, 30, 120, 400, store="SV"),
                _sales_row(prior_start, "alpha", "Alpha", 800, 1000, 40, 320, 200),
                _sales_row(prior_start, "beta", "Beta", 1000, 1200, 50, 350, 200, store="SV"),
            ]
        )
        prepared_catalog = pd.DataFrame(
            [
                _catalog_row("alpha", "Alpha", 50, 500),
                _catalog_row("beta", "Beta", 120, 2400),
            ]
        )

        old_prepare_sales_exports = bmp.prepare_sales_exports
        old_prepare_catalog_exports = bmp.prepare_catalog_exports
        old_load_sales_exports = bmp._load_sales_exports
        old_load_catalog_exports = bmp._load_catalog_exports
        old_prepare_catalog_for_all_brands = bmp.prepare_catalog_for_all_brands
        old_build_catalog_merge_maps = bmp.build_catalog_merge_maps
        old_prepare_sales_df_all_brands = bmp._prepare_sales_df_all_brands
        captured = {}
        try:
            def fake_prepare_sales_exports(**kwargs):
                captured["sales_allow_export"] = kwargs.get("allow_export")
                return {"MV": Path("sales.xlsx")}, [], False

            def fake_prepare_catalog_exports(*args, **kwargs):
                captured["catalog_run_export"] = kwargs.get("run_export")
                return [Path("catalog.csv")], [], False

            bmp.prepare_sales_exports = fake_prepare_sales_exports
            bmp.prepare_catalog_exports = fake_prepare_catalog_exports
            bmp._load_sales_exports = lambda *_args, **_kwargs: {"MV": pd.DataFrame([{"raw": 1}])}
            bmp._load_catalog_exports = lambda *_args, **_kwargs: pd.DataFrame([{"raw": 1}])
            bmp.prepare_catalog_for_all_brands = lambda *_args, **_kwargs: prepared_catalog.copy()
            bmp.build_catalog_merge_maps = lambda *_args, **_kwargs: {}
            bmp._prepare_sales_df_all_brands = lambda *_args, **_kwargs: prepared_sales.copy()

            with tempfile.TemporaryDirectory() as tmpdir:
                artifacts = bmp.generate_owner_brand_rollup_packet(
                    start_date=start_day,
                    end_date=end_day,
                    stores=["MV", "SV"],
                    output_root=Path(tmpdir),
                    top_n=2,
                    email=False,
                    include_creditflow=False,
                    credit_ledger_path=str(Path(tmpdir) / "ledger.json"),
                )

                self.assertTrue(artifacts.pdf_path.exists())
                self.assertTrue(artifacts.scorecard_csv_path.exists())
                self.assertTrue(artifacts.summary_csv_path.exists())
                self.assertEqual(artifacts.brand_count, 2)
                scorecard = pd.read_csv(artifacts.scorecard_csv_path)
                self.assertEqual(scorecard["brand_name"].tolist(), ["Alpha", "Beta"])
                self.assertFalse(captured["sales_allow_export"])
                self.assertFalse(captured["catalog_run_export"])
        finally:
            bmp.prepare_sales_exports = old_prepare_sales_exports
            bmp.prepare_catalog_exports = old_prepare_catalog_exports
            bmp._load_sales_exports = old_load_sales_exports
            bmp._load_catalog_exports = old_load_catalog_exports
            bmp.prepare_catalog_for_all_brands = old_prepare_catalog_for_all_brands
            bmp.build_catalog_merge_maps = old_build_catalog_merge_maps
            bmp._prepare_sales_df_all_brands = old_prepare_sales_df_all_brands


if __name__ == "__main__":
    unittest.main()
