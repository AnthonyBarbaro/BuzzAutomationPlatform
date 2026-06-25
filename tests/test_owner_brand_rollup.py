import json
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
    def setUp(self):
        self._old_owner_load_deals_brand_config = bmp._owner_load_deals_brand_config
        bmp._owner_load_deals_brand_config = lambda: ({}, "test empty deals")

    def tearDown(self):
        bmp._owner_load_deals_brand_config = self._old_owner_load_deals_brand_config

    def test_safe_math_handles_zero_and_new_prior(self):
        self.assertEqual(bmp.safe_div(10, 0, default=-1), -1)
        self.assertAlmostEqual(bmp.safe_div(10, 4), 2.5)
        self.assertIsNone(bmp.pct_change(10, 0))
        self.assertEqual(bmp.pct_change(0, 0), 0.0)
        self.assertAlmostEqual(bmp.pct_change(125, 100), 0.25)
        self.assertAlmostEqual(bmp.pp_change(0.42, 0.35), 0.07)

    def test_owner_store_comparison_only_shows_for_multiple_stores(self):
        self.assertFalse(bmp._owner_should_show_store_comparison(["MV"]))
        self.assertFalse(bmp._owner_should_show_store_comparison(["MV", "MV"]))
        self.assertTrue(bmp._owner_should_show_store_comparison(["MV", "LM"]))

    def test_parse_email_recipients_splits_and_dedupes(self):
        self.assertEqual(
            bmp.parse_email_recipients([
                "anthony@buzzcannabis.com, donna@buzzcannabis.com",
                "anthony@buzzcannabis.com; kevin@buzzcannabis.com",
            ]),
            [
                "anthony@buzzcannabis.com",
                "donna@buzzcannabis.com",
                "kevin@buzzcannabis.com",
            ],
        )

    def test_owner_rollup_email_html_contains_scorecard_preview_and_attachments(self):
        summary = {
            "total_net_sales": 1600,
            "top_brands_net_sales": 1600,
            "top_brands_sales_share": 1.0,
            "average_real_margin": 0.35,
            "average_margin_with_assumed_kickback": 0.42,
            "expected_support_amount": 200,
            "total_credit_gap": 50,
            "projected_support_amount": 200,
            "inventory_value": 900,
            "days_supply": 12.5,
            "brands_reviewed": 1,
            "deal_config_source": "test deals",
            "stores": "MV, SV",
        }
        scorecard = pd.DataFrame([{
            "rank": 1,
            "brand_name": "Alpha",
            "net_revenue": 1600,
            "sales_vs_prior_pct": 0.25,
            "prior_net_revenue": 1280,
            "units": 80,
            "margin_real": 0.35,
            "margin_with_assumed_kickback": 0.42,
            "discount_rate": 0.20,
            "expected_credit_amount": 200,
            "credit_gap": 50,
            "days_supply": 12.5,
            "status": "Watch",
            "recommended_action": "Confirm support",
        }])
        with tempfile.TemporaryDirectory() as tmpdir:
            attachment = Path(tmpdir) / "Owner Top Brands Review - MV.pdf"
            attachment.write_bytes(b"pdf")
            body = bmp._owner_rollup_email_html(
                date(2026, 5, 8),
                date(2026, 5, 14),
                summary,
                scorecard=scorecard,
                store_summaries=[{
                    "store": "MV",
                    "total_net_sales": 1000,
                    "brands_reviewed": 1,
                    "average_real_margin": 0.40,
                    "average_margin_with_assumed_kickback": 0.45,
                    "expected_support_amount": 100,
                    "total_credit_gap": 25,
                    "inventory_value": 500,
                    "days_supply": 8.2,
                    "top_brand": "Alpha",
                    "top_brand_sales": 1000,
                    "top_action": "Confirm support",
                }],
                attachments=[attachment],
            )

        self.assertIn("Owner Top Brands Review", body)
        self.assertIn("Store Snapshot", body)
        self.assertIn(">MV<", body)
        self.assertIn("Top Brand Read", body)
        self.assertIn("Alpha", body)
        self.assertIn("Confirm support", body)
        self.assertIn("Owner Top Brands Review - MV.pdf", body)
        self.assertIn("Total Net Sales", body)

    def test_owner_inventory_link_lookup_uses_inventory_other_and_normalizes_brand(self):
        payload = {
            "date": "2026-06-24",
            "day": "Wednesday",
            "generated_at": "2026-06-24T06:00:00",
            "folders": {
                "Claybourne Co.": {"link": "https://example.com/regular-claybourne"},
                "Alpha": {"link": "https://example.com/regular-alpha"},
            },
            "other_folder": {
                "parent_folder_name": "INVENTORY_OTHER",
                "folder_name": "OTHER",
                "brand_folders": {
                    "Claybourne Co": {"link": "https://example.com/other-claybourne"},
                },
            },
        }
        with tempfile.TemporaryDirectory() as tmpdir:
            manifest_path = Path(tmpdir) / "latest.json"
            manifest_path.write_text(json.dumps(payload), encoding="utf-8")

            lookup = bmp.load_owner_inventory_link_lookup(manifest_path)

        claybourne = bmp._owner_inventory_link_for_brand("Claybourne Co.", "claybourne", lookup)
        self.assertIsNotNone(claybourne)
        self.assertEqual(claybourne["link"], "https://example.com/other-claybourne")
        self.assertEqual(claybourne["source"], "INVENTORY_OTHER / OTHER")
        self.assertEqual(claybourne["manifest_date"], "2026-06-24")

        alpha = bmp._owner_inventory_link_for_brand("Alpha", "", lookup)
        self.assertIsNotNone(alpha)
        self.assertEqual(alpha["link"], "https://example.com/regular-alpha")

    def test_owner_week_range_label_shows_full_and_partial_weeks(self):
        self.assertEqual(
            bmp._owner_week_range_label(date(2026, 4, 27), date(2026, 4, 26), date(2026, 6, 24)),
            "Apr 27-May 03",
        )
        self.assertEqual(
            bmp._owner_week_range_label(date(2026, 6, 22), date(2026, 4, 26), date(2026, 6, 24)),
            "Jun 22-Jun 24",
        )
        self.assertEqual(
            bmp._owner_week_range_label(date(2026, 4, 27), date(2026, 4, 26), date(2026, 6, 24), multiline=True),
            "Apr 27-\nMay 03",
        )

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

    def test_scorecard_assumes_30pct_kickback_only_when_discount_is_over_50pct(self):
        start_day = date(2026, 5, 8)
        end_day = date(2026, 5, 14)
        sales = pd.DataFrame(
            [
                _sales_row(start_day, "over", "Over 50 Discount", 600, 1400, 30, 120, 800),
                _sales_row(start_day, "high", "High Discount", 600, 1100, 30, 120, 500),
                _sales_row(start_day, "exact", "Exact 50 Discount", 600, 1200, 30, 120, 600),
                _sales_row(start_day, "normal", "Normal Discount", 700, 1000, 35, 280, 300),
            ]
        )
        catalog = pd.DataFrame(
            [
                _catalog_row("over", "Over 50 Discount", 20, 300),
                _catalog_row("high", "High Discount", 20, 300),
                _catalog_row("exact", "Exact 50 Discount", 20, 300),
                _catalog_row("normal", "Normal Discount", 20, 300),
            ]
        )

        scorecard = bmp.build_owner_brand_rollup_scorecard(
            sales,
            catalog,
            start_day,
            end_day,
            top_n=4,
            targets_payload={
                "default_target_margin": 0.35,
                "default_max_discount_rate": 0.45,
                "default_max_days_supply": 60,
                "default_min_sell_through": 0.25,
                "brand_targets": {},
            },
        )

        over = scorecard[scorecard["brand_name"] == "Over 50 Discount"].iloc[0]
        self.assertAlmostEqual(over["discount_rate"], 800 / 1400)
        self.assertAlmostEqual(over["assumed_kickback_rate"], 0.30)
        self.assertAlmostEqual(over["assumed_kickback_amount"], 180.0)
        self.assertAlmostEqual(over["margin_with_assumed_kickback"], 0.50)
        high = scorecard[scorecard["brand_name"] == "High Discount"].iloc[0]
        self.assertAlmostEqual(high["discount_rate"], 500 / 1100)
        self.assertAlmostEqual(high["assumed_kickback_rate"], 0.0)
        self.assertAlmostEqual(high["assumed_kickback_amount"], 0.0)
        self.assertAlmostEqual(high["margin_with_assumed_kickback"], 0.20)
        exact = scorecard[scorecard["brand_name"] == "Exact 50 Discount"].iloc[0]
        self.assertAlmostEqual(exact["discount_rate"], 0.50)
        self.assertAlmostEqual(exact["assumed_kickback_rate"], 0.0)
        normal = scorecard[scorecard["brand_name"] == "Normal Discount"].iloc[0]
        self.assertAlmostEqual(normal["assumed_kickback_rate"], 0.0)

    def test_scorecard_uses_deals_py_config_for_deal_support(self):
        start_day = date(2026, 5, 8)
        end_day = date(2026, 5, 14)
        bmp._owner_load_deals_brand_config = lambda: ({
            "Claybourne": {
                "days": ["Monday", "Tuesday", "Wednesday", "Thursday", "Friday", "Saturday", "Sunday"],
                "discount": 0.50,
                "kickback": 0.30,
                "brands": ["Claybourne |"],
            }
        }, "test dynamic deals")
        claybourne = _sales_row(start_day, "claybourne", "Claybourne Co.", 1000, 1500, 40, 200, 500)
        claybourne["_product_raw"] = "Claybourne | Gelato Flower"
        sales = pd.DataFrame([claybourne])
        catalog = pd.DataFrame([_catalog_row("claybourne", "Claybourne Co.", 20, 400)])

        scorecard = bmp.build_owner_brand_rollup_scorecard(
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
        )

        row = scorecard.iloc[0]
        self.assertEqual(row["brand_name"], "Claybourne Co.")
        self.assertAlmostEqual(row["assumed_kickback_rate"], 0.0)
        self.assertAlmostEqual(row["deal_expected_credit_amount"], 240.0)
        self.assertAlmostEqual(row["expected_credit_amount"], 240.0)
        self.assertAlmostEqual(row["projected_support_amount"], 240.0)
        self.assertAlmostEqual(row["credit_gap"], 240.0)
        self.assertAlmostEqual(row["margin_with_assumed_kickback"], 0.44)
        self.assertEqual(row["deal_rows"], 1)
        self.assertIn("Claybourne", row["support_source"])
        self.assertEqual(row["deal_config_source"], "test dynamic deals")
        self.assertEqual(row["recommended_action"], "Collect deal support")

    def test_owner_deal_loader_uses_current_deals_refresh(self):
        import deals

        bmp._owner_load_deals_brand_config = self._old_owner_load_deals_brand_config
        old_refresh = deals.refresh_brand_criteria
        old_source = getattr(deals, "brand_config_source", "")
        try:
            def fake_refresh(**_kwargs):
                deals.brand_config_source = "test current config"
                return {
                    "Dynamic Deal": {
                        "days": ["Friday"],
                        "kickback": 0.42,
                        "brands": ["Dynamic |"],
                    }
                }

            deals.refresh_brand_criteria = fake_refresh
            criteria, source = bmp._owner_load_deals_brand_config()
        finally:
            deals.refresh_brand_criteria = old_refresh
            deals.brand_config_source = old_source

        self.assertEqual(source, "test current config")
        self.assertIn("Dynamic Deal", criteria)
        self.assertAlmostEqual(criteria["Dynamic Deal"]["kickback"], 0.42)

    def test_scorecard_accepts_multi_rule_deal_config_lists(self):
        start_day = date(2026, 5, 8)
        end_day = date(2026, 5, 14)
        bmp._owner_load_deals_brand_config = lambda: ({
            "Dynamic Multi": [
                {
                    "rule_name": "Dynamic Friday",
                    "brands": ["Dynamic |"],
                    "days": ["Friday"],
                    "kickback": 0.25,
                },
                {
                    "rule_name": "Dynamic Saturday",
                    "brands": ["Dynamic |"],
                    "days": ["Saturday"],
                    "kickback": 0.10,
                },
            ]
        }, "test multi-rule deals")
        sales_row = _sales_row(start_day, "dynamic", "Dynamic Multi", 1000, 1500, 40, 200, 500)
        sales_row["_product_raw"] = "Dynamic | Gelato Flower"
        scorecard = bmp.build_owner_brand_rollup_scorecard(
            pd.DataFrame([sales_row]),
            pd.DataFrame([_catalog_row("dynamic", "Dynamic Multi", 20, 400)]),
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
        )

        row = scorecard.iloc[0]
        self.assertAlmostEqual(row["deal_expected_credit_amount"], 200.0)
        self.assertEqual(row["deal_config_names"], "Dynamic Multi")
        self.assertEqual(row["deal_rule_names"], "Dynamic Friday")

    def test_direct_brand_deal_match_wins_over_product_name_match(self):
        start_day = date(2026, 5, 7)
        end_day = date(2026, 5, 8)
        bmp._owner_load_deals_brand_config = lambda: ({
            "Ghost": {
                "brands": ["Ghost |"],
                "days": ["Thursday"],
                "kickback": 0.30,
            },
            "Hashish": {
                "brands": ["Hashish"],
                "days": ["Monday", "Tuesday", "Wednesday", "Thursday", "Friday", "Saturday", "Sunday"],
                "kickback": 0.25,
            },
        }, "test hashish deals")
        sale = _sales_row(start_day, "hashish", "Hashish", 1000, 1500, 40, 200, 500)
        sale["_product_raw"] = "Ghost | Hashish 1G"
        scorecard = bmp.build_owner_brand_rollup_scorecard(
            pd.DataFrame([sale]),
            pd.DataFrame([_catalog_row("hashish", "Hashish", 20, 400)]),
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
        )

        row = scorecard.iloc[0]
        self.assertEqual(row["deal_config_names"], "Hashish")
        self.assertEqual(row["support_source"], "Hashish")
        self.assertAlmostEqual(row["deal_expected_credit_amount"], 200.0)

    def test_owner_brand_detail_hides_zero_received_support_card(self):
        base_row = pd.Series({
            "rank": 1,
            "brand_key": "alpha",
            "brand_name": "Alpha",
            "net_revenue": 1000,
            "sales_share_pct": 1.0,
            "sales_vs_prior_pct": 0.0,
            "prior_net_revenue": 1000,
            "units": 10,
            "prior_units": 10,
            "margin_real": 0.20,
            "margin_with_assumed_kickback": 0.40,
            "target_margin": 0.35,
            "expected_credit_amount": 200,
            "received_credit_amount": 0,
            "received_credit_margin": 0.20,
            "discount_rate": 0.50,
            "assumed_kickback_rate": 0.0,
            "inventory_value": 500,
            "days_supply": 10,
            "credit_gap": 200,
            "deal_rows": 1,
            "creditflow_rows": 0,
            "top_store": "MV",
            "top_category": "FLOWER",
            "top_product": "Alpha Product",
            "status": "Fix",
            "recommended_action": "Collect support",
            "support_source": "Alpha",
        })
        labels = []
        old_card = bmp._owner_detail_metric_card
        try:
            def fake_card(label, *args, **kwargs):
                labels.append(label)
                return str(label)

            bmp._owner_detail_metric_card = fake_card
            bmp._owner_brand_detail_page(
                [],
                base_row,
                pd.DataFrame(columns=["brand_key"]),
                ["MV"],
                date(2026, 5, 1),
                date(2026, 5, 7),
                catalog_all_df=pd.DataFrame(columns=["brand_key"]),
            )
            self.assertNotIn("Received Support", labels)

            labels.clear()
            with_received = base_row.copy()
            with_received["received_credit_amount"] = 50
            bmp._owner_brand_detail_page(
                [],
                with_received,
                pd.DataFrame(columns=["brand_key"]),
                ["MV"],
                date(2026, 5, 1),
                date(2026, 5, 7),
                catalog_all_df=pd.DataFrame(columns=["brand_key"]),
            )
            self.assertIn("Received Support", labels)
        finally:
            bmp._owner_detail_metric_card = old_card

    def test_compact_brand_trend_rows_can_hide_tickets_column(self):
        trend = pd.DataFrame([
            {
                "period": "May 2026",
                "period_start": pd.Timestamp("2026-05-01"),
                "net_revenue": 156557,
                "prior_net_revenue": 27315,
                "sales_vs_prior_pct": 4.731,
                "units": 9433,
                "tickets": 1117,
                "margin_real": 0.427,
                "discount_rate": 0.441,
            }
        ])

        compact_rows = bmp._brand_trend_rows(trend, include_tickets=False)
        full_rows = bmp._brand_trend_rows(trend, include_tickets=True)

        self.assertEqual(compact_rows[0], ["Period", "Sales", "+/-", "Units", "Margin", "Disc"])
        self.assertEqual(len(compact_rows[1]), 6)
        self.assertNotIn("1,117", compact_rows[1])
        self.assertEqual(full_rows[0][-1], "Tix")
        self.assertEqual(full_rows[1][-1], "1,117")

    def test_product_inventory_snapshot_groups_same_category_price_and_cost(self):
        start_day = date(2026, 5, 1)
        end_day = date(2026, 5, 30)
        recent = _sales_row(end_day, "alpha", "Alpha", 100, 120, 2, 40, 20)
        recent.update({
            "_product_raw": "Alpha | Gelato Flower",
            "_tx_key": "tx-recent",
            "merge_price_basis": "40.00",
            "merge_cost_basis": "20.00",
        })
        older = _sales_row(date(2026, 5, 20), "alpha", "Alpha", 150, 180, 3, 60, 30)
        older.update({
            "_product_raw": "Alpha | Kush Flower",
            "_tx_key": "tx-older",
            "merge_price_basis": "40.00",
            "merge_cost_basis": "20.00",
        })
        other_price = _sales_row(end_day, "alpha", "Alpha", 50, 60, 1, 20, 10)
        other_price.update({
            "_product_raw": "Alpha | Premium Flower",
            "_tx_key": "tx-other",
            "merge_price_basis": "50.00",
            "merge_cost_basis": "30.00",
        })
        sales = pd.DataFrame([recent, older, other_price])

        catalog = pd.DataFrame([
            {
                "brand_key": "alpha",
                "brand_name": "Alpha",
                "_product_raw": "Alpha | Gelato Flower",
                "_store_abbr": "MV",
                "category_normalized": "FLOWER",
                "Price_Used": 40.0,
                "Cost": 20.0,
                "Available": 5,
                "Inventory_Value": 100.0,
            },
            {
                "brand_key": "alpha",
                "brand_name": "Alpha",
                "_product_raw": "Alpha | Kush Flower",
                "_store_abbr": "MV",
                "category_normalized": "FLOWER",
                "Price_Used": 40.0,
                "Cost": 20.0,
                "Available": 7,
                "Inventory_Value": 140.0,
            },
            {
                "brand_key": "alpha",
                "brand_name": "Alpha",
                "_product_raw": "Alpha | Premium Flower",
                "_store_abbr": "MV",
                "category_normalized": "FLOWER",
                "Price_Used": 50.0,
                "Cost": 30.0,
                "Available": 4,
                "Inventory_Value": 120.0,
            },
        ])

        products, categories = bmp.build_brand_product_inventory_snapshot(
            sales,
            catalog,
            start_day,
            end_day,
            selected_store_codes=["MV"],
        )

        self.assertEqual(len(products), 2)
        grouped = products[products["snapshot_product_key"] == "FLOWER|P40.00|C20.00"].iloc[0]
        self.assertAlmostEqual(grouped["net_revenue"], 250.0)
        self.assertAlmostEqual(grouped["units"], 5.0)
        self.assertAlmostEqual(grouped["units_on_hand"], 12.0)
        self.assertAlmostEqual(grouped["units_sold_7d"], 2.0)
        self.assertAlmostEqual(grouped["units_sold_14d"], 5.0)
        self.assertAlmostEqual(grouped["units_sold_30d"], 5.0)
        self.assertEqual(grouped["merged_count"], 2)
        self.assertIn("Gelato Flower", grouped["product_list"])
        self.assertIn("Kush Flower", grouped["product_list"])
        self.assertEqual(categories.iloc[0]["category"], "FLOWER")
        self.assertAlmostEqual(categories.iloc[0]["units_on_hand"], 16.0)

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
            old_load_inventory_links = bmp.load_owner_inventory_link_lookup
            try:
                bmp.load_owner_inventory_link_lookup = lambda *args, **kwargs: {}
                bmp.build_owner_brand_rollup_pdf(
                    out_pdf,
                    start_day,
                    end_day,
                    ["MV", "SV"],
                    scorecard,
                    summary,
                    include_brand_cards=True,
                )
            finally:
                bmp.load_owner_inventory_link_lookup = old_load_inventory_links

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
        old_send_owner_brand_rollup_email = bmp.send_owner_brand_rollup_email
        old_load_inventory_links = bmp.load_owner_inventory_link_lookup
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
            bmp.load_owner_inventory_link_lookup = lambda *args, **kwargs: {}

            def fake_send_owner_brand_rollup_email(
                pdf_path,
                start,
                end,
                summary,
                to_email,
                extra_attachments=None,
                scorecard=None,
                store_summaries=None,
                logger=None,
            ):
                captured["email_pdf_exists"] = Path(pdf_path).exists()
                captured["email_to"] = to_email
                captured["email_start"] = start
                captured["email_end"] = end
                captured["email_extra_attachments"] = [Path(p) for p in (extra_attachments or [])]
                captured["email_scorecard_brands"] = scorecard["brand_name"].tolist() if scorecard is not None else []
                captured["email_store_summaries"] = list(store_summaries or [])

            bmp.send_owner_brand_rollup_email = fake_send_owner_brand_rollup_email

            with tempfile.TemporaryDirectory() as tmpdir:
                artifacts = bmp.generate_owner_brand_rollup_packet(
                    start_date=start_day,
                    end_date=end_day,
                    stores=["MV", "SV"],
                    output_root=Path(tmpdir),
                    top_n=2,
                    include_creditflow=False,
                    credit_ledger_path=str(Path(tmpdir) / "ledger.json"),
                )

                self.assertTrue(artifacts.pdf_path.exists())
                self.assertTrue(artifacts.scorecard_csv_path.exists())
                self.assertTrue(artifacts.summary_csv_path.exists())
                self.assertEqual(artifacts.brand_count, 2)
                self.assertEqual(len(artifacts.store_pdf_paths), 2)
                for store_pdf in artifacts.store_pdf_paths:
                    self.assertTrue(store_pdf.exists())
                scorecard = pd.read_csv(artifacts.scorecard_csv_path)
                self.assertEqual(scorecard["brand_name"].tolist(), ["Alpha", "Beta"])
                self.assertFalse(captured["sales_allow_export"])
                self.assertFalse(captured["catalog_run_export"])
                self.assertTrue(captured["email_pdf_exists"])
                self.assertEqual(bmp.parse_email_recipients(captured["email_to"]), bmp.OWNER_BRAND_ROLLUP_EMAILS)
                self.assertEqual(captured["email_start"], start_day)
                self.assertEqual(captured["email_end"], end_day)
                self.assertEqual(captured["email_extra_attachments"], artifacts.store_pdf_paths)
                self.assertEqual(captured["email_scorecard_brands"], ["Alpha", "Beta"])
                self.assertEqual([row["store"] for row in captured["email_store_summaries"]], ["MV", "SV"])
                self.assertEqual([row["top_brand"] for row in captured["email_store_summaries"]], ["Alpha", "Beta"])
        finally:
            bmp.prepare_sales_exports = old_prepare_sales_exports
            bmp.prepare_catalog_exports = old_prepare_catalog_exports
            bmp._load_sales_exports = old_load_sales_exports
            bmp._load_catalog_exports = old_load_catalog_exports
            bmp.prepare_catalog_for_all_brands = old_prepare_catalog_for_all_brands
            bmp.build_catalog_merge_maps = old_build_catalog_merge_maps
            bmp._prepare_sales_df_all_brands = old_prepare_sales_df_all_brands
            bmp.send_owner_brand_rollup_email = old_send_owner_brand_rollup_email
            bmp.load_owner_inventory_link_lookup = old_load_inventory_links


if __name__ == "__main__":
    unittest.main()
