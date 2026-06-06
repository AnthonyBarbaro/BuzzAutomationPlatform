import sys
import tempfile
import unittest
from datetime import date, timedelta
from pathlib import Path
from unittest.mock import patch

import owner_snapshot as osnap


def _daily_frame(start: date, days: int, base_net: float = 1000.0) -> osnap.pd.DataFrame:
    rows = []
    for i in range(days):
        d = start + timedelta(days=i)
        net = float(base_net + (i * 10.0))
        tickets = 10.0 + (i % 3)
        profit = net * 0.40
        discount = net * 0.08
        gross = net + discount
        rows.append({
            "date": d,
            "net_revenue": net,
            "gross_sales": gross,
            "tickets": tickets,
            "items": tickets * 2,
            "discount": discount,
            "discount_main": discount,
            "loyalty_discount": 0.0,
            "discount_rate": discount / gross,
            "basket": net / tickets,
            "items_per_ticket": 2.0,
            "net_price_per_item": net / (tickets * 2),
            "profit": profit,
            "profit_real": profit,
            "margin": profit / net,
            "margin_real": profit / net,
            "cogs": net - profit,
            "cogs_real": net - profit,
            "returns_net": 0.0,
            "returns_tickets": 0.0,
            "weight_sold": 0.0,
        })
    return osnap.pd.DataFrame(rows)


class OwnerSnapshotForecastTests(unittest.TestCase):
    def setUp(self):
        self.tmp = tempfile.TemporaryDirectory()
        self.old_paths = {
            "FORECAST_DIR": osnap.FORECAST_DIR,
            "FORECAST_HISTORY_PATH": osnap.FORECAST_HISTORY_PATH,
            "FORECAST_MODEL_PATH": osnap.FORECAST_MODEL_PATH,
            "FORECAST_META_PATH": osnap.FORECAST_META_PATH,
            "FORECAST_BACKTEST_DIR": osnap.FORECAST_BACKTEST_DIR,
            "FORECAST_BACKTEST_DETAIL_PATH": osnap.FORECAST_BACKTEST_DETAIL_PATH,
            "FORECAST_BACKTEST_SUMMARY_PATH": osnap.FORECAST_BACKTEST_SUMMARY_PATH,
        }
        root = Path(self.tmp.name) / "forecast"
        osnap.FORECAST_DIR = root
        osnap.FORECAST_HISTORY_PATH = root / "daily_history.csv.gz"
        osnap.FORECAST_MODEL_PATH = root / "month_end_forecaster.joblib"
        osnap.FORECAST_META_PATH = root / "month_end_forecaster_meta.json"
        osnap.FORECAST_BACKTEST_DIR = root / "backtests"
        osnap.FORECAST_BACKTEST_DETAIL_PATH = osnap.FORECAST_BACKTEST_DIR / "latest_detail.csv"
        osnap.FORECAST_BACKTEST_SUMMARY_PATH = osnap.FORECAST_BACKTEST_DIR / "latest_summary.csv"

    def tearDown(self):
        for name, value in self.old_paths.items():
            setattr(osnap, name, value)
        self.tmp.cleanup()

    def test_partial_store_run_does_not_upsert_all_history(self):
        hist = osnap.forecast_upsert_history(
            {"MV": _daily_frame(date(2026, 5, 1), 2)},
            include_all_stores=False,
        )

        self.assertEqual(set(hist["store_code"].astype(str)), {"MV"})

    def test_full_store_run_upserts_all_history(self):
        daily_map = {
            abbr: _daily_frame(date(2026, 5, 1), 1, base_net=1000.0 + idx)
            for idx, abbr in enumerate(osnap._configured_store_codes())
        }
        hist = osnap.forecast_upsert_history(daily_map, include_all_stores=True)

        self.assertIn("ALL", set(hist["store_code"].astype(str)))
        all_net = float(hist[hist["store_code"] == "ALL"]["net_revenue"].sum())
        store_net = float(hist[hist["store_code"] != "ALL"]["net_revenue"].sum())
        self.assertAlmostEqual(all_net, store_net)

    def test_robust_weekday_average_ignores_far_outlier(self):
        self.assertAlmostEqual(
            osnap._robust_forecast_average([12000.0, 12000.0, 20000.0, 12000.0]),
            12000.0,
        )

    def test_weekday_calendar_prediction_counts_remaining_month_weekdays(self):
        net_by_wd = {
            0: 11000.0,
            1: 12000.0,
            2: 13000.0,
            3: 17000.0,
            4: 15000.0,
            5: 18000.0,
            6: 14000.0,
        }
        rows = []
        cur = date(2026, 3, 1)
        while cur <= date(2026, 5, 10):
            net = net_by_wd[cur.weekday()]
            if cur == date(2026, 4, 7):  # Tuesday spike; normal Tuesday is 12k
                net = 20000.0
            rows.append({
                "date": cur,
                "net_revenue": net,
                "gross_sales": net * 1.08,
                "tickets": 100.0,
                "items": 200.0,
                "discount": net * 0.08,
                "profit": net * 0.40,
            })
            cur += timedelta(days=1)

        hist = osnap._daily_to_history_rows("MV", osnap.pd.DataFrame(rows))
        fc = osnap._weekday_profile_baseline(hist, "MV", date(2026, 5, 10))

        may_mtd = sum(
            row["net_revenue"]
            for row in rows
            if date(2026, 5, 1) <= row["date"] <= date(2026, 5, 10)
        )
        expected_remaining = 0.0
        cur = date(2026, 5, 11)
        while cur <= date(2026, 5, 31):
            expected_remaining += net_by_wd[cur.weekday()]
            cur += timedelta(days=1)

        self.assertAlmostEqual(fc["net_pred"], may_mtd + expected_remaining, delta=1.0)

    def test_smart_baseline_scales_current_month_run_rate(self):
        rows = []
        for start, days, daily_net in [
            (date(2026, 3, 1), 31, 1000.0),
            (date(2026, 4, 1), 30, 1000.0),
            (date(2026, 5, 1), 10, 2000.0),
        ]:
            for i in range(days):
                d = start + timedelta(days=i)
                rows.append({
                    "date": d,
                    "net_revenue": daily_net,
                    "gross_sales": daily_net * 1.08,
                    "tickets": 100.0,
                    "items": 200.0,
                    "discount": daily_net * 0.08,
                    "profit": daily_net * 0.40,
                })

        hist = osnap._daily_to_history_rows("MV", osnap.pd.DataFrame(rows))
        fc = osnap._smart_month_end_baseline(hist, "MV", date(2026, 5, 10))

        self.assertGreater(fc["net_pred"], 20000.0)
        self.assertGreater(fc["net_pred"], 55000.0)
        self.assertLess(fc["net_pred"], 63000.0)

    def test_partial_pipeline_does_not_predict_all(self):
        bundle = osnap.run_month_end_forecast_pipeline(
            {"MV": _daily_frame(date(2026, 5, 1), 10)},
            as_of=date(2026, 5, 10),
            selected_store_codes=["MV"],
        )

        self.assertFalse(bundle["include_all"])
        self.assertIn("MV", bundle["stores"])
        self.assertNotIn("ALL", bundle["stores"])

    def test_ml_gate_requires_ten_percent_net_error_improvement(self):
        summary = osnap.pd.DataFrame([
            {
                "scope": "pooled",
                "store_code": osnap.FORECAST_POOLED_STORE_CODE,
                "model_key": "baseline",
                "model": "baseline",
                "metric": "net",
                "asof_bucket": "D08-D14",
                "n": 10,
                "mae": 0.0,
                "mape": 0.10,
                "median_ape": 0.10,
                "actual_to_pred_p10": 0.95,
                "actual_to_pred_p90": 1.05,
            },
            {
                "scope": "pooled",
                "store_code": osnap.FORECAST_POOLED_STORE_CODE,
                "model_key": "ml",
                "model": "ml",
                "metric": "net",
                "asof_bucket": "D08-D14",
                "n": 10,
                "mae": 0.0,
                "mape": 0.091,
                "median_ape": 0.091,
                "actual_to_pred_p10": 0.95,
                "actual_to_pred_p90": 1.05,
            },
        ])

        self.assertFalse(osnap.forecast_ml_beats_baseline(summary, "MV", date(2026, 5, 10)))
        summary.loc[summary["model_key"] == "ml", "mape"] = 0.089
        self.assertTrue(osnap.forecast_ml_beats_baseline(summary, "MV", date(2026, 5, 10)))

    def test_single_point_forecast_keeps_backtest_error_and_clamps_to_mtd(self):
        fc = {
            "store_code": "MV",
            "as_of": "2026-05-10",
            "asof_bucket": "D08-D14",
            "model_key": "baseline",
            "net_pred": 100.0,
            "mtd_net": 95.0,
            "profit_pred": 40.0,
            "mtd_profit": 20.0,
            "tickets_pred": 20.0,
            "mtd_tickets": 10.0,
            "discount_pred": 5.0,
            "mtd_discount": 2.0,
        }
        summary = osnap.pd.DataFrame([
            {
                "scope": "pooled",
                "store_code": osnap.FORECAST_POOLED_STORE_CODE,
                "model_key": "baseline",
                "model": "baseline",
                "metric": metric,
                "asof_bucket": "D08-D14",
                "n": 10,
                "mae": 0.0,
                "mape": 0.10,
                "median_ape": 0.10,
                "actual_to_pred_p10": 0.80,
                "actual_to_pred_p90": 1.20,
            }
            for metric in ["net", "profit", "tickets", "discount"]
        ])

        ranged = osnap.apply_backtest_forecast_range(fc, summary)

        self.assertEqual(ranged["net_pred"], 100.0)
        self.assertEqual(ranged["net_pred_low"], 100.0)
        self.assertEqual(ranged["net_pred_high"], 100.0)
        self.assertEqual(ranged["remaining_net_low"], 5.0)
        self.assertEqual(ranged["remaining_net_high"], 5.0)
        self.assertEqual(ranged["range_source"], "single_point")
        self.assertAlmostEqual(ranged["backtest_net_mape"], 0.10)

    def test_forecast_backtest_cli_writes_csv_outputs(self):
        hist = osnap._daily_to_history_rows("MV", _daily_frame(date(2026, 1, 1), 31))
        osnap._save_history(hist)

        result = osnap.run_forecast_backtest_cli(include_ml=False)

        self.assertTrue(result["detail_path"].exists())
        self.assertTrue(result["summary_path"].exists())
        self.assertTrue(osnap.FORECAST_BACKTEST_DETAIL_PATH.exists())
        self.assertTrue(osnap.FORECAST_BACKTEST_SUMMARY_PATH.exists())
        self.assertFalse(result["detail"].empty)
        self.assertFalse(result["summary"].empty)

    def test_forecast_backtest_main_exits_before_snapshot_generation(self):
        original_argv = sys.argv[:]
        try:
            sys.argv = ["owner_snapshot.py", "--forecast-backtest"]
            with patch.object(osnap, "run_forecast_backtest_cli") as backtest, patch.object(osnap, "setup_fonts") as setup_fonts:
                osnap.main()
        finally:
            sys.argv = original_argv

        backtest.assert_called_once()
        setup_fonts.assert_not_called()


if __name__ == "__main__":
    unittest.main()
