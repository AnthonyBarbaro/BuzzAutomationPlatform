import io
import json
import sys
import tempfile
import unittest
from contextlib import redirect_stdout
from datetime import date
from pathlib import Path

import owner_snapshot as osnap


class OwnerSnapshotLoyaltyTests(unittest.TestCase):
    def test_item_level_loyalty_points_adjustment_detail(self):
        products = [
            {
                "productId": 101,
                "productName": "Blue Dream 3.5g",
                "brandName": "Buzz Brand",
                "category": "Flower",
                "masterCategory": "Flower",
            }
        ]
        transactions = [
            {
                "transactionId": 9001,
                "transactionDateLocalTime": "2026-05-05T10:15:00",
                "completedByUser": "Bud A",
                "customerId": 123,
                "loyaltyEarned": 5,
                "loyaltySpent": 20,
                "items": [
                    {
                        "productId": 101,
                        "transactionItemId": 555,
                        "quantity": 1,
                        "totalPrice": 40,
                        "totalDiscount": 12,
                        "unitCost": 10,
                        "discounts": [
                            {
                                "discountName": "Loyalty Points Adjustment",
                                "discountReason": "Manual loyalty correction",
                                "amount": 7,
                                "transactionItemId": 555,
                                "approvedByUserName": "Manager One",
                                "pointsAddedByUser": "Anthony Barbaro",
                            }
                        ],
                    }
                ],
            }
        ]

        sales = osnap.normalize_api_sales_rows("MV", transactions, products)
        detail = osnap.compute_loyalty_points_adjustment_detail(
            sales,
            date(2026, 5, 5),
            date(2026, 5, 5),
        )

        self.assertEqual(len(detail), 1)
        row = detail.iloc[0]
        self.assertEqual(row["order_id"], "9001")
        self.assertEqual(row["product"], "Buzz Brand | Blue Dream 3.5g")
        self.assertEqual(row["discount_approved_by"], "Manager One")
        self.assertEqual(row["points_added_by"], "Anthony Barbaro")
        self.assertEqual(row["completed_by"], "Bud A")
        self.assertAlmostEqual(row["loyalty_adjustment_discount"], 7.0)

    def test_transaction_level_loyalty_discount_allocates_to_items(self):
        products = [
            {"productId": 1, "productName": "Item One", "brandName": "Brand A", "category": "Edible"},
            {"productId": 2, "productName": "Item Two", "brandName": "Brand B", "category": "Edible"},
        ]
        transactions = [
            {
                "transactionId": 9002,
                "transactionDateLocalTime": "2026-05-05T11:00:00",
                "completedByUser": "Bud B",
                "discounts": [
                    {
                        "discountName": "Loyalty Points Adjustment",
                        "discountReason": "Cart-level loyalty adjustment",
                        "amount": 10,
                        "transactionItemId": 0,
                    }
                ],
                "items": [
                    {"productId": 1, "transactionItemId": 1, "quantity": 1, "totalPrice": 30, "totalDiscount": 0, "unitCost": 5},
                    {"productId": 2, "transactionItemId": 2, "quantity": 1, "totalPrice": 70, "totalDiscount": 0, "unitCost": 10},
                ],
            }
        ]

        sales = osnap.normalize_api_sales_rows("MV", transactions, products)
        detail = osnap.compute_loyalty_points_adjustment_detail(
            sales,
            date(2026, 5, 5),
            date(2026, 5, 5),
        )

        self.assertEqual(len(detail), 2)
        self.assertAlmostEqual(float(detail["loyalty_adjustment_discount"].sum()), 10.0)
        amounts = sorted(round(float(v), 2) for v in detail["loyalty_adjustment_discount"])
        self.assertEqual(amounts, [3.0, 7.0])
        self.assertTrue(detail["api_note"].str.contains("Allocated from transaction-level discount").all())

    def test_discount_detail_report_fills_discount_approved_by(self):
        detail = osnap.pd.DataFrame(
            [
                {
                    "store": "MV",
                    "order_time": "2026-05-06 13:04",
                    "order_id": "233664035",
                    "customer_name": "JOSEPH LESTER GALEA",
                    "product": "Josh Wax | Flower 3.5g | H | LCGZ",
                    "loyalty_adjustment_discount": 0.95,
                    "discount_name": "Loyalty Points Adjustment",
                    "discount_approved_by": osnap.API_FIELD_UNAVAILABLE,
                    "api_note": "Approval/add-point user fields only populate if Dutchie sends them",
                }
            ]
        )
        raw_discount_detail = osnap.pd.DataFrame(
            [
                {
                    "Order Time": "2026-05-06 13:04",
                    "Order ID": "233664035",
                    "Customer Name": "JOSEPH LESTER GALEA",
                    "Product Name": "Josh Wax | Flower 3.5g | H | LCGZ",
                    "Discounted Amount": 0.95,
                    "Discount Name": "Loyalty Points Adjustment",
                    "Discount Approved By": "Anthony Barbaro",
                    "Budtender Name": "Anthony Barbaro",
                }
            ]
        )

        approvals = osnap.normalize_discount_detail_approvals(
            "MV",
            raw_discount_detail,
            date(2026, 5, 1),
            date(2026, 5, 6),
        )
        enriched = osnap.enrich_loyalty_detail_with_discount_approvals(detail, approvals)

        self.assertEqual(enriched.iloc[0]["discount_approved_by"], "Anthony Barbaro")
        self.assertEqual(
            enriched.iloc[0]["discount_approval_source"],
            "Dutchie Backoffice Discount Detail Report",
        )

    def test_backoffice_loyalty_adjustment_report_normalizes_receipt_customer_and_editor(self):
        raw = osnap.pd.DataFrame(
            [
                {
                    "Adjustment Date": "2026-05-06 14:03",
                    "Receipt No": "233663818",
                    "Customer Name": "Jane Customer",
                    "Points Adjustment": 125,
                    "Adjusted By": "Cashier User",
                    "ApprovingManager": "Anthony Barbaro",
                    "Reason": "Manual point edit",
                }
            ]
        )

        detail = osnap.normalize_backoffice_loyalty_adjustments(
            "MV",
            raw,
            date(2026, 5, 6),
            date(2026, 5, 6),
        )

        self.assertEqual(len(detail), 1)
        row = detail.iloc[0]
        self.assertEqual(row["receipt_number"], "233663818")
        self.assertEqual(row["customer_name"], "Jane Customer")
        self.assertEqual(row["adjusted_by"], "Anthony Barbaro")
        self.assertEqual(row["approving_manager"], "Anthony Barbaro")
        self.assertEqual(row["transaction_by"], "Cashier User")
        self.assertAlmostEqual(float(row["points_delta"]), 125.0)
        self.assertEqual(row["source"], "Dutchie Backoffice loyalty adjustment report")

    def test_customer_counts_use_first_seen_in_loaded_window(self):
        raw = osnap.pd.DataFrame(
            [
                {"Order Time": "2026-04-30 10:00", "Customer ID": "old-customer"},
                {"Order Time": "2026-05-06 11:00", "Customer ID": "old-customer"},
                {"Order Time": "2026-05-06 12:00", "Customer ID": "new-today"},
                {"Order Time": "2026-05-03 12:00", "Customer ID": "new-mtd"},
                {"Order Time": "2026-05-06 13:00", "Customer ID": ""},
            ]
        )

        today = osnap.compute_customer_counts(raw, date(2026, 5, 6), date(2026, 5, 6))
        mtd = osnap.compute_customer_counts(raw, date(2026, 5, 1), date(2026, 5, 6))

        self.assertEqual(today, {"new": 1, "total": 2})
        self.assertEqual(mtd, {"new": 2, "total": 3})

    def test_loyalty_maps_filter_to_report_month(self):
        detail = osnap.pd.DataFrame(
            [
                {"order_time": "2026-04-30 10:00", "order_id": "old"},
                {"order_time": "2026-05-01 10:00", "order_id": "may-start"},
                {"order_time": "2026-05-06 10:00", "order_id": "may-report"},
            ]
        )
        register = osnap.pd.DataFrame(
            [
                {"date": "2026-04-30 10:00", "customer_name": "Old"},
                {"date": "2026-05-06 10:00", "customer_name": "May"},
            ]
        )

        details, registers = osnap.filter_loyalty_maps_to_range(
            {"MV": detail},
            {"MV": register},
            date(2026, 5, 1),
            date(2026, 5, 6),
        )

        self.assertEqual(details["MV"]["order_id"].tolist(), ["may-start", "may-report"])
        self.assertEqual(registers["MV"]["customer_name"].tolist(), ["May"])

    def test_loyalty_no_data_marker_can_cover_mtd_from_wide_window(self):
        with tempfile.TemporaryDirectory() as tmp:
            root = Path(tmp)
            marker_dir = root / "2026-03-07_to_2026-05-06"
            marker_dir.mkdir(parents=True)
            marker = marker_dir / "LG - Loyalty Adjustment Report - LEMON GROVE - 2026-03-07_to_2026-05-06.NO_DATA.json"
            marker.write_text(json.dumps({
                "report": "Loyalty Adjustment Report",
                "store_code": "LG",
                "store_name": "Buzz Cannabis - Lemon Grove",
                "start_date": "2026-03-07",
                "end_date": "2026-05-06",
                "status": "no_data",
            }), encoding="utf-8")

            stores = osnap.existing_loyalty_adjustment_no_data_store_codes(
                date(2026, 5, 1),
                date(2026, 5, 6),
                ["LG", "MV"],
                root=root,
                data_start_day=date(2026, 3, 7),
            )

        self.assertEqual(stores, {"LG"})

    def test_discount_detail_no_data_marker_must_cover_requested_window(self):
        with tempfile.TemporaryDirectory() as tmp:
            root = Path(tmp)
            one_day_dir = root / "2026-05-06"
            one_day_dir.mkdir(parents=True)
            one_day_marker = one_day_dir / "LM - Discount Detail Report - LA MESA - 2026-05-06_to_2026-05-06.NO_DATA.json"
            one_day_marker.write_text(json.dumps({
                "report": "Discount Detail Report",
                "store_code": "LM",
                "store_name": "Buzz Cannabis-La Mesa",
                "start_date": "2026-05-06",
                "end_date": "2026-05-06",
                "status": "no_data",
            }), encoding="utf-8")

            self.assertEqual(
                osnap.existing_discount_detail_no_data_store_codes(
                    date(2026, 5, 1),
                    date(2026, 5, 6),
                    ["LM"],
                    root=root,
                ),
                set(),
            )

            mtd_dir = root / "2026-05-01_to_2026-05-06"
            mtd_dir.mkdir(parents=True)
            mtd_marker = mtd_dir / "LM - Discount Detail Report - LA MESA - 2026-05-01_to_2026-05-06.NO_DATA.json"
            mtd_marker.write_text(json.dumps({
                "report": "Discount Detail Report",
                "store_code": "LM",
                "store_name": "Buzz Cannabis-La Mesa",
                "start_date": "2026-05-01",
                "end_date": "2026-05-06",
                "status": "no_data",
            }), encoding="utf-8")

            stores = osnap.existing_discount_detail_no_data_store_codes(
                date(2026, 5, 1),
                date(2026, 5, 6),
                ["LM"],
                root=root,
            )

        self.assertEqual(stores, {"LM"})

    def test_loyalty_authorized_by_prefers_approval_not_processor(self):
        row = {
            "discount_approved_by": "Manager One",
            "edited_or_processed_by": "Cashier One",
            "completed_by": "Budtender One",
        }

        self.assertEqual(osnap.loyalty_authorized_by(row), "Manager One")

    def test_loyalty_authorized_by_does_not_guess_from_processor(self):
        row = {
            "discount_approved_by": osnap.API_FIELD_UNAVAILABLE,
            "edited_or_processed_by": "Cashier One",
            "completed_by": "Budtender One",
        }

        self.assertEqual(osnap.loyalty_authorized_by(row), "API n/a")

    def test_single_dash_email_alias_matches_user_command(self):
        original_argv = sys.argv[:]
        try:
            sys.argv = (
                "owner_snapshot.py --report-day 2026-05-06 --run-export --use-api "
                "--stores MV LG LM WP SV NC -email anthony@buzzcannabis.com "
                "--workers 6 --no-forecast"
            ).split()
            args = osnap.parse_cli_args()
        finally:
            sys.argv = original_argv

        self.assertEqual(args.email, ["anthony@buzzcannabis.com"])
        self.assertEqual(args.stores, ["MV", "LG", "LM", "WP", "SV", "NC"])
        self.assertEqual(args.workers, 6)
        self.assertEqual(args.export_source, "api")

    def test_reporting_filter_excludes_entire_retailer_order(self):
        raw = osnap.pd.DataFrame(
            [
                {
                    "Order Time": "2026-05-07 11:21",
                    "Order ID": "transfer-1",
                    "Customer Type": "Retailer",
                    "Product Name": "Flower A",
                    "Total Inventory Sold": 347,
                    "Gross Sales": 15626.0,
                    "Net Sales": 15626.0,
                    "Discounted Amount": 0.0,
                    "Inventory Cost": 5106.18,
                    "Order Profit": 10519.82,
                },
                {
                    "Order Time": "2026-05-07 11:21",
                    "Order ID": "transfer-1",
                    "Customer Type": "Adult Use",
                    "Product Name": "Flower B",
                    "Total Inventory Sold": 1,
                    "Gross Sales": 50.0,
                    "Net Sales": 50.0,
                    "Discounted Amount": 0.0,
                    "Inventory Cost": 20.0,
                    "Order Profit": 30.0,
                },
                {
                    "Order Time": "2026-05-07 12:30",
                    "Order ID": "sale-1",
                    "Customer Type": "Adult Use",
                    "Product Name": "Regular Sale",
                    "Total Inventory Sold": 2,
                    "Gross Sales": 120.0,
                    "Net Sales": 100.0,
                    "Discounted Amount": 20.0,
                    "Inventory Cost": 40.0,
                    "Order Profit": 60.0,
                },
            ]
        )

        with redirect_stdout(io.StringIO()):
            filtered = osnap.filter_reporting_customer_types(raw, store_code="SV")

        self.assertEqual(filtered["Order ID"].tolist(), ["sale-1"])
        self.assertAlmostEqual(float(filtered["Net Sales"].sum()), 100.0)

    def test_reporting_filter_keeps_data_without_customer_type_column(self):
        raw = osnap.pd.DataFrame(
            [
                {
                    "Order Time": "2026-05-07 12:30",
                    "Order ID": "sale-1",
                    "Net Sales": 100.0,
                }
            ]
        )

        filtered = osnap.filter_reporting_customer_types(raw, store_code="SV")

        self.assertIs(filtered, raw)

    def test_reporting_filter_excludes_api_retailer_type_id(self):
        raw = osnap.pd.DataFrame(
            [
                {
                    "Order Time": "2026-05-07 11:21",
                    "Order ID": "transfer-1",
                    "Customer Type": 5,
                    "Gross Sales": 15626.0,
                    "Net Sales": 15626.0,
                },
                {
                    "Order Time": "2026-05-07 12:30",
                    "Order ID": "sale-1",
                    "Customer Type": 2,
                    "Gross Sales": 120.0,
                    "Net Sales": 100.0,
                },
            ]
        )

        with redirect_stdout(io.StringIO()):
            filtered = osnap.filter_reporting_customer_types(raw, store_code="SV")

        self.assertEqual(filtered["Order ID"].tolist(), ["sale-1"])

    def test_owner_snapshot_defaults_run_api_export_with_six_workers(self):
        original_argv = sys.argv[:]
        try:
            sys.argv = ["owner_snapshot.py"]
            args = osnap.parse_cli_args()
        finally:
            sys.argv = original_argv

        self.assertTrue(args.run_export)
        self.assertEqual(args.export_source, "api")
        self.assertEqual(args.workers, 6)

    def test_santee_is_in_owner_snapshot_default_store_selection(self):
        self.assertIn("SE", osnap._selected_store_codes(None))
        self.assertEqual(osnap._store_name_from_abbr("SE"), "Buzz Cannabis - Santee")
        self.assertEqual(osnap.store_label(osnap._store_name_from_abbr("SE")), "SANTEE")


if __name__ == "__main__":
    unittest.main()
