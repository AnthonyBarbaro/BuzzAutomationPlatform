import tempfile
import unittest
from datetime import date
from pathlib import Path

import pandas as pd

from autoJob import (
    DEALS_EXPORT_COLUMNS,
    _iter_sales_api_chunks,
    _normalize_sales_api_export_rows,
    _write_deals_compatible_sales_export,
)


class AutoJobApiSalesTests(unittest.TestCase):
    def test_iter_sales_api_chunks_splits_large_ranges(self):
        chunks = _iter_sales_api_chunks(date(2026, 1, 1), date(2026, 2, 10), max_days=30)
        self.assertEqual(
            chunks,
            [
                (date(2026, 1, 1), date(2026, 1, 30)),
                (date(2026, 1, 31), date(2026, 2, 10)),
            ],
        )

    def test_normalize_sales_api_export_rows_builds_deals_compatible_frame(self):
        transactions_payload = [
            {
                "transactionId": 101,
                "transactionDateLocalTime": "2026-04-01T10:15:00",
                "completedByUser": "Bud A",
                "customerName": "Alice Example",
                "customerTypeName": "Adult Use",
                "items": [
                    {
                        "productId": 1,
                        "quantity": 2,
                        "unitWeight": 3.5,
                        "totalPrice": 40,
                        "totalDiscount": 5,
                        "unitCost": 4.5,
                        "packageId": "PKG-1",
                    }
                ],
            },
            {
                "transactionId": 202,
                "transactionDateLocalTime": "2026-04-01T11:00:00",
                "completedByUser": "Bud B",
                "customerTypeName": "Medical",
                "isReturn": True,
                "items": [
                    {
                        "productId": 1,
                        "quantity": 1,
                        "unitWeight": 3.5,
                        "totalPrice": 20,
                        "totalDiscount": 0,
                        "unitCost": 4.5,
                    }
                ],
            },
        ]
        products_payload = [
            {
                "productId": 1,
                "productName": "Test Flower 3.5g",
                "category": "Flower",
                "vendorName": "Vendor Alpha",
                "producerName": "Producer Alpha",
                "sku": "SKU-1",
                "upc": "UPC-1",
            }
        ]

        frame = _normalize_sales_api_export_rows(transactions_payload, products_payload)

        self.assertEqual(list(frame.columns), DEALS_EXPORT_COLUMNS)
        self.assertEqual(len(frame), 2)

        sale_row = frame.iloc[0]
        self.assertEqual(sale_row["Order ID"], "101")
        self.assertEqual(sale_row["Customer Name"], "Alice Example")
        self.assertEqual(sale_row["Vendor Name"], "Vendor Alpha")
        self.assertEqual(sale_row["Gross Sales"], 40.0)
        self.assertEqual(sale_row["Discounted Amount"], 5.0)
        self.assertEqual(sale_row["Net Sales"], 35.0)
        self.assertEqual(sale_row["Inventory Cost"], 9.0)
        self.assertEqual(sale_row["Order Profit"], 26.0)

        return_row = frame.iloc[1]
        self.assertEqual(return_row["Order ID"], "202")
        self.assertEqual(return_row["Total Inventory Sold"], -1.0)
        self.assertEqual(return_row["Gross Sales"], -20.0)
        self.assertEqual(return_row["Net Sales"], -20.0)
        self.assertEqual(return_row["Inventory Cost"], -4.5)
        self.assertEqual(return_row["Order Profit"], -15.5)
        self.assertFalse(pd.isna(return_row["Return Date"]))

    def test_write_deals_compatible_sales_export_places_header_on_row_five(self):
        frame = pd.DataFrame(
            [
                {
                    "Order ID": "101",
                    "Order Time": pd.Timestamp("2026-04-01T10:15:00"),
                    "Budtender Name": "Bud A",
                    "Customer Name": "Alice Example",
                    "Customer Type": "Adult Use",
                    "Vendor Name": "Vendor Alpha",
                    "Product Name": "Test Flower 3.5g",
                    "Category": "Flower",
                    "Package ID": "PKG-1",
                    "Batch ID": "",
                    "External Package ID": "PKG-1",
                    "Total Inventory Sold": 2.0,
                    "Unit Weight Sold": 3.5,
                    "Total Weight Sold": 7.0,
                    "Gross Sales": 40.0,
                    "Inventory Cost": 9.0,
                    "Discounted Amount": 5.0,
                    "Loyalty as Discount": 0.0,
                    "Net Sales": 35.0,
                    "Return Date": pd.NaT,
                    "UPC GTIN (Canada)": "UPC-1",
                    "Provincial SKU (Canada)": "SKU-1",
                    "Producer": "Producer Alpha",
                    "Order Profit": 26.0,
                }
            ],
            columns=DEALS_EXPORT_COLUMNS,
        )

        with tempfile.TemporaryDirectory() as tmp_dir:
            output_path = Path(tmp_dir) / "salesMV.xlsx"
            _write_deals_compatible_sales_export(frame, output_path)
            loaded = pd.read_excel(output_path, header=4)

        self.assertEqual(list(loaded.columns), DEALS_EXPORT_COLUMNS)
        self.assertEqual(loaded.iloc[0]["Order ID"], 101)
        self.assertEqual(loaded.iloc[0]["Product Name"], "Test Flower 3.5g")


if __name__ == "__main__":
    unittest.main()
