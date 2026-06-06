import tempfile
import unittest
from datetime import date
from pathlib import Path
from unittest.mock import patch

import pandas as pd

import getSalesReport as gsr


class GetSalesReportChunkingTests(unittest.TestCase):
    def test_iter_export_chunks_splits_large_ranges(self):
        chunks = gsr._iter_export_chunks(date(2026, 1, 1), date(2026, 2, 10))
        self.assertEqual(
            chunks,
            [
                (pd.Timestamp("2026-01-01").to_pydatetime(), pd.Timestamp("2026-01-30").to_pydatetime()),
                (pd.Timestamp("2026-01-31").to_pydatetime(), pd.Timestamp("2026-02-10").to_pydatetime()),
            ],
        )

    def test_merge_export_chunk_files_keeps_header_on_row_five(self):
        columns = ["Order ID", "Order Time", "Product Name"]
        frame_one = pd.DataFrame(
            [["101", pd.Timestamp("2026-01-01T09:00:00"), "Alpha"]],
            columns=columns,
        )
        frame_two = pd.DataFrame(
            [["202", pd.Timestamp("2026-01-31T09:00:00"), "Beta"]],
            columns=columns,
        )

        with tempfile.TemporaryDirectory() as tmp_dir:
            tmp_path = Path(tmp_dir)
            part_one = tmp_path / "part_one.xlsx"
            part_two = tmp_path / "part_two.xlsx"
            output_path = tmp_path / "salesMV.xlsx"

            gsr._write_merged_export(frame_one, part_one)
            gsr._write_merged_export(frame_two, part_two)

            merged_rows = gsr._merge_export_chunk_files([part_one, part_two], output_path)
            loaded = pd.read_excel(output_path, header=gsr.EXPORT_HEADER_ROW_INDEX)

        self.assertEqual(merged_rows, 2)
        self.assertEqual(list(loaded.columns), columns)
        self.assertEqual(loaded["Order ID"].astype(str).tolist(), ["101", "202"])

    def test_merge_export_chunk_files_preserves_all_empty_columns(self):
        columns = ["Order ID", "Order Time", "Product Name", "Return Date"]
        frame_one = pd.DataFrame(
            [["101", pd.Timestamp("2026-01-01T09:00:00"), "Alpha", pd.NaT]],
            columns=columns,
        )
        frame_two = pd.DataFrame(
            [["202", pd.Timestamp("2026-01-31T09:00:00"), "Beta", pd.NaT]],
            columns=columns,
        )

        with tempfile.TemporaryDirectory() as tmp_dir:
            tmp_path = Path(tmp_dir)
            part_one = tmp_path / "part_one.xlsx"
            part_two = tmp_path / "part_two.xlsx"
            output_path = tmp_path / "salesMV.xlsx"

            gsr._write_merged_export(frame_one, part_one)
            gsr._write_merged_export(frame_two, part_two)

            merged_rows = gsr._merge_export_chunk_files([part_one, part_two], output_path)
            loaded = pd.read_excel(output_path, header=gsr.EXPORT_HEADER_ROW_INDEX)

        self.assertEqual(merged_rows, 2)
        self.assertEqual(list(loaded.columns), columns)
        self.assertTrue(loaded["Return Date"].isna().all())

    def test_export_store_date_range_merges_chunk_files_into_canonical_store_file(self):
        columns = ["Order ID", "Order Time", "Product Name"]

        def fake_export(current_store, start_date, end_date, target_filename=None, attempts=None):
            path = Path(tmp_dir) / (target_filename or gsr._expected_store_filename(current_store))
            frame = pd.DataFrame(
                [[f"{start_date:%Y%m%d}", pd.Timestamp(start_date), f"Chunk {start_date:%Y-%m-%d}"]],
                columns=columns,
            )
            gsr._write_merged_export(frame, path)
            return True

        with tempfile.TemporaryDirectory() as tmp_dir:
            with patch.object(gsr, "_files_dir", return_value=tmp_dir):
                with patch.object(gsr, "export_store_with_retries", side_effect=fake_export):
                    ok = gsr.export_store_date_range(
                        "Buzz Cannabis - Mission Valley",
                        date(2026, 1, 1),
                        date(2026, 2, 10),
                    )

            final_path = Path(tmp_dir) / "salesMV.xlsx"
            temp_chunks = list(Path(tmp_dir).glob("salesMV__part_*.xlsx"))
            loaded = pd.read_excel(final_path, header=gsr.EXPORT_HEADER_ROW_INDEX)

            self.assertTrue(ok)
            self.assertTrue(final_path.exists())
            self.assertFalse(temp_chunks)
            self.assertEqual(len(loaded), 2)
            self.assertEqual(loaded["Order ID"].astype(str).tolist(), ["20260101", "20260131"])


if __name__ == "__main__":
    unittest.main()
