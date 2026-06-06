import unittest
from datetime import datetime

from kickback_report_link_emailer import (
    ANTHONY_EMAIL,
    ReportMatch,
    _extract_folder_id,
    _file_matches_brand,
    anthony_only_recipients,
    build_email_bodies,
    filter_reports_by_date_ranges,
    parse_date_range_text,
)


class KickbackReportLinkEmailerTests(unittest.TestCase):
    def test_extract_folder_id_supports_url_and_raw_id(self):
        folder_id = "1DeUaZcMM3cE5L0seh0QzfdpA1rvfCov3"
        self.assertEqual(
            _extract_folder_id(f"https://drive.google.com/drive/folders/{folder_id}"),
            folder_id,
        )
        self.assertEqual(_extract_folder_id(folder_id), folder_id)

    def test_file_matches_brand_ignores_spacing_and_case(self):
        self.assertTrue(_file_matches_brand("Pusha_report_2026-04-10_to_2026-04-11.xlsx", "pusha"))
        self.assertTrue(_file_matches_brand("WYLD GoodTide_report_2026-04-10_to_2026-04-11.xlsx", "good tide"))
        self.assertFalse(_file_matches_brand("Raw Garden_report_2026-04-10_to_2026-04-11.xlsx", "pusha"))

    def test_build_email_bodies_contains_share_lines_and_support(self):
        matches = [
            ReportMatch(
                root_key="2026",
                root_label="2026_Kickback",
                folder_path=("04-06 to 04-12",),
                file_name="Pusha_report_2026-04-10_to_2026-04-11.xlsx",
                file_id="abc123",
                web_view_link="https://drive.google.com/file/d/abc123/view",
                start_date=datetime(2026, 4, 10),
                end_date=datetime(2026, 4, 11),
            ),
            ReportMatch(
                root_key="2025",
                root_label="2025_Kickback",
                folder_path=("Sep 29 to Oct 05",),
                file_name="Pusha_report_2025-10-01_to_2025-10-05.xlsx",
                file_id="def456",
                web_view_link="https://drive.google.com/file/d/def456/view",
                start_date=datetime(2025, 10, 1),
                end_date=datetime(2025, 10, 5),
            ),
        ]

        text_body, html_body = build_email_bodies(
            "Pusha",
            matches,
            "Please include anthony@buzzcannabis.com and donna@buzzcannabis.com.",
        )

        self.assertIn('Share "Pusha_report_2026-04-10_to_2026-04-11.xlsx"', text_body)
        self.assertIn("Folder: 04-06 to 04-12", text_body)
        self.assertIn("https://drive.google.com/file/d/abc123/view", text_body)
        self.assertIn("2026_Kickback", html_body)
        self.assertIn("2025_Kickback", html_body)
        self.assertIn("Support", html_body)

    def test_parse_date_range_text_uses_default_year(self):
        self.assertEqual(
            parse_date_range_text("4/26-5/02", 2026),
            (datetime(2026, 4, 26).date(), datetime(2026, 5, 2).date()),
        )

    def test_recipient_policy_forces_anthony_only(self):
        self.assertEqual(
            anthony_only_recipients(["rep@example.com", "donna@buzzcannabis.com"]),
            [ANTHONY_EMAIL],
        )

    def test_date_range_filter_matches_retail_week_to_drive_folder_offset(self):
        matches = [
            ReportMatch(
                root_key="2026",
                root_label="2026_Kickback",
                folder_path=("04-27 to 05-03",),
                file_name="KANHA_report_2026-04-30_to_2026-05-03.xlsx",
                file_id="kanha-week",
                web_view_link="https://drive.google.com/file/d/kanha-week/view",
                start_date=datetime(2026, 4, 30),
                end_date=datetime(2026, 5, 3),
            ),
            ReportMatch(
                root_key="2026",
                root_label="2026_Kickback",
                folder_path=("05-04 to 05-10",),
                file_name="KANHA_report_2026-05-07_to_2026-05-10.xlsx",
                file_id="next-week",
                web_view_link="https://drive.google.com/file/d/next-week/view",
                start_date=datetime(2026, 5, 7),
                end_date=datetime(2026, 5, 10),
            ),
        ]

        selected, missing = filter_reports_by_date_ranges(
            matches,
            [parse_date_range_text("4/26-5/02", 2026)],
        )

        self.assertEqual(missing, [])
        self.assertEqual([match.file_id for match in selected], ["kanha-week"])


if __name__ == "__main__":
    unittest.main()
