import json
import tempfile
import unittest
from pathlib import Path

from weekly_aged_flower_inventory_report import (
    DEFAULT_BRANDS_FILE,
    filter_requests,
    load_batch_config,
    load_alias_map,
    recipients_from_requests,
    aliases_for_brand,
)


class WeeklyAgedFlowerInventoryReportTests(unittest.TestCase):
    def test_json_config_loads_brand_aliases_and_recipients(self):
        with tempfile.TemporaryDirectory() as tmpdir:
            path = Path(tmpdir) / "brands.json"
            path.write_text(
                json.dumps(
                    {
                        "recipients": ["manager@example.com"],
                        "age_days": 120,
                        "brands": [
                            {"brand": "710 Labs", "aliases": ["710", "710labs"]},
                            {"brand": "Hashish", "enabled": False},
                        ],
                    }
                ),
                encoding="utf-8",
            )

            config = load_batch_config(path)

        self.assertEqual(config.age_days, 120)
        self.assertEqual(config.recipients, ["manager@example.com"])
        self.assertEqual(len(config.brands), 2)
        self.assertEqual(config.brands[0].brand, "710 Labs")
        self.assertEqual(config.brands[0].aliases, ["710", "710labs"])
        self.assertFalse(config.brands[1].enabled)

    def test_brand_config_style_synonyms_expand_to_separate_brands(self):
        with tempfile.TemporaryDirectory() as tmpdir:
            path = Path(tmpdir) / "brand_config2.json"
            path.write_text(
                json.dumps(
                    {
                        "test_mode": True,
                        "test_email": "test@example.com",
                        "brands": [
                            {
                                "brand_synonyms": ["Camino", "CANN"],
                                "folder_name": "Camino / CANN",
                                "emails": ["donna@example.com"],
                            }
                        ]
                    }
                ),
                encoding="utf-8",
            )

            config = load_batch_config(path)

        self.assertEqual(DEFAULT_BRANDS_FILE.name, "brand_config2.json")
        self.assertEqual(config.recipients, ["test@example.com"])
        self.assertEqual([brand.brand for brand in config.brands], ["Camino", "CANN"])
        self.assertEqual(recipients_from_requests(config.brands), ["donna@example.com"])

    def test_text_config_uses_pipe_separated_aliases(self):
        with tempfile.TemporaryDirectory() as tmpdir:
            path = Path(tmpdir) / "brands.txt"
            path.write_text("# comment\n710 Labs | 710 | 710labs\nHashish\n", encoding="utf-8")

            config = load_batch_config(path)

        self.assertEqual(config.brands[0].brand, "710 Labs")
        self.assertEqual(config.brands[0].aliases, ["710", "710labs"])
        self.assertEqual(config.brands[1].brand, "Hashish")

    def test_alias_file_is_merged_by_normalized_brand(self):
        with tempfile.TemporaryDirectory() as tmpdir:
            path = Path(tmpdir) / "aliases.json"
            path.write_text(json.dumps({"710 Labs": ["710", "710 labs"]}), encoding="utf-8")
            config_path = Path(tmpdir) / "brands.txt"
            config_path.write_text("710 Labs | 710labs\n", encoding="utf-8")

            alias_map = load_alias_map(path)
            request = load_batch_config(config_path).brands[0]

        self.assertEqual(aliases_for_brand(request, alias_map), ["710 Labs", "710labs", "710"])

    def test_filter_requests_matches_aliases(self):
        with tempfile.TemporaryDirectory() as tmpdir:
            path = Path(tmpdir) / "brands.txt"
            path.write_text("710 Labs | 710labs\nHashish\n", encoding="utf-8")
            requests = load_batch_config(path).brands

        selected = filter_requests(requests, {"710labs"})

        self.assertEqual([request.brand for request in selected], ["710 Labs"])


if __name__ == "__main__":
    unittest.main()
