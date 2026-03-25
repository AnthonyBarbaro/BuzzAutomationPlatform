import tempfile
import unittest

from dutchie_api_reports import (
    canonical_env_map,
    local_date_range_to_utc_strings,
    parse_format_names,
    parse_report_names,
    parse_store_codes,
    resolve_store_keys,
)


class DutchieApiReportsTests(unittest.TestCase):
    def test_parse_store_codes_supports_spaces_and_commas(self):
        self.assertEqual(
            parse_store_codes(["mv, lg", "lm", "MV"]),
            ["MV", "LG", "LM"],
        )

    def test_parse_report_names_deduplicates_values(self):
        self.assertEqual(
            parse_report_names(["sales, catalog", "inventory", "sales"]),
            ["sales", "catalog", "inventory"],
        )

    def test_parse_format_names_deduplicates_values(self):
        self.assertEqual(
            parse_format_names(["json,csv", "json"]),
            ["json", "csv"],
        )

    def test_resolve_store_keys_supports_multiple_env_name_patterns(self):
        env_map = {
            "DUTCHIE_API_KEY_MV": "mv-key",
            "LG": "lg-key",
            "LM_DUTCHIE_API_KEY": "lm-key",
        }
        self.assertEqual(
            resolve_store_keys(env_map, ["MV", "LG", "LM", "SV"]),
            {
                "MV": "mv-key",
                "LG": "lg-key",
                "LM": "lm-key",
            },
        )

    def test_local_date_range_to_utc_strings_respects_pacific_offset(self):
        start_utc, end_utc = local_date_range_to_utc_strings(
            "2026-03-24",
            "2026-03-24",
            "America/Los_Angeles",
        )
        self.assertEqual(start_utc, "2026-03-24T07:00:00Z")
        self.assertEqual(end_utc, "2026-03-25T06:59:59Z")

    def test_canonical_env_map_normalizes_case_and_whitespace(self):
        with tempfile.NamedTemporaryFile("w+", suffix=".env") as handle:
            handle.write(" dutchie_api_key_mv = mv-key \n")
            handle.flush()
            env_map = canonical_env_map(handle.name)
        self.assertEqual(env_map["DUTCHIE_API_KEY_MV"], "mv-key")


if __name__ == "__main__":
    unittest.main()
