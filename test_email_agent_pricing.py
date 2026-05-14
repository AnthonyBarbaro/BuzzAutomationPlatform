import unittest

from email_agent import EmailRecord, build_pricing_analysis_draft, extract_cost_retail_lines


TRADITIONAL_EMAIL = """traditional

8ths - 14 (sell price $55)
15g - 38 (130)
non infused preroll - $3 (8)
infused preroll - $6 (13)

asteroids

8ths - 4.50 (20)
sugar - $6 (20)

with 30% off
margin at 45%
"""


def make_email_record(body: str) -> EmailRecord:
    return EmailRecord(
        message_id="msg-1",
        thread_id="thread-1",
        gmail_message_id_header="<msg-1@example.com>",
        references_header="",
        sender="Donna Isho <donna@buzzcannabis.com>",
        sender_email="donna@buzzcannabis.com",
        subject="Traditional",
        date="Thu, 14 May 2026 11:37:25 -0700",
        snippet="",
        body=body,
        label_ids=[],
    )


class EmailAgentPricingTest(unittest.TestCase):
    def test_extract_cost_retail_lines_groups_items(self) -> None:
        rows = extract_cost_retail_lines(TRADITIONAL_EMAIL)

        self.assertEqual(
            [row.name for row in rows],
            [
                "Traditional 8ths",
                "Traditional 15g",
                "Traditional Non-infused preroll",
                "Traditional Infused preroll",
                "Asteroids 8ths",
                "Asteroids Sugar",
            ],
        )
        self.assertEqual(rows[0].cost, 14)
        self.assertEqual(rows[0].retail, 55)
        self.assertEqual(rows[3].cost, 6)
        self.assertEqual(rows[3].retail, 13)

    def test_build_pricing_analysis_draft_for_cost_retail_margin_email(self) -> None:
        plain_text, html_body = build_pricing_analysis_draft(make_email_record(TRADITIONAL_EMAIL), 0.50)

        self.assertIn("5 of 6 items are at or above the 45% margin target.", plain_text)
        self.assertIn("Traditional Infused preroll (34.1%)", plain_text)
        self.assertIn("Traditional Non-infused preroll (46.4%)", plain_text)
        self.assertIn("45% target retail: $15.59", plain_text)
        self.assertIn("I would round to at least $16.00", plain_text)
        self.assertIn("<table", html_body)


if __name__ == "__main__":
    unittest.main()
