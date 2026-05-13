# Email Agent Setup

This service watches Gmail, classifies incoming messages with OpenAI, applies Gmail labels, and creates Gmail draft replies for review. It never sends an email by itself.

The example config starts in dry-run mode. Your local `email_agent_config.json` can be switched to live draft mode with `safety.dry_run=false`, `safety.create_drafts=true`, and `report_drafts.auto_generate=true`.

## Files

- `email_agent.py` - the service runner.
- `email_agent_config.example.json` - copy this to `email_agent_config.json`.
- `token_gmail_agent.json` - created during Gmail OAuth for read/modify/draft access.

## First Run

```bash
cp email_agent_config.example.json email_agent_config.json
export OPENAI_API_KEY="your-key"
.venv/bin/python email_agent.py --config email_agent_config.json --once
```

If `OPENAI_API_KEY` is already in the repo `.env`, you do not need to export it manually. The agent loads `.env` by default through the `openai.env_file` setting.

The Gmail token for the older email scripts only has send permission. This agent needs Gmail modify/draft scopes, so it uses `token_gmail_agent.json` and will ask Google to approve the new access once.

## What It Does

- Deal report requests get labeled `AI Agent/Report Requests`.
- Inventory report requests get labeled `AI Agent/Report Requests`.
- Pricing/math requests get labeled `AI Agent/Needs Human` and can get a draft with the retail/discount/wholesale math.
- Headset/radio emails get labeled `AI Agent/Headset`.
- Important or sensitive messages get labeled `AI Agent/Needs Human`.
- Low-confidence classifications get labeled `AI Agent/Review`.
- Processed messages get labeled `AI Agent/Processed`.

By default it does not send emails, delete emails, archive emails, or run legacy report jobs automatically. In live draft mode, inventory request emails can generate report files, upload them to Drive, and create a reply draft with the links.

## Draft-Only Report Replies

For inventory requests, set this in `email_agent_config.json`:

```json
{
  "safety": {
    "dry_run": false,
    "auto_run_reports": false,
    "create_drafts": true
  },
  "report_drafts": {
    "auto_generate": true,
    "default_age_days": 90,
    "inventory_full_update": true
  }
}
```

With that setup:

- "Can you send a Hashish inventory report?" refreshes the inventory inputs, builds the Hashish inventory workbook, uploads it, and creates a Gmail draft reply with the Drive link.
- "Can you send Hashish inventory and aging report?" creates both the full inventory workbook and the aged flower inventory report, then drafts one reply with both links.
- "710Labs products older than three months" becomes a 90-day aged flower report.
- If the brand is missing, the agent drafts a quick reply asking for the brand name.

## Pricing Math Drafts

If someone forwards pricing like current retail, proposed retail, wholesale, and a promo discount, the agent keeps the email in human review but drafts the math response. It pulls discount/kickback context from `deals_brand_config.csv` / `deals.py` when it can, then puts the short answer first and uses a formatted Gmail table for customer price, wholesale, kickback, net cost, and margin movement.

For example, Jeeter is configured as 50% off with a 30% inventory-cost kickback, so the draft treats the effective cost as wholesale minus 30%.

Example:

```bash
.venv/bin/python email_agent.py --config email_agent_config.json --process-message-id 19e1e657940aaa9a
```

Use `--dry-run` with the same command to preview the draft in the terminal instead of creating a Gmail draft.

To process the latest matching email from one sender:

```bash
.venv/bin/python email_agent.py --config email_agent_config.json --from-sender donna@buzzcannabis.com --max-results 1
```

If that thread was already labeled/drafted and you want a fresh draft anyway:

```bash
.venv/bin/python email_agent.py --config email_agent_config.json --from-sender donna@buzzcannabis.com --include-processed --force-draft --max-results 1
```

Add `--dry-run` to preview it in the terminal first.

## Semi-Automatic Review

Every classification/action is written to `.email_agent_review_queue.jsonl` by default. That file is an audit trail of what the agent saw, how it classified it, and the draft text it wanted to use.

For a desktop-style approval popup before Gmail draft creation, set this in `email_agent_config.json`:

```json
{
  "review": {
    "popup_before_draft": true
  }
}
```

When enabled, the agent opens a small window showing the sender, subject, intent, summary, and proposed reply. You can edit the reply text and click **Create Gmail Draft**, or click **Skip Draft**.

If the service is running on a headless server with no desktop display, no popup can be shown. In that case the proposal is still saved to the review queue, and the message is labeled for review.

To inspect the latest queued proposals from a terminal:

```bash
.venv/bin/python email_agent.py --config email_agent_config.json --show-review-queue 10
```

## Watch Mode

```bash
.venv/bin/python email_agent.py --config email_agent_config.json --watch
```

Your local config uses `max_messages_per_poll=1` so the first live run does not create a pile of old drafts at once. Leave it running and it will pick up new messages one scan at a time.

## Manual Job Runs

These call the existing repo scripts:

```bash
.venv/bin/python email_agent.py --config email_agent_config.json --run-job weekly_deals
.venv/bin/python email_agent.py --config email_agent_config.json --run-job inventory
.venv/bin/python email_agent.py --config email_agent_config.json --run-job aged_710_flower
```

`weekly_deals` currently maps to `autoJob.py`, which can send brand deal emails as part of the existing workflow. Keep automatic job execution off until you are comfortable with that behavior.

For a one-off aged flower inventory request like "710Labs products older than three months":

```bash
.venv/bin/python aged_flower_inventory_report.py --brand "710 Labs" --brand-alias 710 --age-days 90
```

Use any brand by changing `--brand`, for example:

```bash
.venv/bin/python aged_flower_inventory_report.py --brand Hashish --age-days 90
```

The output is written locally under `reports/aged_inventory/<date>/` as both `.xlsx` and `.csv`, then uploaded to Google Drive under `aged_inventory/<date>/<brand>/`. Use `--no-drive-upload` for local files only.

For a one-off full inventory report using the same report-building flow as `BrandInventoryGUIemailer.py`:

```bash
.venv/bin/python brand_inventory_report_job.py --brand Hashish
```

Use `--no-refresh` to reuse the current files in `files/`, or `--no-drive-upload` to test without uploading.

## Systemd Service

Create `/etc/systemd/system/buzz-email-agent.service`:

```ini
[Unit]
Description=Buzz Email Agent
After=network-online.target
Wants=network-online.target

[Service]
Type=simple
WorkingDirectory=/home/anthony/projects/BuzzPythonGUI
ExecStart=/home/anthony/projects/BuzzPythonGUI/.venv/bin/python email_agent.py --config email_agent_config.json --watch
Restart=always
RestartSec=10

[Install]
WantedBy=multi-user.target
```

Then:

```bash
sudo systemctl daemon-reload
sudo systemctl enable --now buzz-email-agent
sudo journalctl -u buzz-email-agent -f
```
