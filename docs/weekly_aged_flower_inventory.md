# Weekly Aged Flower Inventory

Use `weekly_aged_flower_inventory_report.py` to generate one aged flower report
per brand, upload the files to Drive, and optionally email one digest to the
inventory manager.

## Brand List

The default brand list is `brand_config2.json`, the same config file used by
`BrandINVEmailer.py`.

Each value in a `brand_synonyms` list becomes its own aged flower report file.
For example, this config entry creates separate reports for `710 Labs`:

```json
{
  "brands": [
    {
      "brand_synonyms": ["710 Labs"],
      "folder_name": "710 Labs",
      "emails": ["donna@buzzcannabis.com"],
      "days": ["Monday", "Tuesday", "Wednesday", "Thursday", "Friday"]
    }
  ]
}
```

Aliases are still pulled from `brand_aliases_monthly.json` when present. That is
where `710 Labs` gets extra matches like `710` and `710labs`.

The runner also accepts:

- Text files: `Brand Name | Alias 1 | Alias 2`
- CSV files with columns like `brand`, `aliases`, `emails`, `enabled`, `age_days`
- Any alternate JSON file passed with `--brands-file`

## Manual Commands

Test locally without Drive or email:

```bash
cd /home/anthony/projects/BuzzPythonGUI
.venv/bin/python weekly_aged_flower_inventory_report.py --no-drive-upload
```

Generate reports and upload Drive links without emailing:

```bash
cd /home/anthony/projects/BuzzPythonGUI
.venv/bin/python weekly_aged_flower_inventory_report.py
```

Generate, upload, and email the digest:

```bash
cd /home/anthony/projects/BuzzPythonGUI
.venv/bin/python weekly_aged_flower_inventory_report.py --send-email
```

Send the digest to a one-off test recipient:

```bash
cd /home/anthony/projects/BuzzPythonGUI
.venv/bin/python weekly_aged_flower_inventory_report.py --send-email --email-to anthony@buzzcannabis.com
```

The output lands in `reports/aged_inventory/<date>/`:

- One `.xlsx` and one `.csv` per brand
- `weekly_aged_flower_inventory_manifest.json`
- `weekly_aged_flower_inventory_links.txt`

Drive uploads go under `aged_inventory/<date>/<brand>/`.

## Weekly Cron

Run `crontab -e` and add:

```cron
0 8 * * MON cd /home/anthony/projects/BuzzPythonGUI && /home/anthony/projects/BuzzPythonGUI/.venv/bin/python weekly_aged_flower_inventory_report.py --send-email >> /home/anthony/projects/BuzzPythonGUI/reports/aged_inventory/weekly_aged_flower_inventory.log 2>&1
```

This runs every Monday at 8:00 AM server time. The command uses
`token_drive.json` for Drive and `token_gmail.json` for Gmail, so run the manual
email command once first if either token needs OAuth refresh.
