#!/usr/bin/env python3
import json
import os
import re
from io import StringIO
from pathlib import Path
from urllib.parse import parse_qs, urlparse

import pandas as pd
import requests


DEFAULT_STORES = ["MV", "LM", "SV", "LG", "NC", "WP"]
DAY_ORDER = ["Monday", "Tuesday", "Wednesday", "Thursday", "Friday", "Saturday", "Sunday"]
CSV_COLUMNS = [
    "brand",
    "rule_name",
    "vendors",
    "brands",
    "days",
    "discount",
    "kickback",
    "categories",
    "stores",
    "include_phrases",
    "excluded_phrases",
    "include_units",
    "enabled",
    "notes",
]
KEY_COLUMNS = ["brand", "rule_name"]

BASE_DIR = Path(__file__).resolve().parent
CSV_CONFIG_PATH = BASE_DIR / "deals_brand_config.csv"
URL_CONFIG_PATH = BASE_DIR / "deals_brand_config_url.txt"
JSON_CONFIG_PATH = BASE_DIR / "deals_brand_config.json"
SHEET_URL_CONFIG_PATH = BASE_DIR / "deals_brand_config_sheet_url.txt"
CREDENTIALS_PATH = BASE_DIR / "credentials.json"
TOKEN_PATH = BASE_DIR / "token_sheets.json"

CONFIG_URL_ENV = "DEALS_BRAND_CONFIG_URL"
CONFIG_SHEET_URL_ENV = "DEALS_BRAND_CONFIG_SHEET_URL"


def _split_config_list(value):
    if isinstance(value, (list, tuple, set)):
        return [str(item).strip() for item in value if str(item).strip()]
    if value is None or pd.isna(value):
        return []

    text = str(value).replace("\r", "\n").replace("\n", ";").strip()
    if not text:
        return []
    return [part.strip() for part in text.split(";") if part.strip()]


def _parse_sheet_days(value):
    items = _split_config_list(value)
    if not items:
        return []

    normalized = []
    for item in items:
        lower_item = item.lower()
        if lower_item in {"all", "everyday", "daily"}:
            return DAY_ORDER.copy()

        match = next((day for day in DAY_ORDER if day.lower() == lower_item), None)
        normalized.append(match or item)
    return normalized


def _parse_sheet_stores(value):
    items = _split_config_list(value)
    if not items:
        return []
    if any(item.lower() in {"all", "default", "*"} for item in items):
        return DEFAULT_STORES.copy()
    return [item.upper() for item in items]


def _parse_rate(value):
    if value is None or pd.isna(value):
        return None

    text = str(value).strip()
    if not text:
        return None

    if text.endswith("%"):
        return float(text[:-1].strip()) / 100.0

    numeric = float(text)
    if numeric > 1:
        numeric /= 100.0
    return numeric


def _format_rate(value):
    if value is None or pd.isna(value):
        return ""

    pct = float(value) * 100.0
    rounded = round(pct)
    if abs(pct - rounded) < 1e-9:
        return f"{int(rounded)}%"
    return f"{pct:.2f}".rstrip("0").rstrip(".") + "%"


def _parse_bool(value):
    if value is None or pd.isna(value):
        return None
    if isinstance(value, bool):
        return value

    text = str(value).strip().lower()
    if not text:
        return None
    if text in {"1", "true", "yes", "y", "on"}:
        return True
    if text in {"0", "false", "no", "n", "off"}:
        return False
    return None


def _looks_like_json_text(text):
    stripped = str(text or "").lstrip()
    return stripped.startswith("{") or stripped.startswith("[")


def _looks_like_deals_criteria(value):
    deal_keys = {
        "vendors",
        "days",
        "discount",
        "kickback",
        "rules",
        "brands",
        "categories",
        "stores",
        "include_phrases",
        "excluded_phrases",
        "include_units",
    }

    if isinstance(value, dict):
        return bool(deal_keys.intersection(value.keys()))

    if isinstance(value, list):
        return any(isinstance(item, dict) and deal_keys.intersection(item.keys()) for item in value)

    return False


def _compress_brand_rules(grouped_rules):
    return {
        brand: rules[0] if len(rules) == 1 else rules
        for brand, rules in grouped_rules.items()
        if rules
    }


def _normalize_rules_for_export(criteria):
    if isinstance(criteria, list):
        base = {}
        rules = list(criteria)
        explicit_rule_names = [bool((rule or {}).get("rule_name")) for rule in rules]
    else:
        base = dict(criteria or {})
        raw_rules = base.pop("rules", None)
        rules = list(raw_rules) if raw_rules else [{}]
        explicit_rule_names = [bool((rule or {}).get("rule_name")) for rule in rules]

    multi_rule = len(rules) > 1
    out = []
    for index, rule in enumerate(rules, 1):
        effective = dict(base)
        effective.update(rule or {})

        explicit_rule_name = explicit_rule_names[index - 1]
        rule_name = str(effective.get("rule_name", "")).strip()
        if multi_rule and not rule_name:
            rule_name = f"Rule {index}"

        if "vendors" not in effective:
            effective["vendors"] = base.get("vendors", [])
        if "days" not in effective:
            effective["days"] = base.get("days", [])
        if "stores" not in effective and "stores" in base:
            effective["stores"] = base.get("stores", [])

        out.append(
            {
                "rule": effective,
                "rule_name": rule_name if multi_rule or explicit_rule_name else "",
            }
        )
    return out


def flatten_brand_criteria(criteria_by_brand):
    rows = []
    for brand, criteria in (criteria_by_brand or {}).items():
        for rule_info in _normalize_rules_for_export(criteria):
            rule = rule_info["rule"]
            stores = _split_config_list(rule.get("stores"))
            if stores and set(stores) == set(DEFAULT_STORES):
                stores = []

            rows.append(
                {
                    "brand": str(brand).strip(),
                    "rule_name": rule_info["rule_name"],
                    "vendors": ";".join(_split_config_list(rule.get("vendors"))),
                    "brands": ";".join(_split_config_list(rule.get("brands"))),
                    "days": ";".join(_parse_sheet_days(rule.get("days"))),
                    "discount": _format_rate(rule.get("discount")),
                    "kickback": _format_rate(rule.get("kickback")),
                    "categories": ";".join(_split_config_list(rule.get("categories"))),
                    "stores": ";".join(stores),
                    "include_phrases": ";".join(_split_config_list(rule.get("include_phrases"))),
                    "excluded_phrases": ";".join(_split_config_list(rule.get("excluded_phrases"))),
                    "include_units": bool(rule.get("include_units", False)),
                    "enabled": True,
                    "notes": "",
                }
            )

    df = pd.DataFrame(rows, columns=CSV_COLUMNS)
    if df.empty:
        return pd.DataFrame(columns=CSV_COLUMNS)

    return df.sort_values(by=["brand", "rule_name"], kind="stable").reset_index(drop=True)


def write_brand_criteria_csv(criteria_by_brand, csv_path=CSV_CONFIG_PATH):
    df = flatten_brand_criteria(criteria_by_brand)
    csv_path = Path(csv_path)
    df.to_csv(csv_path, index=False)
    return csv_path, df


def _load_brand_criteria_from_json_text(text, source_name):
    config = json.loads(text)
    if not isinstance(config, dict):
        raise ValueError(f"{source_name} must contain a JSON object.")

    if isinstance(config.get("brand_criteria"), dict):
        criteria = config["brand_criteria"]
    else:
        criteria = {
            key: value
            for key, value in config.items()
            if _looks_like_deals_criteria(value)
        }

    if not isinstance(criteria, dict) or not criteria:
        raise ValueError(f"{source_name} did not contain any usable brand criteria.")
    return criteria


def _load_brand_criteria_from_csv_text(text, source_name):
    df = pd.read_csv(StringIO(text), keep_default_na=False)
    df.columns = [str(col).strip().lower() for col in df.columns]

    if "brand" not in df.columns:
        raise ValueError(f"{source_name} is missing the required 'brand' column.")

    grouped_rules = {}
    for _, row in df.iterrows():
        enabled = _parse_bool(row.get("enabled"))
        if enabled is False:
            continue

        brand_name = str(row.get("brand", "")).strip()
        if not brand_name:
            continue

        rule = {}
        rule_name = str(row.get("rule_name", "")).strip()
        if rule_name:
            rule["rule_name"] = rule_name

        vendors = _split_config_list(row.get("vendors"))
        if vendors:
            rule["vendors"] = vendors

        brands = _split_config_list(row.get("brands"))
        if brands:
            rule["brands"] = brands

        days = _parse_sheet_days(row.get("days"))
        if days:
            rule["days"] = days

        discount = _parse_rate(row.get("discount"))
        if discount is not None:
            rule["discount"] = discount

        kickback = _parse_rate(row.get("kickback"))
        if kickback is not None:
            rule["kickback"] = kickback

        categories = _split_config_list(row.get("categories"))
        if categories:
            rule["categories"] = categories

        stores = _parse_sheet_stores(row.get("stores"))
        if stores:
            rule["stores"] = stores

        include_phrases = _split_config_list(row.get("include_phrases"))
        if include_phrases:
            rule["include_phrases"] = include_phrases

        excluded_phrases = _split_config_list(row.get("excluded_phrases"))
        if excluded_phrases:
            rule["excluded_phrases"] = excluded_phrases

        include_units = _parse_bool(row.get("include_units"))
        if include_units is not None:
            rule["include_units"] = include_units

        grouped_rules.setdefault(brand_name, []).append(rule)

    if not grouped_rules:
        raise ValueError(f"{source_name} did not contain any enabled brand rows.")
    return _compress_brand_rules(grouped_rules)


def _load_brand_criteria_from_url(url):
    response = requests.get(url, timeout=20)
    response.raise_for_status()
    text = response.text

    if _looks_like_json_text(text) or url.lower().split("?", 1)[0].endswith(".json"):
        return _load_brand_criteria_from_json_text(text, url)
    return _load_brand_criteria_from_csv_text(text, url)


def discover_brand_config_source():
    env_url = os.getenv(CONFIG_URL_ENV, "").strip()
    if env_url:
        return ("url", env_url, f"environment variable {CONFIG_URL_ENV}")

    if URL_CONFIG_PATH.exists():
        url = URL_CONFIG_PATH.read_text(encoding="utf-8").strip()
        if url:
            return ("url", url, str(URL_CONFIG_PATH))

    if JSON_CONFIG_PATH.exists():
        return ("json_file", JSON_CONFIG_PATH, str(JSON_CONFIG_PATH))

    if CSV_CONFIG_PATH.exists():
        return ("csv_file", CSV_CONFIG_PATH, str(CSV_CONFIG_PATH))

    return None


def load_brand_criteria(default_criteria, log_source=True, log_errors=True):
    source_info = discover_brand_config_source()
    if source_info is None:
        if log_source:
            print("[INFO] Using built-in deals brand config from deals.py.")
        return default_criteria, "built-in deals.py"

    source_type, source_value, source_label = source_info
    try:
        if source_type == "url":
            criteria = _load_brand_criteria_from_url(source_value)
        elif source_type == "json_file":
            criteria = _load_brand_criteria_from_json_text(
                Path(source_value).read_text(encoding="utf-8"),
                source_label,
            )
        elif source_type == "csv_file":
            criteria = _load_brand_criteria_from_csv_text(
                Path(source_value).read_text(encoding="utf-8"),
                source_label,
            )
        else:
            raise ValueError(f"Unsupported config source type: {source_type}")

        if log_source:
            print(f"[INFO] Loaded deals brand config from {source_label}.")
        return criteria, source_label
    except Exception as exc:
        if log_errors:
            print(
                f"[WARN] Failed to load deals brand config from {source_label}: {exc}. "
                "Falling back to built-in deals.py config."
            )
        return default_criteria, "built-in deals.py"


def discover_sheet_sync_url():
    env_url = os.getenv(CONFIG_SHEET_URL_ENV, "").strip()
    if env_url:
        return env_url

    if SHEET_URL_CONFIG_PATH.exists():
        url = SHEET_URL_CONFIG_PATH.read_text(encoding="utf-8").strip()
        if url:
            return url

    return None


def _parse_sheet_target(url):
    parsed = urlparse(url)
    match = re.search(r"/spreadsheets/d/([a-zA-Z0-9-_]+)", parsed.path)
    if not match:
        raise ValueError("Expected an editable Google Sheets URL with /spreadsheets/d/<spreadsheetId>.")

    spreadsheet_id = match.group(1)
    query = parse_qs(parsed.query)
    gid_text = query.get("gid", [None])[0]
    if gid_text is None and parsed.fragment.startswith("gid="):
        gid_text = parsed.fragment.split("=", 1)[1]

    gid = int(gid_text) if gid_text not in (None, "") else None
    return spreadsheet_id, gid


def _sheet_start_range(sheet_title):
    escaped = sheet_title.replace("'", "''")
    return f"'{escaped}'!A1"


def _sheet_full_range(sheet_title):
    escaped = sheet_title.replace("'", "''")
    return f"'{escaped}'"


def authenticate_sheets():
    from google.auth.transport.requests import Request
    from google.oauth2.credentials import Credentials
    from google_auth_oauthlib.flow import InstalledAppFlow
    from googleapiclient.discovery import build

    scopes = [
        "https://www.googleapis.com/auth/spreadsheets",
        "https://www.googleapis.com/auth/drive.file",
    ]

    creds = None
    if TOKEN_PATH.exists():
        creds = Credentials.from_authorized_user_file(str(TOKEN_PATH), scopes)

    if not creds or not creds.valid:
        if creds and creds.expired and creds.refresh_token:
            creds.refresh(Request())
        else:
            flow = InstalledAppFlow.from_client_secrets_file(str(CREDENTIALS_PATH), scopes)
            creds = flow.run_local_server(port=0)
        TOKEN_PATH.write_text(creds.to_json(), encoding="utf-8")

    return build("sheets", "v4", credentials=creds)


def _find_sheet_info(service, spreadsheet_id, gid):
    metadata = service.spreadsheets().get(spreadsheetId=spreadsheet_id).execute()
    sheets = metadata.get("sheets", [])
    if not sheets:
        raise ValueError("Spreadsheet has no sheets.")

    if gid is None:
        target_sheet = sheets[0]
    else:
        target_sheet = None
        for sheet in sheets:
            props = sheet.get("properties", {})
            if props.get("sheetId") == gid:
                target_sheet = sheet
                break

        if target_sheet is None:
            raise ValueError(f"Could not find a sheet tab with gid {gid}.")

    props = target_sheet.get("properties", {})
    return {
        "title": props.get("title"),
        "sheet_id": props.get("sheetId"),
        "banded_ranges": target_sheet.get("bandedRanges", []),
    }


def _rgb(hex_color):
    hex_color = hex_color.lstrip("#")
    return {
        "red": int(hex_color[0:2], 16) / 255.0,
        "green": int(hex_color[2:4], 16) / 255.0,
        "blue": int(hex_color[4:6], 16) / 255.0,
    }


def _column_width_requests(sheet_id, headers):
    preferred_widths = {
        "brand": 180,
        "rule_name": 240,
        "vendors": 280,
        "brands": 180,
        "days": 170,
        "discount": 95,
        "kickback": 95,
        "categories": 180,
        "stores": 95,
        "include_phrases": 180,
        "excluded_phrases": 180,
        "include_units": 105,
        "enabled": 90,
        "notes": 260,
    }

    requests = []
    for index, header in enumerate(headers):
        width = preferred_widths.get(str(header).strip())
        if width is None:
            continue
        requests.append(
            {
                "updateDimensionProperties": {
                    "range": {
                        "sheetId": sheet_id,
                        "dimension": "COLUMNS",
                        "startIndex": index,
                        "endIndex": index + 1,
                    },
                    "properties": {"pixelSize": width},
                    "fields": "pixelSize",
                }
            }
        )

    return requests


def _format_synced_sheet(service, spreadsheet_id, sheet_info, headers, total_rows):
    sheet_id = sheet_info["sheet_id"]
    column_count = max(len(headers), 1)
    total_rows = max(total_rows, 1)

    all_range = {
        "sheetId": sheet_id,
        "startRowIndex": 0,
        "endRowIndex": total_rows,
        "startColumnIndex": 0,
        "endColumnIndex": column_count,
    }
    header_range = {
        "sheetId": sheet_id,
        "startRowIndex": 0,
        "endRowIndex": 1,
        "startColumnIndex": 0,
        "endColumnIndex": column_count,
    }
    data_range = {
        "sheetId": sheet_id,
        "startRowIndex": 1,
        "endRowIndex": total_rows,
        "startColumnIndex": 0,
        "endColumnIndex": column_count,
    }

    requests = []

    for banded_range in sheet_info.get("banded_ranges", []):
        banded_range_id = banded_range.get("bandedRangeId")
        if banded_range_id is not None:
            requests.append({"deleteBanding": {"bandedRangeId": banded_range_id}})

    requests.append(
        {
            "updateSheetProperties": {
                "properties": {
                    "sheetId": sheet_id,
                    "gridProperties": {"frozenRowCount": 1},
                    "tabColor": _rgb("#4B6A4F"),
                },
                "fields": "gridProperties.frozenRowCount,tabColor",
            }
        }
    )

    requests.append(
        {
            "repeatCell": {
                "range": header_range,
                "cell": {
                    "userEnteredFormat": {
                        "backgroundColor": _rgb("#244B3C"),
                        "horizontalAlignment": "CENTER",
                        "verticalAlignment": "MIDDLE",
                        "wrapStrategy": "WRAP",
                        "textFormat": {
                            "bold": True,
                            "foregroundColor": _rgb("#FFFFFF"),
                            "fontSize": 10,
                        },
                    }
                },
                "fields": (
                    "userEnteredFormat(backgroundColor,"
                    "horizontalAlignment,verticalAlignment,wrapStrategy,textFormat)"
                ),
            }
        }
    )

    if total_rows > 1:
        requests.append(
            {
                "repeatCell": {
                    "range": data_range,
                    "cell": {
                        "userEnteredFormat": {
                            "verticalAlignment": "MIDDLE",
                            "wrapStrategy": "WRAP",
                            "textFormat": {"fontSize": 10},
                        }
                    },
                    "fields": "userEnteredFormat(verticalAlignment,wrapStrategy,textFormat)",
                }
            }
        )

        requests.append(
            {
                "addBanding": {
                    "bandedRange": {
                        "range": all_range,
                        "rowProperties": {
                            "headerColor": _rgb("#244B3C"),
                            "firstBandColor": _rgb("#F6FAF6"),
                            "secondBandColor": _rgb("#EAF3EA"),
                        },
                    }
                }
            }
        )

    if total_rows > 1:
        centered_headers = {
            "days",
            "discount",
            "kickback",
            "stores",
            "include_units",
            "enabled",
        }
        centered_indexes = [i for i, header in enumerate(headers) if str(header).strip() in centered_headers]
        for index in centered_indexes:
            requests.append(
                {
                    "repeatCell": {
                        "range": {
                            "sheetId": sheet_id,
                            "startRowIndex": 1,
                            "endRowIndex": total_rows,
                            "startColumnIndex": index,
                            "endColumnIndex": index + 1,
                        },
                        "cell": {
                            "userEnteredFormat": {
                                "horizontalAlignment": "CENTER",
                                "verticalAlignment": "MIDDLE",
                            }
                        },
                        "fields": "userEnteredFormat(horizontalAlignment,verticalAlignment)",
                    }
                }
            )

    requests.extend(_column_width_requests(sheet_id, headers))

    requests.append({"setBasicFilter": {"filter": {"range": all_range}}})
    requests.append(
        {
            "updateDimensionProperties": {
                "range": {
                    "sheetId": sheet_id,
                    "dimension": "ROWS",
                    "startIndex": 0,
                    "endIndex": 1,
                },
                "properties": {"pixelSize": 32},
                "fields": "pixelSize",
            }
        }
    )

    service.spreadsheets().batchUpdate(
        spreadsheetId=spreadsheet_id,
        body={"requests": requests},
    ).execute()


def _preserve_existing_columns(export_df, existing_values):
    if not existing_values:
        return export_df

    headers = [str(value).strip() for value in existing_values[0]]
    if not headers:
        return export_df

    existing_rows = existing_values[1:]
    normalized_rows = []
    oversized_row_count = 0
    for row in existing_rows:
        row_values = list(row)
        if len(row_values) > len(headers):
            row_values = row_values[: len(headers)]
            oversized_row_count += 1
        elif len(row_values) < len(headers):
            row_values.extend([""] * (len(headers) - len(row_values)))
        normalized_rows.append(row_values)

    if oversized_row_count:
        print(
            f"[WARN] Ignoring extra values in {oversized_row_count} existing sheet row(s) "
            "beyond the last named header column."
        )

    existing_df = pd.DataFrame(normalized_rows, columns=headers)
    existing_df.columns = [str(col).strip() for col in existing_df.columns]

    if existing_df.empty:
        return export_df

    preserve_cols = [col for col in existing_df.columns if col not in export_df.columns]
    if "notes" in existing_df.columns and "notes" not in preserve_cols:
        preserve_cols.append("notes")

    key_cols = [col for col in KEY_COLUMNS if col in export_df.columns and col in existing_df.columns]
    if not preserve_cols or not key_cols:
        return export_df

    preserved = existing_df[key_cols + preserve_cols].drop_duplicates(subset=key_cols, keep="first")
    merged = export_df.merge(preserved, on=key_cols, how="left", suffixes=("", "__existing"))

    if "notes__existing" in merged.columns:
        merged["notes"] = merged["notes"].where(
            merged["notes"].astype(str).str.strip() != "",
            merged["notes__existing"].fillna(""),
        )
        merged.drop(columns=["notes__existing"], inplace=True)

    for col in preserve_cols:
        existing_col = f"{col}__existing"
        if existing_col in merged.columns:
            merged[col] = merged[existing_col]
            merged.drop(columns=[existing_col], inplace=True)

    ordered_cols = list(export_df.columns) + [col for col in preserve_cols if col not in export_df.columns]
    return merged[ordered_cols]


def sync_brand_criteria_to_sheet(criteria_by_brand, sheet_url):
    spreadsheet_id, gid = _parse_sheet_target(sheet_url)
    export_df = flatten_brand_criteria(criteria_by_brand)
    service = authenticate_sheets()
    sheet_info = _find_sheet_info(service, spreadsheet_id, gid)
    sheet_title = sheet_info["title"]

    existing_values = (
        service.spreadsheets()
        .values()
        .get(spreadsheetId=spreadsheet_id, range=_sheet_full_range(sheet_title))
        .execute()
        .get("values", [])
    )
    export_df = _preserve_existing_columns(export_df, existing_values)

    values = [export_df.columns.tolist()] + export_df.fillna("").astype(str).values.tolist()
    full_range = _sheet_full_range(sheet_title)
    start_range = _sheet_start_range(sheet_title)

    service.spreadsheets().values().clear(
        spreadsheetId=spreadsheet_id,
        range=full_range,
        body={},
    ).execute()
    service.spreadsheets().values().update(
        spreadsheetId=spreadsheet_id,
        range=start_range,
        valueInputOption="RAW",
        body={"values": values},
    ).execute()

    _format_synced_sheet(
        service=service,
        spreadsheet_id=spreadsheet_id,
        sheet_info=sheet_info,
        headers=list(export_df.columns),
        total_rows=len(values),
    )

    return sheet_title, len(export_df)


def sync_brand_config_artifacts(criteria_by_brand, sync_sheet=False):
    csv_path, df = write_brand_criteria_csv(criteria_by_brand)
    result = {
        "csv_path": csv_path,
        "row_count": len(df),
        "sheet_synced": False,
        "sheet_title": None,
        "sheet_skip_reason": None,
    }

    if not sync_sheet:
        return result

    sheet_url = discover_sheet_sync_url()
    if not sheet_url:
        result["sheet_skip_reason"] = (
            f"No editable Google Sheet URL found. Add it to {SHEET_URL_CONFIG_PATH.name} "
            f"or set {CONFIG_SHEET_URL_ENV}."
        )
        return result

    sheet_title, row_count = sync_brand_criteria_to_sheet(criteria_by_brand, sheet_url)
    result["sheet_synced"] = True
    result["sheet_title"] = sheet_title
    result["row_count"] = row_count
    return result
