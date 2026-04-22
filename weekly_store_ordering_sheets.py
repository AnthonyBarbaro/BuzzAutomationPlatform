#!/usr/bin/env python3
from __future__ import annotations

from datetime import date, datetime
import math
from typing import Any, Iterable, Mapping, Sequence

import pandas as pd
import numpy as np

from deals_brand_config_sync import _parse_sheet_target, authenticate_sheets


def parse_spreadsheet_target(target: str) -> tuple[str, int | None]:
    text = str(target or "").strip()
    if not text:
        raise ValueError("Missing Google Sheets target.")
    if "/spreadsheets/d/" in text:
        return _parse_sheet_target(text)
    return text, None


def build_summary_rows(summary: Mapping[str, Any], max_rows: int = 3) -> list[list[Any]]:
    items = [(str(label), _sheet_safe_value(value)) for label, value in summary.items()]
    if not items:
        return []

    row_count = max(1, min(int(max_rows), len(items)))
    pairs_per_row = int(math.ceil(len(items) / float(row_count)))
    rows: list[list[Any]] = [[] for _ in range(row_count)]
    for index, (label, value) in enumerate(items):
        row_index = min(index // pairs_per_row, row_count - 1)
        rows[row_index].extend([label, value])
    return [row for row in rows if row]


def build_sheet_matrix(summary_rows: Sequence[Sequence[Any]], df: pd.DataFrame) -> tuple[list[list[Any]], int]:
    frame = df.copy()
    headers = list(frame.columns)
    rows = [build_summary_row_padding(row, len(headers)) for row in summary_rows]
    rows.append(headers)
    for values in frame.itertuples(index=False, name=None):
        rows.append([_sheet_safe_value(value) for value in values])
    header_row_number = len(summary_rows) + 1
    return rows, header_row_number


def build_readme_rows(
    store_code: str,
    store_name: str,
    output_flags: Mapping[str, Any] | None = None,
    week_of: str | None = None,
    tab_titles: Mapping[str, Any] | None = None,
    manual_columns: Sequence[str] | None = None,
    snapshot_generated_at: str | None = None,
) -> list[list[Any]]:
    normalized_store = str(store_code or "").strip().upper()
    clean_store_name = str(store_name or "").strip()
    store_label = f"{normalized_store} - {clean_store_name}" if clean_store_name else normalized_store

    enabled_outputs = {str(key).strip().lower(): bool(value) for key, value in (output_flags or {}).items()}
    clean_tab_titles = {
        str(key).strip().lower(): str(value).strip()
        for key, value in (tab_titles or {}).items()
        if str(value or "").strip()
    }
    clean_manual_columns = [str(value).strip() for value in (manual_columns or []) if str(value).strip()]

    latest_review_title = clean_tab_titles.get("review", "Created on the next live run.")
    latest_week = str(week_of or "").strip() or "Populated after the first live run."
    latest_snapshot = str(snapshot_generated_at or "").strip() or "Updated on the next live run."

    if not enabled_outputs:
        current_output = "Google Sheet output is controlled by the repo config. The next live run will refresh this note."
    elif enabled_outputs.get("auto") and enabled_outputs.get("review"):
        current_output = "This repo currently writes both the AUTO and REVIEW tabs to Google Sheets."
    elif enabled_outputs.get("auto"):
        current_output = "This repo currently writes the AUTO tab to Google Sheets."
    else:
        current_output = "This repo currently writes the REVIEW tab to Google Sheets."

    latest_tabs: list[str] = []
    for tab_kind in ("review", "auto"):
        title = clean_tab_titles.get(tab_kind)
        if title:
            latest_tabs.append(f"- {title}")
    latest_tabs_text = "\n".join(latest_tabs) if latest_tabs else "Generated week tabs will appear here after the first live run."

    manual_columns_text = (
        ", ".join(clean_manual_columns)
        if clean_manual_columns
        else "None. This sheet is script-owned and safe to rerun."
    )

    start_here = "\n".join(
        [
            "1. Open the newest generated tab for the current week, usually REVIEW.",
            "2. Use the filter arrows on the header row. Filters are already turned on for the weekly tab.",
            "3. Work the red and orange rows first. Those are the items sitting furthest below par.",
        ]
    )
    vendor_brand_guide = (
        "Filter Brand first, then Category. The weekly sheet stays line-by-line by product so each SKU keeps "
        "its own row and par target."
    )
    filter_stack = "\n".join(
        [
            "Recommended filter order:",
            "Brand -> Category",
            "Then optionally sort Available low-to-high to see the biggest par gaps first.",
        ]
    )
    read_suggestions = (
        "Use Available versus Par Level together with Units Sold 7d/14d/30d to spot which rows are "
        "short and which ones are already covered."
    )
    order_workflow = "\n".join(
        [
            "1. Review the red rows first, then orange, then yellow.",
            "2. Compare Available to Par Level to estimate the gap for each line item.",
            "3. Use the 14d and 30d sales columns to sanity-check whether the SKU still deserves shelf space.",
            "4. If a SKU is gone, look for replacements nearby in the same brand and category.",
        ]
    )
    rerun_safety = (
        "Reruns for the same week update the same script-owned ordering rows, so you can refresh the par view "
        "without rebuilding the sheet layout by hand."
    )

    return [
        ["Buzz Weekly Store Ordering", ""],
        ["Store", store_label],
        [
            "Use This Workbook For",
            "Review weekly reorder suggestions for this store and capture final buying decisions in the weekly ordering tabs.",
        ],
        ["Start Here", start_here],
        ["Pick A Vendor Or Brand", vendor_brand_guide],
        ["Recommended Filters", filter_stack],
        ["How To Read The Suggestions", read_suggestions],
        ["How To Work The Order", order_workflow],
        ["Current Google Sheet Output", current_output],
        ["Latest Week Generated", latest_week],
        ["Latest Snapshot Generated At", latest_snapshot],
        ["Latest Review Tab", latest_review_title],
        ["Latest Weekly Tabs", latest_tabs_text],
        ["Rerun Safety", rerun_safety],
        ["Manual Columns Preserved On Rerun", manual_columns_text],
        [
            "Do Not Edit",
            "Row Key or the script-owned metric columns. Those values are regenerated and may change on rerun.",
        ],
        [
            "Run Defaults",
            "If you omit dates, the script uses today in America/Los_Angeles and defaults the tab week to the next Monday once the current week's Monday has passed.",
        ],
        ["Data Source", "Dutchie API inventory, product, and transaction data."],
    ]


def merge_preserved_review_columns(
    review_df: pd.DataFrame,
    existing_values: Sequence[Sequence[Any]] | None,
    manual_columns: Sequence[str],
    key_column: str = "Row Key",
) -> pd.DataFrame:
    if not existing_values:
        return review_df

    header_values = list(existing_values[0]) if existing_values else []
    if not header_values:
        return review_df

    headers = [str(value).strip() for value in header_values]
    try:
        key_index = headers.index(key_column)
    except ValueError:
        return review_df

    source = review_df.copy()
    for column in manual_columns:
        if column not in source.columns:
            source[column] = ""

    preserved_rows: dict[str, dict[str, Any]] = {}
    for raw_row in existing_values[1:]:
        row = list(raw_row)
        if len(row) < len(headers):
            row.extend([""] * (len(headers) - len(row)))
        if len(row) > len(headers):
            row = row[: len(headers)]
        row_key = str(row[key_index]).strip()
        if not row_key:
            continue
        preserved_rows[row_key] = {
            column: row[headers.index(column)] if column in headers else ""
            for column in manual_columns
        }

    if not preserved_rows:
        return source

    source[key_column] = source[key_column].fillna("").astype(str)
    for column in manual_columns:
        source[column] = source.apply(
            lambda row: _preserve_manual_value(
                current_value=row.get(column, ""),
                preserved_value=preserved_rows.get(str(row.get(key_column, "")).strip(), {}).get(column, ""),
            ),
            axis=1,
        )
    return source


def read_sheet_values(service: Any, spreadsheet_id: str, title: str) -> list[list[Any]]:
    try:
        return (
            service.spreadsheets()
            .values()
            .get(spreadsheetId=spreadsheet_id, range=_sheet_range(title))
            .execute()
            .get("values", [])
        )
    except Exception:
        return []


def upsert_readme_tab(
    service: Any,
    spreadsheet_id: str,
    store_code: str,
    store_name: str,
    output_flags: Mapping[str, Any] | None = None,
    week_of: str | None = None,
    tab_titles: Mapping[str, Any] | None = None,
    manual_columns: Sequence[str] | None = None,
    snapshot_generated_at: str | None = None,
    title: str = "README",
) -> dict[str, Any]:
    sheet_info = ensure_sheet(service, spreadsheet_id, title, "readme")
    values = build_readme_rows(
        store_code=store_code,
        store_name=store_name,
        output_flags=output_flags,
        week_of=week_of,
        tab_titles=tab_titles,
        manual_columns=manual_columns,
        snapshot_generated_at=snapshot_generated_at,
    )
    total_rows = max(len(values), 1)
    total_columns = max(max((len(row) for row in values), default=0), 2)

    service.spreadsheets().values().clear(
        spreadsheetId=spreadsheet_id,
        range=_sheet_range(title),
        body={},
    ).execute()
    service.spreadsheets().values().update(
        spreadsheetId=spreadsheet_id,
        range=_sheet_start_range(title),
        valueInputOption="RAW",
        body={"values": values},
    ).execute()

    refreshed_info = get_sheet_info_by_title(service, spreadsheet_id, title)
    if not refreshed_info:
        raise RuntimeError(f"Unable to reload sheet metadata for {title}.")
    _format_readme_sheet(
        service=service,
        spreadsheet_id=spreadsheet_id,
        sheet_info=refreshed_info,
        total_rows=total_rows,
        total_columns=total_columns,
    )
    return {
        "title": title,
        "sheet_id": sheet_info["sheet_id"],
        "rows_written": len(values),
    }


def upsert_ordering_tab(
    service: Any,
    spreadsheet_id: str,
    title: str,
    summary_rows: Sequence[Sequence[Any]],
    df: pd.DataFrame,
    sheet_kind: str,
    hidden_headers: Iterable[str] | None = None,
) -> dict[str, Any]:
    sheet_info = ensure_sheet(service, spreadsheet_id, title, sheet_kind)
    values, header_row_number = build_sheet_matrix(summary_rows, df)
    total_rows = max(len(values), 1)
    total_columns = max(len(df.columns), 2)

    service.spreadsheets().values().clear(
        spreadsheetId=spreadsheet_id,
        range=_sheet_range(title),
        body={},
    ).execute()
    service.spreadsheets().values().update(
        spreadsheetId=spreadsheet_id,
        range=_sheet_start_range(title),
        valueInputOption="RAW",
        body={"values": values},
    ).execute()

    refreshed_info = get_sheet_info_by_title(service, spreadsheet_id, title)
    if not refreshed_info:
        raise RuntimeError(f"Unable to reload sheet metadata for {title}.")
    _format_ordering_sheet(
        service=service,
        spreadsheet_id=spreadsheet_id,
        sheet_info=refreshed_info,
        headers=list(df.columns),
        total_rows=total_rows,
        total_columns=total_columns,
        summary_row_count=len(summary_rows),
        summary_rows=summary_rows,
        summary_column_count=max((len(row) for row in summary_rows), default=0),
        header_row_number=header_row_number,
        hidden_headers=set(hidden_headers or []),
        sheet_kind=sheet_kind,
    )

    return {
        "title": title,
        "sheet_id": sheet_info["sheet_id"],
        "header_row_number": header_row_number,
        "rows_written": max(len(df), 0),
    }


def move_sheet_to_index(service: Any, spreadsheet_id: str, title: str, index: int) -> None:
    sheet_info = get_sheet_info_by_title(service, spreadsheet_id, title)
    if not sheet_info or sheet_info.get("sheet_id") is None:
        return

    service.spreadsheets().batchUpdate(
        spreadsheetId=spreadsheet_id,
        body={
            "requests": [
                {
                    "updateSheetProperties": {
                        "properties": {
                            "sheetId": int(sheet_info["sheet_id"]),
                            "index": max(int(index), 0),
                        },
                        "fields": "index",
                    }
                }
            ]
        },
    ).execute()


def move_latest_tabs_next_to_readme(
    service: Any,
    spreadsheet_id: str,
    titles: Sequence[str],
    readme_title: str = "README",
) -> list[str]:
    ordered_titles: list[str] = []
    seen: set[str] = set()
    for raw_title in titles:
        title = str(raw_title or "").strip()
        if not title or title == readme_title or title in seen:
            continue
        ordered_titles.append(title)
        seen.add(title)

    move_sheet_to_index(service, spreadsheet_id, readme_title, 0)
    next_index = 1
    for title in ordered_titles:
        move_sheet_to_index(service, spreadsheet_id, title, next_index)
        next_index += 1
    return ordered_titles


def ensure_sheet(service: Any, spreadsheet_id: str, title: str, sheet_kind: str) -> dict[str, Any]:
    existing = get_sheet_info_by_title(service, spreadsheet_id, title)
    if existing:
        return existing

    color = _tab_color(sheet_kind)
    response = (
        service.spreadsheets()
        .batchUpdate(
            spreadsheetId=spreadsheet_id,
            body={
                "requests": [
                    {
                        "addSheet": {
                            "properties": {
                                "title": title,
                                "tabColor": color,
                            }
                        }
                    }
                ]
            },
        )
        .execute()
    )
    add_reply = (response.get("replies") or [{}])[0].get("addSheet", {})
    props = add_reply.get("properties", {})
    return {
        "title": props.get("title", title),
        "sheet_id": props.get("sheetId"),
        "banded_ranges": [],
        "conditional_rules": [],
    }


def get_sheet_info_by_title(service: Any, spreadsheet_id: str, title: str) -> dict[str, Any] | None:
    metadata = service.spreadsheets().get(spreadsheetId=spreadsheet_id).execute()
    for sheet in metadata.get("sheets", []):
        props = sheet.get("properties", {})
        if props.get("title") != title:
            continue
        return {
            "title": props.get("title"),
            "sheet_id": props.get("sheetId"),
            "banded_ranges": sheet.get("bandedRanges", []),
            "conditional_rules": sheet.get("conditionalFormats", []),
        }
    return None


def _format_ordering_sheet(
    service: Any,
    spreadsheet_id: str,
    sheet_info: Mapping[str, Any],
    headers: Sequence[str],
    total_rows: int,
    total_columns: int,
    summary_row_count: int,
    summary_rows: Sequence[Sequence[Any]],
    summary_column_count: int,
    header_row_number: int,
    hidden_headers: set[str],
    sheet_kind: str,
) -> None:
    sheet_id = int(sheet_info["sheet_id"])
    header_row_index = header_row_number - 1
    data_start_row_index = header_row_number
    requests: list[dict[str, Any]] = []

    for banded_range in sheet_info.get("banded_ranges", []):
        banded_range_id = banded_range.get("bandedRangeId")
        if banded_range_id is not None:
            requests.append({"deleteBanding": {"bandedRangeId": banded_range_id}})

    conditional_rules = list(sheet_info.get("conditional_rules", []))
    for index in range(len(conditional_rules) - 1, -1, -1):
        requests.append(
            {
                "deleteConditionalFormatRule": {
                    "sheetId": sheet_id,
                    "index": index,
                }
            }
        )

    requests.append(
        {
            "updateSheetProperties": {
                "properties": {
                    "sheetId": sheet_id,
                    "gridProperties": {"frozenRowCount": header_row_number},
                    "tabColor": _tab_color(sheet_kind),
                },
                "fields": "gridProperties.frozenRowCount,tabColor",
            }
        }
    )

    if summary_row_count:
        requests.append(
            {
                "repeatCell": {
                    "range": {
                        "sheetId": sheet_id,
                        "startRowIndex": 0,
                        "endRowIndex": summary_row_count,
                        "startColumnIndex": 0,
                        "endColumnIndex": min(total_columns, max(summary_column_count, 2)),
                    },
                    "cell": {
                        "userEnteredFormat": {
                            "backgroundColor": _rgb("#F4F7F1"),
                            "verticalAlignment": "MIDDLE",
                            "textFormat": {"fontSize": 10},
                        }
                    },
                    "fields": "userEnteredFormat(backgroundColor,verticalAlignment,textFormat)",
                }
            }
        )
        requests.append(
            {
                "repeatCell": {
                    "range": {
                        "sheetId": sheet_id,
                        "startRowIndex": 0,
                        "endRowIndex": summary_row_count,
                        "startColumnIndex": 0,
                        "endColumnIndex": min(total_columns, max(summary_column_count, 2)),
                    },
                    "cell": {
                        "userEnteredFormat": {
                            "horizontalAlignment": "LEFT",
                            "textFormat": {"fontSize": 10},
                        }
                    },
                    "fields": "userEnteredFormat(horizontalAlignment,textFormat)",
                }
            }
        )
        for column_index in range(0, summary_column_count, 2):
            requests.append(
                {
                    "repeatCell": {
                        "range": {
                            "sheetId": sheet_id,
                            "startRowIndex": 0,
                            "endRowIndex": summary_row_count,
                            "startColumnIndex": column_index,
                            "endColumnIndex": min(column_index + 1, total_columns),
                        },
                        "cell": {
                            "userEnteredFormat": {
                                "textFormat": {"bold": True, "fontSize": 10},
                            }
                        },
                        "fields": "userEnteredFormat.textFormat",
                    }
                }
            )
    header_color = "#244B3C" if sheet_kind.lower() == "auto" else "#2B4D70"
    requests.append(
        {
            "repeatCell": {
                "range": {
                    "sheetId": sheet_id,
                    "startRowIndex": header_row_index,
                    "endRowIndex": header_row_index + 1,
                    "startColumnIndex": 0,
                    "endColumnIndex": total_columns,
                },
                "cell": {
                    "userEnteredFormat": {
                        "backgroundColor": _rgb(header_color),
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
                    "userEnteredFormat(backgroundColor,horizontalAlignment,"
                    "verticalAlignment,wrapStrategy,textFormat)"
                ),
            }
        }
    )

    if total_rows > header_row_number:
        requests.append(
            {
                "repeatCell": {
                    "range": {
                        "sheetId": sheet_id,
                        "startRowIndex": data_start_row_index,
                        "endRowIndex": total_rows,
                        "startColumnIndex": 0,
                        "endColumnIndex": total_columns,
                    },
                    "cell": {
                        "userEnteredFormat": {
                            "horizontalAlignment": "CENTER",
                            "verticalAlignment": "MIDDLE",
                            "wrapStrategy": "WRAP",
                            "textFormat": {"fontSize": 10},
                        }
                    },
                    "fields": "userEnteredFormat(horizontalAlignment,verticalAlignment,wrapStrategy,textFormat)",
                }
            }
        )
        requests.append(
            {
                "addBanding": {
                    "bandedRange": {
                        "range": {
                            "sheetId": sheet_id,
                            "startRowIndex": header_row_index,
                            "endRowIndex": total_rows,
                            "startColumnIndex": 0,
                            "endColumnIndex": total_columns,
                        },
                        "rowProperties": {
                            "headerColor": _rgb(header_color),
                            "firstBandColor": _rgb("#FBFCFA"),
                            "secondBandColor": _rgb("#F1F5EE"),
                        },
                    }
                }
            }
        )
        requests.append(
            {
                "setBasicFilter": {
                    "filter": {
                        "range": {
                            "sheetId": sheet_id,
                            "startRowIndex": header_row_index,
                            "endRowIndex": total_rows,
                            "startColumnIndex": 0,
                            "endColumnIndex": total_columns,
                        }
                    }
                }
            }
        )

    requests.append(
        {
            "updateDimensionProperties": {
                "range": {
                    "sheetId": sheet_id,
                    "dimension": "ROWS",
                    "startIndex": header_row_index,
                    "endIndex": header_row_index + 1,
                },
                "properties": {"pixelSize": 33},
                "fields": "pixelSize",
            }
        }
    )
    if total_rows > header_row_number:
        requests.append(
            {
                "updateDimensionProperties": {
                    "range": {
                        "sheetId": sheet_id,
                        "dimension": "ROWS",
                        "startIndex": data_start_row_index,
                        "endIndex": total_rows,
                    },
                    "properties": {"pixelSize": 33},
                    "fields": "pixelSize",
                }
            }
        )
    requests.extend(_column_width_requests(sheet_id, headers))
    requests.extend(_data_alignment_requests(sheet_id, headers, total_rows, header_row_number))
    requests.extend(
        _number_format_requests(
            sheet_id,
            headers,
            total_rows,
            header_row_number,
            summary_row_count,
            summary_rows,
        )
    )
    requests.extend(_conditional_format_requests(sheet_id, headers, total_rows, header_row_number))
    requests.extend(_hidden_column_requests(sheet_id, headers, hidden_headers))

    if requests:
        service.spreadsheets().batchUpdate(
            spreadsheetId=spreadsheet_id,
            body={"requests": requests},
        ).execute()


def _format_readme_sheet(
    service: Any,
    spreadsheet_id: str,
    sheet_info: Mapping[str, Any],
    total_rows: int,
    total_columns: int,
) -> None:
    sheet_id = int(sheet_info["sheet_id"])
    requests: list[dict[str, Any]] = []

    for banded_range in sheet_info.get("banded_ranges", []):
        banded_range_id = banded_range.get("bandedRangeId")
        if banded_range_id is not None:
            requests.append({"deleteBanding": {"bandedRangeId": banded_range_id}})

    conditional_rules = list(sheet_info.get("conditional_rules", []))
    for index in range(len(conditional_rules) - 1, -1, -1):
        requests.append(
            {
                "deleteConditionalFormatRule": {
                    "sheetId": sheet_id,
                    "index": index,
                }
            }
        )

    requests.append(
        {
            "updateSheetProperties": {
                "properties": {
                    "sheetId": sheet_id,
                    "gridProperties": {"frozenRowCount": 1},
                    "tabColor": _tab_color("readme"),
                },
                "fields": "gridProperties.frozenRowCount,tabColor",
            }
        }
    )

    requests.append(
        {
            "repeatCell": {
                "range": {
                    "sheetId": sheet_id,
                    "startRowIndex": 0,
                    "endRowIndex": total_rows,
                    "startColumnIndex": 0,
                    "endColumnIndex": total_columns,
                },
                "cell": {
                    "userEnteredFormat": {
                        "backgroundColor": _rgb("#FFFFFF"),
                        "horizontalAlignment": "LEFT",
                        "verticalAlignment": "MIDDLE",
                        "wrapStrategy": "WRAP",
                        "textFormat": {"fontSize": 10},
                    }
                },
                "fields": (
                    "userEnteredFormat(backgroundColor,horizontalAlignment,"
                    "verticalAlignment,wrapStrategy,textFormat)"
                ),
            }
        }
    )
    requests.append(
        {
            "repeatCell": {
                "range": {
                    "sheetId": sheet_id,
                    "startRowIndex": 0,
                    "endRowIndex": 1,
                    "startColumnIndex": 0,
                    "endColumnIndex": total_columns,
                },
                "cell": {
                    "userEnteredFormat": {
                        "backgroundColor": _rgb("#F6EDD9"),
                        "textFormat": {"bold": True, "fontSize": 14},
                    }
                },
                "fields": "userEnteredFormat(backgroundColor,textFormat)",
            }
        }
    )
    requests.append(
        {
            "repeatCell": {
                "range": {
                    "sheetId": sheet_id,
                    "startRowIndex": 1,
                    "endRowIndex": total_rows,
                    "startColumnIndex": 0,
                    "endColumnIndex": 1,
                },
                "cell": {
                    "userEnteredFormat": {
                        "backgroundColor": _rgb("#F7F7F7"),
                        "textFormat": {"bold": True},
                    }
                },
                "fields": "userEnteredFormat(backgroundColor,textFormat)",
            }
        }
    )
    requests.append(
        {
            "updateDimensionProperties": {
                "range": {
                    "sheetId": sheet_id,
                    "dimension": "COLUMNS",
                    "startIndex": 0,
                    "endIndex": 1,
                },
                "properties": {"pixelSize": 240},
                "fields": "pixelSize",
            }
        }
    )
    requests.append(
        {
            "updateDimensionProperties": {
                "range": {
                    "sheetId": sheet_id,
                    "dimension": "COLUMNS",
                    "startIndex": 1,
                    "endIndex": 2,
                },
                "properties": {"pixelSize": 620},
                "fields": "pixelSize",
            }
        }
    )
    requests.append(
        {
            "updateDimensionProperties": {
                "range": {
                    "sheetId": sheet_id,
                    "dimension": "ROWS",
                    "startIndex": 0,
                    "endIndex": 1,
                },
                "properties": {"pixelSize": 38},
                "fields": "pixelSize",
            }
        }
    )
    if total_rows > 1:
        requests.append(
            {
                "updateDimensionProperties": {
                    "range": {
                        "sheetId": sheet_id,
                        "dimension": "ROWS",
                        "startIndex": 1,
                        "endIndex": total_rows,
                    },
                    "properties": {"pixelSize": 60},
                    "fields": "pixelSize",
                }
            }
        )

    if requests:
        service.spreadsheets().batchUpdate(
            spreadsheetId=spreadsheet_id,
            body={"requests": requests},
        ).execute()


def _number_format_requests(
    sheet_id: int,
    headers: Sequence[str],
    total_rows: int,
    header_row_number: int,
    summary_row_count: int,
    summary_rows: Sequence[Sequence[Any]],
) -> list[dict[str, Any]]:
    if total_rows <= 0:
        return []

    requests: list[dict[str, Any]] = []
    currency_headers = {"Cost", "Price"}
    integer_headers = {
        "Available",
        "Par Level",
        "Units Sold 7d",
        "Units Sold 14d",
        "Units Sold 30d",
    }
    decimal_headers: set[str] = set()
    date_headers: set[str] = set()

    for index, header in enumerate(headers):
        pattern = None
        if header in currency_headers:
            pattern = "$#,##0.00"
        elif header in integer_headers:
            pattern = "0"
        elif header in decimal_headers:
            pattern = "0.0"
        elif header in date_headers:
            pattern = "yyyy-mm-dd"
        if not pattern:
            continue

        requests.append(
            {
                "repeatCell": {
                    "range": {
                        "sheetId": sheet_id,
                        "startRowIndex": header_row_number,
                        "endRowIndex": total_rows,
                        "startColumnIndex": index,
                        "endColumnIndex": index + 1,
                    },
                    "cell": {
                        "userEnteredFormat": {
                            "numberFormat": {
                                "type": _number_format_type(pattern),
                                "pattern": pattern,
                            }
                        }
                    },
                    "fields": "userEnteredFormat.numberFormat",
                }
            }
        )

    if summary_row_count:
        summary_formats = {
            "Total Inventory Value": "$#,##0.00",
            "Total SKUs Considered": "0",
            "Total SKUs Needing Order": "0",
            "Total 7d Units Sold": "0",
            "Total 14d Units Sold": "0",
            "Total 30d Units Sold": "0",
            "Brands Included (count)": "0",
            "Vendors Included (count)": "0",
        }
        for row_index, row in enumerate(summary_rows):
            for column_index in range(0, len(row), 2):
                label = str(row[column_index]).strip() if column_index < len(row) else ""
                pattern = summary_formats.get(label)
                value_column_index = column_index + 1
                if value_column_index >= len(row):
                    continue
                requests.append(
                    {
                        "repeatCell": {
                            "range": {
                                "sheetId": sheet_id,
                                "startRowIndex": row_index,
                                "endRowIndex": row_index + 1,
                                "startColumnIndex": value_column_index,
                                "endColumnIndex": value_column_index + 1,
                            },
                            "cell": {
                                "userEnteredFormat": {
                                    "horizontalAlignment": "LEFT",
                                }
                            },
                            "fields": "userEnteredFormat.horizontalAlignment",
                        }
                    }
                )
                if not pattern:
                    continue
                requests.append(
                    {
                        "repeatCell": {
                            "range": {
                                "sheetId": sheet_id,
                                "startRowIndex": row_index,
                                "endRowIndex": row_index + 1,
                                "startColumnIndex": value_column_index,
                                "endColumnIndex": value_column_index + 1,
                            },
                            "cell": {
                                "userEnteredFormat": {
                                    "numberFormat": {
                                        "type": _number_format_type(pattern),
                                        "pattern": pattern,
                                    }
                                }
                            },
                            "fields": "userEnteredFormat.numberFormat",
                        }
                    }
                )
    return requests


def _conditional_format_requests(
    sheet_id: int,
    headers: Sequence[str],
    total_rows: int,
    header_row_number: int,
) -> list[dict[str, Any]]:
    if total_rows <= header_row_number:
        return []

    available_index = _safe_index(headers, "Available")
    par_index = _safe_index(headers, "Par Level")
    if available_index is None or par_index is None:
        return []

    start_row = header_row_number + 1
    data_range = {
        "sheetId": sheet_id,
        "startRowIndex": header_row_number,
        "endRowIndex": total_rows,
        "startColumnIndex": 0,
        "endColumnIndex": len(headers),
    }

    requests: list[dict[str, Any]] = []
    available_col = _column_letter(available_index + 1)
    par_col = _column_letter(par_index + 1)
    requests.append(
        {
            "addConditionalFormatRule": {
                "index": 0,
                "rule": {
                    "ranges": [data_range],
                    "booleanRule": {
                        "condition": {
                            "type": "CUSTOM_FORMULA",
                            "values": [{"userEnteredValue": f"=AND(${par_col}{start_row}>0,${available_col}{start_row}<=0)"}],
                        },
                        "format": {
                            "backgroundColor": _rgb("#FAD7D2"),
                            "textFormat": {"bold": True},
                        },
                    },
                },
            }
        }
    )
    requests.append(
        {
            "addConditionalFormatRule": {
                "index": 1,
                "rule": {
                    "ranges": [data_range],
                    "booleanRule": {
                        "condition": {
                            "type": "CUSTOM_FORMULA",
                            "values": [{"userEnteredValue": f"=AND(${par_col}{start_row}>0,${available_col}{start_row}>0,${available_col}{start_row}<(${par_col}{start_row}*0.5))"}],
                        },
                        "format": {
                            "backgroundColor": _rgb("#FCE8C9"),
                        },
                    },
                },
            }
        }
    )
    requests.append(
        {
            "addConditionalFormatRule": {
                "index": 2,
                "rule": {
                    "ranges": [data_range],
                    "booleanRule": {
                        "condition": {
                            "type": "CUSTOM_FORMULA",
                            "values": [{"userEnteredValue": f"=AND(${par_col}{start_row}>0,${available_col}{start_row}<${par_col}{start_row})"}],
                        },
                        "format": {
                            "backgroundColor": _rgb("#FFF2D9"),
                        },
                    },
                },
            }
        }
    )
    return requests


def _hidden_column_requests(sheet_id: int, headers: Sequence[str], hidden_headers: set[str]) -> list[dict[str, Any]]:
    requests: list[dict[str, Any]] = []
    for index, header in enumerate(headers):
        hidden = header in hidden_headers
        requests.append(
            {
                "updateDimensionProperties": {
                    "range": {
                        "sheetId": sheet_id,
                        "dimension": "COLUMNS",
                        "startIndex": index,
                        "endIndex": index + 1,
                    },
                    "properties": {"hiddenByUser": hidden},
                    "fields": "hiddenByUser",
                }
            }
        )
    return requests


def _data_alignment_requests(
    sheet_id: int,
    headers: Sequence[str],
    total_rows: int,
    header_row_number: int,
) -> list[dict[str, Any]]:
    if total_rows <= header_row_number:
        return []

    left_headers = {"Product"}
    requests: list[dict[str, Any]] = []
    for index, header in enumerate(headers):
        if header not in left_headers:
            continue
        requests.append(
            {
                "repeatCell": {
                    "range": {
                        "sheetId": sheet_id,
                        "startRowIndex": header_row_number,
                        "endRowIndex": total_rows,
                        "startColumnIndex": index,
                        "endColumnIndex": index + 1,
                    },
                    "cell": {
                        "userEnteredFormat": {
                            "horizontalAlignment": "LEFT",
                        }
                    },
                    "fields": "userEnteredFormat.horizontalAlignment",
                }
            }
        )
    return requests


def _column_width_requests(sheet_id: int, headers: Sequence[str]) -> list[dict[str, Any]]:
    widths = {
        "Row Key": 140,
        "Brand": 140,
        "Category": 120,
        "Product": 420,
        "Available": 90,
        "Par Level": 90,
        "Cost": 90,
        "Price": 90,
        "Units Sold 7d": 100,
        "Units Sold 14d": 105,
        "Units Sold 30d": 105,
    }
    requests: list[dict[str, Any]] = []
    for index, header in enumerate(headers):
        width = widths.get(str(header).strip(), 110)
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


def _preserve_manual_value(current_value: Any, preserved_value: Any) -> Any:
    text = str(current_value or "").strip()
    if text:
        return current_value
    return preserved_value


def _sheet_safe_value(value: Any) -> Any:
    if value is None:
        return ""
    if isinstance(value, np.generic):
        value = value.item()
    if isinstance(value, pd.Timestamp):
        if pd.isna(value):
            return ""
        return value.date().isoformat() if value.time() == datetime.min.time() else value.isoformat(sep=" ")
    if isinstance(value, datetime):
        return value.isoformat(sep=" ")
    if isinstance(value, date):
        return value.isoformat()
    if isinstance(value, (bool, int, float, str)):
        try:
            if pd.isna(value):
                return ""
        except Exception:
            pass
        return value
    try:
        if pd.isna(value):
            return ""
    except Exception:
        pass
    return str(value)


def build_summary_row_padding(row: Sequence[Any], width: int) -> list[Any]:
    padded = [_sheet_safe_value(value) for value in row]
    if len(padded) < width:
        padded.extend([""] * (width - len(padded)))
    return padded


def _sheet_start_range(title: str) -> str:
    escaped = str(title).replace("'", "''")
    return f"'{escaped}'!A1"


def _sheet_range(title: str) -> str:
    escaped = str(title).replace("'", "''")
    return f"'{escaped}'"


def _tab_color(sheet_kind: str) -> dict[str, float]:
    kind = sheet_kind.lower()
    if kind == "auto":
        return _rgb("#4B6A4F")
    if kind == "readme":
        return _rgb("#8A6B3D")
    return _rgb("#356A8A")


def _rgb(hex_color: str) -> dict[str, float]:
    value = hex_color.lstrip("#")
    return {
        "red": int(value[0:2], 16) / 255.0,
        "green": int(value[2:4], 16) / 255.0,
        "blue": int(value[4:6], 16) / 255.0,
    }


def _number_format_type(pattern: str) -> str:
    if "%" in pattern:
        return "PERCENT"
    if "yyyy" in pattern.lower():
        return "DATE"
    return "NUMBER"


def _safe_index(values: Sequence[str], target: str) -> int | None:
    try:
        return list(values).index(target)
    except ValueError:
        return None


def _column_letter(column_number: int) -> str:
    letters = []
    number = int(column_number)
    while number > 0:
        number, remainder = divmod(number - 1, 26)
        letters.append(chr(65 + remainder))
    return "".join(reversed(letters))


__all__ = [
    "authenticate_sheets",
    "build_readme_rows",
    "build_sheet_matrix",
    "build_summary_rows",
    "ensure_sheet",
    "get_sheet_info_by_title",
    "merge_preserved_review_columns",
    "move_latest_tabs_next_to_readme",
    "move_sheet_to_index",
    "parse_spreadsheet_target",
    "read_sheet_values",
    "upsert_readme_tab",
    "upsert_ordering_tab",
]
