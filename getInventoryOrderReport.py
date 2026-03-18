#!/usr/bin/env python3

import argparse
import os
import re
import time
import traceback
from datetime import date, datetime, timedelta

from selenium import webdriver
from selenium.common.exceptions import TimeoutException
from selenium.webdriver.chrome.options import Options
from selenium.webdriver.chrome.service import Service
from webdriver_manager.chrome import ChromeDriverManager

import getSalesReport as sales_report
from inventory_order_reports import ORDER_REPORT_WINDOWS, order_report_filename

REPORT_URL = "https://dusk.backoffice.dutchie.com/reports/inventory/reports/inventory-order-report"
EXPORT_ATTEMPTS_PER_WINDOW = 3


def compute_windows(anchor_day=None):
    end_day = anchor_day or date.today()
    windows = []
    for days in ORDER_REPORT_WINDOWS:
        start_day = end_day - timedelta(days=days)
        windows.append((days, start_day, end_day))
    return windows


def launch_browser(download_dir):
    os.makedirs(download_dir, exist_ok=True)

    chrome_options = Options()
    chrome_options.add_argument("--disable-gpu")
    chrome_options.add_argument("--disable-software-rasterizer")
    chrome_options.add_argument("--disable-features=VizDisplayCompositor")
    chrome_options.add_argument("--window-size=1920,1080")
    chrome_options.add_argument("--no-sandbox")
    chrome_options.add_argument("--disable-dev-shm-usage")

    if os.getenv("BUZZ_HEADLESS", "1").strip().lower() not in ("0", "false", "no"):
        chrome_options.add_argument("--headless=new")

    chrome_options.add_experimental_option("detach", True)
    chrome_options.add_experimental_option("excludeSwitches", ["enable-logging"])
    chrome_options.add_experimental_option(
        "prefs",
        {
            "download.default_directory": os.path.abspath(download_dir),
            "download.prompt_for_download": False,
            "download.directory_upgrade": True,
            "safebrowsing.enabled": True,
        },
    )

    driver = webdriver.Chrome(
        service=Service(ChromeDriverManager().install()),
        options=chrome_options,
    )
    driver.get(REPORT_URL)
    sales_report.driver = driver
    return driver


def click_actions_and_export(current_store, window_days, files_dir):
    try:
        print(f"\n=== Exporting inventory order report for {current_store} ({window_days}d) ===")

        before_snapshot = sales_report._snapshot_files(files_dir)
        print("Files before download:", set(before_snapshot.keys()))

        sales_report.robust_click(
            sales_report.By.ID,
            "actions-menu-button",
            "Actions button",
            timeout=12,
            attempts=4,
        )
        time.sleep(1)

        sales_report.robust_click(
            sales_report.By.XPATH,
            "//li[contains(text(),'Export')]",
            "Export option",
            timeout=12,
            attempts=4,
        )

        exported_path = sales_report._wait_for_downloaded_export(
            files_dir,
            before_snapshot,
            timeout=sales_report.EXPORT_DOWNLOAD_TIMEOUT_SECONDS,
        )
        if not exported_path:
            print(
                f"No inventory order export detected within "
                f"{sales_report.EXPORT_DOWNLOAD_TIMEOUT_SECONDS}s."
            )
            return False

        extension = os.path.splitext(exported_path)[1] or ".xlsx"
        new_filename = order_report_filename(current_store, window_days, extension=extension)
        new_path = os.path.join(files_dir, new_filename)

        if os.path.exists(new_path):
            os.remove(new_path)
        os.rename(exported_path, new_path)

        if os.path.getsize(new_path) <= 0:
            print(f"[WARN] Downloaded file is empty: {new_path}")
            return False

        print(f"Saved order report: {new_filename}")
        return True

    except TimeoutException:
        print("An inventory order element could not be found or clicked within the timeout period.")
        return False
    except Exception:
        print(f"An error occurred during order export: {traceback.format_exc()}")
        return False


def export_window_with_retries(current_store, window_days, start_date, end_date, files_dir):
    for attempt in range(1, EXPORT_ATTEMPTS_PER_WINDOW + 1):
        print(f"[EXPORT] {current_store} {window_days}d: attempt {attempt}/{EXPORT_ATTEMPTS_PER_WINDOW}")
        try:
            sales_report.set_date_range(start_date, end_date)
            sales_report.click_run_button()
            if click_actions_and_export(current_store, window_days, files_dir):
                return True
        except Exception:
            print(
                f"[WARN] Inventory order export attempt {attempt} failed for "
                f"{current_store} {window_days}d: {traceback.format_exc()}"
            )
        time.sleep(3)
    return False


def clear_existing_order_reports(files_dir):
    os.makedirs(files_dir, exist_ok=True)
    pattern = re.compile(r"^inventory_order_(7d|14d|30d)_[A-Za-z0-9]+\.(xlsx|xls|csv)$", re.IGNORECASE)
    for name in os.listdir(files_dir):
        if not pattern.match(name):
            continue
        path = os.path.join(files_dir, name)
        if os.path.isfile(path):
            os.remove(path)
            print(f"[INFO] Deleted old order report: {path}")


def run_inventory_order_report(output_dir="files", anchor_day=None, stores=None):
    files_dir = os.path.abspath(output_dir)
    clear_existing_order_reports(files_dir)

    store_names = stores or list(sales_report.store_abbr_map.keys())
    windows = compute_windows(anchor_day=anchor_day)

    driver = launch_browser(files_dir)
    failed_exports = []

    try:
        sales_report.login(driver)
        for store in store_names:
            if not sales_report.select_dropdown_item(store):
                failed_exports.append(f"{store}: store selection failed")
                continue

            for window_days, start_day, end_day in windows:
                ok = export_window_with_retries(
                    current_store=store,
                    window_days=window_days,
                    start_date=datetime.combine(start_day, datetime.min.time()),
                    end_date=datetime.combine(end_day, datetime.min.time()),
                    files_dir=files_dir,
                )
                if not ok:
                    failed_exports.append(
                        f"{store}: {window_days}d ({start_day.isoformat()} -> {end_day.isoformat()})"
                    )
    finally:
        driver.quit()

    if failed_exports:
        raise RuntimeError("Inventory order export failed for: " + ", ".join(failed_exports))


def parse_args():
    parser = argparse.ArgumentParser(
        description="Download Dutchie inventory order reports for 7d, 14d, and 30d windows."
    )
    parser.add_argument(
        "output_dir",
        nargs="?",
        default="files",
        help="Directory where the report exports will be saved (default: files)",
    )
    parser.add_argument(
        "--end-date",
        help="Anchor end date in YYYY-MM-DD format. Defaults to today.",
    )
    return parser.parse_args()


def main():
    args = parse_args()
    anchor_day = datetime.strptime(args.end_date, "%Y-%m-%d").date() if args.end_date else None
    run_inventory_order_report(output_dir=args.output_dir, anchor_day=anchor_day)


if __name__ == "__main__":
    main()
