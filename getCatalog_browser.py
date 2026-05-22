#!/usr/bin/env python3
"""
Legacy browser-based catalog exporter used as a fallback when the Dutchie API
path is unavailable or fails.
"""

from __future__ import annotations

import argparse
import os
import time
from datetime import datetime

from selenium import webdriver
from selenium.common.exceptions import TimeoutException
from selenium.webdriver.chrome.options import Options
from selenium.webdriver.chrome.service import Service
from selenium.webdriver.common.by import By
from selenium.webdriver.support import expected_conditions as EC
from selenium.webdriver.support.ui import WebDriverWait
from webdriver_manager.chrome import ChromeDriverManager

import getSalesReport as sales_report

CATALOG_URL = "https://dusk.backoffice.dutchie.com/products/catalog"
MIN_CATALOG_EXPORT_BYTES = 128
CATALOG_HEADER_MARKERS = ("SKU", "Product", "Price")
CATALOG_EXPORT_ATTEMPTS = 3
CATALOG_READY_TIMEOUT_SECONDS = 180
EXPORT_MODAL_TIMEOUT_SECONDS = 60
POST_STORE_SELECT_WAIT_SECONDS = 12
PRE_EXPORT_SETTLE_SECONDS = 20


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
    driver.get(CATALOG_URL)
    sales_report.driver = driver
    return driver


def is_valid_catalog_export(path):
    """
    Dutchie can occasionally return a tiny/blank CSV if export is clicked before
    the catalog table is ready. Do not let that replace a real store file.
    """
    try:
        if not path or not os.path.isfile(path):
            return False
        if os.path.getsize(path) < MIN_CATALOG_EXPORT_BYTES:
            return False
        with open(path, "r", encoding="utf-8-sig", errors="ignore") as handle:
            first_line = handle.readline()
    except OSError:
        return False

    return all(marker in first_line for marker in CATALOG_HEADER_MARKERS)


def wait_for_catalog_ready(timeout=CATALOG_READY_TIMEOUT_SECONDS):
    driver = sales_report.driver
    wait = WebDriverWait(driver, timeout)

    # The older working catalog exporter used a fixed sleep before exporting.
    # Dutchie's catalog grid is virtualized enough that row/text probes can lie,
    # especially on Mission Valley, so only require Actions to exist and then
    # give the store switch time to finish.
    wait.until(EC.element_to_be_clickable((By.ID, "actions-menu-button")))
    time.sleep(PRE_EXPORT_SETTLE_SECONDS)


def rename_catalog_export(downloaded_path, current_store, output_dir):
    if not is_valid_catalog_export(downloaded_path):
        raise RuntimeError(
            f"Downloaded catalog export for {current_store} was blank or invalid: {downloaded_path}"
        )

    extension = os.path.splitext(downloaded_path)[1] or ".csv"
    today_str = datetime.now().strftime("%m-%d-%Y")
    store_abbr = sales_report.store_abbr_map.get(current_store, "UNK")
    destination = os.path.join(output_dir, f"{today_str}_{store_abbr}{extension}")
    temp_destination = f"{destination}.tmp"

    if os.path.exists(temp_destination):
        os.remove(temp_destination)
    os.rename(downloaded_path, temp_destination)

    if os.path.exists(destination):
        os.remove(destination)
    os.rename(temp_destination, destination)
    print(f"[SAVED] {current_store}: {destination}")


def click_actions_and_export(current_store, output_dir):
    driver = sales_report.driver
    wait = WebDriverWait(driver, EXPORT_MODAL_TIMEOUT_SECONDS)

    try:
        print(f"[WAIT] {current_store}: waiting for catalog table to load...")
        wait_for_catalog_ready()
        before_snapshot = sales_report._snapshot_files(output_dir)
        sales_report.robust_click(
            By.ID,
            "actions-menu-button",
            "Actions button",
            timeout=EXPORT_MODAL_TIMEOUT_SECONDS,
            attempts=4,
        )
        time.sleep(2)

        export_option = wait.until(
            EC.element_to_be_clickable(
                (By.CSS_SELECTOR, "li[data-testid='catalog-list-actions-menu-item-export']")
            )
        )
        driver.execute_script("arguments[0].click();", export_option)

        export_csv_button = wait.until(
            EC.element_to_be_clickable(
                (By.CSS_SELECTOR, "[data-testid='export-table-modal-export-csv-button']")
            )
        )
        driver.execute_script("arguments[0].click();", export_csv_button)

        downloaded_path = sales_report._wait_for_downloaded_export(
            output_dir,
            before_snapshot,
            timeout=sales_report.EXPORT_DOWNLOAD_TIMEOUT_SECONDS,
        )
        if not downloaded_path:
            raise RuntimeError(f"No catalog export was downloaded for {current_store}.")

        rename_catalog_export(downloaded_path, current_store, output_dir)
        return True
    except TimeoutException as exc:
        raise RuntimeError(f"Catalog export timed out for {current_store}: {exc}") from exc


def resolve_store_names(raw_values):
    if not raw_values:
        return list(sales_report.store_abbr_map.keys())

    code_to_name = {abbr.upper(): name for name, abbr in sales_report.store_abbr_map.items()}
    resolved = []
    for raw in raw_values:
        value = str(raw).strip()
        if not value:
            continue
        resolved.append(code_to_name.get(value.upper(), value))
    return resolved


def run_catalog_browser_export(output_dir="files", stores=None):
    output_dir = os.path.abspath(output_dir)
    os.makedirs(output_dir, exist_ok=True)
    store_names = resolve_store_names(stores)

    driver = launch_browser(output_dir)
    failures = []

    try:
        sales_report.login(driver)
        for store_name in store_names:
            print(f"[FETCH] Browser catalog export for {store_name}")
            if not sales_report.select_dropdown_item(store_name):
                failures.append(f"{store_name}: store selection failed")
                continue
            print(f"[WAIT] {store_name}: letting store catalog settle for {POST_STORE_SELECT_WAIT_SECONDS}s")
            time.sleep(POST_STORE_SELECT_WAIT_SECONDS)
            last_error = None
            for attempt in range(1, CATALOG_EXPORT_ATTEMPTS + 1):
                try:
                    print(f"[EXPORT] {store_name}: attempt {attempt}/{CATALOG_EXPORT_ATTEMPTS}")
                    click_actions_and_export(store_name, output_dir)
                    last_error = None
                    break
                except Exception as exc:
                    last_error = exc
                    print(f"[WARN] {store_name}: catalog export attempt {attempt} failed: {exc}")
                    time.sleep(4)
            if last_error is not None:
                failures.append(f"{store_name}: {last_error}")
    finally:
        driver.quit()

    if failures:
        raise RuntimeError("Browser catalog export failed for: " + "; ".join(failures))


def parse_args():
    parser = argparse.ArgumentParser(
        description="Download Dutchie catalog CSVs through the legacy browser export flow."
    )
    parser.add_argument(
        "output_dir",
        nargs="?",
        default="files",
        help="Directory where the catalog CSV exports will be saved (default: files)",
    )
    parser.add_argument(
        "--stores",
        nargs="*",
        help="Optional store codes or store names to export.",
    )
    return parser.parse_args()


def main():
    args = parse_args()
    run_catalog_browser_export(output_dir=args.output_dir, stores=args.stores)


if __name__ == "__main__":
    main()
