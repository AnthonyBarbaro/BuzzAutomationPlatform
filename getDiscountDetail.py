#!/usr/bin/env python3

import argparse
import json
import os
import re
import time
import traceback
from concurrent.futures import ProcessPoolExecutor, as_completed
from datetime import datetime
from pathlib import Path
from typing import Dict, Optional, Sequence

import pandas as pd
from selenium import webdriver
from selenium.common.exceptions import (
    ElementClickInterceptedException,
    NoSuchElementException,
    StaleElementReferenceException,
    TimeoutException,
)
from selenium.webdriver.chrome.options import Options
from selenium.webdriver.chrome.service import Service
from selenium.webdriver.common.by import By
from selenium.webdriver.common.keys import Keys
from selenium.webdriver.support import expected_conditions as EC
from selenium.webdriver.support.ui import WebDriverWait
from webdriver_manager.chrome import ChromeDriverManager

import getSalesReport as sales_report


REPORT_NAME = "Discount Detail Report"
REPORT_SLUG = "discount-detail-report"
REPORTS_LIST_URL = "https://dusk.backoffice.dutchie.com/reports/marketing/reports"
REPORT_URL = f"{REPORTS_LIST_URL}/{REPORT_SLUG}"
EXPORT_ATTEMPTS_PER_STORE = 5
DEFAULT_BROWSER_WORKERS = 2
MAX_BROWSER_WORKERS = 3
BROWSER_SESSION_ATTEMPTS = 2
REPORT_READY_TIMEOUT_SECONDS = 90
DATE_INPUT_TIMEOUT_SECONDS = 45
DEFAULT_HEADER_ROW_INDEX = 4

BLOCKING_SELECTORS = [
    "div.notification",
    ".MuiSnackbar-root",
    ".MuiAlert-root",
    ".MuiBackdrop-root",
]


def _store_label(store_name: str) -> str:
    label = store_name.replace("Buzz Cannabis", "").strip()
    label = label.replace("(", "").replace(")", "")
    label = re.sub(r"^-+", "", label).strip()
    return (label or store_name).upper()


def _safe_filename(value: str) -> str:
    value = re.sub(r"\s+", " ", str(value or "").strip())
    return re.sub(r"[^a-zA-Z0-9 _\-\(\)\.]", "_", value)


def _store_code_for_name(store_name: str) -> str:
    return sales_report.store_abbr_map.get(
        store_name,
        re.sub(r"[^A-Z0-9]+", "", store_name.upper())[:8] or "STORE",
    )


def _selected_store_names(stores: Optional[Sequence[str]]) -> list[str]:
    if not stores:
        return list(sales_report.store_abbr_map.keys())

    by_code = {abbr.upper(): name for name, abbr in sales_report.store_abbr_map.items()}
    by_name = {name.lower(): name for name in sales_report.store_abbr_map}
    selected: list[str] = []
    for raw in stores:
        raw_text = str(raw).strip()
        if not raw_text:
            continue
        pieces = [raw_text] if raw_text.lower() in by_name else raw_text.replace(",", " ").split()
        for piece in pieces:
            value = piece.strip()
            if not value:
                continue
            store_name = by_code.get(value.upper()) or by_name.get(value.lower()) or value
            if store_name not in selected:
                selected.append(store_name)
    return selected


def discount_detail_filename(
    current_store: str,
    start_date: datetime,
    end_date: datetime,
    extension: str = ".xlsx",
) -> str:
    store_code = _store_code_for_name(current_store)
    nice = _store_label(current_store)
    ext = extension if extension.startswith(".") else f".{extension}"
    return _safe_filename(
        f"{store_code} - {REPORT_NAME} - {nice} - "
        f"{start_date:%Y-%m-%d}_to_{end_date:%Y-%m-%d}{ext}"
    )


def no_data_marker_filename(
    current_store: str,
    start_date: datetime,
    end_date: datetime,
) -> str:
    store_code = _store_code_for_name(current_store)
    nice = _store_label(current_store)
    return _safe_filename(
        f"{store_code} - {REPORT_NAME} - {nice} - "
        f"{start_date:%Y-%m-%d}_to_{end_date:%Y-%m-%d}.NO_DATA.json"
    )


def no_data_marker_path(
    current_store: str,
    start_date: datetime,
    end_date: datetime,
    output_dir: str | os.PathLike[str],
) -> Path:
    return Path(output_dir).resolve() / no_data_marker_filename(current_store, start_date, end_date)


def write_no_data_marker(
    current_store: str,
    start_date: datetime,
    end_date: datetime,
    output_dir: str | os.PathLike[str],
) -> Path:
    marker_path = no_data_marker_path(current_store, start_date, end_date, output_dir)
    marker_path.parent.mkdir(parents=True, exist_ok=True)
    payload = {
        "report": REPORT_NAME,
        "store_code": _store_code_for_name(current_store),
        "store_name": current_store,
        "start_date": start_date.strftime("%Y-%m-%d"),
        "end_date": end_date.strftime("%Y-%m-%d"),
        "status": "no_data",
        "created_at": datetime.now().isoformat(timespec="seconds"),
    }
    marker_path.write_text(json.dumps(payload, indent=2, sort_keys=True), encoding="utf-8")
    print(f"Saved no-data marker: {marker_path.name}")
    return marker_path


def launch_browser(download_dir: str | os.PathLike[str], assign_global_driver: bool = True):
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

    driver = webdriver.Chrome(service=Service(ChromeDriverManager().install()), options=chrome_options)
    driver.get(REPORT_URL)
    if assign_global_driver:
        sales_report.driver = driver
    return driver


def _canonical_text(value: str) -> str:
    return re.sub(r"[^a-z0-9]+", "", str(value or "").lower())


def wait_for_blocking_ui_to_clear(driver, timeout: int = 8) -> bool:
    end = time.time() + timeout
    while time.time() < end:
        visible = False
        for selector in BLOCKING_SELECTORS:
            try:
                elems = driver.find_elements(By.CSS_SELECTOR, selector)
            except Exception:
                elems = []
            for elem in elems:
                try:
                    if elem.is_displayed():
                        visible = True
                        break
                except StaleElementReferenceException:
                    continue
            if visible:
                break
        if not visible:
            return True
        time.sleep(0.25)
    return False


def open_report_page(driver, timeout: int = REPORT_READY_TIMEOUT_SECONDS) -> bool:
    if REPORT_SLUG not in str(driver.current_url):
        driver.get(REPORT_URL)
    if wait_for_report_page_ready(driver, timeout=timeout):
        return True

    driver.get(REPORTS_LIST_URL)
    try:
        WebDriverWait(driver, 30).until(
            EC.presence_of_element_located((By.XPATH, f"//*[normalize-space()='{REPORT_NAME}']"))
        )
        cells = driver.find_elements(By.XPATH, f"//*[normalize-space()='{REPORT_NAME}']")
        target = None
        for cell in cells:
            try:
                if cell.get_attribute("role") == "row":
                    target = cell
                    break
                target = cell.find_element(By.XPATH, './ancestor::*[@role="row"][1]')
                break
            except Exception:
                continue
        if target is None:
            return False
        driver.execute_script("arguments[0].scrollIntoView({block: 'center'});", target)
        time.sleep(0.3)
        try:
            target.click()
        except Exception:
            driver.execute_script("arguments[0].click();", target)
    except Exception:
        return False
    return wait_for_report_page_ready(driver, timeout=timeout)


def login_to_dutchie(driver) -> None:
    wait = WebDriverWait(driver, 20)
    username_value = getattr(sales_report, "username", "")
    password_value = getattr(sales_report, "password", "")
    username_input = wait.until(EC.element_to_be_clickable((By.CSS_SELECTOR, "input[data-testid='auth_input_username']")))
    password_input = wait.until(EC.element_to_be_clickable((By.CSS_SELECTOR, "input[data-testid='auth_input_password']")))

    for elem, value in [(username_input, username_value), (password_input, password_value)]:
        elem.click()
        elem.send_keys(Keys.CONTROL, "a")
        elem.send_keys(Keys.DELETE)
        elem.send_keys(value)
        driver.execute_script(
            """
            arguments[0].dispatchEvent(new Event('input', { bubbles: true }));
            arguments[0].dispatchEvent(new Event('change', { bubbles: true }));
            """,
            elem,
        )

    login_button = wait.until(EC.element_to_be_clickable((By.CSS_SELECTOR, "button[data-testid='auth_button_go-green']")))
    try:
        login_button.click()
    except Exception:
        driver.execute_script("arguments[0].click();", login_button)
    time.sleep(2)


def ensure_logged_in(driver, timeout: int = 90) -> bool:
    end = time.time() + timeout
    login_attempted = False
    while time.time() < end:
        if REPORT_SLUG not in str(driver.current_url):
            driver.get(REPORT_URL)

        if wait_for_report_page_ready(driver, timeout=5):
            return True

        try:
            username_inputs = driver.find_elements(By.CSS_SELECTOR, "input[data-testid='auth_input_username']")
            password_inputs = driver.find_elements(By.CSS_SELECTOR, "input[data-testid='auth_input_password']")
            if username_inputs and password_inputs and not login_attempted:
                login_attempted = True
                login_to_dutchie(driver)
                driver.get(REPORT_URL)
                if wait_for_report_page_ready(driver, timeout=20):
                    return True
        except Exception as exc:
            print(f"[DISCOUNT DETAIL EXPORT] Login/page readiness wait is still settling: {exc}")

        time.sleep(1.0)

    return open_report_page(driver, timeout=10)


def _visible_date_inputs(driver) -> list:
    candidates = []
    locators = [
        (By.CSS_SELECTOR, "input#input-input_"),
        (By.XPATH, "//input[@id='input-input_']"),
        (By.XPATH, "//input[contains(@placeholder, '/') or contains(@value, '/')]"),
    ]
    for by, locator in locators:
        try:
            candidates.extend(driver.find_elements(by, locator))
        except Exception:
            continue

    visible = []
    seen = set()
    for elem in candidates:
        try:
            key = elem.id
            placeholder = (elem.get_attribute("placeholder") or "").strip().lower()
            data_testid = (elem.get_attribute("data-testid") or "").strip().lower()
            input_type = (elem.get_attribute("type") or "").strip().lower()
            if "auth_input" in data_testid or placeholder in {"username", "password"} or input_type == "password":
                continue
            if key in seen or not elem.is_displayed() or not elem.is_enabled():
                continue
            seen.add(key)
            visible.append(elem)
        except StaleElementReferenceException:
            continue
    return visible


def wait_for_date_inputs(driver, timeout: int = DATE_INPUT_TIMEOUT_SECONDS) -> list:
    end = time.time() + timeout
    last_url = ""
    while time.time() < end:
        wait_for_blocking_ui_to_clear(driver, timeout=2)
        inputs = _visible_date_inputs(driver)
        if len(inputs) >= 2:
            return inputs[:2]
        last_url = driver.current_url
        time.sleep(0.5)
    raise TimeoutException(f"Could not find two date inputs on {REPORT_NAME}. Current URL: {last_url}")


def wait_for_report_page_ready(driver, timeout: int = REPORT_READY_TIMEOUT_SECONDS) -> bool:
    wait = WebDriverWait(driver, timeout)
    try:
        wait.until(lambda d: REPORT_SLUG in str(d.current_url))
        wait.until(EC.presence_of_element_located((By.CSS_SELECTOR, "[data-testid='header_select_location']")))
        wait_for_date_inputs(driver, timeout=min(timeout, DATE_INPUT_TIMEOUT_SECONDS))
        return True
    except TimeoutException:
        return False


def click_store_dropdown(driver) -> bool:
    dropdown_locators = [
        (By.CSS_SELECTOR, "[data-testid='header_select_location']"),
        (By.XPATH, "//div[@data-testid='header_select_location']"),
        (By.XPATH, "//button[@data-testid='header_select_location']"),
    ]
    last_error = None
    for by, locator in dropdown_locators:
        try:
            wait_for_blocking_ui_to_clear(driver, timeout=4)
            elem = WebDriverWait(driver, 15).until(EC.element_to_be_clickable((by, locator)))
            driver.execute_script("arguments[0].scrollIntoView({block: 'center'});", elem)
            time.sleep(0.2)
            try:
                elem.click()
            except Exception:
                driver.execute_script("arguments[0].click();", elem)
            time.sleep(0.8)
            if driver.find_elements(By.XPATH, "//li[@role='option' or @data-testid]"):
                return True
        except Exception as exc:
            last_error = exc
    print(f"Dropdown not found or not clickable: {last_error}")
    return False


def select_store(driver, store_name: str) -> bool:
    target = _canonical_text(store_name)
    for attempt in range(1, 4):
        try:
            if not open_report_page(driver):
                raise TimeoutException(f"{REPORT_NAME} page did not become ready.")
            if not click_store_dropdown(driver):
                raise TimeoutException("Store dropdown did not open.")

            options = driver.find_elements(By.XPATH, "//li[@role='option' or @data-testid]")
            option_texts = []
            exact_match = None
            partial_match = None
            for option in options:
                try:
                    text = (option.text or "").strip()
                    test_id = option.get_attribute("data-testid") or ""
                    option_texts.append(text or test_id)
                    option_key = _canonical_text(text or test_id)
                    if option_key == target:
                        exact_match = option
                        break
                    if target in option_key or option_key in target:
                        partial_match = partial_match or option
                except StaleElementReferenceException:
                    continue

            chosen = exact_match or partial_match
            if chosen is None:
                raise NoSuchElementException(
                    f"No dropdown option matched store '{store_name}'. Visible options: {option_texts}"
                )

            driver.execute_script("arguments[0].scrollIntoView({block: 'center'});", chosen)
            time.sleep(0.2)
            try:
                chosen.click()
            except Exception:
                driver.execute_script("arguments[0].click();", chosen)
            time.sleep(1.0)
            wait_for_date_inputs(driver)
            print(f"Selected store: {store_name}")
            return True
        except (TimeoutException, NoSuchElementException, ElementClickInterceptedException, StaleElementReferenceException) as exc:
            print(f"[WARN] Store selection attempt {attempt}/3 failed for '{store_name}': {exc}")
            driver.get(REPORT_URL)
            time.sleep(1.5)

    print(f"[ERROR] Could not select store '{store_name}' after retries.")
    return False


def set_date_range(driver, start_date: datetime, end_date: datetime) -> None:
    if not open_report_page(driver):
        raise TimeoutException(f"{REPORT_NAME} page did not become ready before setting dates.")

    start_input_str = start_date.strftime("%m/%d/%Y")
    end_input_str = end_date.strftime("%m/%d/%Y")
    date_inputs = wait_for_date_inputs(driver)

    for elem, value in zip(date_inputs[:2], [start_input_str, end_input_str]):
        driver.execute_script("arguments[0].scrollIntoView({block: 'center'});", elem)
        try:
            elem.click()
            elem.send_keys(Keys.CONTROL, "a")
            elem.send_keys(Keys.DELETE)
            elem.send_keys(value)
            elem.send_keys(Keys.TAB)
        except Exception:
            pass
        driver.execute_script(
            """
            const input = arguments[0];
            const value = arguments[1];
            const setter = Object.getOwnPropertyDescriptor(window.HTMLInputElement.prototype, 'value').set;
            setter.call(input, value);
            input.dispatchEvent(new Event('input', { bubbles: true }));
            input.dispatchEvent(new Event('change', { bubbles: true }));
            input.dispatchEvent(new Event('blur', { bubbles: true }));
            """,
            elem,
            value,
        )
        time.sleep(0.2)
    print(f"Set date range: {start_input_str} to {end_input_str}")
    time.sleep(0.8)


def click_run_button(driver) -> None:
    previous_driver = getattr(sales_report, "driver", None)
    sales_report.driver = driver
    try:
        sales_report.click_run_button()
    finally:
        sales_report.driver = previous_driver


def table_has_no_data(driver) -> bool:
    no_data_xpath = (
        "//*[contains(translate(normalize-space(), 'ABCDEFGHIJKLMNOPQRSTUVWXYZ', 'abcdefghijklmnopqrstuvwxyz'), 'no data') "
        "or contains(translate(normalize-space(), 'ABCDEFGHIJKLMNOPQRSTUVWXYZ', 'abcdefghijklmnopqrstuvwxyz'), 'no records') "
        "or contains(translate(normalize-space(), 'ABCDEFGHIJKLMNOPQRSTUVWXYZ', 'abcdefghijklmnopqrstuvwxyz'), 'no results')]"
    )
    try:
        nodes = driver.find_elements(By.XPATH, no_data_xpath)
    except Exception:
        return False
    for node in nodes:
        try:
            if node.is_displayed():
                return True
        except Exception:
            continue
    return False


def click_actions_and_export(current_store: str, start_date: datetime, end_date: datetime, output_dir: str | os.PathLike[str]) -> Optional[Path]:
    try:
        print(f"\n=== Exporting {REPORT_NAME} for store: {current_store} ===")
        files_dir = os.path.abspath(output_dir)
        before_snapshot = sales_report._snapshot_files(files_dir)

        if table_has_no_data(sales_report.driver):
            print(f"[DISCOUNT DETAIL EXPORT] No rows visible for {current_store}.")
            write_no_data_marker(current_store, start_date, end_date, files_dir)
            return None

        sales_report.robust_click(By.ID, "actions-menu-button", "Actions button", timeout=12, attempts=4)
        time.sleep(1)
        sales_report.robust_click(By.XPATH, "//li[contains(text(),'Export')]", "Export option", timeout=12, attempts=4)

        exported_path = sales_report._wait_for_downloaded_export(
            files_dir,
            before_snapshot,
            timeout=sales_report.EXPORT_DOWNLOAD_TIMEOUT_SECONDS,
        )
        if not exported_path:
            print(f"No discount detail export detected within {sales_report.EXPORT_DOWNLOAD_TIMEOUT_SECONDS}s.")
            return None

        extension = os.path.splitext(exported_path)[1] or ".xlsx"
        new_filename = discount_detail_filename(current_store, start_date, end_date, extension=extension)
        new_path = Path(files_dir) / new_filename
        if new_path.exists():
            new_path.unlink()
        os.rename(exported_path, new_path)
        if new_path.stat().st_size <= 0:
            print(f"[WARN] Downloaded discount detail file is empty: {new_path}")
            return None

        print(f"Saved discount detail report: {new_filename}")
        return new_path

    except TimeoutException:
        print("A discount detail report element could not be found or clicked within the timeout period.")
        return None
    except Exception:
        print(f"An error occurred during discount detail export: {traceback.format_exc()}")
        return None


def export_store_with_retries(
    current_store: str,
    start_date: datetime,
    end_date: datetime,
    output_dir: str | os.PathLike[str],
    attempts: int = EXPORT_ATTEMPTS_PER_STORE,
) -> Optional[Path]:
    for attempt in range(1, attempts + 1):
        print(f"[DISCOUNT DETAIL EXPORT] {current_store}: attempt {attempt}/{attempts}")
        try:
            if not select_store(sales_report.driver, current_store):
                raise TimeoutException(f"Could not select store '{current_store}' on {REPORT_NAME}.")
            set_date_range(sales_report.driver, start_date, end_date)
            click_run_button(sales_report.driver)
            exported = click_actions_and_export(current_store, start_date, end_date, output_dir)
            if exported:
                return exported
            if table_has_no_data(sales_report.driver):
                marker_path = no_data_marker_path(current_store, start_date, end_date, output_dir)
                if not marker_path.exists():
                    write_no_data_marker(current_store, start_date, end_date, output_dir)
                return None
        except Exception:
            print(f"[WARN] Discount detail export attempt {attempt} failed for {current_store}: {traceback.format_exc()}")
        time.sleep(3)
    return None


def _export_store_with_retries_with_driver(
    driver,
    current_store: str,
    start_date: datetime,
    end_date: datetime,
    output_dir: str | os.PathLike[str],
    attempts: int = EXPORT_ATTEMPTS_PER_STORE,
) -> Optional[Path]:
    previous_driver = getattr(sales_report, "driver", None)
    sales_report.driver = driver
    try:
        return export_store_with_retries(current_store, start_date, end_date, output_dir, attempts=attempts)
    finally:
        sales_report.driver = previous_driver


def _run_single_store_browser_export(
    store: str,
    start_date: datetime,
    end_date: datetime,
    output_dir: str | os.PathLike[str],
) -> tuple[str, Optional[Path], Optional[str]]:
    store_code = _store_code_for_name(store)
    store_dir = Path(output_dir).resolve() / f"browser_{store_code}"
    store_dir.mkdir(parents=True, exist_ok=True)

    last_error = None
    for browser_attempt in range(1, BROWSER_SESSION_ATTEMPTS + 1):
        driver = launch_browser(store_dir, assign_global_driver=False)
        try:
            previous_driver = getattr(sales_report, "driver", None)
            sales_report.driver = driver
            try:
                if not ensure_logged_in(driver):
                    last_error = (
                        f"{store}: {REPORT_NAME} page did not become ready "
                        f"(browser attempt {browser_attempt}/{BROWSER_SESSION_ATTEMPTS})"
                    )
                    continue
                exported = _export_store_with_retries_with_driver(driver, store, start_date, end_date, store_dir)
            finally:
                sales_report.driver = previous_driver

            if not exported:
                marker_path = no_data_marker_path(store, start_date, end_date, store_dir)
                if marker_path.exists():
                    final_marker = Path(output_dir).resolve() / marker_path.name
                    if final_marker.exists():
                        final_marker.unlink()
                    marker_path.replace(final_marker)
                return store_code, None, None

            final_path = Path(output_dir).resolve() / exported.name
            if final_path.exists():
                final_path.unlink()
            exported.replace(final_path)
            return store_code, final_path, None
        except Exception:
            last_error = f"{store}: {traceback.format_exc()}"
        finally:
            driver.quit()
        time.sleep(2)

    return store_code, None, last_error or f"{store}: {REPORT_NAME} browser session failed"


def run_discount_detail_report(
    start_date: datetime,
    end_date: datetime,
    output_dir: str | os.PathLike[str] = "files",
    stores: Optional[Sequence[str]] = None,
    fail_on_error: bool = True,
    workers: int = DEFAULT_BROWSER_WORKERS,
) -> Dict[str, Path]:
    output_path = Path(output_dir).resolve()
    output_path.mkdir(parents=True, exist_ok=True)
    selected_stores = _selected_store_names(stores)
    worker_count = max(1, min(int(workers or 1), len(selected_stores) or 1, MAX_BROWSER_WORKERS))

    exported_paths: Dict[str, Path] = {}
    failures: list[str] = []

    if worker_count > 1:
        print(f"[DISCOUNT DETAIL EXPORT] Using {worker_count} browser workers.")
        with ProcessPoolExecutor(max_workers=worker_count) as executor:
            future_map = {
                executor.submit(_run_single_store_browser_export, store, start_date, end_date, output_path): store
                for store in selected_stores
            }
            for future in as_completed(future_map):
                store = future_map[future]
                try:
                    result = future.result()
                except Exception:
                    failures.append(f"{store}: {traceback.format_exc()}")
                    continue
                if not isinstance(result, tuple) or len(result) != 3:
                    failures.append(f"{store}: unexpected worker result: {result!r}")
                    continue
                store_code, exported, error = result
                if error:
                    failures.append(error)
                elif exported:
                    exported_paths[store_code] = exported
    else:
        driver = launch_browser(output_path)
        try:
            if not ensure_logged_in(driver):
                failures.append(f"{REPORT_NAME} page did not become ready after login.")
            else:
                for store in selected_stores:
                    try:
                        exported = export_store_with_retries(store, start_date, end_date, output_path)
                        if exported:
                            exported_paths[_store_code_for_name(store)] = exported
                    except Exception:
                        failures.append(f"{store}: {traceback.format_exc()}")
        finally:
            driver.quit()

    if worker_count > 1:
        for store in selected_stores:
            try:
                temp_dir = output_path / f"browser_{_store_code_for_name(store)}"
                if temp_dir.exists():
                    for child in temp_dir.iterdir():
                        if child.is_file():
                            child.unlink()
                    temp_dir.rmdir()
            except OSError:
                pass

    if failures:
        message = "Discount detail export failed for: " + "; ".join(failures)
        if fail_on_error:
            raise RuntimeError(message)
        print(f"[WARN] {message}")

    return exported_paths


def _clean_columns(columns: Sequence[object]) -> list[str]:
    cleaned = []
    seen = {}
    for col in columns:
        text = re.sub(r"\s+", " ", str(col or "").strip())
        if not text or text.lower().startswith("unnamed"):
            text = "column"
        base = text
        count = seen.get(base.lower(), 0)
        seen[base.lower()] = count + 1
        if count:
            text = f"{base}_{count + 1}"
        cleaned.append(text)
    return cleaned


def detect_header_row(path: Path, sheet_name: str | int = 0, scan_rows: int = 12) -> int:
    preview = pd.read_excel(path, sheet_name=sheet_name, header=None, nrows=scan_rows)
    best_idx = DEFAULT_HEADER_ROW_INDEX
    best_score = -1
    expected = {
        "locationname",
        "orderid",
        "ordertime",
        "customername",
        "customercontact",
        "patientmedicalcardid",
        "caregiverid",
        "productname",
        "brandname",
        "packageid",
        "unitprice",
        "grosssales",
        "discountedamount",
        "discountpercent",
        "netsales",
        "discountname",
        "discountdescription",
        "discountcode",
        "budtendername",
        "responsibleforsale",
        "discountapprovedby",
        "vendorname",
        "discountappliedconsumergroup1",
        "discountappliedconsumergroup2",
        "discountappliedconsumergroup3",
        "discountappliedconsumergroup4",
        "discountappliedconsumergroup5",
    }
    for idx, row in preview.iterrows():
        values = {_canonical_text(v) for v in row.tolist() if str(v).strip() and str(v).lower() != "nan"}
        score = len(values & expected)
        if score > best_score:
            best_idx = int(idx)
            best_score = score
    return best_idx if best_score >= 3 else DEFAULT_HEADER_ROW_INDEX


def read_discount_detail_export(path: str | os.PathLike[str], header_row: Optional[int] = None) -> pd.DataFrame:
    report_path = Path(path)
    if not report_path.exists():
        raise FileNotFoundError(report_path)

    suffix = report_path.suffix.lower()
    if suffix in {".xlsx", ".xlsm", ".xls"}:
        row_idx = detect_header_row(report_path) if header_row is None else int(header_row)
        df = pd.read_excel(report_path, header=row_idx)
    elif suffix == ".csv":
        df = pd.read_csv(report_path)
    else:
        raise ValueError(f"Unsupported discount detail export file type: {report_path}")

    df = df.dropna(how="all").reset_index(drop=True)
    df.columns = _clean_columns(df.columns)
    return df


def parse_args():
    parser = argparse.ArgumentParser(description=f"Download Dutchie Backoffice {REPORT_NAME}.")
    parser.add_argument("--start-date", required=True, help="Start date in YYYY-MM-DD format.")
    parser.add_argument("--end-date", required=True, help="End date in YYYY-MM-DD format.")
    parser.add_argument("--output-dir", default="files", help="Directory where exports are saved.")
    parser.add_argument("--stores", nargs="*", help="Optional store codes or names, for example: MV LG.")
    parser.add_argument(
        "--strict",
        action="store_true",
        help="Fail the command if any selected store cannot export. By default, successful stores are kept and failures are warnings.",
    )
    parser.add_argument(
        "--workers",
        type=int,
        default=DEFAULT_BROWSER_WORKERS,
        help=f"Parallel browser sessions. Default: {DEFAULT_BROWSER_WORKERS}; max: {MAX_BROWSER_WORKERS}.",
    )
    return parser.parse_args()


def main():
    args = parse_args()
    start_dt = datetime.strptime(args.start_date, "%Y-%m-%d")
    end_dt = datetime.strptime(args.end_date, "%Y-%m-%d")
    exported = run_discount_detail_report(
        start_dt,
        end_dt,
        args.output_dir,
        args.stores,
        fail_on_error=args.strict,
        workers=args.workers,
    )
    print(f"[DISCOUNT DETAIL EXPORT] Exported {len(exported)} file(s).")


if __name__ == "__main__":
    main()
