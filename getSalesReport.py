import os
import re
import subprocess
import tkinter as tk
from tkinter import filedialog, messagebox
from tkinter.ttk import Combobox
import pandas as pd
from openpyxl import load_workbook
from openpyxl.styles import Font, Alignment, PatternFill
import traceback
from datetime import datetime, timedelta
import calendar
import time
from selenium import webdriver
from selenium.webdriver.chrome.service import Service
from selenium.webdriver.common.keys import Keys
from webdriver_manager.chrome import ChromeDriverManager
from selenium.webdriver.chrome.options import Options
from selenium.webdriver.support.ui import WebDriverWait
from selenium.webdriver.common.by import By
from selenium.webdriver.support import expected_conditions as EC
from selenium.common.exceptions import (
    NoSuchElementException,
    TimeoutException,
    ElementClickInterceptedException,
    StaleElementReferenceException,
)
from login import username, password

CONFIG_FILE = "config.txt"
INPUT_COLUMNS = ['Available', 'Product', 'Category', 'Brand']

store_abbr_map = {
    "Buzz Cannabis - Mission Valley": "MV",
    "Buzz Cannabis-La Mesa": "LM",
    "Buzz Cannabis - SORRENTO VALLEY" : "SV",
    "Buzz Cannabis - Lemon Grove" : "LG",
    "Buzz Cannabis (National City)" : "NC",  # ✅ Add this line
    "Buzz Cannabis Wildomar Palomar" : "WP"
}

start_str = None
end_str = None
driver = None

BLOCKING_SELECTORS = [
    "div.notification",
    ".MuiSnackbar-root",
    ".MuiAlert-root",
    ".MuiBackdrop-root",
]
REPORT_READY_TIMEOUT_SECONDS = 180
EXPORT_DOWNLOAD_TIMEOUT_SECONDS = 300
EXPORT_ATTEMPTS_PER_STORE = 3

LOADING_SELECTORS = [
    "[data-testid='loading-spinner_icon']",
    "[aria-label='Loading'][aria-valuetext='Loading']",
]


def _is_loading_data_visible():
    """
    Returns True if the report loading indicator is currently visible.
    """
    for selector in LOADING_SELECTORS:
        try:
            elems = driver.find_elements(By.CSS_SELECTOR, selector)
        except Exception:
            elems = []
        for elem in elems:
            try:
                if elem.is_displayed():
                    return True
            except StaleElementReferenceException:
                continue

    # Fallback: visible text node used by the UI while loading.
    try:
        text_nodes = driver.find_elements(
            By.XPATH,
            "//*[contains(normalize-space(), 'Loading data...')]",
        )
    except Exception:
        text_nodes = []
    for node in text_nodes:
        try:
            if node.is_displayed():
                return True
        except StaleElementReferenceException:
            continue

    return False


def wait_for_loading_data_cycle(timeout=REPORT_READY_TIMEOUT_SECONDS, appear_wait=10, stable_seconds=1.2):
    """
    After clicking Run, wait for loading to complete before export actions.
    - If loading appears, require it to disappear and stay clear briefly.
    - If loading never appears within appear_wait, continue.
    """
    start = time.time()
    saw_loading = False
    clear_since = None

    while time.time() - start < timeout:
        loading_visible = _is_loading_data_visible()

        if loading_visible:
            saw_loading = True
            clear_since = None
            time.sleep(0.25)
            continue

        if saw_loading:
            if clear_since is None:
                clear_since = time.time()
            elif (time.time() - clear_since) >= stable_seconds:
                return True
        elif (time.time() - start) >= appear_wait:
            return True

        time.sleep(0.25)

    return False


def _wait_for_blocking_ui(timeout=8):
    """
    Wait briefly for toast/snackbar/backdrop overlays that can intercept clicks.
    """
    end = time.time() + timeout
    while time.time() < end:
        visible_blocker = False
        for selector in BLOCKING_SELECTORS:
            try:
                elems = driver.find_elements(By.CSS_SELECTOR, selector)
            except Exception:
                elems = []
            for elem in elems:
                try:
                    if elem.is_displayed():
                        visible_blocker = True
                        break
                except StaleElementReferenceException:
                    continue
            if visible_blocker:
                break
        if not visible_blocker:
            return True
        time.sleep(0.2)
    return False

def robust_click(by, locator, label, timeout=12, attempts=4):
    """
    Click helper with retry + JS fallback for intercepted clicks.
    """
    last_error = None
    for attempt in range(1, attempts + 1):
        try:
            _wait_for_blocking_ui(timeout=4)
            wait = WebDriverWait(driver, timeout)
            elem = wait.until(EC.presence_of_element_located((by, locator)))
            driver.execute_script("arguments[0].scrollIntoView({block: 'center', inline: 'center'});", elem)
            time.sleep(0.2)
            elem = wait.until(EC.element_to_be_clickable((by, locator)))
            elem.click()
            return True
        except (ElementClickInterceptedException, StaleElementReferenceException, TimeoutException) as e:
            last_error = e
            try:
                elem = driver.find_element(by, locator)
                driver.execute_script("arguments[0].scrollIntoView({block: 'center', inline: 'center'});", elem)
                driver.execute_script("arguments[0].click();", elem)
                return True
            except Exception as js_e:
                last_error = js_e
                print(f"[WARN] {label} click attempt {attempt}/{attempts} failed: {js_e}")
                time.sleep(0.6)

    raise TimeoutException(f"Could not click '{label}' after {attempts} attempts: {last_error}")

def wait_for_report_ready(timeout=REPORT_READY_TIMEOUT_SECONDS):
    """
    After clicking Run, wait until UI is ready for Actions/Export.
    """
    end = time.time() + timeout
    while time.time() < end:
        _wait_for_blocking_ui(timeout=1)
        if _is_loading_data_visible():
            time.sleep(0.35)
            continue
        try:
            actions_btn = driver.find_element(By.ID, "actions-menu-button")
            if not actions_btn.is_displayed():
                time.sleep(0.5)
                continue

            is_enabled = actions_btn.is_enabled()
            disabled_attr = str(actions_btn.get_attribute("disabled") or "").lower()
            aria_disabled = str(actions_btn.get_attribute("aria-disabled") or "").lower()
            aria_busy = str(actions_btn.get_attribute("aria-busy") or "").lower()

            if (
                is_enabled
                and disabled_attr not in ("true", "disabled")
                and aria_disabled != "true"
                and aria_busy != "true"
            ):
                return True
        except (NoSuchElementException, StaleElementReferenceException):
            pass
        time.sleep(0.5)
    return False

def wait_for_new_file(download_directory, before_files, timeout=12):
    end_time = time.time() + timeout
    while time.time() < end_time:
        after_files = set(os.listdir(download_directory))
        new_files = after_files - before_files
        if new_files:
            return list(new_files)[0]
        time.sleep(1)
    return None

def launchBrowser():
    files_dir = os.path.join(os.path.dirname(os.path.abspath(__file__)), "files")
    os.makedirs(files_dir, exist_ok=True)

    chrome_options = Options()

    # ---- Stability fixes for Ubuntu ----
    chrome_options.add_argument("--disable-gpu")
    chrome_options.add_argument("--disable-software-rasterizer")
    chrome_options.add_argument("--disable-features=VizDisplayCompositor")

    # Fixed window size instead of maximize (prevents compositor redraws)
    chrome_options.add_argument("--window-size=1920,1080")

    # Other stability flags
    chrome_options.add_argument("--no-sandbox")
    chrome_options.add_argument("--disable-dev-shm-usage")
    chrome_options.add_argument("--headless=new")
    # Keep browser open after script (your existing behavior)
    chrome_options.add_experimental_option("detach", True)
    chrome_options.add_experimental_option("excludeSwitches", ["enable-logging"])

    prefs = {
        "download.default_directory": files_dir,
        "download.prompt_for_download": False,
        "download.directory_upgrade": True,
        "safebrowsing.enabled": True
    }
    chrome_options.add_experimental_option("prefs", prefs)

    driver = webdriver.Chrome(
        service=Service(ChromeDriverManager().install()),
        options=chrome_options
    )

    driver.get("https://dusk.backoffice.dutchie.com/reports/sales/reports/sales-report")
    return driver

def login(driver):
    wait = WebDriverWait(driver, 10)
    wait.until(EC.element_to_be_clickable((By.CSS_SELECTOR, "input[data-testid='auth_input_username']"))).send_keys(username)
    wait.until(EC.element_to_be_clickable((By.CSS_SELECTOR, "input[data-testid='auth_input_password']"))).send_keys(password)
    login_button = wait.until(EC.element_to_be_clickable((By.CSS_SELECTOR, "button[data-testid='auth_button_go-green']")))
    login_button.click()
    time.sleep(1)

def click_dropdown():
    """ Clicks the store dropdown to open the list of options. """
    wait = WebDriverWait(driver, 10)
    dropdown_xpath = "//div[@data-testid='header_select_location']"
    
    try:
        # Wait for dropdown to be clickable
        dropdown = wait.until(EC.element_to_be_clickable((By.XPATH, dropdown_xpath)))
        driver.execute_script("arguments[0].scrollIntoView({behavior: 'smooth', block: 'center'});", dropdown)
        dropdown.click()
        time.sleep(2)  # Small delay to allow options to load
    except TimeoutException:
        print("Dropdown not found or not clickable")

def select_dropdown_item(item_text):
    """ Selects the given store from the dropdown menu. """
    wait = WebDriverWait(driver, 10)
    
    try:
        click_dropdown()  # Open the dropdown first

        # Ensure store names match exact `data-testid` attribute
        formatted_text = item_text.replace(" ", "-")  # Ensure matching format for testid
        item_xpath = f"//li[@data-testid='rebrand-header_menu-item_{item_text}']"

        # Wait for the store option to be visible and clickable
        item = wait.until(EC.element_to_be_clickable((By.XPATH, item_xpath)))

        # Scroll into view in case it's hidden
        driver.execute_script("arguments[0].scrollIntoView({behavior: 'smooth', block: 'center'});", item)
        time.sleep(0.5)  # Allow animation delay

        # Click using JavaScript (useful if Selenium `.click()` doesn’t work)
        driver.execute_script("arguments[0].click();", item)
        print(f"Selected store: {item_text}")

        time.sleep(1)  # Give time for selection to register
        return True
    except (TimeoutException, NoSuchElementException) as e:
        print(f"Error selecting store '{item_text}': {e}")
        return False
def set_date_range(start_date, end_date):
    global start_str, end_str

    start_str = start_date.strftime("%m-%d-%Y")
    end_str = end_date.strftime("%m-%d-%Y")

    start_input_str = start_date.strftime("%m/%d/%Y")
    end_input_str = end_date.strftime("%m/%d/%Y")

    wait = WebDriverWait(driver, 10)
    date_inputs = wait.until(EC.presence_of_all_elements_located((By.ID, "input-input_")))
    

    # Clear and input start date
    date_inputs[0].send_keys(Keys.CONTROL, "a")
    date_inputs[0].send_keys(Keys.DELETE)
    date_inputs[0].send_keys(start_input_str)

    # Clear and input end date
    date_inputs[1].send_keys(Keys.CONTROL, "a")
    date_inputs[1].send_keys(Keys.DELETE)
    date_inputs[1].send_keys(end_input_str)

    print(f"Set date range: {start_input_str} to {end_input_str}")
    time.sleep(1)

def click_run_button():
    robust_click(By.XPATH, "//button[contains(normalize-space(),'Run')]", "Run button", timeout=15, attempts=5)
    print("Run button clicked successfully.")

    # Wait for "Loading data..." to complete before touching Actions/Export.
    if not wait_for_loading_data_cycle():
        raise TimeoutException("Loading data did not finish after clicking Run.")

    # Then wait for Actions button to be genuinely ready.
    if not wait_for_report_ready():
        raise TimeoutException("Report did not reach ready state after clicking Run.")

def monitor_folder_for_new_file(folder_path, before_files, timeout=120):
    """Monitor a folder for new files."""
    start_time = time.time()
    while time.time() - start_time < timeout:
        current_files = set(os.listdir(folder_path))
        new_files = current_files - before_files
        if new_files:
            # Return the first fully downloaded file
            for file in new_files:
                if not file.endswith('.crdownload'):  # Exclude partially downloaded files
                    return file
        time.sleep(1)
    return None

def wait_until_file_is_stable(file_path, stable_time=2, max_wait=30):
    """Wait until a file's size is stable."""
    start_time = time.time()
    last_size = -1
    stable_start = None

    while True:
        try:
            current_size = os.path.getsize(file_path)
        except FileNotFoundError:
            current_size = -1

        if current_size == last_size and current_size != -1:
            if stable_start is None:
                stable_start = time.time()
            elif time.time() - stable_start >= stable_time:
                return True
        else:
            stable_start = None

        last_size = current_size
        if time.time() - start_time > max_wait:
            return False
        time.sleep(1)

def _snapshot_files(folder_path):
    snap = {}
    for name in os.listdir(folder_path):
        path = os.path.join(folder_path, name)
        if not os.path.isfile(path):
            continue
        try:
            snap[name] = (os.path.getmtime(path), os.path.getsize(path))
        except OSError:
            continue
    return snap

def _expected_store_filename(current_store):
    if current_store == "Buzz Cannabis - Mission Valley":
        return "salesMV.xlsx"
    if current_store == "Buzz Cannabis-La Mesa":
        return "salesLM.xlsx"
    if current_store == "Buzz Cannabis - SORRENTO VALLEY":
        return "salesSV.xlsx"
    if current_store == "Buzz Cannabis - Lemon Grove":
        return "salesLG.xlsx"
    if current_store == "Buzz Cannabis (National City)":
        return "salesNC.xlsx"
    if current_store == "Buzz Cannabis Wildomar Palomar":
        return "salesWP.xlsx"
    timestamp = datetime.now().strftime("%Y%m%d%H%M%S")
    safe_name = re.sub(r"[^a-zA-Z0-9]+", "_", current_store).strip("_")
    return f"sales_{safe_name}_{timestamp}.xlsx"

def _wait_for_downloaded_export(files_dir, before_snapshot, timeout=EXPORT_DOWNLOAD_TIMEOUT_SECONDS):
    """
    Wait for a finished export file by detecting either:
    - a new file, or
    - an existing file whose mtime/size changed.
    """
    end = time.time() + timeout
    while time.time() < end:
        snap = _snapshot_files(files_dir)
        active_partial = any(
            name.endswith(".crdownload") or name.endswith(".tmp")
            for name in snap.keys()
        )

        changed_paths = []
        for name, stat in snap.items():
            if name.endswith(".crdownload") or name.endswith(".tmp"):
                continue
            prev = before_snapshot.get(name)
            if prev is None or prev != stat:
                changed_paths.append((os.path.join(files_dir, name), stat[0]))

        if changed_paths and not active_partial:
            changed_paths.sort(key=lambda x: x[1], reverse=True)
            for path, _ in changed_paths:
                if not os.path.isfile(path):
                    continue
                if wait_until_file_is_stable(path, stable_time=2, max_wait=25):
                    return path
        time.sleep(1)
    return None

def clickActionsAndExport(current_store):
    try:
        print(f"\n=== Exporting data for store: {current_store} ===")
        files_dir = os.path.join(os.path.dirname(os.path.abspath(__file__)), "files")
        os.makedirs(files_dir, exist_ok=True)

        # Capture initial state of the download folder
        before_snapshot = _snapshot_files(files_dir)
        print("Files before download:", set(before_snapshot.keys()))

        # Click the Actions button
        robust_click(By.ID, "actions-menu-button", "Actions button", timeout=12, attempts=4)
        print("Actions button clicked successfully.")
        time.sleep(1)

        # Select the Export option
        robust_click(By.XPATH, "//li[contains(text(),'Export')]", "Export option", timeout=12, attempts=4)
        print("Export option clicked successfully.")
        exported_path = _wait_for_downloaded_export(
            files_dir,
            before_snapshot,
            timeout=EXPORT_DOWNLOAD_TIMEOUT_SECONDS,
        )

        if not exported_path:
            print(f"No export file detected within {EXPORT_DOWNLOAD_TIMEOUT_SECONDS}s.")
            print("Files after download:", set(_snapshot_files(files_dir).keys()))
            return False

        print(f"Export file detected: {os.path.basename(exported_path)}")
        new_filename = _expected_store_filename(current_store)
        new_path = os.path.join(files_dir, new_filename)

        # Replace old target if present, then move into canonical store filename.
        try:
            if os.path.exists(new_path):
                os.remove(new_path)
            os.rename(exported_path, new_path)
            if os.path.getsize(new_path) <= 0:
                print(f"[WARN] Downloaded file is empty: {new_path}")
                return False
            print(f"Renamed file to: {new_filename}")
            return True
        except Exception as e:
            print(f"Error renaming file: {e}")
            return False

    except TimeoutException:
        print("An element could not be found or clicked within the timeout period.")
        return False
    except Exception as e:
        print(f"An error occurred during export: {traceback.format_exc()}")
        return False

def export_store_with_retries(current_store, start_date, end_date, attempts=EXPORT_ATTEMPTS_PER_STORE):
    for attempt in range(1, attempts + 1):
        print(f"[EXPORT] {current_store}: attempt {attempt}/{attempts}")
        try:
            set_date_range(start_date, end_date)
            click_run_button()
            if clickActionsAndExport(current_store):
                return True
        except Exception:
            print(f"[WARN] Export attempt {attempt} failed for {current_store}: {traceback.format_exc()}")
        time.sleep(3)
    return False

def update_days_combobox(year_combo, month_combo, day_combo):
    # Weekday abbreviations
    weekday_abbr = ["Mon", "Tue", "Wed", "Thu", "Fri", "Sat", "Sun"]

    y = int(year_combo.get())
    m = month_combo.current()+1
    now = datetime.now()
    # last day of month
    last_day = calendar.monthrange(y, m)[1]

    # If current month & year, limit days to today
    if y == now.year and m == now.month:
        last_day = min(last_day, now.day)

    # build list with weekdays
    day_values = []
    for day_num in range(1, last_day+1):
        dt = datetime(y, m, day_num)
        wday_abbr = weekday_abbr[dt.weekday()]  # Monday=0
        day_values.append(f"{day_num} ({wday_abbr})")

    day_combo['values'] = day_values
    # if current selection too large, reset
    current_day_idx = day_combo.current()
    if current_day_idx == -1 or current_day_idx >= len(day_values):
        day_combo.current(0)

def create_store_checkboxes(frame):
    """
    Creates three checkboxes for the three store locations,
    with each checkbox selected by default.
    Returns a dict of {store_name: IntVar}.
    """
    store_vars = {}

    # Mission Valley
    varMV = tk.IntVar(value=1)
    cbMV = tk.Checkbutton(frame, text="Buzz Cannabis - Mission Valley", variable=varMV)
    cbMV.pack(anchor='w')
    store_vars["Buzz Cannabis - Mission Valley"] = varMV

    # La Mesa
    varLM = tk.IntVar(value=1)
    cbLM = tk.Checkbutton(frame, text="Buzz Cannabis-La Mesa", variable=varLM)
    cbLM.pack(anchor='w')
    store_vars["Buzz Cannabis-La Mesa"] = varLM

    # Sorrento Valley
    varSV = tk.IntVar(value=1)
    cbSV = tk.Checkbutton(frame, text="Buzz Cannabis - SORRENTO VALLEY", variable=varSV)
    cbSV.pack(anchor='w')
    store_vars["Buzz Cannabis - SORRENTO VALLEY"] = varSV
    
    # Lemon Grove
    varLG = tk.IntVar(value=1)
    cbLG = tk.Checkbutton(frame, text="Buzz Cannabis - Lemon Grove", variable=varLG)
    cbLG.pack(anchor='w')
    store_vars["Buzz Cannabis - Lemon Grove"] = varLG
    
    # National City

    varNC = tk.IntVar(value=1)
    cbNC = tk.Checkbutton(frame, text="Buzz Cannabis (National City)", variable=varNC)
    cbNC.pack(anchor='w')
    store_vars["Buzz Cannabis (National City)"] = varNC  # ✅ Add this line
    # Wildomar Palomar
    varWP = tk.IntVar(value=1)
    cbWP = tk.Checkbutton(frame,
            text="Buzz Cannabis Wildomar Palomar", variable=varWP)
    cbWP.pack(anchor='w')
    store_vars["Buzz Cannabis Wildomar Palomar"] = varWP
    return store_vars
def open_gui_and_run():
    root = tk.Tk()
    root.title("Select Date Range")

    this_year = datetime.now().year
    YEAR_RANGE = [str(this_year-1), str(this_year)]
    MONTHS = ["January","February","March","April","May","June","July","August","September","October","November","December"]

    def create_date_selector(frame, label_text):
        tk.Label(frame, text=label_text, font=("Arial", 12, "bold")).pack(pady=(10,5))
        
        subframe = tk.Frame(frame)
        subframe.pack(pady=5)

        year_combo = Combobox(subframe, values=YEAR_RANGE, state='readonly', width=8)
        year_combo.current(YEAR_RANGE.index(str(this_year)))
        year_combo.grid(row=0, column=0, padx=5)

        month_combo = Combobox(subframe, values=MONTHS, state='readonly', width=10)
        current_month = datetime.now().month
        month_combo.current(current_month-1)
        month_combo.grid(row=0, column=1, padx=5)

        day_combo = Combobox(subframe, state='readonly', width=10)  # widened to accommodate " (Mon)"
        day_combo.grid(row=0, column=2, padx=5)

        def on_year_month_change(*args):
            update_days_combobox(year_combo, month_combo, day_combo)

        year_combo.bind("<<ComboboxSelected>>", on_year_month_change)
        month_combo.bind("<<ComboboxSelected>>", on_year_month_change)

        # initial populate days
        update_days_combobox(year_combo, month_combo, day_combo)
        selected_year = int(year_combo.get())
        selected_month = month_combo.current()+1
        now = datetime.now()
        if selected_year == now.year and selected_month == now.month:
            today_day = now.day
            day_combo.current(today_day-1)
        else:
            day_combo.current(0)

        return year_combo, month_combo, day_combo

    # GUI Layout
    main_frame = tk.Frame(root)
    main_frame.pack(pady=20, padx=20)

    # Create date selectors
    start_year_combo, start_month_combo, start_day_combo = create_date_selector(main_frame, "Select Start Date:")
    end_year_combo, end_month_combo, end_day_combo = create_date_selector(main_frame, "Select End Date:")

    # Create checkboxes for selecting stores
    tk.Label(main_frame, text="Select Store(s):", font=("Arial", 12, "bold")).pack(pady=(10,5), anchor='w')
    store_vars = create_store_checkboxes(main_frame)

    def on_ok():
        # Gather date info
        sy = int(start_year_combo.get())
        sm = start_month_combo.current()+1
        sday_str = start_day_combo.get().split()[0]  # "1 (Mon)" -> "1"
        sd = int(sday_str)

        ey = int(end_year_combo.get())
        em = end_month_combo.current()+1
        eday_str = end_day_combo.get().split()[0]
        ed = int(eday_str)

        start_date = datetime(sy, sm, sd)
        end_date = datetime(ey, em, ed)

        if start_date > end_date:
            messagebox.showerror("Error", "Start date cannot be after End date.")
            return

        # Determine which stores are selected
        selected_stores = []
        for store_name, var in store_vars.items():
            if var.get() == 1:  # if box checked
                selected_stores.append(store_name)

        # If none selected, default to all
        if not selected_stores:
            selected_stores = [
                "Buzz Cannabis - Mission Valley",
                "Buzz Cannabis-La Mesa",
                "Buzz Cannabis - SORRENTO VALLEY",
                "Buzz Cannabis - Lemon Grove",
                "Buzz Cannabis (National City)",  # ✅ Add this line
                "Buzz Cannabis Wildomar Palomar"
            ]

        # Close GUI
        root.destroy()

        # Launch browser, login, iterate over stores
        global driver
        driver = launchBrowser()
        login(driver)
        failed_stores = []

        for store in selected_stores:
            try:
                if not select_dropdown_item(store):
                    print(f"[WARN] Could not select store: {store}")
                    failed_stores.append(store)
                    continue
                ok = export_store_with_retries(store, start_date, end_date)
                if not ok:
                    print(f"[WARN] Export failed for store: {store}")
                    failed_stores.append(store)
            except Exception:
                print(f"[WARN] Store run failed for {store}: {traceback.format_exc()}")
                failed_stores.append(store)
                continue

        driver.quit()
        if failed_stores:
            print(f"[WARN] Failed stores: {failed_stores}")

    tk.Button(root, text="OK", command=on_ok, font=("Arial", 12, "bold"), bg="lightblue").pack(pady=10)

    root.mainloop()
def run_sales_report(start_date, end_date):
    """Runs the full sales report process."""
    store_names = [
        "Buzz Cannabis - Mission Valley",
        "Buzz Cannabis-La Mesa",
        "Buzz Cannabis - SORRENTO VALLEY",
        "Buzz Cannabis - Lemon Grove",
        "Buzz Cannabis (National City)",
        "Buzz Cannabis Wildomar Palomar"
    ]
    global driver
    driver = launchBrowser()
    login(driver)
    failed_stores = []

    try:
        for store in store_names:
            try:
                if not select_dropdown_item(store):
                    print(f"[WARN] Could not select store: {store}")
                    failed_stores.append(store)
                    continue
                ok = export_store_with_retries(store, start_date, end_date)
                if not ok:
                    print(f"[WARN] Export failed for store: {store}")
                    failed_stores.append(store)
            except Exception:
                print(f"[WARN] Store run failed for {store}: {traceback.format_exc()}")
                failed_stores.append(store)
                continue
    finally:
        driver.quit()

    if failed_stores:
        raise RuntimeError(f"Export failed for store(s): {', '.join(failed_stores)}")
# Main execution through GUI
if __name__ == "__main__":
    open_gui_and_run()
