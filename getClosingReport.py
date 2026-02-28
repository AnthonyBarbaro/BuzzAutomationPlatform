import os
import re
import time
import traceback
from datetime import datetime, timedelta
import calendar

import tkinter as tk
from tkinter import messagebox
from tkinter.ttk import Combobox

from selenium import webdriver
from selenium.webdriver.chrome.service import Service
from webdriver_manager.chrome import ChromeDriverManager
from selenium.webdriver.chrome.options import Options
from selenium.webdriver.support.ui import WebDriverWait
from selenium.webdriver.common.by import By
from selenium.webdriver.common.keys import Keys
from selenium.webdriver.support import expected_conditions as EC
from selenium.common.exceptions import (
    NoSuchElementException,
    TimeoutException,
    ElementClickInterceptedException,
    StaleElementReferenceException,
)

# Import your login credentials
from login import username, password


# ---------------------------------------------------------------------
# 1) Original script logic
# ---------------------------------------------------------------------

BLOCKING_SELECTORS = [
    ".MuiBackdrop-root",
    ".MuiSnackbar-root",
    ".MuiAlert-root",
    "div.notification",
]

LOADING_SELECTORS = [
    "[data-testid='loading-spinner_icon']",
    "[aria-label='Loading'][aria-valuetext='Loading']",
]

def launchBrowser():
    """Launch Chrome, go to the Dusk Closing Report page."""
    chrome_options = Options()
    chrome_options.add_argument("--window-size=1920,1080")
    chrome_options.add_argument("--no-sandbox")
    chrome_options.add_argument("--disable-dev-shm-usage")
    if os.getenv("BUZZ_HEADLESS", "1") != "0":
        chrome_options.add_argument("--headless=new")
    chrome_options.add_experimental_option("detach", True)
    chrome_options.add_experimental_option('excludeSwitches', ['enable-logging'])
    driver = webdriver.Chrome(service=Service(ChromeDriverManager().install()), options=chrome_options)
    driver.get("https://dusk.backoffice.dutchie.com/reports/closing-report/registers")
    return driver

def login(driver):
    """Login using your stored credentials."""
    wait = WebDriverWait(driver, 10)
    wait.until(EC.element_to_be_clickable((By.CSS_SELECTOR, "input[data-testid='auth_input_username']"))).send_keys(username)
    wait.until(EC.element_to_be_clickable((By.CSS_SELECTOR, "input[data-testid='auth_input_password']"))).send_keys(password)
    login_button = wait.until(EC.element_to_be_clickable((By.CSS_SELECTOR, "button[data-testid='auth_button_go-green']")))
    login_button.click()
    wait_for_reports_page_ready(driver)

def wait_for_reports_page_ready(driver, timeout=40):
    """Wait until the post-login report page is fully interactive."""
    wait = WebDriverWait(driver, timeout)
    try:
        wait.until(lambda d: "closing-report/registers" in d.current_url)
    except TimeoutException:
        pass
    wait.until(EC.presence_of_element_located((By.CSS_SELECTOR, "[data-testid='header_select_location']")))

def wait_for_blocking_ui_to_clear(driver, timeout=8):
    """Wait briefly for overlays/snackbars that can intercept clicks."""
    end = time.time() + timeout
    while time.time() < end:
        blocker_visible = False
        for selector in BLOCKING_SELECTORS:
            try:
                elems = driver.find_elements(By.CSS_SELECTOR, selector)
            except Exception:
                elems = []
            for elem in elems:
                try:
                    if elem.is_displayed():
                        blocker_visible = True
                        break
                except StaleElementReferenceException:
                    continue
            if blocker_visible:
                break
        if not blocker_visible:
            return True
        time.sleep(0.2)
    return False

def is_loading_data_visible(driver):
    """Return True if report-loading indicators are visible."""
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

    try:
        text_nodes = driver.find_elements(
            By.XPATH,
            "//*[contains(normalize-space(), 'Loading data')]"
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

def wait_for_loading_data_cycle(driver, timeout=120, appear_wait=10, stable_seconds=1.2):
    """
    After clicking Run, wait for loading to settle.
    - If loading appears, require it to disappear and stay clear.
    - If it never appears quickly, continue.
    """
    start = time.time()
    saw_loading = False
    clear_since = None

    while time.time() - start < timeout:
        loading_visible = is_loading_data_visible(driver)

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

def table_has_no_data(driver):
    """Detect common empty-table messages."""
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
        except StaleElementReferenceException:
            continue
    return False

def robust_click(driver, by, locator, label, timeout=12, attempts=4):
    """Click helper with retry + JS fallback."""
    last_error = None
    for attempt in range(1, attempts + 1):
        try:
            wait_for_blocking_ui_to_clear(driver, timeout=3)
            wait = WebDriverWait(driver, timeout)
            elem = wait.until(EC.presence_of_element_located((by, locator)))
            driver.execute_script("arguments[0].scrollIntoView({block: 'center', inline: 'center'});", elem)
            time.sleep(0.2)
            elem = wait.until(EC.element_to_be_clickable((by, locator)))
            elem.click()
            return True
        except (TimeoutException, ElementClickInterceptedException, StaleElementReferenceException) as e:
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

def _canonical_store_name(value):
    return re.sub(r"[^a-z0-9]", "", (value or "").lower())

def _get_store_options(driver):
    wait = WebDriverWait(driver, 10)

    def _visible_options(drv):
        options = drv.find_elements(
            By.XPATH,
            "//li[@role='option' or contains(@data-testid,'rebrand-header_menu-item_')]"
        )
        visible = []
        for opt in options:
            try:
                if opt.is_displayed():
                    visible.append(opt)
            except StaleElementReferenceException:
                continue
        return visible

    return wait.until(lambda d: _visible_options(d) or False)

def _select_store_option(driver, store_name):
    target = _canonical_store_name(store_name)
    options = _get_store_options(driver)
    option_texts = []

    exact_match = None
    partial_match = None
    for option in options:
        try:
            option_text = (option.text or option.get_attribute("innerText") or "").strip()
        except StaleElementReferenceException:
            continue

        if option_text:
            option_texts.append(option_text)

        option_key = _canonical_store_name(option_text)
        if not option_key:
            continue
        if option_key == target:
            exact_match = option
            break
        if target in option_key or option_key in target:
            partial_match = partial_match or option

    chosen = exact_match or partial_match
    if not chosen:
        raise NoSuchElementException(
            f"No dropdown option matched store '{store_name}'. "
            f"Visible options: {option_texts}"
        )

    driver.execute_script("arguments[0].scrollIntoView({block: 'center'});", chosen)
    time.sleep(0.2)
    try:
        chosen.click()
    except Exception:
        driver.execute_script("arguments[0].click();", chosen)

def click_dropdown(driver):
    """ Click the store dropdown to open the list of store options. """
    dropdown_locators = [
        (By.CSS_SELECTOR, "[data-testid='header_select_location']"),
        (By.XPATH, "//div[@data-testid='header_select_location']"),
        (By.XPATH, "//button[@data-testid='header_select_location']"),
    ]

    last_error = None
    for by, locator in dropdown_locators:
        try:
            robust_click(driver, by, locator, "Store dropdown", timeout=15, attempts=4)
            _get_store_options(driver)
            return True
        except Exception as e:
            last_error = e

    print(f"Dropdown not found or not clickable: {last_error}")
    return False

def select_store(driver, store_name):
    for attempt in range(1, 4):
        try:
            wait_for_blocking_ui_to_clear(driver, timeout=4)
            if not click_dropdown(driver):
                raise TimeoutException("Store dropdown did not open.")
            _select_store_option(driver, store_name)
            time.sleep(0.8)

            # Validate by checking the location control text after selection.
            header_text = driver.find_element(
                By.CSS_SELECTOR, "[data-testid='header_select_location']"
            ).text
            if _canonical_store_name(store_name) in _canonical_store_name(header_text):
                return True

            # Fallback success path: if dropdown is closed, assume click took effect.
            if not driver.find_elements(By.XPATH, "//li[@role='option']"):
                return True

            raise TimeoutException(
                f"Store header text did not update. Expected '{store_name}', got '{header_text}'."
            )
        except (TimeoutException, NoSuchElementException, ElementClickInterceptedException, StaleElementReferenceException) as e:
            print(f"[WARN] Store selection attempt {attempt}/3 failed for '{store_name}': {e}")
            time.sleep(1)

    print(f"[ERROR] Could not select store '{store_name}' after retries.")
    return False

def click_date_input_field(driver):
    """ Click on the date input field to open the date-picker safely. """
    wait = WebDriverWait(driver, 10)
    date_input_id = "input-input_"

    try:
        # Wait for the backdrop to disappear first (avoids click interception)
        wait.until(EC.invisibility_of_element_located((By.CLASS_NAME, "MuiBackdrop-root")))

        # Now try clicking the field
        date_input = wait.until(EC.element_to_be_clickable((By.ID, date_input_id)))
        driver.execute_script("arguments[0].scrollIntoView(true);", date_input)
        driver.execute_script("arguments[0].click();", date_input)

    except Exception as e:
        print("[ERROR] Clicking date input field failed:", e)


def click_dates_in_calendar(driver, day_of_month):
    """
    For the 'closing-report/registers' page, select a single day at a time.
    Then click the 'Run' button to refresh the table, using JavaScript clicks 
    and small waits to avoid intercept issues.
    """
    wait = WebDriverWait(driver, 10)
    try:
        # 1) Click the day in the datepicker (via JS to reduce "intercept" issues)
        day_div_xpath = f"//div[text()='{day_of_month}']"
        day_div = wait.until(EC.element_to_be_clickable((By.XPATH, day_div_xpath)))
        driver.execute_script("arguments[0].click();", day_div)
        time.sleep(0.5)

        # 2) Press ESC to close date picker if it remains open
        body = driver.find_element(By.TAG_NAME, "body")
        body.send_keys(Keys.ESCAPE)
        time.sleep(0.5)

        # OPTIONAL: If you know an overlay is blocking, you can wait for invisibility.
        # Example CSS from your error might be "div.sc-kwdoa-D.loTCwi"
        # try:
        #    wait.until(EC.invisibility_of_element_located((By.CSS_SELECTOR, "div.sc-kwdoa-D.loTCwi")))
        # except TimeoutException:
        #    pass

        # 3) Use JavaScript click on the 'Run' button
        run_button = wait.until(EC.element_to_be_clickable((By.XPATH, "//button[contains(text(), 'Run')]")))
        driver.execute_script("arguments[0].scrollIntoView({behavior: 'smooth', block: 'center'});", run_button)
        driver.execute_script("arguments[0].click();", run_button)
        if not wait_for_loading_data_cycle(driver, timeout=120):
            print("[WARN] Loading indicator did not fully settle after clicking Run.")
        return True

    except (TimeoutException, ElementClickInterceptedException) as e:
        print("Could not click the day or the Run button.")
        print(f"Error details: {e}")
        return False

def extract_monetary_values(driver):
    """
    Extract the first 3 monetary cells and parse them as float.
    Returns [] if the report has no data or cells never appear.
    """
    wait_for_loading_data_cycle(driver, timeout=120)
    selectors = [
        "[class*='table-cell-right-']",
        "[class*='table-cell-right']",
        "td[class*='right']",
        "div[class*='right']",
    ]

    elements = []
    deadline = time.time() + 45
    while time.time() < deadline:
        wait_for_blocking_ui_to_clear(driver, timeout=2)

        if table_has_no_data(driver):
            print("[INFO] Report returned no data for this date/store.")
            return []

        for selector in selectors:
            try:
                found = driver.find_elements(By.CSS_SELECTOR, selector)
            except Exception:
                found = []

            visible = []
            for elem in found:
                try:
                    if elem.is_displayed():
                        visible.append(elem)
                except StaleElementReferenceException:
                    continue

            if visible:
                elements = visible
                break

        if elements:
            break

        time.sleep(0.35)

    if not elements:
        print("[WARN] Timed out waiting for monetary cells in report table.")
        return []

    monetary_values = []
    for element in elements:
        value_text = (element.text or "").strip()
        if not value_text:
            continue
        if not re.search(r"[\d$(),.-]", value_text):
            continue
        try:
            numeric_value = float(value_text
                .replace('$', '')
                .replace(',', '')
                .replace('(', '-')
                .replace(')', '')
            )
            monetary_values.append(numeric_value)
            if len(monetary_values) >= 3:
                break
        except ValueError:
            continue
    return monetary_values
def change_month_if_needed(driver, target_date):
    import sys

    wait = WebDriverWait(driver, 10)

    try:
        wait.until(EC.visibility_of_element_located((By.CLASS_NAME, "MuiPopover-paper")))

        header = wait.until(EC.presence_of_element_located((By.XPATH, "//div[@data-testid='date-picker-header']")))

        # Convert month name to number
        month_str_to_num = {month: i for i, month in enumerate(calendar.month_name) if month}

        def get_displayed_date():
            month_str = header.find_element(By.TAG_NAME, "span").text.strip()
            year_val = int(header.find_element(By.TAG_NAME, "input").get_attribute("value"))
            month_val = month_str_to_num.get(month_str, 0)
            return year_val, month_val

        # Get current and target dates
        current_year, current_month = get_displayed_date()
        target_year, target_month = target_date.year, target_date.month
        delta_months = (target_year - current_year) * 12 + (target_month - current_month)


        # ✅ EARLY EXIT if already correct
        if delta_months == 0:
            return

        # Locate the arrow buttons directly
        left_arrow = wait.until(EC.element_to_be_clickable((
            By.XPATH, "//div[@data-testid='date-picker-header']/div[1]"
        )))
        right_arrow = wait.until(EC.element_to_be_clickable((
            By.XPATH, "//div[@data-testid='date-picker-header']/div[3]"
        )))

        # Click as needed
        for i in range(abs(delta_months)):
            if delta_months > 0:
                driver.execute_script("arguments[0].click();", right_arrow)
            else:
                driver.execute_script("arguments[0].click();", left_arrow)

            time.sleep(1)
            current_year, current_month = get_displayed_date()

            if current_year == target_year and current_month == target_month:
                break
        else:
            print(f"[ERROR] Failed to reach target date: {target_month}/{target_year}")
            driver.quit()
            sys.exit(1)

    except Exception as e:
        print(f"[!] Failed to switch to correct month/year: {e}")
        driver.quit()
        sys.exit(1)

def process_single_day(driver, date_to_run):
    """
    Given a Python date object (date_to_run),
    1) Click into date field,
    2) Select that day in the calendar,
    3) Extract monetary values,
    4) Print out your result (like your original script).
    """
    # Convert day of month to a string (for the datepicker)
    day_str = str(date_to_run.day)
    # Click date input, then click the day in the datepicker
    click_date_input_field(driver)
    change_month_if_needed(driver, date_to_run)
    if not click_dates_in_calendar(driver, day_str):
        print(f"{date_to_run.strftime('%m/%d')}: Skipping due to calendar/run click failure.")
        return

    # Extract values
    gross = extract_monetary_values(driver)

    # Format the date mm/dd
    formatted_date = date_to_run.strftime('%m/%d')
    if len(gross) >= 2:
        print("\033[1m--------------------------------\033[0m")
        print(f"\033[1m{formatted_date} {gross[0]} {gross[1]}\033[0m")
        print("\033[1m--------------------------------\033[0m")
        if gross[0] != 0:
            # ratio example
            print(float((-1 * gross[1]) / gross[0]))
        else:
            print("Gross[0] is zero, cannot compute ratio.")
    else:
        print(f"{formatted_date}: Not enough data to calculate sales.")


# ---------------------------------------------------------------------
# 2) Adding a GUI to pick the date range and stores
# ---------------------------------------------------------------------

def create_store_checkboxes(frame):
    """
    Create 4 checkboxes for the store locations you want to handle,
    each store is checked by default.
    Returns a dict of {store_name: IntVar}.
    """
    store_vars = {}

    store_map = [
        "Buzz Cannabis - Mission Valley",
        "Buzz Cannabis-La Mesa",
        "Buzz Cannabis - SORRENTO VALLEY",
        "Buzz Cannabis - Lemon Grove",
        "Buzz Cannabis (National City)",
        "Buzz Cannabis Wildomar Palomar"
    ]

    for store_name in store_map:
        var = tk.IntVar(value=1)
        cb = tk.Checkbutton(frame, text=store_name, variable=var)
        cb.pack(anchor='w')
        store_vars[store_name] = var

    return store_vars

def update_days_combobox(year_combo, month_combo, day_combo):
    """
    Refreshes the day_combobox when year or month changes,
    taking into account actual days in the selected month/year.
    """
    weekday_abbr = ["Mon", "Tue", "Wed", "Thu", "Fri", "Sat", "Sun"]
    y = int(year_combo.get())
    m = month_combo.current() + 1
    now = datetime.now()

    # last day of that month
    last_day = calendar.monthrange(y, m)[1]
    # If user picked the current month/year, limit day up to 'today'
    if y == now.year and m == now.month:
        last_day = min(last_day, now.day)

    day_values = []
    for day_num in range(1, last_day + 1):
        dt = datetime(y, m, day_num)
        wday_abbr = weekday_abbr[dt.weekday()]
        day_values.append(f"{day_num} ({wday_abbr})")

    day_combo['values'] = day_values
    if day_combo.current() == -1 or day_combo.current() >= len(day_values):
        day_combo.current(0)

def create_date_selector(frame, label_text, year_options):
    """
    Creates a row of combo boxes for picking Year, Month, Day,
    plus a label. Returns (year_combo, month_combo, day_combo).
    """
    tk.Label(frame, text=label_text, font=("Arial", 12, "bold")).pack(pady=(10,5))
    
    subframe = tk.Frame(frame)
    subframe.pack(pady=5)

    year_combo = Combobox(subframe, values=year_options, state='readonly', width=8)
    year_combo.current(len(year_options)-1)  # default to most recent year
    year_combo.grid(row=0, column=0, padx=5)

    MONTHS = ["January","February","March","April","May","June","July","August","September","October","November","December"]
    month_combo = Combobox(subframe, values=MONTHS, state='readonly', width=10)
    current_month = datetime.now().month
    month_combo.current(current_month-1)
    month_combo.grid(row=0, column=1, padx=5)

    day_combo = Combobox(subframe, state='readonly', width=10)
    day_combo.grid(row=0, column=2, padx=5)

    def on_year_month_change(*args):
        update_days_combobox(year_combo, month_combo, day_combo)

    year_combo.bind("<<ComboboxSelected>>", on_year_month_change)
    month_combo.bind("<<ComboboxSelected>>", on_year_month_change)

    # Initial population of days
    update_days_combobox(year_combo, month_combo, day_combo)
    now = datetime.now()
    sel_year = int(year_combo.get())
    sel_month = month_combo.current()+1
    if sel_year == now.year and sel_month == now.month:
        day_combo.current(now.day - 1)
    else:
        day_combo.current(0)

    return year_combo, month_combo, day_combo

def get_date_from_comboboxes(year_combo, month_combo, day_combo):
    """
    Convert the user’s GUI selection into a datetime object.
    """
    y = int(year_combo.get())
    m = month_combo.current() + 1
    d = int(day_combo.get().split()[0])  # "15 (Mon)" -> 15
    return datetime(y, m, d)

def open_gui_and_run():
    """
    Launch a GUI that:
    1) Asks for a start date
    2) Asks for an end date
    3) Asks which stores to process
    4) Launches the closing-report logic for each day in the range + each store
    """
    root = tk.Tk()
    root.title("Select Date Range for Closing Report")

    main_frame = tk.Frame(root)
    main_frame.pack(pady=20, padx=20)

    this_year = datetime.now().year
    year_range = [str(this_year-1), str(this_year)]

    # --- Start Date ---
    start_year_combo, start_month_combo, start_day_combo = create_date_selector(
        main_frame, "Select Start Date:", year_range
    )
    # --- End Date ---
    end_year_combo, end_month_combo, end_day_combo = create_date_selector(
        main_frame, "Select End Date:", year_range
    )

    # --- Store checkboxes ---
    tk.Label(main_frame, text="Select Store(s):", font=("Arial", 12, "bold")).pack(pady=(10,5), anchor='w')
    store_vars = create_store_checkboxes(main_frame)

    def on_ok():
        # 1) Get start/end from combos
        start_date = get_date_from_comboboxes(start_year_combo, start_month_combo, start_day_combo)
        end_date = get_date_from_comboboxes(end_year_combo, end_month_combo, end_day_combo)

        if start_date > end_date:
            messagebox.showerror("Date Error", "Start date cannot be after End date.")
            return

        # 2) Build list of dates from start_date to end_date (inclusive)
        date_list = []
        current = start_date
        while current <= end_date:
            date_list.append(current)
            current += timedelta(days=1)

        # 3) Which stores are selected?
        selected_stores = [
            store_name for store_name, var in store_vars.items() if var.get() == 1
        ]
        if not selected_stores:
            messagebox.showinfo("No Store Selected", "No stores selected. Exiting.")
            root.destroy()
            return

        # 4) Close GUI
        root.destroy()

        # 5) Launch browser, login once
        driver = launchBrowser()
        login(driver)

        # 6) For each selected store:
        for store_name in selected_stores:
            if not select_store(driver, store_name):
                print(f"Skipping store {store_name} due to selection error.")
                continue
            print(f"\n\033[1m--- Processing store {store_name} ---\033[0m")

            # 7) For each date in the range, run your daily logic
            for date_to_run in date_list:
                # short delay between days (optional)
                time.sleep(3)
                try:
                    process_single_day(driver, date_to_run)
                except Exception:
                    print(f"[ERROR] Failed processing {date_to_run.strftime('%Y-%m-%d')} for {store_name}")
                    print(traceback.format_exc())
                    continue

        # 8) Done
        driver.quit()
        print("\nAll processing completed successfully.")

    # "OK" Button
    tk.Button(root, text="OK", command=on_ok, font=("Arial", 12, "bold"), bg="lightblue").pack(pady=10)
    root.mainloop()


# Run if called directly
if __name__ == "__main__":
    open_gui_and_run()
