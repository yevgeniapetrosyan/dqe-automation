import csv
import time
import traceback
from pathlib import Path

from selenium import webdriver
from selenium.webdriver.chrome.options import Options
from selenium.webdriver.common.by import By
from selenium.webdriver.support.ui import WebDriverWait
from selenium.webdriver.support import expected_conditions as EC
from selenium.common.exceptions import TimeoutException, NoSuchElementException, StaleElementReferenceException, ElementClickInterceptedException, WebDriverException

BASE_DIR = Path(__file__).resolve().parent
REPORT_FILE = BASE_DIR / "report.html"
OUTPUT_CSV = BASE_DIR / "table.csv"
SCREENSHOT_DIR = BASE_DIR / "screenshots"


def init_driver(headless: bool = True) -> webdriver.Chrome:
    options = Options()
    if headless:
        options.add_argument("--headless=new")
    options.add_argument("--window-size=1600,1200")
    options.add_argument("--disable-gpu")
    options.add_argument("--disable-extensions")
    options.add_argument("--disable-infobars")
    options.add_argument("--disable-dev-shm-usage")
    options.add_argument("--no-sandbox")
    options.add_argument("--allow-file-access-from-files")
    options.add_argument("--disable-web-security")
    options.add_experimental_option("excludeSwitches", ["enable-logging"])
    return webdriver.Chrome(options=options)


def ensure_screenshot_dir() -> Path:
    SCREENSHOT_DIR.mkdir(parents=True, exist_ok=True)
    return SCREENSHOT_DIR


def save_screenshot(driver: webdriver.Chrome, filename: str) -> Path:
    path = ensure_screenshot_dir() / filename
    driver.save_screenshot(str(path))
    print(f"Screenshot saved: {path}")
    return path


def wait_for_table(driver: webdriver.Chrome, timeout: int = 20):
    table_locator = (By.CLASS_NAME, "table")
    return WebDriverWait(driver, timeout).until(
        EC.visibility_of_element_located(table_locator)
    )


def extract_table_rows(table_element):
    columns = table_element.find_elements(By.CLASS_NAME, "y-column")
    if not columns:
        raise NoSuchElementException("No columns found under table using By.CLASS_NAME('y-column').")

    headers = []
    columns_cells = []
    max_rows = 0

    for column in columns:
        header = column.find_element(By.XPATH, './/*[@id="header"]').text.strip()
        headers.append(header)

        cells = column.find_elements(By.CSS_SELECTOR, ".cell-text")
        row_values = [cell.text.strip() for cell in cells if cell.text.strip() != header]
        columns_cells.append(row_values)
        max_rows = max(max_rows, len(row_values))

    if not headers:
        raise ValueError("Table root found, but no headers were extracted.")

    rows = []
    for row_index in range(max_rows):
        row = [columns_cells[col_index][row_index] if row_index < len(columns_cells[col_index]) else "" for col_index in range(len(columns_cells))]
        rows.append(row)

    if not rows:
        raise ValueError("Extracted zero rows from the table.")

    return headers, rows


def save_to_csv(headers, rows, output_file: Path):
    try:
        with output_file.open("w", newline="", encoding="utf-8") as csvfile:
            writer = csv.writer(csvfile)
            writer.writerow(headers)
            writer.writerows(rows)
        print(f"Saved table content to: {output_file}")
    except PermissionError as exc:
        print(f"Permission denied when saving CSV {output_file}: {exc}")
    except OSError as exc:
        print(f"Failed to save CSV {output_file}: {exc}")


def wait_for_chart(driver: webdriver.Chrome, timeout: int = 20):
    return WebDriverWait(driver, timeout).until(
        EC.presence_of_element_located((By.CLASS_NAME, "pielayer"))
    )


def extract_doughnut_data(chart_element):
    labels = chart_element.find_elements(By.CSS_SELECTOR, "text.slicetext[data-notex='1']")
    data = []

    for label in labels:
        tspans = label.find_elements(By.TAG_NAME, "tspan")
        if len(tspans) >= 2:
            category = tspans[0].text.strip()
            value = tspans[1].text.strip()
            if category or value:
                data.append((category, value))
        elif len(tspans) == 1:
            # fallback if the slice label is on a single line
            text = tspans[0].text.strip()
            if text:
                parts = [part.strip() for part in text.split(" ") if part.strip()]
                if len(parts) >= 2:
                    data.append((" ".join(parts[:-1]), parts[-1]))
                else:
                    data.append((text, ""))

    return data


def save_doughnut_csv(data, index: int):
    output_file = BASE_DIR / f"doughnut{index}.csv"
    headers = ["Category", "Value"]
    save_to_csv(headers, data, output_file)
    return output_file


def get_filter_buttons(driver: webdriver.Chrome):
    scrollbox = WebDriverWait(driver, 20).until(
        EC.presence_of_element_located((By.CLASS_NAME, "scrollbox"))
    )
    buttons = scrollbox.find_elements(By.CLASS_NAME, "traces")
    if not buttons:
        raise NoSuchElementException("No filter buttons found under .scrollbox .traces")
    return buttons


def click_filter_button(driver: webdriver.Chrome, index: int):
    try:
        buttons = get_filter_buttons(driver)
        if index >= len(buttons):
            raise IndexError(f"Filter index {index} is out of range.")

        button = buttons[index]
        driver.execute_script("arguments[0].scrollIntoView({block: 'center'});", button)
        button.click()
        return True
    except (StaleElementReferenceException, ElementClickInterceptedException):
        print(f"Retrying click for filter index {index} due to stale/intercepted element.")
        buttons = get_filter_buttons(driver)
        button = buttons[index]
        driver.execute_script("arguments[0].scrollIntoView({block: 'center'});", button)
        button.click()
        return True


def capture_sequential_screenshot(driver: webdriver.Chrome, index: int) -> Path:
    filename = f"screenshot{index}.png"
    return save_screenshot(driver, filename)


def process_doughnut_filters(driver: webdriver.Chrome, chart_element):
    try:
        filter_buttons = get_filter_buttons(driver)
    except Exception as exc:
        print("Unable to locate doughnut filter buttons:", exc)
        return

    save_doughnut_csv(extract_doughnut_data(chart_element), 0)
    capture_sequential_screenshot(driver, 0)

    for i in range(len(filter_buttons)):
        try:
            print(f"Applying filter option {i + 1}/{len(filter_buttons)}")
            if click_filter_button(driver, i):
                time.sleep(1)
                wait_for_chart(driver)
                chart_element = wait_for_chart(driver)
                save_doughnut_csv(extract_doughnut_data(chart_element), i + 1)
                capture_sequential_screenshot(driver, i + 1)
        except Exception as exc:
            print(f"Failed to apply filter index {i}:", exc)
            traceback.print_exc()


def main():
    if not REPORT_FILE.exists():
        raise FileNotFoundError(f"Report file not found: {REPORT_FILE}")

    driver = None
    try:
        driver = init_driver(headless=True)
        report_url = REPORT_FILE.resolve().as_uri()
        driver.get(report_url)
        print(f"Loaded report: {report_url}")

        save_screenshot(driver, "page_loaded.png")

        table_element = wait_for_table(driver)
        save_screenshot(driver, "table_visible.png")

        headers, rows = extract_table_rows(table_element)
        save_to_csv(headers, rows, OUTPUT_CSV)
        print(f"Extracted {len(rows)} rows and {len(headers)} columns.")

        try:
            chart_element = wait_for_chart(driver)
            process_doughnut_filters(driver, chart_element)
        except TimeoutException:
            print("No doughnut chart found; skipping chart interaction.")

    except Exception as exc:
        print("Error during automation:")
        traceback.print_exc()
    finally:
        if driver:
            driver.quit()


if __name__ == "__main__":
    main()