import argparse
import os
import sys
import time
from datetime import datetime
from pathlib import Path
import shutil

from selenium import webdriver
from selenium.webdriver.common.by import By
from selenium.webdriver.support.ui import WebDriverWait
from selenium.webdriver.support import expected_conditions as EC
from selenium.webdriver.chrome.service import Service
from selenium.webdriver.chrome.options import Options
from webdriver_manager.chrome import ChromeDriverManager


def _format_date_for_template_from_iso(iso_date: str):
    # Accepts YYYY-MM-DD or YYYYMMDD or YYYY-MM-DDTHH:MM:SS
    try:
        dt = datetime.fromisoformat(iso_date)
    except Exception:
        # try YYYYMMDD
        if len(iso_date) == 8 and iso_date.isdigit():
            dt = datetime.strptime(iso_date, "%Y%m%d")
        else:
            raise
    return {
        "date": dt.strftime("%Y%m%d"),
        "yyyy": dt.strftime("%Y"),
        "mm": dt.strftime("%m"),
        "dd": dt.strftime("%d"),
    }


def download_one(url_template: str, iso_date: str, dest_dir: str, username: str = None, password: str = None) -> str:
    """Download CSV using Selenium + Chrome. Simple, stable, reliable."""
    os.makedirs(dest_dir, exist_ok=True)
    fmt = _format_date_for_template_from_iso(iso_date)

    if "{date}" in url_template:
        url = url_template.format(date=fmt["date"])
    else:
        url = url_template.format(**fmt)

    print(f"INFO: Starting download for {iso_date}", file=sys.stderr)

    # Set up Chrome options
    chrome_options = Options()
    chrome_options.add_argument("--headless")
    chrome_options.add_argument("--no-sandbox")
    chrome_options.add_argument("--disable-dev-shm-usage")
    
    # Set download directory to dest_dir
    prefs = {"download.default_directory": os.path.abspath(dest_dir)}
    chrome_options.add_experimental_option("prefs", prefs)

    # Create driver with auto-managed ChromeDriver
    driver = webdriver.Chrome(
        service=Service(ChromeDriverManager().install()),
        options=chrome_options
    )

    try:
        print(f"INFO: Navigating to {url}", file=sys.stderr)
        driver.get(url)
        
        # Wait for page to load
        time.sleep(3)

        # Accept cookie banner if present
        try:
            accept_btn = WebDriverWait(driver, 5).until(
                EC.element_to_be_clickable((By.ID, "accept-btn"))
            )
            accept_btn.click()
            print(f"INFO: Accepted cookie banner", file=sys.stderr)
            time.sleep(1)
        except Exception:
            print(f"INFO: No cookie banner or already accepted", file=sys.stderr)

        # Login if credentials provided
        if username and password:
            try:
                username_field = WebDriverWait(driver, 5).until(
                    EC.presence_of_element_located((By.ID, "identifier"))
                )
                username_field.send_keys(username)
                print(f"INFO: Entered username", file=sys.stderr)
                
                password_field = driver.find_element(By.CSS_SELECTOR, 'input[name="password"]')
                password_field.send_keys(password)
                print(f"INFO: Entered password", file=sys.stderr)
                
                login_btn = driver.find_element(By.CSS_SELECTOR, 'button:has-text("Sign in with password")')
                login_btn.click()
                print(f"INFO: Clicked login", file=sys.stderr)
                
                time.sleep(5)  # Wait for login to complete
            except Exception as e:
                print(f"INFO: Login skipped or failed ({e})", file=sys.stderr)

        # Click download button
        try:
            download_btn = WebDriverWait(driver, 10).until(
                EC.element_to_be_clickable((By.ID, "search_primary"))
            )
            print(f"INFO: Found download button, clicking...", file=sys.stderr)
            download_btn.click()
            
            # Wait for download to start
            time.sleep(3)
        except Exception as e:
            raise RuntimeError(f"Could not find or click download button: {e}")

        # Find the downloaded file
        time.sleep(2)
        files = os.listdir(dest_dir)
        csv_files = [f for f in files if f.endswith(".csv")]
        
        if not csv_files:
            raise RuntimeError(f"No CSV file found in {dest_dir} after download attempt")

        # Use the most recently modified CSV
        csv_file = max(csv_files, key=lambda f: os.path.getmtime(os.path.join(dest_dir, f)))
        src_path = os.path.join(dest_dir, csv_file)
        
        # Rename to standardized name
        dest_filename = f"data_{fmt['date']}.csv"
        dest_path = os.path.join(dest_dir, dest_filename)
        
        if src_path != dest_path:
            shutil.move(src_path, dest_path)
        
        print(f"INFO: Downloaded to {dest_path}", file=sys.stderr)
        return dest_path

    finally:
        driver.quit()


def main(argv=None):
    parser = argparse.ArgumentParser()
    parser.add_argument("--url-template", required=True)
    parser.add_argument("--date", required=True)
    parser.add_argument("--dest-dir", required=True)
    parser.add_argument("--username", default=None)
    parser.add_argument("--password", default=None)

    args = parser.parse_args(argv)

    try:
        out = download_one(args.url_template, args.date, args.dest_dir, args.username, args.password)
        # Print the resulting path as the last stdout line for the parent process to read
        print(out)
        return 0
    except Exception as e:
        print("ERROR:", type(e).__name__, str(e), file=sys.stderr)
        return 2


if __name__ == "__main__":
    raise SystemExit(main())
