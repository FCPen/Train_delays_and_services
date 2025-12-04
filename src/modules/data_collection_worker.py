import argparse
import os
import shutil
import sys
from pathlib import Path
from datetime import datetime

from playwright.sync_api import sync_playwright


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
    os.makedirs(dest_dir, exist_ok=True)
    fmt = _format_date_for_template_from_iso(iso_date)

    if "{date}" in url_template:
        url = url_template.format(date=fmt["date"])
    else:
        url = url_template.format(**fmt)

    with sync_playwright() as p:
        browser = p.chromium.launch(headless=True)
        page = browser.new_page()

        try:
            page.goto(url, wait_until="networkidle")

            if username and password:
                # These selectors were discovered for Realtime Trains; adjust if necessary
                try:
                    page.fill("#identifier", username)
                except Exception:
                    pass
                try:
                    page.fill('input[name="password"]', password)
                except Exception:
                    pass
                try:
                    page.click('button:has-text("Sign in with password")')
                except Exception:
                    pass

            download_button_selector = '#search_primary'

            with page.expect_download() as download_info:
                page.click(download_button_selector)
                # small wait for download initiation
                page.wait_for_timeout(1000)

            download = download_info.value
            temp_path = download.path()

            filename = f"data_{fmt['date']}.csv"
            dest_path = os.path.join(dest_dir, filename)

            # Move to destination
            shutil.move(str(temp_path), dest_path)

            return dest_path

        finally:
            browser.close()


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
