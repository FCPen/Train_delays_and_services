from datetime import date, timedelta, datetime
import argparse
import os
import sys
import time
from typing import Iterable, Optional, Tuple
import urllib.parse
import requests
from requests.auth import HTTPBasicAuth
from requests.adapters import HTTPAdapter
from requests import exceptions as req_exceptions
from urllib3.util.retry import Retry
import getpass
from pathlib import Path
import shutil
import asyncio
import nest_asyncio

# Allow nested async event loops (needed for Jupyter notebooks)
nest_asyncio.apply()

try:
	from playwright.async_api import async_playwright
	HAS_PLAYWRIGHT = True
except ImportError:
	HAS_PLAYWRIGHT = False


async def _download_csv_with_browser_async(url_template: str, d: date, dest_dir: str, username: str = None, password: str = None) -> str:
	"""Async version of download for Jupyter notebooks."""
	os.makedirs(dest_dir, exist_ok=True)
	fmt = _format_date_for_template(d)

	# Format the URL for this date
	if "{date}" in url_template:
		url = url_template.format(date=fmt["date"])
	else:
		url = url_template.format(**fmt)

	async with async_playwright() as p:
		# Launch browser (headless=True for silent mode)
		browser = await p.chromium.launch(headless=True)
		page = await browser.new_page()

		try:
			await page.goto(url, wait_until="networkidle")

			# [CUSTOMIZE] Login if needed
			if username and password:
				await _login_playwright_async(page, username, password)

			# [CUSTOMIZE] Find and click the download button
			download_button_selector = '#search_primary'  # Realtime Trains download button

			# Set up download handler before clicking
			async with page.expect_download() as download_info:
				await page.click(download_button_selector)
				# Wait a moment for download to start
				await page.wait_for_timeout(1000)

			# Get the downloaded file
			download = await download_info.value
			temp_path = download.path()

			# Derive filename (use date-based naming for consistency)
			filename = f"data_{fmt['date']}.csv"
			dest_path = os.path.join(dest_dir, filename)

			# Move downloaded file to destination
			shutil.move(str(temp_path), dest_path)

			return dest_path

		finally:
			await browser.close()


async def _login_playwright_async(page, username: str, password: str) -> None:
	"""Async version of login."""
	# Realtime Trains login form selectors
	username_selector = '#identifier'
	password_selector = 'input[name="password"]'
	login_button_selector = 'button:has-text("Sign in with password")'

	# Fill in credentials
	await page.fill(username_selector, username)
	await page.fill(password_selector, password)

	# Click login button and wait for navigation
	await page.click(login_button_selector)
	await page.wait_for_load_state("networkidle")


def download_csv_with_browser(url_template: str, d: date, dest_dir: str, username: str = None, password: str = None) -> str:
	"""Download a CSV using Playwright browser automation for button-click workflows.

	This function works in both CLI and Jupyter notebook environments.

	Returns the path to the saved file.
	"""
	if not HAS_PLAYWRIGHT:
		raise ImportError("Playwright is not installed. Run: pip install playwright && playwright install")

	# Run async function in the event loop (works in both CLI and Jupyter)
	return asyncio.run(_download_csv_with_browser_async(url_template, d, dest_dir, username, password))
def _format_date_for_template(d: date) -> dict:
	return {
		"date": d.strftime("%Y%m%d"),
		"yyyy": d.strftime("%Y"),
		"mm": d.strftime("%m"),
		"dd": d.strftime("%d"),
	}


def download_csv_for_date(url_template: str, d: date, dest_dir: str, retries: int = 3, timeout: int = 30,
						  auth: Optional[Tuple[str, str]] = None) -> str:
	"""
	Download a CSV for a specific date using `url_template`.

	url_template may include either `{date}` (formatted as YYYYMMDD) or the named fields
	`{yyyy}`, `{mm}`, `{dd}`. The file will be saved in `dest_dir` with the remote filename
	or a fallback name `data_YYYYMMDD.csv`.

	Returns the path to the saved file.
	Raises urllib.error.HTTPError on persistent HTTP errors.
	"""
	os.makedirs(dest_dir, exist_ok=True)
	fmt = _format_date_for_template(d)
	if "{date}" in url_template:
		url = url_template.format(date=fmt["date"])
	else:
		url = url_template.format(**fmt)

	# derive filename from URL
	filename = os.path.basename(urllib.parse.urlparse(url).path)
	if not filename: 
		filename = f"data_RDG_{fmt['date']}.csv"

	dest_path = os.path.join(dest_dir, filename)

	# use requests with a session-level retry strategy
	session = requests.Session()
	retry_strategy = Retry(total=retries, backoff_factor=1, status_forcelist=[429, 500, 502, 503, 504])
	adapter = HTTPAdapter(max_retries=retry_strategy)
	session.mount("https://", adapter)
	session.mount("http://", adapter)

	attempt = 0
	while attempt < max(1, retries):
		try:
			# include HTTP basic auth if provided
			kwargs = {"timeout": timeout, "headers": {"User-Agent": "train-data-collector/1.0"}}
			if auth:
				# requests accepts a (user, pass) tuple or an HTTPBasicAuth instance
				kwargs["auth"] = HTTPBasicAuth(auth[0], auth[1])
			resp = session.get(url, **kwargs)
			resp.raise_for_status()
			body = resp.content
			with open(dest_path, "wb") as fh:
				fh.write(body)
			return dest_path
		except req_exceptions.HTTPError as e:
			status = None
			if hasattr(e, 'response') and e.response is not None:
				status = e.response.status_code
			# For 404, don't retry
			if status == 404:
				raise
			attempt += 1
			time.sleep(1 + attempt)
		except req_exceptions.RequestException:
			attempt += 1
			time.sleep(1 + attempt)
	raise RuntimeError(f"Failed to download {url} after {retries} attempts")


def daterange(start_date: date, end_date: date) -> Iterable[date]:
	"""
	Yield dates from start_date to end_date inclusive.
	"""
	current = start_date
	while current <= end_date:
		yield current
		current = current + timedelta(days=1)


def collect_csvs(start_date: date, end_date: date, url_template: str, output_file: str, dest_dir: str = "../resources",
				 auth: Optional[Tuple[str, str]] = None) -> str:
	"""Download CSVs for each day in [start_date, end_date] and merge into `output_file`.

	The header from the first successfully downloaded file is preserved; subsequent headers are skipped.
	Returns the path to `output_file`.
	"""
	os.makedirs(os.path.dirname(output_file) or ".", exist_ok=True)
	downloaded_files = []
	for d in daterange(start_date, end_date):
		try:
			path = download_csv_for_date(url_template, d, dest_dir, auth=auth)
			print(f"Downloaded {d.isoformat()} -> {path}")
			downloaded_files.append(path)
		except req_exceptions.HTTPError as e:
			status = None
			if hasattr(e, 'response') and e.response is not None:
				status = e.response.status_code
			print(f"Skipping {d.isoformat()}: HTTP {status}")
		except Exception as e:
			print(f"Skipping {d.isoformat()}: {e}")

	if not downloaded_files:
		raise RuntimeError("No files were downloaded; cannot create merged CSV")

	# concatenate, preserving header from first file only
	first = True
	with open(output_file, "w", encoding="utf-8") as out_f:
		for fp in downloaded_files:
			with open(fp, "r", encoding="utf-8", errors="replace") as in_f:
				lines = in_f.readlines()
				if not lines:
					continue
				if first:
					out_f.writelines(lines)
					first = False
				else:
					# skip header line
					out_f.writelines(lines[1:])

	print(f"Merged {len(downloaded_files)} files into {output_file}")
	return output_file


def _parse_date(s: str) -> date:
	# support YYYY-MM-DD or YYYYMMDD
	try:
		if "-" in s:
			return datetime.strptime(s, "%Y-%m-%d").date()
		return datetime.strptime(s, "%Y%m%d").date()
	except ValueError:
		raise argparse.ArgumentTypeError(f"Invalid date: {s}. Use YYYY-MM-DD or YYYYMMDD")


def main(argv=None):
	parser = argparse.ArgumentParser(description="Download daily Realtime train CSVs and merge into one file.")
	parser.add_argument("start_date", type=_parse_date, help="Start date (inclusive) YYYY-MM-DD or YYYYMMDD")
	parser.add_argument("end_date", type=_parse_date, help="End date (inclusive) YYYY-MM-DD or YYYYMMDD")
	parser.add_argument("url_template", help=("URL template for daily CSV. Use either '{date}' for YYYYMMDD or '{yyyy}', '{mm}', '{dd}' fields. "
												 "Example: https://example.org/data_{date}.csv"))
	parser.add_argument("output", help="Path for merged CSV output file")
	parser.add_argument("--dest-dir", default="../resources", help="Directory to save downloaded daily CSVs")
	parser.add_argument("--username", help="Username for HTTP basic auth or browser login")
	parser.add_argument("--password", help="Password for HTTP basic auth or browser login (avoid using on shared shells)")
	parser.add_argument("--use-browser", action="store_true", help="Use browser automation (Playwright) instead of direct HTTP requests. Required for button-click downloads.")
	args = parser.parse_args(argv)

	start = args.start_date
	end = args.end_date
	if start > end:
		parser.error("start_date must be <= end_date")

	# prepare auth tuple if requested
	auth = None
	if args.username and not args.use_browser:
		pwd = args.password if args.password is not None else getpass.getpass(prompt=f"Password for {args.username}: ")
		auth = (args.username, pwd)

	try:
		if args.use_browser:
			if async_playwright is None:
				print("Error: Playwright not installed. Run: pip install playwright && playwright install")
				sys.exit(1)
			collect_csvs_with_browser(start, end, args.url_template, args.output, dest_dir=args.dest_dir,
										username=args.username, password=args.password)
		else:
			collect_csvs(start, end, args.url_template, args.output, dest_dir=args.dest_dir, auth=auth)
	except Exception as e:
		print(f"Error: {e}")
		sys.exit(2)


def collect_csvs_with_browser(start_date: date, end_date: date, url_template: str, output_file: str,
								dest_dir: str = "../resources", username: str = None, password: str = None) -> str:
	"""Download CSVs using browser automation for each day in [start_date, end_date], then merge.

	Returns the path to the merged output file.
	"""
	os.makedirs(os.path.dirname(output_file) or ".", exist_ok=True)
	downloaded_files = []
	for d in daterange(start_date, end_date):
		try:
			path = download_csv_with_browser(url_template, d, dest_dir, username=username, password=password)
			print(f"Downloaded {d.isoformat()} -> {path}")
			downloaded_files.append(path)
		except Exception as e:
			print(f"Skipping {d.isoformat()}: {e}")

	if not downloaded_files:
		raise RuntimeError("No files were downloaded; cannot create merged CSV")

	# concatenate, preserving header from first file only
	first = True
	with open(output_file, "w", encoding="utf-8") as out_f:
		for fp in downloaded_files:
			with open(fp, "r", encoding="utf-8", errors="replace") as in_f:
				lines = in_f.readlines()
				if not lines:
					continue
				if first:
					out_f.writelines(lines)
					first = False
				else:
					# skip header line
					out_f.writelines(lines[1:])

	print(f"Merged {len(downloaded_files)} files into {output_file}")
	return output_file


if __name__ == "__main__":
	main()
