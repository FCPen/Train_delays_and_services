from datetime import date, timedelta, datetime
import argparse
import os
import sys
import time
from typing import Iterable
import urllib.parse
import requests
from requests.adapters import HTTPAdapter
from requests import exceptions as req_exceptions
from urllib3.util.retry import Retry


def _format_date_for_template(d: date) -> dict:
	return {
		"date": d.strftime("%Y%m%d"),
		"yyyy": d.strftime("%Y"),
		"mm": d.strftime("%m"),
		"dd": d.strftime("%d"),
	}


def download_csv_for_date(url_template: str, d: date, dest_dir: str, retries: int = 3, timeout: int = 30) -> str:
	"""Download a CSV for a specific date using `url_template`.

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
		filename = f"data_{fmt['date']}.csv"

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
			resp = session.get(url, timeout=timeout, headers={"User-Agent": "train-data-collector/1.0"})
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
	"""Yield dates from start_date to end_date inclusive."""
	current = start_date
	while current <= end_date:
		yield current
		current = current + timedelta(days=1)


def collect_csvs(start_date: date, end_date: date, url_template: str, output_file: str, dest_dir: str = "data/raw") -> str:
	"""Download CSVs for each day in [start_date, end_date] and merge into `output_file`.

	The header from the first successfully downloaded file is preserved; subsequent headers are skipped.
	Returns the path to `output_file`.
	"""
	os.makedirs(os.path.dirname(output_file) or ".", exist_ok=True)
	downloaded_files = []
	for d in daterange(start_date, end_date):
		try:
			path = download_csv_for_date(url_template, d, dest_dir)
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
	parser.add_argument("--dest-dir", default="data/raw", help="Directory to save downloaded daily CSVs")
	args = parser.parse_args(argv)

	start = args.start_date
	end = args.end_date
	if start > end:
		parser.error("start_date must be <= end_date")

	try:
		collect_csvs(start, end, args.url_template, args.output, dest_dir=args.dest_dir)
	except Exception as e:
		print(f"Error: {e}")
		sys.exit(2)


if __name__ == "__main__":
	main()
