#!.venv/bin/python
"""
Download SEC company-ticker mapping and save as companies.csv
"""

import csv
import json
from pathlib import Path
from typing import Dict, List, NamedTuple


import requests


URL = "https://www.sec.gov/files/company_tickers.json"
CSV_PATH = Path("companies.csv")

# SEC asks for a contact e-mail in the User-Agent header
HEADERS = {
    "User-Agent": "example@example.com"
}

class Company(NamedTuple):
    cik: int
    ticker: str
    title: str


def parse_blob(blob: Dict[str, dict]) -> List[Company]:
    """
    Convert the raw JSON blob into a list of Company objects.
    """
    companies = []
    for key, value in blob.items():
        # key is a string like "0", "1", …
        companies.append(
            Company(
                cik=value["cik_str"],
                ticker=value["ticker"],
                title=value["title"],
            )
        )
    return companies


def load_from_file(path: Path) -> List[Company]:
    """
    Read JSON from disk and parse it.
    """
    with path.open("r", encoding="utf-8") as fp:
        raw = json.load(fp)
    return parse_blob(raw)

def download_json(url: str) -> Dict[str, dict]:
    """GET the JSON blob from SEC."""
    resp = requests.get(url, headers=HEADERS, timeout=30)
    resp.raise_for_status()
    return resp.json()


def write_csv(companies, csv_path: Path) -> None:
    """Write the list of Company objects to csv_path."""
    with csv_path.open("w", newline="", encoding="utf-8") as f:
        writer = csv.writer(f)
        writer.writerow(["cik", "ticker", "title"])
        for c in companies:
            writer.writerow([c.cik, c.ticker, c.title])


if __name__ == "__main__":
    raw = download_json(URL)
    companies = parse_blob(raw)
    write_csv(companies, CSV_PATH)
    print(f"Saved {len(companies)} rows to {CSV_PATH}")
