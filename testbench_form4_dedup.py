#!/usr/bin/env python3
"""
testbench_form4_dedup.py
Fetch one Form 4 and strip obvious redundancy.
"""

import sqlite3
import re
from typing import Tuple

###############################################################################
# 1.  Grab one filing from the DB
###############################################################################
def fetch_one_form4() -> Tuple[str, str, str]:
    """Return (ticker, accession, raw_text) for one Form 4."""
    with sqlite3.connect("filings.db") as conn:
        conn.row_factory = sqlite3.Row
        cur = conn.cursor()
        row = cur.execute("""
            SELECT ticker, accession, text
            FROM filings
            WHERE form_type = '4'
            LIMIT 1
        """).fetchone()
        if not row:
            raise RuntimeError("No Form 4 found in filings table")
        return row["ticker"], row["accession"], row["text"]

###############################################################################
# 2.  Very small de-duplication / boiler-plate stripper
###############################################################################
import xml.etree.ElementTree as ET

def print_xml_as_filesystem(xml_string):
   try:
       root = ET.fromstring(xml_string)
   except ET.ParseError as e:
       lineno, col = e.position
       lines = xml_string.splitlines()
       print("ParseError:", e)
       print("Line", lineno, "->", repr(lines[lineno-1]))
       print(" " * (len("Line {} -> ".format(lineno)) + col - 1) + "^")
       return

def print_element_paths(element, path="") -> str:
    """
    Returns a string containing all element paths and their non-empty text values
    in the format: "path1=text1\npath2=text2\n..."
    """
    lines = []
    tag = element.tag
    current_path = f"{path}/{tag}" if path else tag
    text = (element.text or "").strip()
    if text:
        lines.append(f"{current_path}={text}")

    for child in element:
        lines.append(print_element_paths(child, current_path))

    return "\n".join(lines)


def strip_redundancy(text: str) -> str:
    """
    Remove the most common repetitive blocks in a Form 4:
      - HTML tags
      - Repeated headers/footers
      - Long sequences of dashes or underscores
      - Empty lines
    This is intentionally *lightweight*; refine as you see fit.
    """
    match = re.search(r"<XML>(.*?)</XML>", text, flags=re.DOTALL | re.IGNORECASE)
    root = ET.fromstring(match.group(1).strip())
    return print_element_paths(root)

###############################################################################
# 3.  Quick visual test
###############################################################################
if __name__ == "__main__":
    ticker, accession, raw = fetch_one_form4()
    print("=" * 80)
    print(f"RAW   ({ticker} / {accession})")
    print("=" * 80)
    print(raw)          # first 2k chars so the console doesn’t explode
    print("\n" + "=" * 80)
    print("CLEANED")
    print("=" * 80)
    cleaned = strip_redundancy(raw)
    print(cleaned)
