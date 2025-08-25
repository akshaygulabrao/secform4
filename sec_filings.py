#!.venv/bin/python
import sqlite3
import shutil
from sec_edgar_downloader import Downloader
import pandas as pd
from pathlib import Path
import re

# ------------------------------------------------------------------
# 1.  CONFIGURATION
# ------------------------------------------------------------------
DB_PATH = Path("filings.db")          # where the SQLite DB lives
CSV_PATH = Path("target_stocks.csv")      # your input file

# ------------------------------------------------------------------
# 2.  PREPARE DATABASE
# ------------------------------------------------------------------
conn = sqlite3.connect(DB_PATH)
conn.execute("""
CREATE TABLE IF NOT EXISTS filings (
    ticker            TEXT,
    form_type         TEXT,
    accession         TEXT,
    text              TEXT,
    acceptance_datetime TEXT,
    PRIMARY KEY (ticker, form_type, accession)
);
""")
conn.commit()

# ------------------------------------------------------------------
# 3.  LOAD COMPANY LIST
# ------------------------------------------------------------------
df = pd.read_csv(CSV_PATH)

# ------------------------------------------------------------------
# 4.  DOWNLOADER
# ------------------------------------------------------------------
dl = Downloader("Akshay Gulabrao", "aksgula22@gmail.com")

root = Path("sec-edgar-filings")
ad_re = re.compile(r"<ACCEPTANCE-DATETIME>(\d{14})", re.I)

# ------------------------------------------------------------------
# 5.  INGEST / CLEAN HELPER
# ------------------------------------------------------------------
def _ingest_and_clean(root: Path, ticker: str, form_type: str) -> None:
    if type(ticker) != 'str': return
    ticker_dir = root / ticker.upper()
    form_dir   = ticker_dir / form_type.upper()

    if not form_dir.exists():
        return

    for accession_dir in form_dir.iterdir():
        accession = accession_dir.name
        txt_file  = accession_dir / "full-submission.txt"

        if txt_file.exists():
            text = txt_file.read_text(encoding="utf-8", errors="ignore")
            m = ad_re.search(text[:1000])
            acceptance_dt = m.group(1) if m else None

            conn.execute(
                """
                INSERT INTO filings
                (ticker, form_type, accession, text, acceptance_datetime)
                VALUES (?, ?, ?, ?, ?)
                """,
                (ticker.upper(), form_type.upper(), accession, text, acceptance_dt),
            )
            conn.commit()

        # Remove the accession directory
        shutil.rmtree(accession_dir, ignore_errors=True)

    # Remove empty directories
    try:
        form_dir.rmdir()
    except OSError:
        pass
    try:
        ticker_dir.rmdir()
    except OSError:
        pass

# ------------------------------------------------------------------
# 6.  MAIN LOOP
# ------------------------------------------------------------------
for _, row in df.iterrows():
    ticker = row['ticker']
    print(row['ticker'])

    # --- 10-K ------------------------------------------------------
    # dl.get("10-K", ticker, limit=1)
    # _ingest_and_clean(root, ticker, "10-K")

    # --- 10-Q ------------------------------------------------------
    # dl.get("10-Q", ticker, limit=5)
    # _ingest_and_clean(root, ticker, "10-Q")

    # --- 8-K -------------------------------------------------------
    # dl.get("8-K", ticker, limit=5)
    # _ingest_and_clean(root, ticker, "8-K")

    # --- Form 4 ----------------------------------------------------
    try:
        dl.get("4", ticker, limit=10, after="2025-08-15")
        _ingest_and_clean(root, ticker, "4")
    except ValueError:
        print(f"Could not fetch ticker {ticker}")
    except:
        print(f"Error with {ticker}")
        raise

# ------------------------------------------------------------------
# 7.  SANITY CHECK
# ------------------------------------------------------------------
count = conn.execute("SELECT COUNT(*) FROM filings").fetchone()[0]
print(f"Loaded {count} filings into {DB_PATH}")
conn.close()
