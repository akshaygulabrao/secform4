#!.venv/bin/python

import sqlite3, re, llm, json, os, sys
from testbench_form4_dedup import strip_redundancy
from typing import List, Tuple

###############################################################################
# 1.  LLM setup
###############################################################################
model = llm.get_model("kimi-k2")          # or any other model you have installed

###############################################################################
# 2.  Helper: very small ETF / fund filter
###############################################################################
# A cheap heuristic: if the ticker appears in a curated list of ETFs/CEFs/UITs
# we skip it.  For production you would hit an API (Refinitiv, Bloomberg, etc.)
# or parse the CIK <-> entity mapping file from SEC.
FUNDS = {
    "SPY","QQQ","IWM","VTI","EFA","EEM","TLT","GLD","SLV","VNQ","AGG",
    "XLF","XLK","XLE","GDX","ARKK","ARKQ","ARKW","TQQQ","SQQQ","UVXY"
}

def is_fund(ticker: str) -> bool:
    return ticker.upper() in FUNDS

###############################################################################
# 3.  Prompt engineering
###############################################################################
SYSTEM_PROMPT = """
You are an expert SEC-filing analyst.
Given the full text of a Form 4 (Statement of Changes in Beneficial Ownership),
classify the sentiment of the transaction(s) into exactly one of:
  - "Bullish"   (open-market purchases, exercise-and-hold, etc.)
  - "Bearish"   (open-market sales, large dispositions)
  - "Neutral"   (10b5-1 plan, automatic vesting, tax withholding, gift, etc.)

Return **only** a single JSON object like:
{"sentiment": "Bullish"}
"""

def classify(text: str) -> str:
    """Call the LLM and return the sentiment string."""
    try:
        resp = model.prompt(text, system=SYSTEM_PROMPT)
        data = json.loads(resp.text().strip())
        return data.get("sentiment", "Unknown")
    except Exception as e:
        print("LLM error:", e, file=sys.stderr)
        return "Error"

###############################################################################
# 4.  Main pipeline
###############################################################################
def main() -> None:
    with sqlite3.connect("filings.db") as conn:
        conn.row_factory = sqlite3.Row
        cur = conn.cursor()

        # Create output table if it doesn't exist
        cur.execute("""
            CREATE TABLE IF NOT EXISTS form4_sentiment (
                ticker TEXT,
                accession TEXT,
                sentiment TEXT,
                PRIMARY KEY (ticker, accession)
            )
        """)

        rows: List[Tuple[str, str, str]] = cur.execute("""
            SELECT ticker, accession, text
            FROM filings
            WHERE form_type = '4'
        """).fetchall()

        for ticker, accession, text in rows:
            if is_fund(ticker):
                continue

            # Skip if we already processed this accession
            if cur.execute("SELECT 1 FROM form4_sentiment WHERE accession = ?", (accession,)).fetchone():
                continue

            sentiment = classify(strip_redundancy(text))
            cur.execute("INSERT INTO form4_sentiment(ticker, accession, sentiment) VALUES (?,?,?)",
                        (ticker, accession, sentiment))
            conn.commit()
            print(f"{ticker}  {accession}  ->  {sentiment}")

if __name__ == "__main__":
    main()
