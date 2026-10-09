"""
EECS E6893 Big Data Analytics - HW2 Q2.2
Standalone data-fetch step for the Airflow watchlist pipeline.

Downloads the latest 30 trading sessions for NVDA, TSLA, and SPY and writes:
    $AIRFLOW_HOME/hw2_watchlist_data/raw_watchlist_prices.csv
"""

from pathlib import Path
import os

import pandas as pd
import yfinance as yf


TICKERS = ["NVDA", "TSLA", "SPY"]

AIRFLOW_HOME = Path(os.environ.get("AIRFLOW_HOME", str(Path.home() / "airflow")))
DATA_DIR = AIRFLOW_HOME / "hw2_watchlist_data"
RAW_CSV = DATA_DIR / "raw_watchlist_prices.csv"


def main():
    DATA_DIR.mkdir(parents=True, exist_ok=True)
    frames = []

    for ticker in TICKERS:
        print(f"Fetching {ticker}...")

        # Request a wider calendar range, then retain the latest 30 trading days.
        hist = yf.Ticker(ticker).history(
            period="3mo",
            interval="1d",
            auto_adjust=False,
        )

        if hist.empty:
            raise RuntimeError(f"No yfinance data returned for {ticker}")

        required = ["Open", "High", "Low", "Close", "Volume"]
        missing = [column for column in required if column not in hist.columns]
        if missing:
            raise RuntimeError(f"{ticker} is missing columns: {missing}")

        hist = hist.tail(30).copy()
        hist.index = pd.to_datetime(hist.index)

        # Keep the CSV easy to read by removing timezone information.
        try:
            hist.index = hist.index.tz_localize(None)
        except TypeError:
            pass

        hist = hist.reset_index()
        date_column = hist.columns[0]
        hist = hist.rename(columns={date_column: "Date"})
        hist["Ticker"] = ticker

        keep = ["Date", "Ticker", "Open", "High", "Low", "Close", "Volume"]
        frames.append(hist[keep])
        print(f"{ticker}: collected {len(hist)} rows")

    combined = pd.concat(frames, ignore_index=True)
    combined = combined.sort_values(["Ticker", "Date"])
    combined.to_csv(RAW_CSV, index=False)

    print(f"Saved {len(combined)} rows to {RAW_CSV}")
    print(combined.tail(10).to_string(index=False))
    print("FETCH COMPLETE")


if __name__ == "__main__":
    main()
