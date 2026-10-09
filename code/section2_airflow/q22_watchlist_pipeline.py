"""
EECS E6893 Big Data Analytics - HW2 Q2.2
End-to-End Stock Watchlist Pipeline

Final working Airflow structure:
1. fetch_data          - BashOperator (runs fetch_watchlist_data.py)
2. analytics_summary   - PythonOperator
3. volatility_alert    - PythonOperator

Outputs:
- raw_watchlist_prices.csv
- watchlist_summary.csv
"""

from pathlib import Path
import os
import shlex
import sys

import pandas as pd
import pendulum

from airflow import DAG
from airflow.operators.bash import BashOperator
from airflow.operators.python import PythonOperator


TICKERS = ["NVDA", "TSLA", "SPY"]
ALERT_THRESHOLD_PCT = 3.0

AIRFLOW_HOME = Path(os.environ.get("AIRFLOW_HOME", str(Path.home() / "airflow")))
DATA_DIR = AIRFLOW_HOME / "hw2_watchlist_data"
RAW_CSV = DATA_DIR / "raw_watchlist_prices.csv"
SUMMARY_CSV = DATA_DIR / "watchlist_summary.csv"

# Keep the network fetch in its own process. This is the final version used
# for the successful Airflow run with SequentialExecutor.
FETCH_SCRIPT = Path(__file__).with_name("fetch_watchlist_data.py")
PYTHON_EXECUTABLE = Path(sys.executable)


def analytics_summary():
    if not RAW_CSV.exists():
        raise FileNotFoundError(f"Missing input file: {RAW_CSV}")

    df = pd.read_csv(RAW_CSV, parse_dates=["Date"])
    summaries = []

    for ticker, g in df.groupby("Ticker"):
        g = g.sort_values("Date").copy()

        initial = g.iloc[0]
        latest = g.iloc[-1]

        return_pct = ((latest["Close"] - initial["Close"]) / initial["Close"]) * 100.0

        g["daily_swing_pct"] = ((g["High"] - g["Low"]) / g["Open"]) * 100.0
        avg_swing_pct = g["daily_swing_pct"].mean()
        latest_swing_pct = g.iloc[-1]["daily_swing_pct"]

        summaries.append(
            {
                "Ticker": ticker,
                "Initial_Date": initial["Date"].date().isoformat(),
                "Latest_Date": latest["Date"].date().isoformat(),
                "Initial_Close": round(float(initial["Close"]), 4),
                "Latest_Close": round(float(latest["Close"]), 4),
                "Return_30D_Pct": round(float(return_pct), 4),
                "Avg_Daily_Swing_Pct": round(float(avg_swing_pct), 4),
                "Latest_Day_Swing_Pct": round(float(latest_swing_pct), 4),
            }
        )

    summary = pd.DataFrame(summaries).sort_values("Ticker")
    summary.to_csv(SUMMARY_CSV, index=False)

    print(f"Saved summary to {SUMMARY_CSV}")
    print(summary.to_string(index=False))


def volatility_alert():
    if not SUMMARY_CSV.exists():
        raise FileNotFoundError(f"Missing summary file: {SUMMARY_CSV}")

    summary = pd.read_csv(SUMMARY_CSV)
    alerts = summary[summary["Latest_Day_Swing_Pct"] >= ALERT_THRESHOLD_PCT]

    if alerts.empty:
        print(
            f"All assets traded within normal volatility ranges "
            f"(< {ALERT_THRESHOLD_PCT:.1f}% latest-day swing)."
        )
    else:
        for _, row in alerts.iterrows():
            print(
                f"ALERT: {row['Ticker']} experienced high volatility "
                f"({row['Latest_Day_Swing_Pct']:.2f}% swing) "
                f"on {row['Latest_Date']}"
            )


with DAG(
    dag_id="hw2_q22_watchlist_pipeline",
    description="Fetch -> analytics -> high-volatility alert",
    start_date=pendulum.datetime(2026, 10, 1, tz="America/New_York"),
    schedule=None,
    catchup=False,
    tags=["EECS6893", "HW2", "Q2.2"],
) as dag:

    fetch = BashOperator(
        task_id="fetch_data",
        bash_command=(
            f"{shlex.quote(str(PYTHON_EXECUTABLE))} "
            f"{shlex.quote(str(FETCH_SCRIPT))}"
        ),
    )

    analyze = PythonOperator(
        task_id="analytics_summary",
        python_callable=analytics_summary,
    )

    alert = PythonOperator(
        task_id="volatility_alert",
        python_callable=volatility_alert,
    )

    fetch >> analyze >> alert
