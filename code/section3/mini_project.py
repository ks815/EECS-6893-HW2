"""
EECS E6893 Big Data Analytics - HW2
Section 3 Mini-Project

Stock Risk and Momentum Analysis
Assets: NVDA, TSLA, SPY

Analysis:
1. Six-month cumulative return
2. Annualized volatility
3. Maximum drawdown
4. 7-day and 20-day moving averages
5. Momentum classification
6. Risk classification
"""

from pathlib import Path

import numpy as np
import pandas as pd
import matplotlib.pyplot as plt
import yfinance as yf


TICKERS = ["NVDA", "TSLA", "SPY"]

OUTPUT_DIR = Path("mini_project_output")
OUTPUT_DIR.mkdir(exist_ok=True)


all_data = []
summary_rows = []


for ticker in TICKERS:
    print(f"\nDownloading {ticker}...")

    df = yf.Ticker(ticker).history(
        period="6mo",
        interval="1d",
        auto_adjust=False,
    )

    if df.empty:
        raise RuntimeError(f"No data returned for {ticker}")

    df = df.copy()

    # Keep timezone from creating CSV/plot problems.
    try:
        df.index = df.index.tz_localize(None)
    except TypeError:
        pass

    # Core analytics.
    df["Daily_Return"] = df["Close"].pct_change()
    df["Cumulative_Return"] = (
        (1 + df["Daily_Return"].fillna(0)).cumprod() - 1
    )

    df["MA_7"] = df["Close"].rolling(7).mean()
    df["MA_20"] = df["Close"].rolling(20).mean()

    df["Rolling_Volatility_20D"] = (
        df["Daily_Return"].rolling(20).std()
        * np.sqrt(252)
    )

    running_max = df["Close"].cummax()

    df["Drawdown"] = (
        df["Close"] / running_max - 1
    )

    initial_close = float(df["Close"].iloc[0])
    latest_close = float(df["Close"].iloc[-1])

    six_month_return = (
        (latest_close - initial_close)
        / initial_close
        * 100
    )

    annualized_volatility = (
        df["Daily_Return"].std()
        * np.sqrt(252)
        * 100
    )

    max_drawdown = (
        df["Drawdown"].min() * 100
    )

    ma7 = float(df["MA_7"].iloc[-1])
    ma20 = float(df["MA_20"].iloc[-1])

    # Momentum classification.
    if latest_close > ma7 > ma20:
        momentum = "Bullish"
    elif latest_close < ma7 < ma20:
        momentum = "Bearish"
    else:
        momentum = "Neutral"

    # Simple risk classification based on annualized volatility.
    if annualized_volatility >= 40:
        risk = "High"
    elif annualized_volatility >= 25:
        risk = "Medium"
    else:
        risk = "Low"

    temp = df.reset_index()
    temp["Ticker"] = ticker
    all_data.append(temp)

    summary_rows.append({
        "Ticker": ticker,
        "Initial_Close": round(initial_close, 2),
        "Latest_Close": round(latest_close, 2),
        "Six_Month_Return_Pct": round(
            six_month_return, 2
        ),
        "Annualized_Volatility_Pct": round(
            annualized_volatility, 2
        ),
        "Max_Drawdown_Pct": round(
            max_drawdown, 2
        ),
        "MA_7": round(ma7, 2),
        "MA_20": round(ma20, 2),
        "Momentum": momentum,
        "Risk_Level": risk,
    })


combined = pd.concat(all_data, ignore_index=True)

summary = pd.DataFrame(summary_rows)

combined.to_csv(
    OUTPUT_DIR / "stock_analysis_data.csv",
    index=False,
)

summary.to_csv(
    OUTPUT_DIR / "stock_risk_momentum_summary.csv",
    index=False,
)


print("\n" + "=" * 85)
print("STOCK RISK AND MOMENTUM SUMMARY")
print("=" * 85)
print(summary.to_string(index=False))


# ---------------------------------------------------------
# Plot 1: cumulative return
# ---------------------------------------------------------
plt.figure(figsize=(10, 6))

for ticker in TICKERS:
    temp = combined[
        combined["Ticker"] == ticker
    ].copy()

    plt.plot(
        temp["Date"],
        temp["Cumulative_Return"] * 100,
        label=ticker,
    )

plt.axhline(0, linewidth=1)
plt.title("Six-Month Cumulative Return")
plt.xlabel("Date")
plt.ylabel("Cumulative Return (%)")
plt.legend()
plt.tight_layout()

plt.savefig(
    OUTPUT_DIR / "cumulative_return.png",
    dpi=180,
)

plt.close()


# ---------------------------------------------------------
# Plot 2: rolling volatility
# ---------------------------------------------------------
plt.figure(figsize=(10, 6))

for ticker in TICKERS:
    temp = combined[
        combined["Ticker"] == ticker
    ].copy()

    plt.plot(
        temp["Date"],
        temp["Rolling_Volatility_20D"] * 100,
        label=ticker,
    )

plt.title("20-Day Rolling Annualized Volatility")
plt.xlabel("Date")
plt.ylabel("Annualized Volatility (%)")
plt.legend()
plt.tight_layout()

plt.savefig(
    OUTPUT_DIR / "rolling_volatility.png",
    dpi=180,
)

plt.close()


# ---------------------------------------------------------
# Plot 3: latest price and moving averages
# ---------------------------------------------------------
plt.figure(figsize=(10, 6))

for ticker in TICKERS:
    temp = combined[
        combined["Ticker"] == ticker
    ].copy()

    # Normalize each series so assets with different prices
    # can be compared on the same chart.
    base = temp["Close"].iloc[0]

    plt.plot(
        temp["Date"],
        temp["Close"] / base * 100,
        label=f"{ticker} Close",
    )

plt.title("Normalized Price Performance")
plt.xlabel("Date")
plt.ylabel("Normalized Price (Start = 100)")
plt.legend()
plt.tight_layout()

plt.savefig(
    OUTPUT_DIR / "normalized_price.png",
    dpi=180,
)

plt.close()


print("\nSaved files:")
print(
    OUTPUT_DIR /
    "stock_analysis_data.csv"
)
print(
    OUTPUT_DIR /
    "stock_risk_momentum_summary.csv"
)
print(
    OUTPUT_DIR /
    "cumulative_return.png"
)
print(
    OUTPUT_DIR /
    "rolling_volatility.png"
)
print(
    OUTPUT_DIR /
    "normalized_price.png"
)

print("\nMINI-PROJECT COMPLETE")
