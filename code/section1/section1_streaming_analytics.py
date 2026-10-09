"""
EECS E6893 Big Data Analytics - HW2
Section 1: Streaming Analytics

What this script does
---------------------
1. Calls the Massive aggregate-bars API 7 times.
2. Each call requests 1-minute AAPL bars for a one-hour historical window.
3. The simulated "current time" advances by 5 minutes each pull.
4. Incoming rows replace overlapping rows already in the Spark DataFrame
   using a left anti join, then new rows are appended.
5. After every pull, 30-minute moving averages are recomputed and stored
   in a separate Spark DataFrame.
6. With PULL_EVERY_SECONDS=300, calls occur at t=0,5,...,30 minutes.

Run this on the Dataproc Jupyter/Spark environment.
Do not put your API key directly in screenshots.
"""

import os
import time
import getpass
from datetime import datetime, timedelta, timezone

import requests
from pyspark.sql import SparkSession, functions as F
from pyspark.sql.types import (
    StructType, StructField, StringType, TimestampType, DoubleType
)
from pyspark.sql.window import Window

# ---------------------------
# Configuration
# ---------------------------
TICKER = "AAPL"
BASE_URL = "https://api.massive.com"
NUM_CALLS = 7
PULL_EVERY_SECONDS = 300       # REQUIRED final run: 300 seconds = 5 minutes
WINDOW_MINUTES = 60
STEP_MINUTES = 5

# Completed past trading day.
# 2026-09-29 was a Tuesday. 13:30 UTC = 09:30 ET while daylight saving is active.
# First API call covers 13:30-14:30 UTC; last covers 14:00-15:00 UTC.
FIRST_WINDOW_END_UTC = datetime(2026, 9, 29, 14, 30, tzinfo=timezone.utc)

# For a quick API smoke test, temporarily set NUM_CALLS = 1 instead of
# compressing all 7 calls into a few seconds (which may hit API rate limits).
# Restore NUM_CALLS = 7 and PULL_EVERY_SECONDS = 300 for the submission run.
assert PULL_EVERY_SECONDS > 0

API_KEY = os.environ.get("MASSIVE_API_KEY")
if not API_KEY:
    API_KEY = getpass.getpass("Enter your Massive API key (hidden): ").strip()
if not API_KEY:
    raise ValueError("Massive API key is required.")

# ---------------------------
# Spark setup
# ---------------------------
spark = SparkSession.builder.appName("HW2_Streaming_AAPL").getOrCreate()
spark.conf.set("spark.sql.session.timeZone", "UTC")

stock_schema = StructType([
    StructField("Stock Name", StringType(), False),
    StructField("UTC Timestamp", TimestampType(), False),
    StructField("c", DoubleType(), True),
    StructField("l", DoubleType(), True),
    StructField("h", DoubleType(), True),
    StructField("o", DoubleType(), True),
    StructField("v", DoubleType(), True),
])

ma_schema = StructType([
    StructField("Datetime", TimestampType(), False),
    StructField("c_MA", DoubleType(), True),
    StructField("l_MA", DoubleType(), True),
    StructField("h_MA", DoubleType(), True),
    StructField("o_MA", DoubleType(), True),
    StructField("v_MA", DoubleType(), True),
])

stock_df = spark.createDataFrame([], stock_schema)
moving_average_df = spark.createDataFrame([], ma_schema)


def to_epoch_ms(dt_utc: datetime) -> int:
    """Convert timezone-aware UTC datetime to Unix milliseconds."""
    return int(dt_utc.timestamp() * 1000)


def pull_one_hour(ticker: str, start_utc: datetime, end_utc: datetime):
    """
    Fetch 1-minute aggregate bars from Massive for [start_utc, end_utc].
    Returns a list of rows matching stock_schema.
    """
    start_ms = to_epoch_ms(start_utc)
    end_ms = to_epoch_ms(end_utc)

    url = (
        f"{BASE_URL}/v2/aggs/ticker/{ticker}/range/1/minute/"
        f"{start_ms}/{end_ms}"
    )
    params = {
        "adjusted": "true",
        "sort": "asc",
        "limit": 50000,
        "apiKey": API_KEY,
    }

    response = requests.get(url, params=params, timeout=30)

    if response.status_code != 200:
        raise RuntimeError(
            f"Massive API failed: HTTP {response.status_code}\n"
            f"{response.text[:500]}"
        )

    payload = response.json()
    results = payload.get("results", [])

    rows = []
    for r in results:
        # Massive key "t" is Unix time in milliseconds.
        ts_utc = datetime.fromtimestamp(r["t"] / 1000.0, tz=timezone.utc)
        # Spark session is UTC; store a naive datetime representing UTC.
        ts_utc_naive = ts_utc.replace(tzinfo=None)

        rows.append((
            ticker,
            ts_utc_naive,
            float(r["c"]) if r.get("c") is not None else None,
            float(r["l"]) if r.get("l") is not None else None,
            float(r["h"]) if r.get("h") is not None else None,
            float(r["o"]) if r.get("o") is not None else None,
            float(r["v"]) if r.get("v") is not None else None,
        ))

    return rows


def incremental_upsert(existing_df, incoming_df):
    """
    Replace overlapping (Stock Name, UTC Timestamp) rows with the newest pull,
    using the assignment hint: a left anti join.
    """
    keys = ["Stock Name", "UTC Timestamp"]
    incoming_keys = incoming_df.select(*keys).dropDuplicates()

    old_non_overlapping = existing_df.join(
        incoming_keys,
        on=keys,
        how="left_anti",
    )

    return (
        old_non_overlapping
        .unionByName(incoming_df)
        .dropDuplicates(keys)
        .orderBy("UTC Timestamp")
    )


def compute_30_minute_moving_averages(all_stock_df):
    """
    Compute trailing 30-minute moving averages ending at each observation time.

    The window is time-based rather than "last 30 rows", so a few missing
    minute bars do not break the 30-minute interpretation.
    """
    with_seconds = all_stock_df.withColumn(
        "_event_seconds",
        F.col("UTC Timestamp").cast("long")
    )

    # Current minute + previous 29 minutes = a 30-minute trailing window.
    w = (
        Window
        .partitionBy("Stock Name")
        .orderBy(F.col("_event_seconds"))
        .rangeBetween(-(29 * 60), 0)
    )

    return (
        with_seconds
        .select(
            F.col("UTC Timestamp").alias("Datetime"),
            F.avg("c").over(w).alias("c_MA"),
            F.avg("l").over(w).alias("l_MA"),
            F.avg("h").over(w).alias("h_MA"),
            F.avg("o").over(w).alias("o_MA"),
            F.avg("v").over(w).alias("v_MA"),
        )
        .orderBy("Datetime")
    )


def incremental_replace_ma(existing_ma_df, fresh_ma_df):
    """
    Keep the moving-average table as a separate incrementally maintained DF.
    Recomputed timestamps replace older versions.
    """
    fresh_keys = fresh_ma_df.select("Datetime").dropDuplicates()
    old_non_overlapping = existing_ma_df.join(
        fresh_keys,
        on="Datetime",
        how="left_anti",
    )
    return (
        old_non_overlapping
        .unionByName(fresh_ma_df)
        .dropDuplicates(["Datetime"])
        .orderBy("Datetime")
    )


# ---------------------------
# Main 30-minute simulation
# ---------------------------
run_start_monotonic = time.monotonic()

for call_idx in range(NUM_CALLS):
    # Keep calls on the intended cadence: t=0, 5, 10, ..., 30 minutes.
    target_elapsed = call_idx * PULL_EVERY_SECONDS
    target_time = run_start_monotonic + target_elapsed
    sleep_seconds = max(0.0, target_time - time.monotonic())
    if sleep_seconds > 0:
        print(f"\nWaiting {sleep_seconds:.1f} seconds for API call {call_idx + 1}...")
        time.sleep(sleep_seconds)

    simulated_end = FIRST_WINDOW_END_UTC + timedelta(minutes=STEP_MINUTES * call_idx)
    simulated_start = simulated_end - timedelta(minutes=WINDOW_MINUTES)

    print("\n" + "=" * 80)
    print(f"API CALL {call_idx + 1}/{NUM_CALLS}")
    print(f"Simulated pull window (UTC): {simulated_start}  ->  {simulated_end}")

    rows = pull_one_hour(TICKER, simulated_start, simulated_end)

    if not rows:
        print("WARNING: API returned no rows for this pull.")
        continue

    incoming_df = spark.createDataFrame(rows, schema=stock_schema)

    # Part (2): incremental update with overlap replacement.
    stock_df = incremental_upsert(stock_df, incoming_df)

    # Part (3): moving averages after every pull.
    fresh_ma_df = compute_30_minute_moving_averages(stock_df)
    moving_average_df = incremental_replace_ma(moving_average_df, fresh_ma_df)

    incoming_count = incoming_df.count()
    stock_count = stock_df.count()
    ma_count = moving_average_df.count()

    print(f"Rows returned in this pull: {incoming_count}")
    print(f"Rows currently in streamed stock DataFrame: {stock_count}")
    print(f"Rows currently in moving-average DataFrame: {ma_count}")

    print("\nLatest streamed rows:")
    stock_df.orderBy(F.col("UTC Timestamp").desc()).show(8, truncate=False)

    print("\nLatest moving-average rows:")
    moving_average_df.orderBy(F.col("Datetime").desc()).show(8, truncate=False)


print("\n" + "#" * 80)
print("FINAL STREAMED STOCK DATAFRAME")
print("#" * 80)
stock_df.orderBy("UTC Timestamp").show(stock_df.count(), truncate=False)

print("\n" + "#" * 80)
print("FINAL 30-MINUTE MOVING-AVERAGE DATAFRAME")
print("#" * 80)
moving_average_df.orderBy("Datetime").show(moving_average_df.count(), truncate=False)

print("\nFinal stock schema:")
stock_df.printSchema()

print("\nFinal moving-average schema:")
moving_average_df.printSchema()

print(f"\nTotal API calls attempted: {NUM_CALLS}")
print("Done.")
