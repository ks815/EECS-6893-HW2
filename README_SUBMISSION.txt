EECS E6893 Big Data Analytics - Homework 2
Final Submission Package - Yihang Sun

REPORT
------
EECS6893_HW2_Complete_Report_Yihang_Sun.pdf

SECTION 1 - STREAMING ANALYTICS
-------------------------------
code/section1/section1_streaming_analytics.py
code/section1/section1_streaming_analytics.ipynb

Final configuration used in the report:
- AAPL, 1-minute bars
- 7 API calls
- 300-second (5-minute) interval
- 60-minute pull window
- 5-minute window shift
- incremental Spark upsert with left_anti + unionByName
- separate time-based 30-minute moving-average DataFrame

SECTION 2 - AIRFLOW
-------------------
code/section2_airflow/hello_world_dag.py
code/section2_airflow/q21_complex_workflow.py
code/section2_airflow/q22_watchlist_pipeline.py
code/section2_airflow/fetch_watchlist_data.py

Important Q2.2 note:
The final successful Q2.2 implementation uses 1 BashOperator + 2 PythonOperators:
fetch_data (BashOperator running fetch_watchlist_data.py)
    -> analytics_summary (PythonOperator)
    -> volatility_alert (PythonOperator)
This matches the successful Airflow run shown in the report.

SECTION 3 - MINI-PROJECT
------------------------
code/section3/mini_project.py
section3_output/stock_risk_momentum_summary.csv

Final mini-project:
NVDA / TSLA / SPY Stock Risk and Momentum Analysis
- six-month return
- annualized volatility
- maximum drawdown
- 7-day and 20-day moving averages
- momentum classification
- risk classification

FILES INTENTIONALLY EXCLUDED
----------------------------
- mini_project_backup.py (old mini-project version)
- old q22_watchlist_pipeline.py with 3 PythonOperators
- outdated README/checklist/tutorial alignment files
- .DS_Store and __MACOSX metadata
