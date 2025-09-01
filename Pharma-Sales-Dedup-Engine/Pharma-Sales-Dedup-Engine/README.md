📊 Healthcare Claims QC Framework (SQL + BI)
🔍 Project Overview

This project builds an automated data quality and QC validation framework for healthcare claims data (RX & MX claims) across multiple products (e.g., Adstiladrin, Menopur, Endometrin, Rebyota, Euflexxa).

The solution ensures accuracy between source claims data (IQVIA, IMS) and aggregated ARD datasets, flagging mismatches with a QC status (Passed/Failed) for proactive issue resolution.

⚙️ Tech Stack

SQL (Snowflake / T-SQL) → Data extraction, joins, QC logic

Python (optional) → Automation + pipeline orchestration

Power BI / Tableau / Plotly → QC dashboard visualization

GitHub → Version control & collaboration

📁 Project Structure

healthcare-qc-framework/
│
├── sql/
│   └── claims_qc_framework.sql    # main QC query
│
├── dashboard/
│   └── qc_dashboard.pbix         
└── README.md

🛠️ Key Features

✅ Validates RX & MX claims across multiple therapeutic areas
✅ Automated QC rules flag mismatches with QC PASSED / QC FAILED
✅ >98% accuracy maintained across reporting sources
✅ Scalable – easily extendable to new drugs, markets, or claim sources
✅ Dashboard highlights trendlines of QC issues, ARD vs. Source counts, and error distributions

🚀 Step-by-Step Process

Extract ARD claims from ferringanalytics.mart.ard_claims

Extract source claims from IQVIA / IMS raw datasets

Aggregate & Join ARD vs. Source claims by product & month

Compute Metrics

ARD_QTY

SRC_QTY

QTY_DIFF

QC_STATUS (PASSED or FAILED)

Output Unified QC Table → Easy to feed into BI tools

📊 Dashboard Example (Power BI / Tableau)

Key visualizations you can build:

QC Pass/Fail Trend (line/bar chart by month & product)

ARD vs. Source Counts (side-by-side bar chart)

QTY_DIFF Heatmap (highlighting largest mismatches)

QC Failure Rate (% failed records over time)

(Here you can later add a screenshot of your Power BI or Tableau dashboard.)

🏆 Impact

Reduced manual QC effort by 30% with automated checks

Improved data reliability for healthcare market analytics

Enabled faster reporting & regulatory compliance in pharma analytics

🔮 Next Steps

Automate SQL execution via Airflow or dbt

Expand framework to international datasets

Integrate with Snowflake tasks/streams for real-time QC
