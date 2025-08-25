# ARD Data QC Validation

**Goal**: Perform data quality checks on ARD (Analytics & Reporting Dashboard) datasets against raw source data.

### ✅ Business Context
Monthly data ingestion from multiple pharma sources (Vinemeds, ICS, Accredo) often resulted in mismatches. Our job was to build an audit system to validate ARD accuracy using raw 867 and 852 data.

### 🔍 Key Work
- Validated over **1 million records** using **Snowflake SQL**
- Applied business rules to **exclude trading partners** like McKesson
- Compared ARD KPIs against source (SalesQty vs SS/InventoryQty)
- Built reusable logic for monthly QC across channels (RETAIL, NON-RETAIL)

### 📊 Outcome
- Reduced data mismatches by **95%**
- Saved 10+ hours/month of manual QC effort
- Boosted confidence in downstream dashboards and analytics

👉 See the SQL template: [queries/867_qc_template.sql](queries/867_qc_template.sql)
