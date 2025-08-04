# Pharma Sales Dedup Engine

**Goal**: Remove duplicate sales records across multiple raw datasets (Vinemeds, ICS, Chargeback) without data loss.

### 🧠 Problem
Multiple data sources included overlapping records for the same NDCs and customers. This affected ARD KPIs and reporting accuracy.

### ⚙️ Solution
- Mapped **trading partners** and **customer names** across sources
- Applied logic based on **Invoice Date**, **Processing Date**, and NDC alignment
- Created CTE pipelines using **Snowflake SQL** to retain highest-priority records

### 📈 Impact
- **Reduced duplication by 100%** in ARD outputs
- Improved reporting clarity for downstream stakeholders
- Saved analysts ~60% of time during monthly data rollups
