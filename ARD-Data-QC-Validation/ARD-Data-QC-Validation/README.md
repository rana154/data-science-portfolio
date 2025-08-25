# 📊 Vinemeds vs ARD 867 QC Validation

## 🎯 Goal
Validate **Vinemeds weekly 867 shipment data** against the **ARD 867 dashboard** to ensure accurate sales and returns reporting at the **drug + market level**.

This QC helps catch mismatches early and builds trust in downstream analytics.

---

## ✅ Business Context
- **Sources:** Vinemeds (raw 867) vs ARD 867 (curated dashboard layer)  
- **Entities Checked:** NDCs mapped to drug brands and therapeutic markets  
- **Challenge:** Mismatches in weekly aggregation, due to cut-off dates, returns, or drop-ship adjustments  
- **Solution:** Build a repeatable Snowflake SQL that aligns both datasets by **NDC, week_end_date, and market**, then flags QC status  

---

## 🔍 Key Work
1. **Define Valid NDCs + Markets**  
   Map each NDC to its drug and therapeutic market for consistent rollups.  

2. **Aggregate Vinemeds Weekly 867**  
   - Normalize `week_end_date` (align to Saturday week close).  
   - Compute sales as: `SS + DS – RV`.  

3. **Label with Drug + Market**  
   Join Vinemeds data to the NDC reference list.  

4. **Aggregate ARD 867 Data**  
   - Filter ARD shipment table by `data_source = 'VINEMEDS_FERRING_WEEKLY_867'`.  
   - Apply same business rules (`sales_qty + drop_ship_sales – returns_to_vendor`).  
   - Map product groups to markets.  

5. **Final Comparison**  
   - Outer join ARD vs Vinemeds by NDC + week.  
   - Compute `QTY_DIFF`.  
   - Flag as `QC PASSED` / `QC FAILED`.  

---

## 📂 Project Structure
├── README.md
├── queries/
│ └── vinemed_vs_ard_qc.sql
---

## ▶️ How to Run
1. Open [`queries/vinemed_vs_ard_qc.sql`](queries/vinemed_vs_ard_qc.sql).  
2. Update schema/table names if needed (replace with your environment).  
3. Run in Snowflake (or any SQL IDE connected to the warehouse).  
4. Review results:
   - `QC PASSED` → ARD matches Vinemeds  
   - `QC FAILED` → investigate mismatches  

---

## 📊 Example Output

| Product     | Market                    | Week_Date  | ARD_QTY | SRC_QTY | QTY_DIFF | QC_Status  |
|-------------|---------------------------|------------|---------|---------|----------|------------|
| MENOPUR     | GONADOTROPINS MARKET      | 2025-01-04 | 1200    | 1180    | -20      | QC FAILED  |
| NOVAREL     | GONADOTROPINS MARKET      | 2025-01-04 | 300     | 300     | 0        | QC PASSED  |

---

## 📈 Outcome
- Single source of truth for **867 weekly QC**  
- Early detection of mismatches due to returns or timing  
- Reusable template for other data sources/channels  

---

👉 [Click here to view the SQL query](queries/vinemed_vs_ard_qc.sql)
