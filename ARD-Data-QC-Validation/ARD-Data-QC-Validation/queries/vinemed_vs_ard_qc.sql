-- Step 1: Define valid NDCs with Market
WITH valid_ndcs AS (
  SELECT '55566750102' AS ndc, 'MENOPUR' AS drug_name, 'GONADOTROPINS MARKET' AS market UNION ALL
  SELECT '55566150201', 'NOVAREL', 'GONADOTROPINS MARKET' UNION ALL
  SELECT '55566980002', 'REBYOTA', 'MICROBIOME MARKET' UNION ALL
  SELECT '55566105001', 'ADSTILADRIN', 'NMIBC BLADDER CANCER MARKET' UNION ALL
  SELECT '55566650003', 'ENDOMETRIN', 'PROGESTERONE MARKET' UNION ALL
  SELECT '55566410001', 'EUFLEXXA', 'HA MARKET'
),
 
-- Step 2: Vinemed data aggregation
vinemed_data AS (
  SELECT
    CAST(
      CASE
        WHEN DAYOFWEEK(TO_DATE(RAWPERIODENDDATE, 'MM/dd/yyyy')) <= 6
          THEN DATEADD(DAY, 6 - DAYOFWEEK(TO_DATE(RAWPERIODENDDATE, 'MM/dd/yyyy')), TO_DATE(RAWPERIODENDDATE, 'MM/dd/yyyy'))
        ELSE DATEADD(DAY, 6, TO_DATE(RAWPERIODENDDATE, 'MM/dd/yyyy'))
      END AS DATE
    ) AS week_end_date,
    TRIM(RAWNDCUPC) AS ndc,
    SUM(
      CAST(COALESCE(SS, 0) AS FLOAT) +
      CAST(COALESCE(DS, 0) AS FLOAT) -
      CAST(COALESCE(RV, 0) AS FLOAT)
    ) AS vinemed_qty
  FROM ferringanalytics.prod_dw.vinemeds_ferring_weekly_867
  WHERE TO_DATE(RAWPERIODENDDATE, 'MM/dd/yyyy') BETWEEN DATE('2024-12-28') AND DATE('2025-07-25')
    AND RAWNDCUPC IS NOT NULL
  GROUP BY 1, 2
),
 
-- Step 3: Add drug names and markets to vinemed
vinemed_labeled AS (
  SELECT
    v.week_end_date,
    v.ndc,
    n.drug_name,
    n.market,
    v.vinemed_qty
  FROM vinemed_data v
  INNER JOIN valid_ndcs n
    ON v.ndc = n.ndc
),
 
-- Step 4: ARD 867 data
ard_data AS (
  SELECT
    CAST(week_end_date AS DATE) AS week_end_date,
    TRIM(src_product_id) AS ndc,
    product_group_name AS drug_name,
    CASE product_group_name
      WHEN 'MENOPUR' THEN 'GONADOTROPINS MARKET'
      WHEN 'NOVAREL' THEN 'GONADOTROPINS MARKET'
      WHEN 'REBYOTA' THEN 'MICROBIOME MARKET'
      WHEN 'ADSTILADRIN' THEN 'NMIBC BLADDER CANCER MARKET'
      WHEN 'ENDOMETRIN' THEN 'PROGESTERONE MARKET'
      WHEN 'EUFLEXXA' THEN 'HA MARKET'
    END AS market,
    SUM(
      CAST(COALESCE(sales_qty, 0) AS FLOAT) +
      CAST(COALESCE(drop_ship_sales, 0) AS FLOAT) -
      CAST(COALESCE(returns_to_vendor, 0) AS FLOAT)
    ) AS ard_qty
  FROM ferringanalytics.mart.ard_867_shipment
  WHERE data_source = 'VINEMEDS_FERRING_WEEKLY_867'
    AND product_group_name IN ('MENOPUR','NOVAREL','REBYOTA','ADSTILADRIN','ENDOMETRIN','EUFLEXXA')
    AND week_end_date BETWEEN DATE('2025-01-01') AND DATE('2025-07-25')
  GROUP BY 1, 2, 3, 4
)
 
-- Step 5: Final comparison
SELECT  
  'VINEMEDS_FERRING_WEEKLY_867' AS data_source,
  'WHOLESALER' AS channel_name,
  COALESCE(v.drug_name, a.drug_name) AS Product,
  COALESCE(v.market, a.market) AS Market,
  COALESCE(v.week_end_date, a.week_end_date) AS week_date,
  COALESCE(a.ard_qty, 0) AS ARD_QTY,
  COALESCE(v.vinemed_qty, 0) AS SRC_QTY,
  COALESCE(v.vinemed_qty, 0) - COALESCE(a.ard_qty, 0) AS QTY_DIFF,
  CASE
        WHEN a.week_end_date IS NULL OR v.week_end_date IS NULL OR a.ard_qty <> v.vinemed_qty THEN 'QC FAILED'
        ELSE 'QC PASSED'
    END AS qc_status
FROM vinemed_labeled v
FULL OUTER JOIN ard_data a
  ON v.ndc = a.ndc
     AND v.week_end_date = a.week_end_date
ORDER BY week_date, Product;
