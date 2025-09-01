WITH 
-- 1. ADSTILADRIN RX CLAIM
ard_rx AS (
    SELECT  
        'IQVIA LAAD ADSTILADRIN' AS data_source,
        'RX CLAIM' AS claim_source_type,
        product_group_name AS product,
        DATE_TRUNC('month', month_date) AS claims_month,
        COUNT(DISTINCT claim_id) AS ard_count,
        MAX(UPPER(market_name)) AS market
    FROM ferringanalytics.mart.ard_claims
    WHERE data_source = 'IQVIA LAAD ADSTILADRIN'
      AND claim_source_type = 'RX CLAIM'
      AND EXTRACT(YEAR FROM month_date) >= 2024
    GROUP BY 1,2,3,4
),
src_rx AS (
    SELECT
        'IQVIA LAAD ADSTILADRIN' AS data_source,
        'RX CLAIM' AS claim_source_type,
        dp.PRODUCT_GROUP AS product,
        DATE_TRUNC('month', rf.SVC_DT) AS claims_month,
        COUNT(DISTINCT rf.CLAIM_ID) AS source_count
    FROM ferring_dw_prod.iqvia.adstiladrin_rxfact_lad rf
    JOIN ferring_dw_prod.iqvia.adstiladrin_dim_product_lad dp
        ON rf.NDC_CD = dp.NDC_CD
    WHERE rf.SVC_DT >= DATE '2024-01-01'
    GROUP BY 1,2,3,4
),
rx_qc AS (
    SELECT
        COALESCE(a.data_source,b.data_source) AS data_source,
        COALESCE(a.claim_source_type,b.claim_source_type) AS channel_name,
        COALESCE(a.product,b.product) AS product,
        CASE 
            WHEN COALESCE(a.product,b.product) = 'ADSTILADRIN' THEN 'NMIBC BLADDER CANCER MARKET'
            ELSE 'BLADDER CANCER MARKET'
        END AS market,
        TO_CHAR(COALESCE(a.claims_month,b.claims_month),'MM/dd/yyyy') AS sale_month,
        COALESCE(a.ard_count,0) AS ARD_QTY,
        COALESCE(b.source_count,0) AS SRC_QTY,
        ABS(COALESCE(a.ard_count,0)-COALESCE(b.source_count,0)) AS QTY_DIFF,
        CASE
            WHEN a.claims_month IS NULL OR b.claims_month IS NULL OR COALESCE(a.ard_count,0) <> COALESCE(b.source_count,0)
            THEN 'QC FAILED'
            ELSE 'QC PASSED'
        END AS qc_status
    FROM ard_rx a
    FULL OUTER JOIN src_rx b
      ON a.product = b.product AND a.claims_month = b.claims_month
),

-- 2. ADSTILADRIN MX CLAIM
mx_src_base AS (
    SELECT DISTINCT
        DX_CLAIM_ID,
        DATE_TRUNC('month', SERVICE_FROM_DATE) AS month
    FROM ferringanalytics.prod_dw.iqvia_mxfact_lad_adstiladrin mx
    LEFT JOIN ferringanalytics.prod_dw.iqvia_dim_product_lad_adstiladrin prd
      ON mx.NDC_CD = prd.NDC_CD
    LEFT JOIN ferringanalytics.prod_dw.iqvia_dim_procedurecode_lad_adstiladrin proc
      ON proc.PROCEDURE_CODE = mx.PROCEDURE_CODE AND proc.PRC_VERS_TYP_ID = mx.PRC_VERS_TYP_ID
    LEFT JOIN ferringanalytics.mart.ref_procedure_to_product ref_prod
      ON proc.PROCEDURE_CODE = ref_prod.procedure_code
    WHERE SERVICE_FROM_DATE >= DATE '2024-01-01'
      AND COALESCE(prd.PRODUCT_NAME, ref_prod.product_name) ILIKE '%ADSTI%'
),
src_mx AS (
    SELECT
        'IQVIA LAAD ADSTILADRIN' AS data_source,
        'MX CLAIM' AS claim_source_type,
        'ADSTILADRIN' AS product,
        month,
        COUNT(DISTINCT DX_CLAIM_ID) AS source_count
    FROM mx_src_base
    GROUP BY 1,2,3,4
),
ard_mx AS (
    SELECT  
        'IQVIA LAAD ADSTILADRIN' AS data_source,
        'MX CLAIM' AS claim_source_type,
        'ADSTILADRIN' AS product,
        DATE_TRUNC('month', month_date) AS month,
        COUNT(DISTINCT claim_id) AS ard_count,
        MAX(UPPER(market_name)) AS market
    FROM ferringanalytics.mart.ard_claims
    WHERE product_group_name = 'ADSTILADRIN'
      AND claim_source_type = 'MX CLAIM'
      AND EXTRACT(YEAR FROM month_date) >= 2024
    GROUP BY 1,2,3,4
),
mx_qc AS (
    SELECT
        COALESCE(a.data_source,b.data_source) AS data_source,
        'MX CLAIM' AS channel_name,
        COALESCE(a.product,b.product) AS product,
        CASE 
            WHEN COALESCE(a.product,b.product) = 'ADSTILADRIN' THEN 'NMIBC BLADDER CANCER MARKET'
            ELSE 'BLADDER CANCER MARKET'
        END AS market,
        TO_DATE(COALESCE(a.month,b.month),'MM/dd/yyyy') AS sale_month,
        COALESCE(a.ard_count,0) AS ARD_QTY,
        COALESCE(b.source_count,0) AS SRC_QTY,
        ABS(COALESCE(a.ard_count,0)-COALESCE(b.source_count,0)) AS QTY_DIFF,
        CASE
            WHEN a.month IS NULL OR b.month IS NULL OR COALESCE(a.ard_count,0) <> COALESCE(b.source_count,0)
            THEN 'QC FAILED'
            ELSE 'QC PASSED'
        END AS qc_status
    FROM ard_mx a
    FULL OUTER JOIN src_mx b
      ON a.product = b.product AND a.month = b.month
),

-- 3. RMMH (MENOPUR + ENDOMETRIN)  — updated
rmmh_ard AS (
    SELECT  
        data_source,
        claim_source_type,
        product_group_name AS product,
        DATE_TRUNC('month', service_date) AS claims_month,
        COUNT(DISTINCT claim_id) AS ard_count,
        MAX(UPPER(market_name)) AS market
    FROM ferringanalytics.mart.ard_claims
    WHERE product_group_name IN ('MENOPUR','ENDOMETRIN')
      AND claim_source_type = 'RX CLAIM'
      AND EXTRACT(YEAR FROM service_date) IN (2024,2025)
    GROUP BY 1,2,3,4
),
rmmh_src AS (
    SELECT
        'IQVIA LAAD RMMH' AS data_source,
        'RX CLAIM' AS claim_source_type,
        dp.PRODUCT_NAME AS product,
        DATE_TRUNC('month', rf.SVC_DT) AS claims_month,
        COUNT(DISTINCT rf.CLAIM_ID) AS source_count
    FROM ferring_dw_prod.iqvia.lad_rxfact rf
    JOIN ferring_dw_prod.iqvia.lad_dim_product dp
      ON rf.NDC_CD = dp.NDC_CD
    WHERE dp.PRODUCT_NAME ILIKE ANY ('%MENOPUR%','%ENDOMETRIN%')
      AND EXTRACT(YEAR FROM rf.SVC_DT) IN (2024,2025)
    GROUP BY 1,2,3,4
),
rmmh_qc AS (
    SELECT
        COALESCE(a.data_source,b.data_source) AS data_source,
        COALESCE(a.claim_source_type,b.claim_source_type) AS channel_name,
        COALESCE(a.product,b.product) AS product,
        'INFERTILITY MARKET' AS market,
        TO_CHAR(COALESCE(a.claims_month,b.claims_month),'MM/dd/yyyy') AS sale_month,
        COALESCE(a.ard_count,0) AS ARD_QTY,
        COALESCE(b.source_count,0) AS SRC_QTY,
        ABS(COALESCE(a.ard_count,0)-COALESCE(b.source_count,0)) AS QTY_DIFF,
        CASE
            WHEN a.claims_month IS NULL OR b.claims_month IS NULL OR COALESCE(a.ard_count,0) <> COALESCE(b.source_count,0)
            THEN 'QC FAILED'
            ELSE 'QC PASSED'
        END AS qc_status
    FROM rmmh_ard a
    FULL OUTER JOIN rmmh_src b
      ON a.product = b.product AND a.claims_month = b.claims_month
),

-- 3b. REBYOTA only — updated
rebyota_ard AS (
    SELECT  
        data_source,
        claim_source_type,
        product_group_name AS product,
        DATE_TRUNC('month', service_date) AS claims_month,
        COUNT(DISTINCT claim_id) AS ard_count,
        MAX(UPPER(market_name)) AS market
    FROM ferringanalytics.mart.ard_claims
    WHERE product_group_name = 'REBYOTA'
      AND claim_source_type = 'RX CLAIM'
      AND EXTRACT(YEAR FROM service_date) IN (2024,2025)
    GROUP BY 1,2,3,4
),
rebyota_src AS (
    SELECT
        'IQVIA LAAD REBYOTA' AS data_source,
        'RX CLAIM' AS claim_source_type,
        dp.PRODUCT_NAME AS product,
        DATE_TRUNC('month', rx_claim.SVC_DT) AS claims_month,
        COUNT(DISTINCT rx_claim.CLAIM_ID) AS source_count
    FROM ferring_dw_prod.iqvia.rbx_rx_fact_lad rx_claim
    JOIN ferring_dw_prod.iqvia.rbx_dim_product_lad dp
      ON rx_claim.NDC_CD = dp.NDC_CD
    WHERE dp.PRODUCT_NAME ILIKE '%REBYOTA%'
      AND EXTRACT(YEAR FROM rx_claim.SVC_DT) IN (2024,2025)
    GROUP BY 1,2,3,4
),
rebyota_qc AS (
    SELECT
        COALESCE(a.data_source,b.data_source) AS data_source,
        COALESCE(a.claim_source_type,b.claim_source_type) AS channel_name,
        COALESCE(a.product,b.product) AS product,
        'MICROBIOME' AS market,
        TO_CHAR(COALESCE(a.claims_month,b.claims_month),'MM/dd/yyyy') AS sale_month,
        COALESCE(a.ard_count,0) AS ARD_QTY,
        COALESCE(b.source_count,0) AS SRC_QTY,
        ABS(COALESCE(a.ard_count,0)-COALESCE(b.source_count,0)) AS QTY_DIFF,
        CASE
            WHEN a.claims_month IS NULL OR b.claims_month IS NULL OR COALESCE(a.ard_count,0) <> COALESCE(b.source_count,0)
            THEN 'QC FAILED'
            ELSE 'QC PASSED'
        END AS qc_status
    FROM rebyota_ard a
    FULL OUTER JOIN rebyota_src b
      ON a.product = b.product AND a.claims_month = b.claims_month
),

-- 4. IMS HA MEDB
ims_max_ard AS (
    SELECT MAX(service_date) AS max_service_date
    FROM ferringanalytics.mart.ard_claims
    WHERE data_source ILIKE '%IMS%' AND EXTRACT(YEAR FROM service_date) >= 2023
),
ims_ard AS (
    SELECT  
        'IMS HA MEDB' AS data_source,
        'MX CLAIM' AS claim_source_type,
        CASE 
            WHEN UPPER(product_group_name) IN ('SYNVISC','SYNVISC-ONE') THEN 'SYNVISC COMBINED'
            ELSE product_group_name
        END AS product,
        DATE_TRUNC('month', service_date) AS claims_month,
        COUNT(DISTINCT claim_id) AS ard_count,
        MAX(UPPER(market_name)) AS market
    FROM ferringanalytics.mart.ard_claims
    WHERE data_source ILIKE '%IMS%' AND EXTRACT(YEAR FROM service_date) >= 2024
    GROUP BY 1,2,3,4
),
ims_src_raw AS (
    SELECT claim_id, svc_fr_dt, drug_note_txt
    FROM ferringanalytics.prod_dw.ims_ha_medb
    WHERE svc_fr_dt IS NOT NULL
      AND EXTRACT(YEAR FROM svc_fr_dt) >= 2023
      AND TO_DATE(svc_fr_dt) <= (SELECT max_service_date FROM ims_max_ard)
),
ims_src_exploded AS (
    SELECT r.svc_fr_dt, r.claim_id, m.product
    FROM ims_src_raw r
    JOIN (SELECT * FROM VALUES
        ('%durolane%', 'DUROLANE'),
        ('%euflexxa%', 'EUFLEXXA'),
        ('%gel-one%', 'GEL-ONE'),
        ('%gelsyn%', 'GELSYN'),
        ('%genvisc%', 'GENVISC 850'),
        ('%hyalgan%', 'HYALGAN (SUPARTZ)'),
        ('%hymovis%', 'HYMOVIS'),
        ('%monovisc%', 'MONOVISC'),
        ('%orthovisc%', 'ORTHOVISC'),
        ('%synojoynt%', 'SYNOJOYN'),
        ('%synvisc-one%', 'SYNVISC COMBINED'),
        ('%synvisc%', 'SYNVISC COMBINED'),
        ('%triluron%', 'TRILURON'),
        ('%trivisc%', 'TRIVISC')
    ) m(pattern, product) ON LOWER(r.drug_note_txt) ILIKE m.pattern
),
ims_src_claims AS (
    SELECT product, DATE_TRUNC('month', svc_fr_dt) AS claims_month, COUNT(DISTINCT claim_id) AS source_count
    FROM ims_src_exploded
    GROUP BY 1,2
),
ims_ard_filtered AS (
    SELECT * 
    FROM ims_ard
    WHERE product IS NOT NULL
),
ims_qc AS (
    SELECT
        'IMS HA MEDB' AS data_source,
        'MX CLAIM' AS channel_name,
        COALESCE(a.product,b.product) AS product,
        'HA MARKET' AS market,
        TO_CHAR(COALESCE(a.claims_month,b.claims_month),'MM/dd/yyyy') AS sale_month,
        COALESCE(a.ard_count,0) AS ARD_QTY,
        COALESCE(b.source_count,0) AS SRC_QTY,
        ABS(COALESCE(a.ard_count,0)-COALESCE(b.source_count,0)) AS QTY_DIFF,
        CASE
            WHEN a.claims_month IS NULL OR b.claims_month IS NULL OR COALESCE(a.ard_count,0) <> COALESCE(b.source_count,0)
            THEN 'QC FAILED'
            ELSE 'QC PASSED'
        END AS qc_status
    FROM ims_ard_filtered a
    FULL OUTER JOIN ims_src_claims b
      ON a.product = b.product AND a.claims_month = b.claims_month
),

-- 5. IQVIA EUFLEXXA
euflexxa_ard AS (
    SELECT  
        'IQVIA EUFLEXXA APLD' AS data_source,
        'MX CLAIM' AS claim_source_type,
        DATE_TRUNC('month', month_date) AS claims_month, 
        COUNT(DISTINCT claim_id) AS ard_count,
        MAX(UPPER(market_name)) AS market
    FROM ferringanalytics.mart.ard_claims 
    WHERE data_source = 'IQVIA EUFLEXXA APLD'
      AND claim_source_type = 'MX CLAIM'
      AND EXTRACT(YEAR FROM month_date) >= 2024
    GROUP BY 1,2,3
),
euflexxa_src AS (
    SELECT 
        'IQVIA EUFLEXXA APLD' AS data_source,
        'MX CLAIM' AS claim_source_type,
        DATE_TRUNC('month', svc_dt) AS claims_month,
        COUNT(DISTINCT claim_id) AS source_count
    FROM ferring_dw_prod.iqvia.dxfactrolling
    WHERE svc_dt >= DATE '2024-01-01'
    GROUP BY 1,2,3
),
euflexxa_qc AS (
    SELECT
        COALESCE(a.data_source,b.data_source) AS data_source,
        COALESCE(a.claim_source_type,b.claim_source_type) AS channel_name,
        'EUFLEXXA' AS product,
        'HA MARKET' AS market,
        TO_CHAR(COALESCE(a.claims_month,b.claims_month),'MM/dd/yyyy') AS sale_month,
        COALESCE(a.ard_count,0) AS ARD_QTY,
        COALESCE(b.source_count,0) AS SRC_QTY,
        ABS(COALESCE(a.ard_count,0)-COALESCE(b.source_count,0)) AS QTY_DIFF,
        CASE
            WHEN a.claims_month IS NULL OR b.claims_month IS NULL OR COALESCE(a.ard_count,0) <> COALESCE(b.source_count,0)
            THEN 'QC FAILED'
            ELSE 'QC PASSED'
        END AS qc_status
    FROM euflexxa_ard a
    FULL OUTER JOIN euflexxa_src b
      ON a.claims_month = b.claims_month
)

-- Final combined QC output
SELECT * FROM rx_qc
UNION ALL
SELECT * FROM mx_qc
UNION ALL
SELECT * FROM rmmh_qc
UNION ALL
SELECT * FROM rebyota_qc
UNION ALL
SELECT * FROM ims_qc
UNION ALL
SELECT * FROM euflexxa_qc
ORDER BY product, sale_month DESC;
