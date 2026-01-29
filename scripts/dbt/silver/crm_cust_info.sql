/*
- Model: `crm_cust_info` 
- Description: Cleansed and standardised raw customer data from the CRM system.
- Transformation:
  - Data standardisation: Converted abbr codes to human-readable terms 
    - cst_marital_status 
    - cst_gndr
  - Data quality improvements: Applied `trim()` and `upper()` to handle whitespaces and case sensitivity. 
  - Duplication strategy: Identified duplicate `cst_id` and used `ROW_NUMBER()` window function to retain only the most recent record.
*/

WITH cust_info AS (
    SELECT *
    FROM {{ source('portfolio','crm_cust_info')}}
)
SELECT
        cst_id, 
        cst_key,
        cst_firstname,
        cst_lastname,
        cst_marital_status,
        cst_gndr,
        cst_create_date,
        DWH_CREATE_DATE
  FROM (
        SELECT
            cst_id,
            cst_key,
            trim(cst_firstname) as cst_firstname,
            trim(cst_lastname) as cst_lastname,
            CASE WHEN UPPER(TRIM(CST_MARITAL_STATUS)) = 'M' THEN 'Married'
                 WHEN UPPER(TRIM(CST_MARITAL_STATUS)) = 'S' THEN 'Single'
                 ELSE 'N/A'
            END AS cst_marital_status,
            CASE WHEN UPPER(TRIM(cst_gndr)) = 'M' THEN 'Male'
                 WHEN UPPER(TRIM(cst_gndr)) = 'F' THEN 'Female' 
                 ELSE 'N/A'
            END AS cst_gndr,
            cst_create_date::date AS cst_create_date,
            row_number() OVER (PARTITION BY cst_id ORDER BY cst_create_date DESC) AS rn,
            sysdate() as DWH_CREATE_DATE
        FROM cust_info )
 WHERE rn = 1
