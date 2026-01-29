/*
- Model: `crm_prd_info` 
- Description: Cleansed and standardised raw product data from the CRM system.
- Transformation:
  - Composite key parsing: extracted meaningful business key from the raw `prd_key`
    - cat_id: Derived from the first 5 characters.
    - sls_prd_key: Extracted sales prd_key starting from the 7th character. 
  - Historical validity period construction (SCD Logic)
    - Used `LEAD()` window function to identify the next record's `prd_start_dt` within the same product key partition.
    - Calculated `prd_end_dt` by subtracting 1 second from the next start date, ensuring continuous history without overlapping time windows.
  - Data standardisation & cleaning
    - Null handling: Applied `ifnull()` to `prd_cost` to ensure financial calculation do not break.
    - Code decoding: Mapped single-character prd_line codes to full category names.
*/



WITH prd_info AS (
    SELECT *
    FROM {{ source('portfolio','crm_prd_info')}}
)
SELECT
        prd_id, 
        cat_id,
        sls_prd_key,
        prd_nm,
        prd_cost,
        prd_line,
        prd_start_dt,
        test as prd_end_dt,
        DWH_CREATE_DATE
  FROM (
        SELECT
            prd_id,
            prd_key,
            replace(substring(prd_key, 1, 5),'-','_') as cat_id,
            substring(prd_key, 7, len(prd_key)) as sls_prd_key,
            prd_nm, 
            ifnull(prd_cost,0) as prd_cost,
            CASE upper(trim(prd_line)) 
                WHEN 'M' THEN 'Mountain'
                WHEN 'R' THEN 'Road'
                WHEN 'S' THEN 'Other Sales'
                WHEN 'T' THEN 'Touring'
                ELSE 'N/A'
            END as prd_line,
            prd_start_dt,
            LEAD(prd_start_dt) OVER (PARTITION BY prd_key ORDER BY prd_start_dt) as prd_end_dt_test,
            dateadd('second',-1, prd_end_dt_test) as test,
            sysdate() as DWH_CREATE_DATE    
        FROM prd_info )
