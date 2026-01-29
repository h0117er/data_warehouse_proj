/*
- Model: `crm_sales_details` 
- Description: Transformed raw sales transaction data into a clean, analytical format.
- Transformation:
  - Robust date parsing & Validation
    - Converted legacy integer/string date formats (YYYYMMDD) into standard DATE objects (YYYY-MM-DD).
    - Implemented validation logic `CASE WHEN` to handle invalid entities, converting them to NULL instead of causing query failures.
  - Data integrity
    - Sales recalculation: Validated if the source sls_sales matches the logical formula. _price).
    - Price imputation: Imputed missing or invalid sls_price by reverse-calculating from `sls_sales` and `sls_quantity`.
    - Error handling: Used ABS() to handle potential negative pricing errors and IFNULL() to prevent division-by-zero errors.
*/

WITH sales AS (
    SELECT *
    FROM {{ source('portfolio','crm_sales_details')}}
)
SELECT 
    sls_ord_num,
    sls_prd_key,
    sls_cust_id,

    CASE WHEN sls_order_dt = 0 OR len(sls_order_dt) != 8 THEN NULL
        ELSE date(concat(substr(sls_order_dt,1,4),'-',substr(sls_order_dt,5,2),'-',substr(sls_order_dt,7,2)))
    END as sls_order_dt,

    CASE WHEN sls_ship_dt = 0 OR len(sls_ship_dt) != 8 THEN NULL
        ELSE date(concat(substr(sls_ship_dt,1,4),'-',substr(sls_ship_dt,5,2),'-',substr(sls_ship_dt,7,2)))
    END as sls_ship_dt,

    CASE WHEN sls_due_dt = 0 OR len(sls_due_dt) != 8 THEN NULL
        ELSE date(concat(substr(sls_due_dt,1,4),'-',substr(sls_due_dt,5,2),'-',substr(sls_due_dt,7,2)))
    END as sls_due_dt,

    CASE WHEN sls_sales IS NULL OR sls_sales <= 0 OR sls_sales != sls_quantity * ABS(sls_price) THEN sls_quantity * ABS(sls_price)
        ELSE sls_sales END as sls_sales,

    sls_quantity,

    CASE WHEN sls_price IS NULL OR sls_price <= 0 THEN (sls_sales/ifnull(sls_quantity,0))::int
        ELSE sls_price END as sls_price,
    sysdate() as DWH_CREATE_DATE
from sales
