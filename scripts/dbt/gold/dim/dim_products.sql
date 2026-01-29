/*
- Model: `dim_products` 
- Description: Created a unified product dimension by enriching CRM product data with ERP category details. Designed as a current state dimension to support daily operational reporting.
- Transformation:
  - Denormalisation: Joined the product table on CRM with the category table from ERP to flatten the hierarchy.
  - current state filtering: Ensured reports reflect the latest product status and pricing by providing a current snapshot view. 
  - Surrogate key generation: Created a Data Warehouse-specific identifier that is independent of source system keys, ensuring stability and performance in fact table joins.
  - Renaming for business context
*/

WITH pi AS (
    SELECT *
    FROM {{ ref('crm_prd_info') }}),
ec AS (
    SELECT *
    FROM {{ ref('erp_category') }})    

SELECT 
    ROW_NUMBER() OVER (ORDER BY pi.prd_id) as product_key,
    pi.prd_id as product_id,
    pi.sls_prd_key as product_number, 
    pi.prd_nm as product_name,
    
    pi.cat_id as category_id,
    ec.cat as category, 
    ec.subcat as subcategory,
    ec.maintenance,
    pi.prd_cost as cost,
    pi.prd_line as product_line,
    pi.prd_start_dt as start_date

FROM pi
LEFT JOIN ec 
       ON pi.cat_id = ec.id 
 WHERE prd_end_dt IS NULL -- filter out all historical data 
