/*
- Model: `dim_customers` 
- Description: Constructed the final customer dimension table by integrating data from disparate systems for downstream reporting and analytics.
- Transformation:
  - System Integration: Used a LEFT JOIN on the CRM dataset as the primary anchor, enriching it with location and demographic data from the ERP system.
  - Master data management: Implemented logic to handle data discrepancies between systems.
    - gender: As CRM data is treated as the Master source, if gender in CRM is missing, the logic falls back to ERP system using COALESCE().
  - Surrogate key generation: Created a Data Warehouse-specific identifier that is independent of source system keys, ensuring stability and performance in fact table joins.
  - Business-friendly naming 
*/

WITH ci AS (
    SELECT *
    FROM {{ ref('crm_cust_info')}}),
cl AS (
    SELECT *
    FROM {{ ref('erp_loc')}}),
ca AS (
    SELECT *
    FROM {{ ref('erp_cust_info')}})   

SELECT 
    ROW_NUMBER() OVER (ORDER BY ci.cst_id) as customer_key,
    ci.cst_id as customer_id, 
    ci.cst_key as customer_number,
    ci.cst_firstname as first_name,
    ci.cst_lastname as last_name,
    cl.cntry as country,
    ci.cst_marital_status as marital_status,
    CASE WHEN ci.cst_gndr != 'N/A' THEN cst_gndr  
         ELSE coalesce(ca.gen, 'N/A') END as gender,  -- CRM is the master for gender info 
    ca.bdate as birthdate,
    ci.cst_create_date as created_date

  FROM ci
  LEFT JOIN ca 
         ON ci.cst_key = ca.cid 
  LEFT JOIN cl 
         ON ci.cst_key = cl.cid 
