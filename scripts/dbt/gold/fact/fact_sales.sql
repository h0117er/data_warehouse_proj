/*
- Model: `fact_sales` 
- Description: Constructed the Fact table of the Star Schema. Aggregated transactional data and mapped it to Dimension tables (dim_customers, dim_products) to enable high-performance slicing and dicing in BI tools.
- Transformation:
  - Star schema implementation: Orchestrated the relationship between the Fact table and Dimension tables.
  - Surrogate key adoption: Supplemented source system natural keys into data warehouse surrogate keys, improving join performance and insulating the fact table from changes in the source system. 
*/

WITH t1 AS (
    SELECT *
    FROM {{ ref('crm_sales_details') }}),
t2 AS (
    SELECT *
    FROM {{ ref('dim_customers') }}),
t3 AS (
    SELECT *
    FROM {{ ref('dim_products') }})

SELECT
        t1.sls_ord_num as order_number,
        t3.product_key, 
        t2.customer_key, 
        t1.sls_order_dt as order_date,
        t1.sls_ship_dt as shipping_date,
        t1.sls_due_dt as due_date,
        t1.sls_sales as sales_amount,
        t1.sls_quantity as quantity,
        t1.sls_price as price

FROM t1
LEFT JOIN t2 ON t1.sls_cust_id = t2.customer_id
LEFT JOIN t3 ON t1.sls_prd_key = t3.product_number 
