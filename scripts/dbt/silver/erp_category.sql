/*
- Model: `erp_category` 
- Description: Cleansed and standardised products' category data from the ERP system.
*/

WITH erp_cat AS (
    SELECT *
    FROM {{ source('portfolio','erp_category')}}
)
select  
    *,
    sysdate() as DWH_CREATE_DATE,
from erp_cat
