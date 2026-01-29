/*
- Model: `erp_loc` 
- Description: Standardised geographic data from the ERP system.
- Transformation:
  - Join key harmonisation: Removed hyphens from `cid` using `REPLACE()` to match customer key in crm_cust_info.
  - Country name normalisation
    - Integrated insistent country codes into full country names. 
    - Handling nulls.
*/


WITH erp_loc AS (
    SELECT *
    FROM {{ source('portfolio','erp_loc')}}
)
select  
    replace(cid, '-', '') as cid, 
    
    CASE WHEN UPPER(TRIM(cntry)) IN ('US','USA') THEN 'United States'
         WHEN UPPER(TRIM(cntry)) = 'DE' THEN 'Germany'
         WHEN len(TRIM(cntry)) < 1 OR TRIM(cntry) IS NULL THEN 'N/A'
         ELSE TRIM(cntry)
     END as cntry,
    
    sysdate() as DWH_CREATE_DATE
from erp_loc
