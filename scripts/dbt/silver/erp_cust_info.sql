/*
- Model: `erp_cust_info` 
- Description: Cleansed and standardised raw customer data from the ERP system.
- Transformation:
  - ID standardisation: Detected inconsistencies in customer_ids and applied `substr()` to strip the first 3 characters to align them with customer_id in the CRM system.
  - Data validity check
    - Implemented business rules to validate bdate: Filtered out dates greater than the current date and before '1924-01-01'.
  - Categorical normalisation: Mapped mixed gender codes ('F', 'FEMALE', 'M', 'MALE') to a consistent set of values ('Female', 'Male', 'N/A') using UPPER() and TRIM() for robust matching.
*/


WITH erp_cust AS (
    SELECT *
    FROM {{ source('portfolio','erp_cust_info')}}
)
SELECT 
    CASE WHEN len(cid) != 10 THEN substr(cid, 4) ELSE cid END as cid,
    
    CASE WHEN bdate > sysdate() OR bdate < '1924-01-01' THEN NULL 
         ELSE bdate END as bdate,

    CASE WHEN UPPER(TRIM(gen)) IN ('F', 'FEMALE') THEN 'Female'
         WHEN UPPER(TRIM(gen)) IN ('M', 'MALE') THEN 'Male'
         ELSE 'N/A'
     END as gen,
    sysdate() as DWH_CREATE_DATE
from erp_cust
