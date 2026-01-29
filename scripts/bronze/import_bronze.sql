
/*-------------------------------------------------------------------------------------------
Create Table
-------------------------------------------------------------------------------------------*/

CREATE OR REPLACE TABLE LOAD_DATA_FROM_CLOUD.crm_cust_info (
    cst_id string,
    cst_key string,
    cst_firstname string,
    cst_lastname string,
    cst_marital_status string,
    cst_gndr string,
    cst_create_date string 
);

/*-------------------------------------------------------------------------------------------
Create Storage Integrations
-------------------------------------------------------------------------------------------*/

CREATE  STORAGE INTEGRATION  gcs_init
TYPE = EXTERNAL_STAGE
STORAGE_PROVIDER = 'GCS'
ENABLED =TRUE 
STORAGE_ALLOWED_LOCATIONS = ('gcs://portfolio_raw'  );

-- Describe the Integrations
DESCRIBE INTEGRATION gcs_init;


-- View our Storage Integrations
SHOW STORAGE INTEGRATIONS;

/*-------------------------------------------------------------------------------------------
Create Stage Objects
-------------------------------------------------------------------------------------------*/

CREATE STAGE gcs_stage_init
URL = 'gcs://portfolio_raw/' 
STORAGE_INTEGRATION = gcs_init; -- created in previous step
-- [ FILE_FORMAT = (  TYPE = { CSV } ) ];

-- Drop unnecessary stage 
-- DROP STAGE IF EXISTS portfolio.load_data_from_cloud.gcs_init_stage;

-- View our Stages
SHOW STAGES;

/*-------------------------------------------------------------------------------------------
Load Data from Stages
-------------------------------------------------------------------------------------------*/

-- Load data from the Google Cloud Stage into the Table
  
COPY INTO portfolio.load_data_from_cloud.crm_cust_info
FROM @gcs_stage_init/raw/cust_info.csv
file_format = (skip_header = 1);
