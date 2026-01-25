# ❄️ Snowflake Data Ingestion Guide
   
This document outlines the step-by-step process of establishing a data ingestion pipeline from Google Cloud Storage to Snowflake.
**Helpful Snowflake Documentation: https://docs.snowflake.com/en/user-guide/data-load-gcs**
Assumes that the schema and tables have already been created and the authorisation in Google Cloud Storage is set as a Storage Object Viewer. 

## 1. Architecture Overview
We follow the **ELT (Extract, Load, Transform)** pattern:
1.  **Extract & Load:** Raw data (CSV, JSON) is loaded from GCS into Snowflake `RAW` tables.
2.  **Transform:** dbt (Data Build Tool) transforms the raw data into analytics-ready models.

---

## 2. Storage Integration (Authentication)
**Concept:** A Storage Integration is a Snowflake object that stores the generated IAM user for your external cloud storage.

> **💡 Best Practice:** Do not create a separate integration for every bucket.
> Create **one integration per Data Source (e.g., GCP Project)** and whitelist multiple buckets using `STORAGE_ALLOWED_LOCATIONS`.
> Configure multiple storage integrations to handle separate cloud accounts or varying access levels within the same bucket.

```sql
Create a single integration for the entire GCP project
CREATE OR REPLACE STORAGE INTEGRATION gcp_main_integration
  TYPE = EXTERNAL_STAGE
  STORAGE_PROVIDER = GCS
  ENABLED = TRUE
  STORAGE_ALLOWED_LOCATIONS = ('gcs://my-data-bucket-a/', 'gcs://my-data-bucket-b/');

---

## 3. Stage
**Concept: Instead of accessing the raw bucket URL directly, we use a Stage as a secure abstraction layer.**

```sql
CREATE OR REPLACE STAGE gcp_raw_stage
  URL = 'gcs://my-data-bucket/'
  STORAGE_INTEGRATION = gcp_analytics_integration
  FILE_FORMAT = my_csv_format;

---

## 4. Execute Data Loading

> Load data from the Stage into the targeted table using the COPY INTO command.
> **OPTIONS**:
  > PATTERN: Filters files using Regular Expressions.
  > FILES: Loads specific files by exact name (alternative to Pattern).
  > ON_ERROR: Defines how to handle load errors
  > FORCE: Reloads files even if they have been loaded before.
  > VALIDATION_MODE: Dry Run. Checks for errors without actually loading data. Returns the rows that would fail.

```sql
COPY INTO raw_sales_table
FROM @gcp_raw_stage/sales/2026/
-- FILE_FORMAT = (SKIP_HEADER = 1)
-- PATTERN = '.*.csv';
