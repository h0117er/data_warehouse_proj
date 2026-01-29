from airflow import DAG
from airflow.providers.common.sql.operators.sql import SQLExecuteQueryOperator
from airflow.providers.standard.operators.bash import BashOperator
from airflow.providers.standard.operators.empty import EmptyOperator
from datetime import datetime, timedelta

# ======================================================
# 1. Configuration
# ======================================================


VENV_DIR = "/course"
DBT_PROJECT_DIR = f"{VENV_DIR}/portfolio"
VENV_ACTIVATE = "source venv/bin/activate"


TABLE_CONFIG = [
    {"table": "crm_cust_info", "stage":"gcs_stage_crm", "path": "cust_info.csv"},
    {"table": "crm_prd_info",  "stage":"gcs_stage_crm", "path": "prd_info.csv"},
    {"table": "crm_sales_details", "stage":"gcs_stage_crm", "path": "sales_details.csv"},
    {"table": "erp_cust_info", "stage":"gcs_stage_erp", "path": "CUST_AZ12.csv"},
    {"table": "erp_loc",       "stage":"gcs_stage_erp", "path": "LOC_A101.csv"},
    {"table": "erp_category",  "stage":"gcs_stage_erp", "path": "PX_CAT_G1V2.csv"},
]

default_args = {
    'owner': 'data_team',
    'retries': 1,
    'retry_delay': timedelta(minutes=2),
}

with DAG(
    'data_pipeline',  # DAG ID
    default_args=default_args,
    schedule=@once, # 
    start_date=datetime(2026, 1, 25),
    catchup=False
) as dag:

    start = EmptyOperator(task_id='start')
    end = EmptyOperator(task_id='end')

    # ======================================================
    # Step 1. Bronze Parallel Loading
    # ======================================================    

    load_tasks = []

    for config in TABLE_CONFIG:
        table_name = config["table"]
        
        dynamic_sql = f"""
            COPY INTO portfolio.bronze.{table_name}
            FROM @portfolio.public.{config["stage"]}/{config["path"]}
            FILE_FORMAT = (TYPE = CSV, SKIP_HEADER = 1)
            ON_ERROR = 'CONTINUE';
        """

        # Generating Task 
        task = SQLExecuteQueryOperator(
            task_id=f'load_{table_name}',
            conn_id='snowflake',
            sql=dynamic_sql
        )

        load_tasks.append(task)

    # ======================================================
    # Step 2. Silver Model (dbt Run)
    # Description: Runs only after the Bronze layer is successfully loaded.
    #======================================================
 
    dbt_run_silver = BashOperator(
        task_id='dbt_run_silver',
        bash_command=f"""
            cd {VENV_DIR} && \
            {VENV_ACTIVATE} && \
            cd {DBT_PROJECT_DIR} && \
            echo "Running Silver Model Run..." && \
            dbt run --select models/silver
        """
    )

    # ======================================================
    # Step 3. DBT Test - for silver models
    # Description: post-transformation data integrity by checking for nulls, duplicates, positive values, and accepted value constraints.
    # ======================================================

    dbt_test_silver = BashOperator(
        task_id='dbt_test_silver',
        bash_command=f"""
            cd {VENV_DIR} && \
            {VENV_ACTIVATE} && \
            cd {DBT_PROJECT_DIR} && \
            echo "Running Silver Tests..." && \
            dbt test --select models/silver
        """
    )


    # ======================================================
    # Step 4. Gold Model (DBT Run)
    # Description: Runs only after the Silver layer is successfully loaded.
    # ======================================================

    dbt_run_gold = BashOperator(
        task_id='dbt_run_gold',
        bash_command=f"""
            cd {VENV_DIR} && \
            {VENV_ACTIVATE} && \
            cd {DBT_PROJECT_DIR} && \
            echo "Running Gold Model Run..." && \
            dbt run --select models/gold
        """
    )

    # ======================================================
    # Step 5. DBT Test - for gold models
    # Description: post-transformation data integrity by checking for nulls, duplicates, positive values, and accepted value constraints.
    # ======================================================

    dbt_test_gold = BashOperator(
        task_id='dbt_test_gold',
        bash_command=f"""
            cd {VENV_DIR} && \
            {VENV_ACTIVATE} && \
            cd {DBT_PROJECT_DIR} && \
            echo "Running Gold Tests..." && \
            dbt test --select models/gold
        """
    )
    start >> load_tasks >> dbt_run_silver >> dbt_test_silver >> dbt_run_gold >> dbt_test_gold >> end

