from airflow import DAG
from airflow.operators.bash import BashOperator
from airflow.providers.snowflake.operators.snowflake import SnowflakeOperator
from datetime import datetime, timedelta

# Default Arguments
default_args = {
    'owner': 'airflow',
    'depends_on_past': False,
    'start_date': datetime(2024, 1, 1),
    'email_on_failure': False,
    'retries': 1,
    'retry_delay': timedelta(minutes=5),
}

# Define DAG
dag = DAG(
    'dbt_financial_transformations',
    default_args=default_args,
    description='DBT pipeline to transform and validate financial data in Snowflake',
    schedule_interval='@daily',
    catchup=False
)

# **✅ Corrected DBT Paths Inside the Airflow Container**
DBT_PROJECT_DIR = "/opt/airflow/dbt_data_pipeline"  # ✅ Corrected path
DBT_PROFILES_DIR = "/opt/airflow/dbt_data_pipeline/config"

# **Run DBT Model Transformations**
dbt_run = BashOperator(
    task_id='dbt_run',
    bash_command=f"dbt run --profiles-dir {DBT_PROFILES_DIR}",
    dag=dag
)

# **Run DBT Tests to Validate Data**
dbt_test = BashOperator(
    task_id='dbt_test',
    bash_command=f"dbt test --profiles-dir {DBT_PROFILES_DIR}",
    dag=dag
)

# **Step 0: Grant necessary privileges for dbt_role**
grant_privileges = SnowflakeOperator(
    task_id='grant_privileges',
    snowflake_conn_id='snowflake_extract',
    sql="""
    -- Grant necessary privileges to DBT_ROLE for DBT_SCHEMA, RAW_DATA_SCHEMA, and JSON_DATA_SCHEMA

    -- Grant on RAW_DATA_SCHEMA
    USE ROLE ACCOUNTADMIN;
    GRANT USAGE ON SCHEMA FINANCIAL_DB_DATA.RAW_DATA_SCHEMA TO ROLE DBT_ROLE;
    GRANT SELECT ON ALL TABLES IN SCHEMA FINANCIAL_DB_DATA.RAW_DATA_SCHEMA TO ROLE DBT_ROLE;
    GRANT CREATE VIEW, MODIFY ON SCHEMA FINANCIAL_DB_DATA.RAW_DATA_SCHEMA TO ROLE DBT_ROLE;

    -- Grant on JSON_DATA_SCHEMA
    GRANT USAGE ON SCHEMA FINANCIAL_DB_DATA.JSON_DATA_SCHEMA TO ROLE DBT_ROLE;
    GRANT SELECT ON ALL TABLES IN SCHEMA FINANCIAL_DB_DATA.JSON_DATA_SCHEMA TO ROLE DBT_ROLE;
    GRANT CREATE VIEW ON SCHEMA FINANCIAL_DB_DATA.JSON_DATA_SCHEMA TO ROLE DBT_ROLE;

    -- Grant on DBT_SCHEMA
    GRANT USAGE ON SCHEMA FINANCIAL_DB_DATA.DBT_SCHEMA TO ROLE DBT_ROLE;
    GRANT CREATE TABLE, CREATE VIEW ON SCHEMA FINANCIAL_DB_DATA.DBT_SCHEMA TO ROLE DBT_ROLE;
    GRANT SELECT, INSERT, UPDATE, DELETE, TRUNCATE ON ALL TABLES IN SCHEMA FINANCIAL_DB_DATA.DBT_SCHEMA TO ROLE DBT_ROLE;
    GRANT SELECT ON ALL VIEWS IN SCHEMA FINANCIAL_DB_DATA.DBT_SCHEMA TO ROLE DBT_ROLE;
    GRANT CREATE VIEW, MODIFY ON SCHEMA FINANCIAL_DB_DATA.DBT_SCHEMA TO ROLE DBT_ROLE;

    -- Reapply grants after table creation
    GRANT USAGE ON SCHEMA FINANCIAL_DB_DATA.DBT_SCHEMA TO ROLE dbt_role;
    GRANT SELECT, INSERT, UPDATE, DELETE, TRUNCATE ON ALL TABLES IN SCHEMA FINANCIAL_DB_DATA.DBT_SCHEMA TO ROLE dbt_role;
    GRANT SELECT ON ALL VIEWS IN SCHEMA FINANCIAL_DB_DATA.DBT_SCHEMA TO ROLE dbt_role;
    GRANT CREATE VIEW, MODIFY ON SCHEMA FINANCIAL_DB_DATA.DBT_SCHEMA TO ROLE dbt_role;

    GRANT SELECT, INSERT, UPDATE, DELETE, TRUNCATE ON FUTURE TABLES IN SCHEMA FINANCIAL_DB_DATA.DBT_SCHEMA TO ROLE dbt_role;
    GRANT SELECT ON FUTURE VIEWS IN SCHEMA FINANCIAL_DB_DATA.DBT_SCHEMA TO ROLE dbt_role;
    
    -- Automatically grant privileges on future tables and views in DBT_SCHEMA
    GRANT SELECT, INSERT, UPDATE, DELETE, TRUNCATE ON FUTURE TABLES IN SCHEMA FINANCIAL_DB_DATA.DBT_SCHEMA TO ROLE DBT_ROLE;
    GRANT SELECT ON FUTURE VIEWS IN SCHEMA FINANCIAL_DB_DATA.DBT_SCHEMA TO ROLE DBT_ROLE;
    """,
    dag=dag
)

# **Step 1: Create Fact Tables in DBT_SCHEMA (NO RAW_DATA_SCHEMA CHANGES)**
create_fact_tables = SnowflakeOperator(
    task_id='create_fact_tables',
    snowflake_conn_id='snowflake_extract',
    sql="""
    
    USE ROLE DBT_ROLE;
    CREATE OR REPLACE TABLE FINANCIAL_DB_DATA.DBT_SCHEMA.BALANCE_SHEET_FACT (
        cik VARCHAR,
        company_name VARCHAR,
        fiscal_year INTEGER,
        fiscal_period VARCHAR,
        financial_metric VARCHAR,
        amount FLOAT,
        unit_of_measurement VARCHAR
    );

    CREATE OR REPLACE TABLE FINANCIAL_DB_DATA.DBT_SCHEMA.CASH_FLOW_FACT (
        cik VARCHAR,
        company_name VARCHAR,
        fiscal_year INTEGER,
        fiscal_period VARCHAR,
        financial_metric VARCHAR,
        amount FLOAT,
        unit_of_measurement VARCHAR
    );

    CREATE OR REPLACE TABLE FINANCIAL_DB_DATA.DBT_SCHEMA.INCOME_STATEMENT_FACT (
        cik VARCHAR,
        company_name VARCHAR,
        fiscal_year INTEGER,
        fiscal_period VARCHAR,
        financial_metric VARCHAR,
        amount FLOAT,
        unit_of_measurement VARCHAR
    );

    """,
    dag=dag
)

# **Step 2: Load Data Into Fact Tables**

# ✅ Load Balance Sheet Data
load_balance_sheet = SnowflakeOperator(
    task_id='load_balance_sheet_fact',
    snowflake_conn_id='snowflake_extract',
    sql=""" 

    USE ROLE DBT_ROLE;

    INSERT INTO FINANCIAL_DB_DATA.DBT_SCHEMA.BALANCE_SHEET_FACT
    SELECT DISTINCT
        s.cik, 
        s.name AS company_name, 
        s.fy AS fiscal_year, 
        s.fp AS fiscal_period, 
        n.tag AS financial_metric, 
        SUM(n.value) AS amount, 
        n.uom AS unit_of_measurement 
    FROM FINANCIAL_DB_DATA.RAW_DATA_SCHEMA.SUB s
    JOIN FINANCIAL_DB_DATA.RAW_DATA_SCHEMA.NUM n
    ON s.adsh = n.adsh
    WHERE n.tag IN ('Assets', 'Liabilities', 'Equity')
    GROUP BY s.cik, s.name, s.fy, s.fp, n.tag, n.uom;
    """,
    dag=dag
)

# ✅ Load Cash Flow Data
load_cash_flow = SnowflakeOperator(
    task_id='load_cash_flow_fact',
    snowflake_conn_id='snowflake_extract',
    sql=""" 

    USE ROLE DBT_ROLE;
    INSERT INTO FINANCIAL_DB_DATA.DBT_SCHEMA.CASH_FLOW_FACT
    SELECT 
        s.cik, 
        s.name AS company_name, 
        s.fy AS fiscal_year, 
        s.fp AS fiscal_period, 
        n.tag AS financial_metric, 
        n.value AS amount, 
        n.uom AS unit_of_measurement 
    FROM FINANCIAL_DB_DATA.RAW_DATA_SCHEMA.SUB s
    JOIN FINANCIAL_DB_DATA.RAW_DATA_SCHEMA.NUM n
    ON s.adsh = n.adsh
    WHERE n.tag IN (
        'NetCashProvidedByUsedInOperatingActivities', 
        'NetCashProvidedByUsedInInvestingActivities',
        'NetCashProvidedByUsedInFinancingActivities'
    );
    """,
    dag=dag
)

# ✅ Load Income Statement Data
load_income_statement = SnowflakeOperator(
    task_id='load_income_statement_fact',
    snowflake_conn_id='snowflake_extract',
    sql=""" 

    USE ROLE DBT_ROLE;
    INSERT INTO FINANCIAL_DB_DATA.DBT_SCHEMA.INCOME_STATEMENT_FACT
    SELECT 
        s.cik, 
        s.name AS company_name, 
        s.fy AS fiscal_year, 
        s.fp AS fiscal_period, 
        n.tag AS financial_metric, 
        SUM(n.value) AS amount, 
        n.uom AS unit_of_measurement 
    FROM FINANCIAL_DB_DATA.RAW_DATA_SCHEMA.SUB s
    JOIN FINANCIAL_DB_DATA.RAW_DATA_SCHEMA.NUM n
    ON s.adsh = n.adsh
    WHERE n.tag IN ('Revenues', 'OperatingIncome', 'NetIncome')
    GROUP BY s.cik, s.name, s.fy, s.fp, n.tag, n.uom;
    """,
    dag=dag
)

# **Set Dependencies**
grant_privileges >> dbt_run
dbt_run >> dbt_test
dbt_test >> create_fact_tables  
create_fact_tables >> [load_balance_sheet, load_cash_flow, load_income_statement]
