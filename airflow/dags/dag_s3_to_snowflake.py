from airflow import DAG
from airflow.providers.snowflake.operators.snowflake import SnowflakeOperator
from airflow.operators.python import PythonOperator
from datetime import datetime, timedelta

# Default arguments for the DAG
default_args = {
    'owner': 'airflow',
    'depends_on_past': False,
    'start_date': datetime(2024, 1, 1),
    'email_on_failure': False,
    'retries': 1,
    'retry_delay': timedelta(minutes=5),
}

# DAG Definition
dag = DAG(
    's3_to_snowflake_financial_data',
    default_args=default_args,
    description='Extract financial data from S3 and load to Snowflake',
    schedule_interval='@daily',
    catchup=False
)

# S3 Bucket and Base Folder Name
S3_BUCKET = "financial-data-processing-2025"
BASE_FOLDER = "Raw_Data"

# Function to dynamically generate the S3 folder path based on the DAG run context
def generate_s3_folder(dag_run, **kwargs):
    # Retrieve year and quarter from dag_run.conf, with default values
    year = dag_run.conf.get('year', '2023')  # Default to '2023' if not found
    quarter = dag_run.conf.get('quarter', 'q4')  # Default to 'q4' if not found
    
    # Dynamically generate the folder path without extra slashes
    s3_folder = f"{year.strip()}{quarter.strip()}/"  # Correct folder format
    
    # Push the folder path to XCom so it can be used later in Snowflake tasks
    task_instance = kwargs['task_instance']
    task_instance.xcom_push(key='s3_folder', value=s3_folder)
    
    return s3_folder


# Snowflake CREATE FILE FORMAT
create_file_format = SnowflakeOperator(
    task_id='create_file_format',
    snowflake_conn_id='snowflake_extract',
    sql="""
    CREATE OR REPLACE FILE FORMAT FINANCIAL_DB_Data.RAW_DATA_SCHEMA.TXT_FILE_FORMAT
    TYPE = CSV
    FIELD_OPTIONALLY_ENCLOSED_BY = '"'
    FIELD_DELIMITER = '\t'  -- ✅ Tab as delimiter
    SKIP_HEADER = 1
    TRIM_SPACE = TRUE
    ERROR_ON_COLUMN_COUNT_MISMATCH = FALSE;
    """,
    dag=dag
)

# Snowflake CREATE TABLE for SUB, NUM, PRE, TAG
create_table = SnowflakeOperator(
    task_id='create_snowflake_tables',
    snowflake_conn_id='snowflake_extract',
    sql="""
    CREATE OR REPLACE TABLE FINANCIAL_DB_Data.RAW_DATA_SCHEMA.SUB (
        adsh VARCHAR,
        cik VARCHAR,
        name VARCHAR,
        sic VARCHAR,
        countryba VARCHAR,
        stprba VARCHAR,
        cityba VARCHAR,
        zipba VARCHAR,
        bas1 VARCHAR,
        bas2 VARCHAR,
        baph VARCHAR,
        countryma VARCHAR,
        stprma VARCHAR,
        cityma VARCHAR,
        zipma VARCHAR,
        mas1 VARCHAR,
        mas2 VARCHAR,
        countryinc VARCHAR,
        stprinc VARCHAR,
        ein VARCHAR,
        former VARCHAR,
        changed VARCHAR,
        afs VARCHAR,
        wksi VARCHAR,
        fye VARCHAR,
        form VARCHAR,
        period VARCHAR,
        fy VARCHAR,
        fp VARCHAR,
        filed VARCHAR,
        accepted VARCHAR,
        prevrpt VARCHAR,
        detail VARCHAR,
        instance VARCHAR,
        nciks VARCHAR,
        aciks VARCHAR
    );

    CREATE OR REPLACE TABLE FINANCIAL_DB_Data.RAW_DATA_SCHEMA.NUM (
        adsh VARCHAR,
        tag VARCHAR,
        version VARCHAR,
        ddate VARCHAR,
        qtrs VARCHAR,
        uom VARCHAR,
        segments VARCHAR,
        coreg VARCHAR,
        value VARCHAR,
        footnote VARCHAR
    );

    CREATE OR REPLACE TABLE FINANCIAL_DB_Data.RAW_DATA_SCHEMA.PRE (
        adsh VARCHAR,
        report VARCHAR,
        line VARCHAR,
        stmt VARCHAR,
        inpth VARCHAR,
        rfile VARCHAR,
        tag VARCHAR,
        version VARCHAR,
        plabel VARCHAR,
        negating VARCHAR
    );

    CREATE OR REPLACE TABLE FINANCIAL_DB_Data.RAW_DATA_SCHEMA.TAG (
        tag VARCHAR,
        version VARCHAR,
        custom VARCHAR,
        abstract VARCHAR,
        datatype VARCHAR,
        iord VARCHAR,
        crdr VARCHAR,
        tlabel VARCHAR,
        doc VARCHAR
    );
    """,
    dag=dag
)

# Snowflake COPY INTO Statements for each table
load_data_to_snowflake = SnowflakeOperator(
    task_id='load_financial_data',
    snowflake_conn_id='snowflake_extract',  # Connection ID defined in the UI
    sql="""
    -- Load data into NUM table
    COPY INTO FINANCIAL_DB_Data.RAW_DATA_SCHEMA.NUM
    FROM @FINANCIAL_DB_Data.RAW_DATA_SCHEMA.RAW_DATA_STAGE
    FILES = ('{{ task_instance.xcom_pull(task_ids='generate_s3_folder', key='s3_folder') }}num.txt')  
    FILE_FORMAT = FINANCIAL_DB_Data.RAW_DATA_SCHEMA.TXT_FILE_FORMAT
    ON_ERROR = CONTINUE;

    -- Load data into PRE table
    COPY INTO FINANCIAL_DB_Data.RAW_DATA_SCHEMA.PRE
    FROM @FINANCIAL_DB_Data.RAW_DATA_SCHEMA.RAW_DATA_STAGE
    FILES = ('{{ task_instance.xcom_pull(task_ids='generate_s3_folder', key='s3_folder') }}pre.txt')  
    FILE_FORMAT = FINANCIAL_DB_Data.RAW_DATA_SCHEMA.TXT_FILE_FORMAT
    ON_ERROR = CONTINUE;

    -- Load data into SUB table
    COPY INTO FINANCIAL_DB_Data.RAW_DATA_SCHEMA.SUB
    FROM @FINANCIAL_DB_Data.RAW_DATA_SCHEMA.RAW_DATA_STAGE
    FILES = ('{{ task_instance.xcom_pull(task_ids='generate_s3_folder', key='s3_folder') }}sub.txt')  
    FILE_FORMAT = FINANCIAL_DB_Data.RAW_DATA_SCHEMA.TXT_FILE_FORMAT
    ON_ERROR = CONTINUE;

    -- Load data into TAG table
    COPY INTO FINANCIAL_DB_Data.RAW_DATA_SCHEMA.TAG
    FROM @FINANCIAL_DB_Data.RAW_DATA_SCHEMA.RAW_DATA_STAGE
    FILES = ('{{ task_instance.xcom_pull(task_ids='generate_s3_folder', key='s3_folder') }}tag.txt')  
    FILE_FORMAT = FINANCIAL_DB_Data.RAW_DATA_SCHEMA.TXT_FILE_FORMAT
    ON_ERROR = CONTINUE;
    """,
    dag=dag
)


# Snowflake CREATE STAGE with IAM Integration
create_stage = SnowflakeOperator(
    task_id='create_s3_stage',
    snowflake_conn_id='snowflake_extract',
    sql=f"""
    CREATE OR REPLACE STAGE FINANCIAL_DB_Data.RAW_DATA_SCHEMA.RAW_DATA_STAGE
    URL = 's3://{S3_BUCKET}/Raw_Data/'
    STORAGE_INTEGRATION = my_s3_integration;
    """,
    dag=dag
)

# Python Operator to generate the S3 folder dynamically and push it to XCom
generate_s3_folder = PythonOperator(
    task_id='generate_s3_folder',
    python_callable=generate_s3_folder,
    provide_context=True,  # This will pass dag_run.conf to the function
    dag=dag
)

# **Set Dependencies**
generate_s3_folder >> create_file_format >> create_stage >> create_table >> load_data_to_snowflake
