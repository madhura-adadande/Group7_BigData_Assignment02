import os
import asyncio
from airflow import DAG
from airflow.providers.snowflake.operators.snowflake import SnowflakeOperator
from airflow.providers.amazon.aws.hooks.s3 import S3Hook
from airflow.operators.python import PythonOperator
from datetime import datetime, timedelta
import json
import pandas as pd
import concurrent.futures
import boto3

# ✅ DAG Default Arguments
default_args = {
    'owner': 'airflow',
    'depends_on_past': False,
    'start_date': datetime(2024, 1, 1),
    'email_on_failure': False,
    'retries': 1,
    'retry_delay': timedelta(minutes=5),
}

# ✅ DAG Definition
dag = DAG(
    'json_transformation_to_snowflake',
    default_args=default_args,
    description='Transform SEC data to JSON & Load to Snowflake',
    schedule_interval='@daily',
    catchup=False
)

# ✅ **S3 & Local Config**
S3_BUCKET = "financial-data-processing-2025"
SEC_FOLDER = "Raw_Data/"  # Base folder in S3
JSON_S3_FOLDER = "Json_Data/"  # Base JSON folder in S3
LOCAL_FOLDER = "/opt/airflow/dags/data/"
LOCAL_TICKER_FILE = "/opt/airflow/dags/data/ticker.txt"

# ✅ **Boto3 S3 Client**
s3 = boto3.client('s3')

# ✅ **Download SEC Files from S3** (Optimized using async)
async def download_sec_files(year: str, quarter: str):
    os.makedirs(LOCAL_FOLDER, exist_ok=True)
    files = ["sub.txt", "num.txt", "pre.txt", "tag.txt"]

    # Dynamically create the SEC folder path based on year and quarter
    SEC_FOLDER = f"Raw_Data/{year}{quarter}/"

    # Run all download operations concurrently
    async def download_file(file):
        s3_key = SEC_FOLDER + file
        local_path = os.path.join(LOCAL_FOLDER, file)
        try:
            print(f"📥 Downloading {file} from S3...")
            s3.download_file(S3_BUCKET, s3_key, local_path)
        except Exception as e:
            print(f"❌ Failed to download {file}: {e}")

    # Use asyncio to download files concurrently
    await asyncio.gather(*(download_file(file) for file in files))

# ✅ **Load Ticker Data**
def load_ticker_data():
    if not os.path.exists(LOCAL_TICKER_FILE):
        print(f"❌ `{LOCAL_TICKER_FILE}` not found! Exiting.")
        return None

    try:
        df = pd.read_csv(LOCAL_TICKER_FILE, delimiter="\t", names=["ticker", "cik"], dtype=str)
        df["cik"] = df["cik"].str.zfill(10)  # Normalize CIKs
        print(f"✅ Loaded {len(df)} tickers.")
        return df
    except Exception as e:
        print(f"❌ Error loading ticker.txt: {e}")
        return None

# ✅ **Transform & Upload JSON Data (Optimized)**
async def transform_and_upload_json(year: str, quarter: str):
    print("🚀 Transforming SEC Data to JSON...")

    # ✅ Load SEC Files (in chunks)
    sub_df = pd.read_csv(os.path.join(LOCAL_FOLDER, "sub.txt"), delimiter="\t", dtype=str)
    num_df = pd.read_csv(os.path.join(LOCAL_FOLDER, "num.txt"), delimiter="\t", dtype=str)
    pre_df = pd.read_csv(os.path.join(LOCAL_FOLDER, "pre.txt"), delimiter="\t", dtype=str)
    tag_df = pd.read_csv(os.path.join(LOCAL_FOLDER, "tag.txt"), delimiter="\t", dtype=str)

    # ✅ Load Ticker Data
    ticker_df = load_ticker_data()
    if ticker_df is None:
        print("❌ No ticker data. Exiting.")
        return

    # ✅ Merge Ticker Data (Optimized Merge)
    sub_df["cik"] = sub_df["cik"].astype(str).str.zfill(10)
    merged_df = sub_df.merge(ticker_df, on="cik", how="left")

    # ✅ Dynamically create the folder path for JSON files based on year and quarter
    dynamic_folder = f"{year.strip()}{quarter.strip()}/"  # User-defined folder path
    JSON_S3_FOLDER = f"Json_Data/{dynamic_folder}"  # Target folder structure

    # ✅ Process & Upload JSON (Batch Processing)
    def process_company_batch(batch_df):
        json_batch = []
        for idx, group in batch_df.groupby("cik"):
            ticker = ticker_df[ticker_df["cik"] == group.iloc[0]["cik"]]["ticker"].values[0] if group.iloc[0]["cik"] in ticker_df["cik"].values else f"Unknown_{group.iloc[0]['cik']}"

            adsh_list = group["adsh"].unique()
            num_data = num_df[num_df["adsh"].isin(adsh_list)]
            pre_data = pre_df[pre_df["adsh"].isin(adsh_list)]
            tag_data = tag_df[tag_df["tag"].isin(num_data["tag"].unique())]

            # 🔹 JSON Structure
            company_json = {
                "cik": group.iloc[0]["cik"],
                "ticker": ticker,
                "company_name": group.iloc[0]["name"],
                "sub": group.to_dict(orient="records"),
                "num": num_data.to_dict(orient="records"),
                "pre": pre_data.to_dict(orient="records"),
                "tag": tag_data.to_dict(orient="records"),
            }

            json_data = json.dumps(company_json, indent=4)
            json_batch.append((ticker, json_data))

        # Upload the batch to S3
        for ticker, json_data in json_batch:
            s3_key = f"{JSON_S3_FOLDER}{ticker}.json"
            try:
                print(f"📤 Uploading {s3_key} to S3...")
                s3.put_object(Bucket=S3_BUCKET, Key=s3_key, Body=json_data, ContentType="application/json")
            except Exception as e:
                print(f"❌ Failed to upload JSON: {e}")

    # ✅ Parallel Processing for Batches (Optimized)
    batch_size = 50  # Increase batch size to handle more data in each process
    with concurrent.futures.ThreadPoolExecutor(max_workers=20) as executor:
        for i in range(0, len(merged_df), batch_size):
            batch_df = merged_df.iloc[i:i + batch_size]
            executor.submit(process_company_batch, batch_df)

# ✅ **Wrapper for Async Functions**
def download_sec_files_sync(year: str, quarter: str):
    asyncio.run(download_sec_files(year, quarter))

def transform_and_upload_json_sync(year: str, quarter: str):
    asyncio.run(transform_and_upload_json(year, quarter))

# ✅ **Load JSON into Snowflake**
load_json_to_snowflake = SnowflakeOperator(
    task_id='load_json_to_snowflake',
    snowflake_conn_id='snowflake_extract',
    sql=f"""
    CREATE OR REPLACE STAGE FINANCIAL_DB_DATA.JSON_DATA_SCHEMA.JSON_STAGE
    URL = 's3://{S3_BUCKET}/{JSON_S3_FOLDER}'
    STORAGE_INTEGRATION = my_s3_integration
    FILE_FORMAT = (TYPE = JSON);

    CREATE OR REPLACE TABLE FINANCIAL_DB_DATA.JSON_DATA_SCHEMA.JSON_TABLE (
        file_name STRING,
        json_content VARIANT
    );

    COPY INTO FINANCIAL_DB_DATA.JSON_DATA_SCHEMA.JSON_TABLE
    (file_name, json_content)
    FROM (
        SELECT METADATA$FILENAME, PARSE_JSON($1)
        FROM @FINANCIAL_DB_DATA.JSON_DATA_SCHEMA.JSON_STAGE
    )
    FILE_FORMAT = (TYPE = JSON)
    ON_ERROR = CONTINUE;
    """,
    dag=dag
)

# ✅ **Airflow Tasks**
download_sec_task = PythonOperator(
    task_id="download_sec_files",
    python_callable=download_sec_files_sync,  # Use sync wrapper for async task
    provide_context=True,  # Pass the context
    op_args=["{{ dag_run.conf['year'] }}", "{{ dag_run.conf['quarter'] }}"],  # Pass dynamic values
    dag=dag
)

transform_json_task = PythonOperator(
    task_id="transform_json",
    python_callable=transform_and_upload_json_sync,  # Use sync wrapper for async task
    provide_context=True,  # Pass the context
    op_args=["{{ dag_run.conf['year'] }}", "{{ dag_run.conf['quarter'] }}"],  # Pass dynamic values
    dag=dag
)

# ✅ **Set Task Dependencies**
download_sec_task >> transform_json_task >> load_json_to_snowflake
