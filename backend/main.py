import os
from fastapi import FastAPI, HTTPException
import requests
import snowflake.connector
from dotenv import load_dotenv
from pydantic import BaseModel
import asyncio

# Load environment variables
load_dotenv()

app = FastAPI()

# Airflow Authentication details
AIRFLOW_USERNAME = os.getenv("AIRFLOW_USERNAME", "airflow")
AIRFLOW_PASSWORD = os.getenv("AIRFLOW_PASSWORD", "airflow")
AIRFLOW_URL = "http://airflow-webserver:8080/api/v1/dags/s3_to_snowflake_financial_data/dagRuns"
AIRFLOW_URL_DBT = "http://airflow-webserver:8080/api/v1/dags/dbt_financial_transformations/dagRuns" 
AIRFLOW_URL_JSON = "http://airflow-webserver:8080/api/v1/dags/json_transformation_to_snowflake/dagRuns" # Ensure URL is correct
AIRFLOW_DAG_ID = os.getenv("AIRFLOW_DAG_ID_Raw", "s3_to_snowflake_financial_data")
AIRFLOW_DAG_ID_DBT = os.getenv("AIRFLOW_DAG_ID_DBT", "dbt_financial_transformations")
AIRFLOW_DAG_ID_JSON = os.getenv("AIRFLOW_DAG_ID_JSON", "json_transformation_to_snowflake")

# Snowflake Configuration
SNOWFLAKE_CONFIG = {
    "user": os.getenv("SNOWFLAKE_USER"),
    "password": os.getenv("SNOWFLAKE_PASSWORD"),
    "account": os.getenv("SNOWFLAKE_ACCOUNT"),
    "warehouse": os.getenv("SNOWFLAKE_WAREHOUSE"),
    "database": os.getenv("SNOWFLAKE_DATABASE"),
    "schema": os.getenv("SNOWFLAKE_SCHEMA"),
    "role": os.getenv("SNOWFLAKE_ROLE"),
}

# Snowflake Connection Function
def get_snowflake_connection():
    try:
        conn = snowflake.connector.connect(
            user=SNOWFLAKE_CONFIG["user"],
            password=SNOWFLAKE_CONFIG["password"],
            account=SNOWFLAKE_CONFIG["account"],
            warehouse=SNOWFLAKE_CONFIG["warehouse"],
            database=SNOWFLAKE_CONFIG["database"],
            schema=SNOWFLAKE_CONFIG["schema"],
            role=SNOWFLAKE_CONFIG["role"]
        )
        return conn
    except Exception as e:
        print(f"❌ Snowflake Connection Error: {e}")
        return None

# Pydantic model for query validation
class QueryRequest(BaseModel):
    query: str

# Root endpoint
@app.get("/")
async def root():
    return {"message": "Financial Data API is running!"}

#dag 1: s3_to_snowflake_financial_data endpoint
@app.post("/trigger_dag/{year}{quarter}")
async def trigger_dag(year: str, quarter: str):
    """Trigger Airflow DAG for Raw Data Processing and pass year and quarter as parameters"""
    
    payload = {
        "conf": {
            "year": year,
            "quarter": quarter
        }
    }

    response = requests.post(
        AIRFLOW_URL,
        json=payload,
        auth=(AIRFLOW_USERNAME, AIRFLOW_PASSWORD)  # Basic Authentication
    )

    if response.status_code in [200, 201, 202]:
        dag_run_id = response.json().get('dag_run_id')
        if dag_run_id:
            return {"message": f"Raw Data DAG triggered successfully for {year} {quarter}!", "dag_run_id": dag_run_id}
        else:
            raise HTTPException(status_code=500, detail="dag_run_id not found in the response")
    else:
        raise HTTPException(status_code=500, detail="Failed to trigger Raw Data DAG")
    
#dag 1: s3_to_snowflake_financial_data status endpoint
@app.get("/check_dag_status/{dag_run_id}")
async def check_dag_status(dag_run_id: str):
    """Check the status of the Raw Data DAG run"""
    if not dag_run_id:
        raise HTTPException(status_code=400, detail="dag_run_id is missing.")

    max_retries = 12  # Retry every 5 seconds (60 seconds total)
    retries = 0
    while retries < max_retries:
        response = requests.get(
            f"{AIRFLOW_URL}/{dag_run_id}",
            auth=(AIRFLOW_USERNAME, AIRFLOW_PASSWORD)  # Basic Authentication
        )

        if response.status_code == 200:
            dag_status = response.json()
            status = dag_status['state']
            if status in ['success', 'failed']:  # Stop if status is either success or failed
                return {"status": status}
            else:
                retries += 1
                await asyncio.sleep(5)  # Wait for 5 seconds before retrying
        else:
            raise HTTPException(status_code=500, detail="Failed to fetch DAG status.")

    raise HTTPException(status_code=500, detail="DAG is still running after several retries.")


#dag 2: dbt_financial_transformations endpoint
@app.post("/trigger_dbt_dag/{year}{quarter}")
async def trigger_dbt_dag(year: str, quarter: str):
    """Trigger Airflow DAG for DBT Transformations and pass year and quarter as parameters"""
    
    payload = {
        "conf": {
            "year": year,
            "quarter": quarter
        }
    }

    response = requests.post(
        AIRFLOW_URL_DBT,
        json=payload,
        auth=(AIRFLOW_USERNAME, AIRFLOW_PASSWORD)  # Basic Authentication
    )

    if response.status_code in [200, 201, 202]:
        dag_run_id = response.json().get('dag_run_id')
        if dag_run_id:
            return {"message": f"DBT DAG triggered successfully for {year} {quarter}!", "dag_run_id": dag_run_id}
        else:
            raise HTTPException(status_code=500, detail="dag_run_id not found in the response")
    else:
        raise HTTPException(status_code=500, detail="Failed to trigger DBT DAG")

#dag 2: dbt_financial_transformations status endpoint
@app.get("/check_dbt_dag_status/{dag_run_id}")
async def check_dbt_dag_status(dag_run_id: str):
    """Check the status of the DBT DAG run"""
    if not dag_run_id:
        raise HTTPException(status_code=400, detail="dag_run_id is missing.")

    max_retries = 144  # Retry every 5 seconds (60 seconds total)
    retries = 0
    while retries < max_retries:
        response = requests.get(
            f"{AIRFLOW_URL_DBT}/{dag_run_id}",
            auth=(AIRFLOW_USERNAME, AIRFLOW_PASSWORD)  # Basic Authentication
        )

        if response.status_code == 200:
            dag_status = response.json()
            status = dag_status['state']
            if status in ['success', 'failed']:  # Stop if status is either success or failed
                return {"status": status}
            else:
                retries += 1
                await asyncio.sleep(5)  # Wait for 5 seconds before retrying
        else:
            raise HTTPException(status_code=500, detail="Failed to fetch DBT DAG status.")

    raise HTTPException(status_code=500, detail="DBT DAG is still running after several retries.")

#dag 3: json_transformation_to_snowflake endpoint
@app.post("/trigger_json_dag/{year}{quarter}")
async def trigger_json_dag(year: str, quarter: str):
    """Trigger Airflow DAG for JSON Transformation and pass year and quarter as parameters"""
    
    payload = {
        "conf": {
            "year": year,
            "quarter": quarter
        }
    }

    response = requests.post(
        AIRFLOW_URL_JSON,
        json=payload,
        auth=(AIRFLOW_USERNAME, AIRFLOW_PASSWORD)  # Basic Authentication
    )

    if response.status_code in [200, 201, 202]:
        dag_run_id = response.json().get('dag_run_id')
        if dag_run_id:
            return {"message": f"JSON DAG triggered successfully for {year} {quarter}!", "dag_run_id": dag_run_id}
        else:
            raise HTTPException(status_code=500, detail="dag_run_id not found in the response")
    else:
        raise HTTPException(status_code=500, detail="Failed to trigger JSON DAG")

#dag 3: json_transformation_to_snowflake status endpoint 
@app.get("/check_json_dag_status/{dag_run_id}")
async def check_json_dag_status(dag_run_id: str):
    """Check the status of the JSON DAG run"""
    if not dag_run_id:
        raise HTTPException(status_code=400, detail="dag_run_id is missing.")

    max_retries = 144  # Retry every 5 seconds (60 seconds total)
    retries = 0
    while retries < max_retries:
        response = requests.get(
            f"{AIRFLOW_URL_JSON}/{dag_run_id}",
            auth=(AIRFLOW_USERNAME, AIRFLOW_PASSWORD)  # Basic Authentication
        )

        if response.status_code == 200:
            dag_status = response.json()
            status = dag_status['state']
            if status in ['success', 'failed']:  # Stop if status is either success or failed
                return {"status": status}
            else:
                retries += 1
                await asyncio.sleep(5)  # Wait for 5 seconds before retrying
        else:
            raise HTTPException(status_code=500, detail="Failed to fetch JSON DAG status.")

    raise HTTPException(status_code=500, detail="JSON DAG is still running after several retries.")

# Run sql query on snowflake endpoint
@app.post("/run_sql_query")
async def run_sql_query(query_request: QueryRequest):
    """Run SQL query on Snowflake and return the result"""
    query = query_request.query  # Get the query from the request body
    try:
        # Connect to Snowflake
        conn = get_snowflake_connection()

        if conn is None:
            raise HTTPException(status_code=500, detail="Failed to connect to Snowflake")

        # Execute the query
        cursor = conn.cursor()
        cursor.execute(query)

        # Fetch the results
        result = cursor.fetchall()

        # Get column names from the result
        columns = [col[0] for col in cursor.description]

        # Return results as a list of dictionaries
        result_data = [dict(zip(columns, row)) for row in result]

        return {"data": result_data}

    except Exception as e:
        raise HTTPException(status_code=500, detail=f"SQL query execution failed: {str(e)}")
