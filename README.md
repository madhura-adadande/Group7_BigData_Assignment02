# Financial Data Pipeline

## Project Description

This project automates the pipeline for financial data by scraping data from a website, storing it in **AWS S3**, then extracting, transforming, and loading it into **Snowflake** using **Airflow** DAGs. It features a **Streamlit** frontend and a **FastAPI** backend, with each component containerized using **Docker** and deployed on **Digital Ocean** for scalability and efficiency.

## Live Links

Streamlit Applicaation: http://138.197.102.155:8501 <br>
Airflow: http://138.197.102.155:8081 <br>
FastAPI: http://138.197.102.155:8000/docs <br>

## Technologies Used

1. Extraction: Seleium
2. Storage and Cloud: Amazon S3, Snowflake
3. Automation: Airflow
4. Frontend Application: Streamlit
5. Backend: FastAPI
6. Validation and Testing: Data Build Tool(DBT)
7. Deployment: Digital Ocean

## Architecture Diagram

1. Financial Data Pipeline

![Diagram_Data_Pipeline](https://github.com/user-attachments/assets/1e4c6ffd-8c06-482f-bd3d-c109e02e5048)


2. Application and Deployment

![Diagram_Application_Deployment](https://github.com/user-attachments/assets/851ee4ad-5e5d-4d86-a740-cae378a15882)


## Directory Structure
```
Financial_Data_Pipeline/
│── airflow/
│   ├── config/
│   │   └── airflow.cfg
│   ├── dags/
│   │   ├── data/
│   │   ├── dag_dbt_transform_validate.py
│   │   ├── dag_json_transform_upload.py
│   │   ├── dag_s3_to_snowflake.py
│   ├── dbt_data_pipeline/
│   │   ├── analyses/
│   │   ├── config/
│   │   │   ├── .env
│   │   │   ├── .user.yml
│   │   │   ├── profiles.yml
│   │   ├── dbt_packages/dbt_utils/
│   │   ├── logs/
│   │   ├── macros/
│   │   ├── models/
│   │   │   ├── marts/
│   │   │   │   ├── balance_sheet_fact.sql
│   │   │   │   ├── cash_flow_fact.sql
│   │   │   │   ├── income_statement_fact.sql
│   │   │   ├── staging/
│   │   │   │   ├── schema.yml
│   │   │   │   ├── sources.yml
│   │   │   │   ├── stg_json_table.sql
│   │   │   │   ├── stg_raw_num.sql
│   │   │   │   ├── stg_raw_pre.sql
│   │   │   │   ├── stg_raw_sub.sql
│   │   │   │   ├── stg_raw_tag.sql
│   │   ├── seeds/
│   │   ├── snapshots/
│   │   ├── target/
│   │   ├── tests/
│── backend/
│   ├── __init__.py
│   ├── .env
│   ├── Dockerfile
│   ├── main.py
│   ├── requirements.txt
│── frontend/
│   ├── .env
│   ├── app.py
│   ├── config.toml
│   ├── Diagram_Application_Deployment.png
│   ├── Diagram_Data_Pipeline.png
│   ├── Dockerfile
│   ├── requirements.txt
│── logs/
│── plugins/
│── venv/
│── .gitignore
│── docker-compose.yaml
│── json_extract.py
│── README.md
│── requirements.txt
│── ticker.txt
│── web_extract.py
```

## Workflow

## Instructions to run this project

1. Clone the repository:
```
https://github.com/madhura-adadande/Big_Data_Content_Extraction.git 
```
2. Set up a Virtual Environment:
```
python -m venv venv_name
venv_name\Scripts\activate #windows
source venv_name/bin/activate #macOS and Linux
```
4. Go to path each folder where requirementss.txt is present:
```
pip install -r requirements.txt
```
5. Ensure Docker is installed
6. Create a .env file in the root directory with Snowflake credentials and Amazon S3 credentials:
```
SNOWFLAKE_USER=your_username
SNOWFLAKE_PASSWORD=your_password
SNOWFLAKE_ACCOUNT=your_snowflake_account
SNOWFLAKE_ROLE=DBT_ROLE
SNOWFLAKE_WAREHOUSE=FINANCIAL_WH
SNOWFLAKE_DATABASE=FINANCIAL_DB_DATA
SNOWFLAKE_SCHEMA_DBT=DBT_SCHEMA
AWS_BUCKET_NAME=your_bucket_name
AWS_ACCESS_KEY_ID=your_access_key
AWS_SECRET_ACCESS_KEY=your_secret_access_key
AWS_REGION=uour_region
AIRFLOW_USERNAME=your_username
AIRFLOW_PASSWORD=your_password
```
7. Since each service is dockerized, just run:
```
docker compose up --build
```
8. Monitor the Airflow orchestration by accessing the Airflow web UI through the link
9. Access the FastAPI interactive API documentation for testing endpoints.
10. On the side bar, navigate to the **Raw Tables** page, select the **year** and **quarter**. Then click on **Trigger Raw Data Processing** button, to run the dag and execute predefined queries and custom query.
11. Go to the **Fact Tables** Page, where the **year** and **quarter** will be automatically selected.Then click on **Trigger FACT Table Processing** button, to run the dag and execute predefined queries and custom query.
12. Navigate to the **Json Table** Page, where the year and quarter will be automatically selected.Then click on **Trigger JSON Data Processing** button,  to run the dag and execute predefined queries and custom query.
