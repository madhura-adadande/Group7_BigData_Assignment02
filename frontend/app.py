import streamlit as st
import requests
import pandas as pd
import time
from PIL import Image
 
# Set Streamlit Page Config
st.set_page_config(page_title="Financial Data Pipeline", layout="wide")
 
# Sidebar Navigation
st.sidebar.title("Navigation")
page = st.sidebar.radio("Go to", ["🏠 Home", "📂 Raw Tables", "📊 Fact Tables", "📈 JSON Table"])
 
# Home Page Content
if page == "🏠 Home":
    st.title("📊 Financial Data Processing Pipeline")
    st.write(
        """
        This project automates the **ETL** process for financial data by scraping data from a website, storing it in **AWS S3**, then extracting, transforming, and loading it into **Snowflake** using **Airflow** DAGs. It features a **Streamlit** frontend and a **FastAPI** backend, with each component containerized using **Docker** and deployed on **Digital Ocean** for scalability and efficiency. \n
        
        **Aim:** A master financial statement database to support analysts conducting fundamental analysis of US public companies.\n
        
        The pipeline ensures seamless data ingestion, transformation, and storage for financial analysis.
        """
    )
     # Load and display the image from the frontend folder
    
    st.subheader("Architecture Diagram")
    img1 = Image.open("Diagram_Data_Pipeline.png")  
    st.image(img1, caption="Data Pipeline Architecture", use_container_width=True)

    img2 = Image.open("Diagram_Application_Deployment.png")  
    st.image(img2, caption="Application and Deployment Architecture", use_container_width=True)

    
    # Data Processing Steps (as a Table)
    st.subheader("Raw Data Processing")
    steps_data = {
        "Steps": ["Scrape Data Links from SEC Markets Data", "Unzip Data Links & Store in S3", "Store the data in Snowflake as 4 tables: Sub, Pre, Tag, Num"],
        "Tools Used": ["Selenium, Chrome Driver Manager", "Amazon S3 (boto3), requests", "Snowflake"]
    }
    df_raw = pd.DataFrame(steps_data)
    df_raw.index = df_raw.index + 1  # Start index from 1
    st.table(df_raw)
 
    st.subheader("Json Data Processing")
    steps_data1 = {
        "Steps": ["Convert Company data to JSON", "Store in S3", "Store the data in Snowflake"],
        "Tools Used": ["Pandas, JSON, Concurrent", "Amazon S3 (boto3)", "Snowflake"]
    }
    df_raw1 = pd.DataFrame(steps_data1)
    df_raw1.index = df_raw1.index + 1  # Start index from 1
    st.table(df_raw1)
 
    st.subheader("Fact Table Processing")
    steps_data2 = {
        "Steps": ["Select 4 tables from Raw Data Schema & Convert into 3 Fact Tables", "Perform data quality tests, Validations"],
        "Tools Used": ["DBT, Snowflake", "DBT"]
    }
    df_raw2 = pd.DataFrame(steps_data2)
    df_raw2.index = df_raw2.index + 1  # Start index from 1
    st.table(df_raw2)
 
    st.subheader("Automation, Application and Deployment")
    steps_data3 = {
        "Steps": ["Orchestrate 3 DAGs for data processing", "Develop a frontend and backend application", "Deploy on Cloud"],
        "Tools Used": ["Airflow, Bash Operator, Docker-Compose.yaml", "Streamlit, FastAPI", "Digital Ocean, Docker"]
    }
    df_raw3 = pd.DataFrame(steps_data3)
    df_raw3.index = df_raw3.index + 1  # Start index from 1
    st.table(df_raw3)
 
# ✅ Raw Tables Page
elif page == "📂 Raw Tables":
    st.title("📂 Raw Financial Data Tables")
    st.write("Explore the SEC Raw Data and Trigger Airflow DAGs!")
 
    # Check if Raw Data DAG has been triggered
    if 'raw_dag_triggered' not in st.session_state:
        st.session_state.raw_dag_triggered = False  # Default to False

    # ✅ Display Data Overview
    st.subheader("Available Raw Tables in Snowflake")
    st.write("""
    - `SUB`: Company submission details.
    - `NUM`: Numeric data related to financial metrics.
    - `PRE`: Presentation links between statements.
    - `TAG`: Metadata related to financial tags.
    """)

    # Dropdown for Year Selection
    year = st.selectbox("Select Year", ["2024", "2023", "2022", "2021", "2020", "2019", "2018", "2017", "2016", "2015", "2014", "2013", "2012", "2011", "2010"])

    # Dropdown for Quarter Selection
    quarter = st.selectbox("Select Quarter", ["q1", "q2", "q3", "q4"])

    # When user clicks "Trigger Raw Data Processing"
    if st.button("Trigger Raw Data Processing 🚀"):
        # Send the selected year and quarter to FastAPI (Backend)
        response = requests.post(f"http://backend:8000/trigger_dag/{year}{quarter}")
        if response.status_code == 200:
            response_data = response.json()
            st.success(f"Airflow DAG triggered successfully for {year} {quarter}!")
            st.session_state.raw_dag_run_id = response_data.get('dag_run_id')  # Capture the DAG run ID
            st.session_state.raw_dag_triggered = True  # Set flag to True
            
            # Display the "Processing..." status
            st.warning("Airflow DAG is running. Current status: Processing... Please wait until completion.")
        else:
            st.error(f"Failed to trigger Airflow DAG for {year} {quarter}. Error: {response.text}")

    # Check Raw Data DAG Status
    if hasattr(st.session_state, 'raw_dag_run_id') and st.session_state.raw_dag_triggered:
        with st.empty():
            dag_status = None  # Initialize the status outside the loop
            for _ in range(12):  # Retry every 5 seconds (12 times for 60 seconds)
                response = requests.get(f"http://backend:8000/check_dag_status/{st.session_state.raw_dag_run_id}")
                if response.status_code == 200:
                    dag_status = response.json().get('status')
                    if dag_status == 'success':
                        st.success("DAG completed successfully! You can now run queries.")
                        break
                    elif dag_status == 'failed':
                        st.error("DAG failed. Please check the logs for more details.")
                        break
                    else:
                        st.warning(f"Airflow DAG is still running. Current status: {dag_status}. Please wait until completion.")
                        time.sleep(5)  # Sleep for 5 seconds before checking again
                else:
                    st.error("Failed to check DAG status.")
                    break

            if dag_status is None:
                st.error("Failed to get DAG status after several retries.")

    # Only show the SQL query part after the DAG is successful
    if st.session_state.raw_dag_triggered and dag_status == 'success':
        # Query Selection for Predefined Table Queries
        st.subheader("Query for Tables in the Schema")

        queries_table = {
            "1": "SELECT * FROM FINANCIAL_DB_DATA.RAW_DATA_SCHEMA.SUB LIMIT 10;",
            "2": "SELECT * FROM FINANCIAL_DB_DATA.RAW_DATA_SCHEMA.PRE LIMIT 10;",
            "3": "SELECT * FROM FINANCIAL_DB_DATA.RAW_DATA_SCHEMA.NUM LIMIT 10;",
            "4": "SELECT * FROM FINANCIAL_DB_DATA.RAW_DATA_SCHEMA.TAG LIMIT 10;"
        }

        query_option_table = st.selectbox("Choose an example query for tables", list(queries_table.keys()), format_func=lambda x: f"Example {x}: {queries_table[x]}")

        table_query = queries_table[query_option_table]

        st.text_area("Your SQL query", table_query, key="sql_query_input_table")

        # Run Query for Table Button
        if st.button("Run Query for Table"):
            # Send the query to FastAPI for execution
            payload = {"query": table_query}
            response = requests.post("http://backend:8000/run_sql_query", json=payload)

            if response.status_code == 200:
                result = response.json()
                if "data" in result:
                    df = pd.DataFrame(result["data"])
                    st.dataframe(df)
                else:
                    st.error("No data returned from the query.")
            else:
                st.error(f"Failed to execute query: {response.text}")


        # **Predefined Raw Queries Section**
        st.subheader("Predefined Raw Data Queries")

        predefined_queries = {
            "1": "SELECT * FROM FINANCIAL_DB_DATA.RAW_DATA_SCHEMA.SUB WHERE name ILIKE '%SKYWORKS SOLUTIONS INC%';",
            "2": "SELECT segments, value FROM FINANCIAL_DB_DATA.RAW_DATA_SCHEMA.NUM WHERE TAG ILIKE 'Revenues' AND VALUE > 1000000000 LIMIT 30;",
            "3": "SELECT TAG, DOC FROM FINANCIAL_DB_DATA.RAW_DATA_SCHEMA.TAG where TAG ILIKE '%Assets%';",
            "4": "SELECT * FROM FINANCIAL_DB_DATA.RAW_DATA_SCHEMA.SUB WHERE CITYBA ILIKE '%miami%';"
        }

        # Dropdown for predefined raw queries
        query_option_raw = st.selectbox("Choose an example raw query", list(predefined_queries.keys()), format_func=lambda x: f"Example {x}: {predefined_queries[x]}")

        # Display the raw query
        raw_query = predefined_queries[query_option_raw]

        # Textarea for "Predefined Raw Data SQL Query"
        st.text_area("Predefined SQL query", raw_query, key="sql_query_input_raw")

        # Run Query for Predefined Raw Data
        if st.button("Run Predefined Raw Query"):
            payload = {"query": raw_query}  # Send the predefined query as a payload to FastAPI
            response = requests.post("http://backend:8000/run_sql_query", json=payload)

            if response.status_code == 200:
                result = response.json()
                if "data" in result:
                    df = pd.DataFrame(result["data"])  # Convert the result into a pandas DataFrame
                    st.dataframe(df)  # Display data nicely in Streamlit
                else:
                    st.error("No data returned from the query.")
            else:
                st.error(f"Failed to execute query: {response.text}")

        # Custom SQL Query Text Area
        st.subheader("Run Your Custom Query")

        # Custom Query Input Box
        custom_query = st.text_area("Enter your SQL query", "", key="sql_query_input_custom")

        # Run Query for Custom Button
        if st.button("Run Custom Query"):
            payload = {"query": custom_query}
            response = requests.post("http://backend:8000/run_sql_query", json=payload)

            if response.status_code == 200:
                result = response.json()
                if "data" in result:
                    df = pd.DataFrame(result["data"])
                    st.dataframe(df)
                else:
                    st.error("No data returned from the query.")
            else:
                st.error(f"Failed to execute query: {response.text}")
    else:
        st.error("Please trigger the DAG first before running queries.")

 
# ✅ Fact Tables Page
elif page == "📊 Fact Tables":
    st.title("📊 Final Financial Fact Tables")
    st.write("Explore aggregated financial data for insights.")
    
    # Check if Fact Tables DAG has been triggered
    if 'fact_dag_triggered' not in st.session_state:
        st.session_state.fact_dag_triggered = False  # Default to False
    
    # Dropdown for Year Selection
    year = st.selectbox("Select Year", ["2024", "2023", "2022", "2021", "2020", "2019", "2018", "2017", "2016", "2015", "2014", "2013", "2012", "2011", "2010"])

    # Dropdown for Quarter Selection
    quarter = st.selectbox("Select Quarter", ["q1", "q2", "q3", "q4"])

    # When user clicks "Trigger Fact Table Processing"
    if st.button("Trigger Fact Table Processing 🚀"):
        # Send the selected year and quarter to FastAPI (Backend)
        response = requests.post(f"http://backend:8000/trigger_dbt_dag/{year}{quarter}")
        if response.status_code == 200:
            response_data = response.json()
            st.success(f"Fact Tables DAG triggered successfully for {year} {quarter}!")
            st.session_state.fact_dag_run_id = response_data.get('dag_run_id')  # Capture the DAG run ID
            st.session_state.fact_dag_triggered = True  # Set flag to True

            # Display the "Processing..." status
            st.warning("Fact Tables DAG is running. Current status: Processing... Please wait until completion.")
        else:
            st.error(f"Failed to trigger Fact Tables DAG for {year} {quarter}. Error: {response.text}")

    # Check Fact Tables DAG Status (Polling for 60 seconds)
    if hasattr(st.session_state, 'fact_dag_run_id') and st.session_state.fact_dag_triggered:
        with st.empty():
            dag_status = None  # Initialize the status outside the loop
            for _ in range(12):  # Retry every 5 seconds (12 times for 60 seconds)
                response = requests.get(f"http://backend:8000/check_dbt_dag_status/{st.session_state.fact_dag_run_id}")
                if response.status_code == 200:
                    dag_status = response.json().get('status')
                    if dag_status == 'success':
                        st.success("Fact Tables DAG completed successfully! You can now run queries.")
                        break
                    elif dag_status == 'failed':
                        st.error("Fact Tables DAG failed. Please check the logs for more details.")
                        break
                    else:
                        st.warning(f"Fact Tables DAG is still running. Current status: {dag_status}. Please wait until completion.")
                        time.sleep(5)  # Sleep for 5 seconds before checking again
                else:
                    st.error("Failed to check Fact Tables DAG status.")
                    break

            if dag_status is None:
                st.error("Failed to get Fact Tables DAG status after several retries.")

    # Query Selection for Fact Tables
    if hasattr(st.session_state, 'fact_dag_triggered') and st.session_state.fact_dag_triggered and dag_status == 'success':
        st.subheader("Query for Fact Tables")

        queries_fact_tables = {
            "1": "SELECT * FROM FINANCIAL_DB_DATA.DBT_SCHEMA.BALANCE_SHEET_FACT LIMIT 10;",
            "2": "SELECT * FROM FINANCIAL_DB_DATA.DBT_SCHEMA.CASH_FLOW_FACT LIMIT 10;",
            "3": "SELECT * FROM FINANCIAL_DB_DATA.DBT_SCHEMA.INCOME_STATEMENT_FACT LIMIT 10;",
            
        }

        query_option_fact = st.selectbox("Choose an example query for Fact Tables", list(queries_fact_tables.keys()), format_func=lambda x: f"Example {x}: {queries_fact_tables[x]}")

        fact_table_query = queries_fact_tables[query_option_fact]

        st.text_area("Your SQL query", fact_table_query, key="sql_query_input_fact")

        # Run Query for Fact Tables Button
        if st.button("Run Query for Fact Tables"):
            payload = {"query": fact_table_query}
            response = requests.post("http://backend:8000/run_sql_query", json=payload)

            if response.status_code == 200:
                result = response.json()
                if "data" in result:
                    df = pd.DataFrame(result["data"])
                    st.dataframe(df)
                else:
                    st.error("No data returned from the query.")
            else:
                st.error(f"Failed to execute query: {response.text}")

        predefined_fact_queries = {
            "1": "SELECT company_name, SUM(amount) AS total_assets FROM DBT_SCHEMA.BALANCE_SHEET_FACT WHERE FINANCIAL_METRIC ILIKE 'Assets' GROUP BY company_name HAVING SUM(amount) > 1000000000 ORDER BY total_assets ASC LIMIT 20;",
            "2": "SELECT company_name, SUM(amount) AS total_revenue FROM DBT_SCHEMA.INCOME_STATEMENT_FACT WHERE FINANCIAL_METRIC ILIKE '%Revenues%' GROUP BY company_name HAVING SUM(amount) > 1000000000 ORDER BY total_revenue ASC LIMIT 20;",
            "3": "SELECT company_name, SUM(amount) AS net_cash_flow FROM DBT_SCHEMA.CASH_FLOW_FACT WHERE FINANCIAL_METRIC = 'NetCashProvidedByUsedInOperatingActivities' GROUP BY company_name HAVING SUM(amount) > 1000000000 ORDER BY net_cash_flow ASC LIMIT 20;"
        }

        # Dropdown for predefined fact queries
        query_option_fact_predefined = st.selectbox("Choose an example fact query", list(predefined_fact_queries.keys()), format_func=lambda x: f"Example {x}: {predefined_fact_queries[x]}")

        # Display the fact query
        fact_query_predefined = predefined_fact_queries[query_option_fact_predefined]

        # Textarea for "Predefined Fact Data SQL Query"
        st.text_area("Predefined SQL query", fact_query_predefined, key="sql_query_input_fact_predefined")

        # Run Query for Predefined Fact Data
        if st.button("Run Predefined Fact Query"):
            payload = {"query": fact_query_predefined}  # Send the predefined query as a payload to FastAPI
            response = requests.post("http://backend:8000/run_sql_query", json=payload)

            if response.status_code == 200:
                result = response.json()
                if "data" in result:
                    df = pd.DataFrame(result["data"])  # Convert the result into a pandas DataFrame
                    st.dataframe(df)  # Display data nicely in Streamlit
                else:
                    st.error("No data returned from the query.")
            else:
                st.error(f"Failed to execute query: {response.text}")

        # Custom SQL Query Text Area
        st.subheader("Run Your Custom Query")

        custom_query_fact = st.text_area("Enter your SQL query for Fact Tables", "", key="sql_query_input_custom_fact")

        if st.button("Run Custom Query for Fact Tables"):
            payload = {"query": custom_query_fact}
            response = requests.post("http://backend:8000/run_sql_query", json=payload)

            if response.status_code == 200:
                result = response.json()
                if "data" in result:
                    df = pd.DataFrame(result["data"])
                    st.dataframe(df)
                else:
                    st.error("No data returned from the query.")
            else:
                st.error(f"Failed to execute query: {response.text}")
    else:
        st.error("Please trigger the DAG first before running queries.")


 # ✅ JSON Table Page
elif page == "📈 JSON Table":
    st.title("📈 Transformed JSON Data")
    st.write("View transformed financial data stored in JSON format.")

    # Check if JSON DAG has been triggered
    if 'json_dag_triggered' not in st.session_state:
        st.session_state.json_dag_triggered = False  # Default to False
    
    # Dropdown for Year Selection
    year = st.selectbox("Select Year", ["2024", "2023", "2022", "2021", "2020", "2019", "2018", "2017", "2016", "2015", "2014", "2013", "2012", "2011", "2010"])

    # Dropdown for Quarter Selection
    quarter = st.selectbox("Select Quarter", ["q1", "q2", "q3", "q4"])

    # When user clicks "Trigger JSON Data Processing"
    if st.button("Trigger JSON Data Processing 🚀"):
        # Send the selected year and quarter to FastAPI (Backend)
        response = requests.post(f"http://backend:8000/trigger_json_dag/{year}{quarter}")
        if response.status_code == 200:
            response_data = response.json()
            st.success(f"JSON DAG triggered successfully for {year} {quarter}!")
            st.session_state.json_dag_run_id = response_data.get('dag_run_id')  # Capture the DAG run ID
            st.session_state.json_dag_triggered = True  # Set flag to True

            # Display the "Processing..." status
            st.warning("JSON Airflow DAG is running. Current status: Processing... This DAG will take a maximum of 12 minutes. Please wait until completion.")
        else:
            st.error(f"Failed to trigger JSON Airflow DAG for {year} {quarter}. Error: {response.text}")

    # Check JSON DAG Status (Polling for 60 seconds)
    if hasattr(st.session_state, 'json_dag_run_id') and st.session_state.json_dag_triggered:
        with st.empty():
            dag_status = None  # Initialize the status outside the loop
            for _ in range(144):  # Retry every 5 seconds (144 times for 60 seconds)
                response = requests.get(f"http://backend:8000/check_json_dag_status/{st.session_state.json_dag_run_id}")
                if response.status_code == 200:
                    dag_status = response.json().get('status')
                    if dag_status == 'success':
                        st.success("JSON DAG completed successfully! You can now run queries.")
                        break
                    elif dag_status == 'failed':
                        st.error("JSON DAG failed. Please check the logs for more details.")
                        break
                    else:
                        st.warning(f"JSON Airflow DAG is still running. Current status: {dag_status}. This DAG will take a maximum of 12 minutes. Please wait until completion.")
                        time.sleep(5)  # Sleep for 5 seconds before checking again
                else:
                    st.error("Failed to check JSON DAG status.")
                    break

            if dag_status is None:
                st.error("Failed to get JSON DAG status after several retries.")

    # Only show the SQL query part after the DAG is successful
    if hasattr(st.session_state, 'json_dag_triggered') and st.session_state.json_dag_triggered and dag_status == 'success':
        # Query Selection for Predefined Queries for JSON Tables
        st.subheader("Predefined Queries")

        queries_json = {
            "1": "SELECT * FROM FINANCIAL_DB_DATA.JSON_DATA_SCHEMA.JSON_TABLE LIMIT 10;",
            "2": "SELECT COUNT(FILE_NAME) AS Total_files FROM FINANCIAL_DB_Data.JSON_DATA_SCHEMA.JSON_TABLE;",
            "3": "SELECT * FROM FINANCIAL_DB_DATA.JSON_DATA_SCHEMA.JSON_TABLE WHERE file_name ILIKE '%Json_Data/2023q2/Unknown_0000038723.json%';",
        }

        # Dropdown for predefined queries related to JSON tables
        query_option_json = st.selectbox("Choose an example query for JSON tables", list(queries_json.keys()), format_func=lambda x: f"Example {x}: {queries_json[x]}")

        # Display the query for JSON tables
        json_query = queries_json[query_option_json]

        # Textarea for "Your SQL Query"
        st.text_area("Your SQL query", json_query, key="sql_query_input_json")

        # Run Query for JSON Button
        if st.button("Run Query for JSON"):
            # Send the query to FastAPI for execution
            payload = {"query": json_query}  # Send the query as a payload to FastAPI
            response = requests.post("http://backend:8000/run_sql_query", json=payload)

            if response.status_code == 200:
                result = response.json()
                if "data" in result:
                    df = pd.DataFrame(result["data"])  # Convert the result into a pandas DataFrame
                    st.dataframe(df)  # Display data nicely in Streamlit
                else:
                    st.error("No data returned from the query.")
            else:
                st.error(f"Failed to execute query: {response.text}")

        # Custom SQL Query Text Area
        st.subheader("Run Your Custom Query")

        # Custom Query Input Box
        custom_query_json = st.text_area("Enter your SQL query for JSON tables", "", key="sql_query_input_custom_json")

        # Run Query for Custom Button
        if st.button("Run Custom Query for JSON"):
            payload = {"query": custom_query_json}  # Send the custom query as a payload to FastAPI
            response = requests.post("http://backend:8000/run_sql_query", json=payload)

            if response.status_code == 200:
                result = response.json()
                if "data" in result:
                    df = pd.DataFrame(result["data"])  # Convert the result into a pandas DataFrame
                    st.dataframe(df)  # Display data nicely in Streamlit
                else:
                    st.error("No data returned from the query.")
            else:
                st.error(f"Failed to execute query: {response.text}")
    else:
        st.error("Please trigger the JSON DAG first before running queries.")


 
