from airflow.sdk import dag, task, chain
from pendulum import datetime
from datetime import timedelta
from airflow.providers.amazon.aws.hooks.s3 import S3Hook
from airflow.providers.snowflake.transfers.copy_into_snowflake import CopyFromExternalStageToSnowflakeOperator
from cosmos import DbtTaskGroup, ProjectConfig, ProfileConfig, ExecutionConfig
from cosmos.profiles import SnowflakeUserPasswordProfileMapping
import requests
import zipfile
import io
from pathlib import Path

doc_md_DAG = """
This DAG is a pipeline that extracts data from the Citibike API and loads it into an S3 bucket.

The DAG is scheduled to run yearly on January 1st.

The DAG is composed of the following tasks:
- extract_data: Extracts data from the Citibike API into AWS S3 bucket
- load_data: Loads data from AWS S3 bucket into Snowflake
"""

# Configuration
S3_BUCKET_NAME = "my-citibike-tripdataa"  # Change this to your desired bucket name
DATA_URL = "https://s3.amazonaws.com/tripdata/2023-citibike-tripdata.zip"

# This gets the directory where your dag.py is located
DAG_FOLDER = Path(__file__).parent

# This points to the dbt folder inside that same directory
DBT_PROJECT_PATH = "/usr/local/airflow/dags/dbt/dbt_transformation"
DBT_EXECUTABLE_PATH = "/usr/local/airflow/dbt_venv/bin/dbt"
profile_config = ProfileConfig(
    profile_name="default",
    target_name="dev",
    profile_mapping=SnowflakeUserPasswordProfileMapping(
        conn_id="snowflake_conn",
        profile_args={"database": "citibike_db", "schema": "citibike_schema"},
    ),
)
execution_config = ExecutionConfig(
    dbt_executable_path=DBT_EXECUTABLE_PATH
)

@dag(
    "Citibike-ELT-Pipeline",
    start_date=datetime(2025, 1, 1),
    schedule="@yearly",
    catchup=False,
    doc_md=doc_md_DAG,
)
def citibike_elt_pipeline():
    # Extract data from the Citibike API into AWS S3 bucket
    @task(retries=3, retry_delay=timedelta(minutes=2))
    def extract_data():
        """
        Extracts data from the Citibike API and uploads CSV files to AWS S3 bucket.
        Creates the bucket if it doesn't exist.
        """
        # Initialize S3 hook with your connection
        s3_hook = S3Hook(aws_conn_id="aws_conn")
        
        # Download the zip file with timeout and retry logic
        print(f"Downloading data from {DATA_URL}")
        response = requests.get(DATA_URL, timeout=300)  # 5 minute timeout
        response.raise_for_status()
        print(f"Downloaded {len(response.content)} bytes")
        
        uploaded_files = []
        
        # Open top-level zip in memory
        with zipfile.ZipFile(io.BytesIO(response.content)) as trip_data:
            monthly_zips = [
                name for name in trip_data.namelist()
                if name.endswith(".zip") and name.startswith("2023")
            ]
            
            print(f"Found {len(monthly_zips)} monthly zip files")
            
            for month_zip_name in monthly_zips:
                # Extract year and month from filename
                filename = month_zip_name.split("/")[-1]
                year = filename[:4]
                month = filename[4:6]
                
                print(f"Processing {year}-{month} data...")
                
                # Read monthly zip
                month_zip_bytes = trip_data.read(month_zip_name)
                
                # Open monthly zip
                with zipfile.ZipFile(io.BytesIO(month_zip_bytes)) as month_zip:
                    for inner_file in month_zip.namelist():
                        if inner_file.endswith(".csv"):
                            csv_bytes = month_zip.read(inner_file)
                            
                            # Create S3 key (path) like: 2023-01/filename.csv
                            s3_key = f"{year}-{month}/{inner_file}"
                            
                            # Upload CSV to S3
                            print(f"Uploading {s3_key} to s3://{S3_BUCKET_NAME}/{s3_key}")
                            s3_hook.load_bytes(
                                bytes_data=csv_bytes,
                                key=s3_key,
                                bucket_name=S3_BUCKET_NAME,
                                replace=True
                            )
                            
                            uploaded_files.append(s3_key)
                            print(f"✓ Uploaded {s3_key}")
        
        print(f"\nTotal files uploaded: {len(uploaded_files)}")
        return {
            "bucket": S3_BUCKET_NAME,
            "files_uploaded": len(uploaded_files),
            "files": uploaded_files
        }

    # Load data from S3 to Snowflake using CopyFromExternalStageToSnowflakeOperator
    load_into_warehouse = CopyFromExternalStageToSnowflakeOperator(
        task_id="load_data",
        snowflake_conn_id="snowflake_conn",
        table="raw_trips",
        stage="my_citibike_stage",
        pattern=".*[.]csv",
        schema="citibike_schema",
        database="citibike_db",
        warehouse="citibike_wh",
        file_format="(type = 'CSV', field_delimiter = ',', skip_header = 1, field_optionally_enclosed_by = '\"')",
    )

    transform_data = DbtTaskGroup(
        group_id="transform_data",
        project_config=ProjectConfig(DBT_PROJECT_PATH),
        profile_config=profile_config,
        execution_config=execution_config,
        operator_args={
            "emit_datasets": True, 
        },
        default_args={"retries":2}
    )

    # Define task dependencies
    extract_data() >> load_into_warehouse >> transform_data

# Instantiate the DAG
citibike_elt_pipeline()