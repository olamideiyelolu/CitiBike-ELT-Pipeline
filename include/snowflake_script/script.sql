USE ROLE accountadmin;
CREATE WAREHOUSE IF NOT EXISTS citibike_wh WITH warehouse_size='x-small';
CREATE DATABASE IF NOT EXISTS citibike_db;
CREATE ROLE IF NOT EXISTS citibike_role;

GRANT USAGE ON WAREHOUSE citibike_wh TO ROLE citibike_role;
GRANT ROLE citibike_role TO USER <USERNAME_HERE>;
GRANT ALL ON DATABASE citibike_db TO ROLE citibike_role;
CREATE SCHEMA IF NOT EXISTS citibike_db.citibike_schema;

USE ROLE citibike_role;

USE DATABASE citibike_db;
USE SCHEMA citibike_schema;



CREATE OR REPLACE STAGE my_citibike_stage
  URL = 's3://your-bucket-name/'
  CREDENTIALS = (
    AWS_KEY_ID = 'AWS_key_credentials' 
    AWS_SECRET_KEY = 'AWS_key_credentials'
  )
  FILE_FORMAT = (
    TYPE = 'CSV' 
    FIELD_DELIMITER = ',' 
    SKIP_HEADER = 1
    NULL_IF = ('NULL', 'null', '')
    TIMESTAMP_FORMAT = 'YYYY-MM-DD HH24:MI:SS.FF3'
);

CREATE OR REPLACE TABLE raw_trips (
    ride_id STRING,
    rideable_type STRING,
    started_at TIMESTAMP_NTZ,
    ended_at TIMESTAMP_NTZ,
    start_station_name STRING,
    start_station_id STRING,
    end_station_name STRING,
    end_station_id STRING,
    start_lat FLOAT,
    start_lng FLOAT,
    end_lat FLOAT,
    end_lng FLOAT,
    member_casual STRING
);
