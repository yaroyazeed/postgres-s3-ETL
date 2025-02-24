"""
This ETL DAG is demonstrating an Extract -> Transform -> Load pipeline
"""
import json
from textwrap import dedent
from datetime import datetime
import pendulum
from airflow import DAG
from airflow.operators.python import PythonOperator
import psycopg2
import sys
import boto3
import os
from botocore import UNSIGNED
from botocore.client import Config
import pandas as pd
import io

HOST="34.89.230.185"
PORT="5432"
USER="yazejibi2622"
REGION="eu-central-1"
DBNAME="d2b_accessment"
PASSWORD="wUrwdz3sDa"
REGION_NAME="eu-central-1"
BUCKET="d2b-internal-assessment-bucket"
KEY="orders_data/orders.csv"

conn = psycopg2.connect(host=HOST, port=PORT, database=DBNAME, user=USER, password=PASSWORD, sslrootcert="SSLCERTIFICATE")
cur = conn.cursor()


with DAG(
    'orders_ETL',
    default_args={'retries': 2},
    description='ETL DAG for orders',
    schedule_interval='@once',
    start_date=pendulum.datetime(2022, 11, 22, tz="UTC"),
    catchup=False,
    tags=['example'],
) as dag:

    # [START extract_function]
    def extract(**kwargs):

        s3 = boto3.client('s3', config=Config(signature_version=UNSIGNED),region_name=REGION_NAME)
        s_buf = io.StringIO()

        bucket_file = s3.get_object(
            Bucket=BUCKET,
            Key=KEY
            )

        try:
            file_df = pd.read_csv(bucket_file['Body'])
            file = file_df.to_csv(s_buf, index=False)
            s_buf.seek(0)

            cur.copy_expert("""
                COPY yazejibi2622_staging.orders FROM STDIN WITH CSV HEADER DELIMITER AS ','
                """, s_buf)

            print("Table ORDERS populated successfully")
            conn.commit()
            conn.close()
        except Exception as e:
            print("Database connection failed due to {}".format(e))                
    # [END extract_function]


    # [START transform_function]
    def transform(**kwargs):
        try:
            cur.execute("""
                CREATE TABLE IF NOT EXISTS yazejibi2622_analytics.agg_public_holiday(
                    ingestion_date      DATE PRIMARY KEY NOT NULL,
                    tt_order_hol_jan    INT         NOT NULL,
                    tt_order_hol_feb    INT         NOT NULL,
                    tt_order_hol_mar    INT         NOT NULL,
                    tt_order_hol_apr    INT         NOT NULL,
                    tt_order_hol_may    INT         NOT NULL,
                    tt_order_hol_jun    INT         NOT NULL,
                    tt_order_hol_jul    INT         NOT NULL,
                    tt_order_hol_aug    INT         NOT NULL,
                    tt_order_hol_sep    INT         NOT NULL,
                    tt_order_hol_oct    INT         NOT NULL,
                    tt_order_hol_nov    INT         NOT NULL,
                    tt_order_hol_dec    INT         NOT NULL
                    )
                """)
            conn.commit()

            cur.execute("""
                WITH CTE_1 AS (
                    Select a.*,
                    b.* 
                    FROM 
                    yazejibi2622_staging.orders a
                    JOIN if_common.dim_dates b ON a.order_date = b.calendar_dt
                    ),
                CTE_2 AS (
                    SELECT * FROM CTE_1
                    WHERE working_day = false AND day_of_the_week_num IN (1,2,3,4,5)
                    ),
                CTE_3 AS(
                    select
                    CURRENT_TIMESTAMP as ingestion_date,
                    SUM(CASE WHEN month_of_the_year_num = 1 THEN 1 ELSE 0 END) as tt_order_hol_jan,
                    SUM(CASE WHEN month_of_the_year_num = 2 THEN 1 ELSE 0 END) as tt_order_hol_feb,
                    SUM(CASE WHEN month_of_the_year_num = 3 THEN 1 ELSE 0 END) as tt_order_hol_mar,
                    SUM(CASE WHEN month_of_the_year_num = 4 THEN 1 ELSE 0 END) as tt_order_hol_apr,
                    SUM(CASE WHEN month_of_the_year_num = 5 THEN 1 ELSE 0 END) as tt_order_hol_may,
                    SUM(CASE WHEN month_of_the_year_num = 6 THEN 1 ELSE 0 END) as tt_order_hol_jun,
                    SUM(CASE WHEN month_of_the_year_num = 7 THEN 1 ELSE 0 END) as tt_order_hol_jul,
                    SUM(CASE WHEN month_of_the_year_num = 8 THEN 1 ELSE 0 END) as tt_order_hol_aug,
                    SUM(CASE WHEN month_of_the_year_num = 9 THEN 1 ELSE 0 END) as tt_order_hol_sep,
                    SUM(CASE WHEN month_of_the_year_num = 10 THEN 1 ELSE 0 END) as tt_order_hol_oct,
                    SUM(CASE WHEN month_of_the_year_num = 11 THEN 1 ELSE 0 END) as tt_order_hol_nov,
                    SUM(CASE WHEN month_of_the_year_num = 12 THEN 1 ELSE 0 END) as tt_order_hol_dec
                    FROM
                    CTE_2            
                    )

                INSERT INTO yazejibi2622_analytics.agg_public_holiday SELECT * FROM CTE_3
                """)

            print("Table agg_public_holiday transformed and created successfully")
            conn.commit()
            conn.close()
        except Exception as e:
            print("Database connection failed due to {}".format(e))                
    # [END transform_function]


    # [START load_function]
    def load(**kwargs):
        resource=boto3.resource('s3',config=Config(signature_version=UNSIGNED),region_name='eu-central-1')
        file = io.StringIO()
        try:
            cur.copy_expert("""
                COPY yazejibi2622_analytics.agg_public_holiday TO STDIN WITH (FORMAT CSV, HEADER, DELIMITER ',')
                """, file)
            resource.Object(BUCKET, 'analytics_export/yazejibi2622/agg_public_holiday.csv').put(Body=file.getvalue())

            print("File best_performing_product.csv exported successfully!")
        except Exception as e:
            print("Database connection failed due to {}".format(e))        
    # [END load_function]


    # [START main_flow]
    extract_task = PythonOperator(
        task_id='extract',
        python_callable=extract,
    )

    transform_task = PythonOperator(
        task_id='transform',
        python_callable=transform,
    )

    load_task = PythonOperator(
        task_id='load',
        python_callable=load,
    )

    extract_task >> transform_task >> load_task