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

ENDPOINT="d2b-internal-assessment-dwh.cxeuj0ektqdz.eu-central-1.rds.amazonaws.com"
PORT="5432"
USER="yazejibi6672"
REGION="eu-central-1"
DBNAME="d2b_assessment"
PASSWORD="3aXYi4XPVw"
REGION_NAME="eu-central-1"
BUCKET="d2b-internal-assessment-bucket"
KEY="orders_data/orders.csv"

conn = psycopg2.connect(host=ENDPOINT, port=PORT, database=DBNAME, user=USER, password=PASSWORD, sslrootcert="SSLCERTIFICATE")
cur = conn.cursor()


with DAG(
    'orders_ETL',
    default_args={'retries': 2},
    description='ETL DAG for orders',
    schedule_interval=None,
    start_date=pendulum.datetime(2021, 1, 1, tz="UTC"),
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
            print(bucket_file)

            file_df = pd.read_csv(bucket_file['Body'])
            file = file_df.to_csv(s_buf, index=False)
            s_buf.seek(0)

            cur.copy_expert("""
                COPY yazejibi6672_staging.orders FROM STDIN WITH CSV HEADER DELIMITER AS ','
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
                WITH CTE_1 AS (
                    Select a.*,
                    b.* 
                    FROM 
                    yazejibi6672_staging.orders a
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

                INSERT INTO yazejibi6672_analytics.agg_public_holiday SELECT * FROM CTE_3
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
        s_buf = io.StringIO()
        try:
            cur.copy_expert("""
                COPY yazejibi6672_analytics.agg_public_holiday TO STDIN WITH (FORMAT CSV, HEADER, DELIMITER ',')
                """, file)
            resource.Object(bucket, 'analytics_export/yazejibi6672/agg_public_holiday.csv').put(Body=file.getvalue())

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