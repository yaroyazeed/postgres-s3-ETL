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
KEY="orders_data/shipment_deliveries.csv"

conn = psycopg2.connect(host=HOST, port=PORT, database=DBNAME, user=USER, password=PASSWORD, sslrootcert="SSLCERTIFICATE")
cur = conn.cursor()


with DAG(
    'shipments_ETL',
    default_args={'retries': 2},
    description='ETL DAG for shipment deliveries',
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
                COPY yazejibi2622_staging.shipments_deliveries FROM STDIN WITH CSV HEADER DELIMITER AS ','
                """, s_buf)

            print("Table SHIPMENT_DELIVERIES populated successfully")
            conn.commit()
            conn.close()
        except Exception as e:
            print("Database connection failed due to {}".format(e))                
    # [END extract_function]


    # [START transform_function]
    def transform(**kwargs):
        try:
            cur.execute(""" 
                CREATE TABLE IF NOT EXISTS yazejibi2622_analytics.agg_shipments(
                    ingestion_date          DATE    PRIMARY KEY     NOT NULL,
                    tt_late_shipments       INT                     NOT NULL,
                    tt_undelivered_items    INT                     NOT NULL
                    );                
                """)
            conn.commit()


            cur.execute("""

                WITH CTE_1 AS (
                    Select a.*,
                    b.* 
                    FROM 
                    yazejibi2622_staging.shipments_deliveries a
                    JOIN 
                    yazejibi2622_staging.orders b ON a.order_id = b.order_id
                    ),
                CTE_2 AS (
                    select
                    CURRENT_TIMESTAMP as ingestion_date,
                    SUM(CASE WHEN shipment_date - order_date >= 6 AND delivery_date is null THEN 1 ELSE 0 END) as tt_late_shipments,
                    SUM(CASE WHEN delivery_date is null AND shipment_date is null AND '2022-09-05' - order_date = 15 THEN 1 ELSE 0 END) as tt_undelivered_items
                    FROM
                    CTE_1 
                    )

                INSERT INTO yazejibi2622_analytics.agg_shipments SELECT * FROM CTE_2
                """)

            print("Table agg_shipments transformed and created successfully")
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
                COPY yazejibi2622_analytics.agg_shipments TO STDIN WITH (FORMAT CSV, HEADER, DELIMITER ',')
                """, file)
            resource.Object(BUCKET, 'analytics_export/yazejibi2622/agg_shipments.csv').put(Body=file.getvalue())

            print("File agg_shipmentscsv exported successfully!")
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