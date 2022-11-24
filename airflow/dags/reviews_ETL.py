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
KEY="orders_data/reviews.csv"

conn = psycopg2.connect(host=ENDPOINT, port=PORT, database=DBNAME, user=USER, password=PASSWORD, sslrootcert="SSLCERTIFICATE")
cur = conn.cursor()


with DAG(
    'reviews_ETL',
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
            print(bucket_file)

            file_df = pd.read_csv(bucket_file['Body'])
            file = file_df.to_csv(s_buf, index=False)
            s_buf.seek(0)

            cur.copy_expert("""
                COPY yazejibi6672_staging.reviews FROM STDIN WITH CSV HEADER DELIMITER AS ','
                """, s_buf)

            print("Table REVIEWS populated successfully")
            conn.commit()
            conn.close()
        except Exception as e:
            print("Database connection failed due to {}".format(e))                
    # [END extract_function]


    # [START transform_function]
    def transform(**kwargs):
        try:
            cur.execute(""" 
                CREATE TABLE IF NOT EXISTS yazejibi6672_analytics.best_performing_product(
                    ingestion_date          DATE    PRIMARY KEY       NOT NULL,
                    product_name            VARCHAR                   NOT NULL,
                    most_ordered_day        DATE                      NOT NULL,
                    is_public_holiday       BOOL                      NOT NULL,
                    tt_review_points        INT                       NOT NULL,
                    pct_one_star_review     FLOAT                     NOT NULL,
                    pct_two_star_review     FLOAT                     NOT NULL,
                    pct_three_star_review   FLOAT                     NOT NULL,
                    pct_four_star_review    FLOAT                     NOT NULL,
                    pct_five_star_review    FLOAT                     NOT NULL,
                    pct_early_shipment      FLOAT                     NOT NULL,
                    pct_late_shipment       FLOAT                     NOT NULL
                    );

                """)
            cur.commit()

            cur.execute("""
                WITH CTE_1 AS (
                Select 
                a.review,
                a.product_id,
                b.product_category,
                b.product_name,
                c.order_id,
                c.order_date,
                d.calendar_dt,
                d.day_of_the_week_num,
                d.working_day,
                e.shipment_id,
                e.shipment_date,
                e.delivery_date
                FROM 
                yazejibi6672_staging.reviews a
                JOIN if_common.dim_products b ON a.product_id = b.product_id
                JOIN yazejibi6672_staging.orders c ON b.product_id = c.product_id::INT
                JOIN if_common.dim_dates d ON c.order_date = d.calendar_dt
                JOIN yazejibi6672_staging.shipment_deliveries e ON c.order_id = e.order_id
                ),

                CTE_2 AS(
                select 
                AVG(review) as average,
                SUM(review) as tt_review_points,
                product_name,
                product_id
                from CTE_1
                group by product_id, product_name
                order by average DESC limit 1
                ),
            
                CTE_3 AS (
                select 
                order_date as order_date,
                count(order_id) as total,
                CASE WHEN working_day = false AND day_of_the_week_num IN (1,2,3,4,5) THEN true ELSE false END AS is_public_holiday,
                product_id
                from CTE_1 
                group by order_date, product_id, working_day, day_of_the_week_num
                ),
            
                CTA_4 AS (
                select distinct review,
                CTE_1.product_id as product_id,
                CASE WHEN review = 1  THEN count(review)*100/sum(count(*)) over() ELSE 0  END AS pct_one_star_review,
                CASE WHEN review = 2  THEN count(review)*100/sum(count(*)) over() ELSE 0  END AS pct_two_star_review,
                CASE WHEN review = 3  THEN count(review)*100/sum(count(*)) over() ELSE 0  END AS pct_three_star_review,
                CASE WHEN review = 4  THEN count(review)*100/sum(count(*)) over() ELSE 0  END AS pct_four_star_review,
                CASE WHEN review = 5  THEN count(review)*100/sum(count(*)) over() ELSE 0  END AS pct_five_star_review
                from CTE_1
                LEFT JOIN CTE_2 ON CTE_1.product_id = CTE_2.product_id
                where CTE_2.product_id = CTE_1.product_id
                group by CTE_1.product_id, review
                ),  
            
            
                CTE_5 AS (
                select
                CTE_1.product_id,
                CASE WHEN shipment_date - order_date >= 6 AND delivery_date is null THEN count(shipment_id)*100/sum(count(*)) over() ELSE 0 END AS pct_late_shipment,
                CASE WHEN shipment_date - order_date <= 6 AND delivery_date is not null THEN count(shipment_id)*100/sum(count(*)) over() ELSE 0 END AS pct_early_shipment
                from 
                CTE_1 
                LEFT JOIN CTE_2 ON CTE_1.product_id = CTE_2.product_id
                where CTE_2.product_id = CTE_1.product_id
                group by CTE_1.product_id, shipment_date, order_date, delivery_date
                ),
            
                CTE_6 AS (
                select product_id, 
                sum(pct_late_shipment) as pct_late_shipment ,
                sum(pct_early_shipment) as pct_early_shipment
                from CTE_5 
                group by product_id
                ),

                CTE_FINAL AS (
                select
                -- a.average,
                CURRENT_TIMESTAMP as ingestion_date,
                a.product_name,
                b.order_date as most_ordered_day,
                b.is_public_holiday,
                a.tt_review_points,
                SUM(c.pct_one_star_review) as pct_one_star_review,
                SUM(c.pct_two_star_review) as pct_two_star_review,
                SUM(c.pct_three_star_review)as pct_three_star_review,
                SUM(c.pct_four_star_review) as pct_four_star_review,
                SUM(c.pct_five_star_review)as pct_five_star_review,
                d.pct_early_shipment as pct_early_shipment,
                d.pct_late_shipment as pct_late_shipment
                from CTE_2 a
                LEFT JOIN CTE_3 b ON a.product_id = b.product_id
                LEFT JOIN CTA_4 c ON b.product_id = c.product_id
                LEFT JOIN CTE_6 d ON c.product_id = d.product_id    
                group by a.average,a.product_name, b.order_date, b.is_public_holiday, a.tt_review_points, b.total, d.pct_late_shipment, d.pct_early_shipment
                order by b.total desc
                limit 1
                )

              
                INSERT INTO yazejibi6672_analytics.best_performing_product SELECT * FROM CTE_FINAL
                """)

            print("Table best_performing_product transformed and created successfully")
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
                COPY yazejibi6672_analytics.best_performing_product TO STDIN WITH (FORMAT CSV, HEADER, DELIMITER ',')
                """, file)
            resource.Object(bucket, 'analytics_export/yazejibi6672/best_performing_product.csv').put(Body=file.getvalue())

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