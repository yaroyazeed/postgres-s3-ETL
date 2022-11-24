import psycopg2
import sys
import boto3
import os
from botocore import UNSIGNED
from botocore.client import Config

ENDPOINT="d2b-internal-assessment-dwh.cxeuj0ektqdz.eu-central-1.rds.amazonaws.com"
PORT="5432"
USER="yazejibi6672"
REGION="eu-central-1"
DBNAME="d2b_assessment"
PASSWORD="3aXYi4XPVw"

try:
    conn = psycopg2.connect(host=ENDPOINT, port=PORT, database=DBNAME, user=USER, password=PASSWORD, sslrootcert="SSLCERTIFICATE")
    cur = conn.cursor()
    #Create Tables
    cur.execute("""
        CREATE TABLE yazejibi6672_staging.orders
        (order_id INT PRIMARY KEY NOT NULL,
             customer_id     INT         NOT NULL,
             order_date      DATE        NOT NULL,
             product_id      VARCHAR     NOT NULL,
             unit_price      INT         NOT NULL,
             quantity        INT         NOT NULL,
             amount          INT         NOT NULL
             );
        """)
    # query_results = cur.fetchall()
    # print(query_results)
    print("Table ORDERS created successfully")
    conn.commit()

    cur.execute("""
        CREATE TABLE yazejibi6672_staging.reviews
        (review INT NOT NULL,
            product_id     INT         NOT NULL
             );
        """)
    print("Table REVIEWS created successfully")
    conn.commit()

    cur.execute("""
        CREATE TABLE yazejibi6672_staging.shipments_deliveries
        (shipment_id        INT    PRIMARY KEY     NOT NULL,
            order_id        INT                    NOT NULL,
            shipment_date   DATE                   NULL,
            delivery_date   DATE                   NULL            
             );
        """)
    print("Table SHIPMENT DELIVERIES created successfully")
    conn.commit()
    conn.close()
except Exception as e:
    print("Database connection failed due to {}".format(e))                
                