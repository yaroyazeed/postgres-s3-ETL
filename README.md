## About this project
This is an ETL pipeline orchestrated using airflow to run once. `Docker-compose.yaml`, `Dockerfile` and `requirements.txt` files have been added to help replicating this application easier.

### Extract Transform Load
This involves
1. Running the `create_tables.py` script in the `scripts` folder to create the tables for raw data to be loaded into.
2. Turn on/Trigger each airflow DAG to import raw data into the tables created in step 1 above.
3. Each airflow DAG has a transformation task in it for each of the transformations requored.
4. Each airflow DAG has a load task in it to export each of the transformed data to a csv file on AWS s3.

This entire ETL process is orchestrated by the airflow setup on Docker.

#### Prerequisites
1. Docker


##### Steps to recreate

1. Clone this repo.
2. Go into airflow folder `cd etl-assessment/airflow`
3. run `docker-compose up`
4. visit [localhost:8080](http://localhost:8080/)
5. from the list of DAGs trigger turn on `orders_ETL`, `shipments_ETL`, `reviews_ETL`
