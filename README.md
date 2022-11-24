## About this project
This is an ETL pipeline orchestrated using airflow to run once. `Docker-compose.yaml`, `Dockerfile` and `requirements.txt` files have been added to help replicating this application easier.

### Prerequisites
1. Docker


#### Steps to recreate

1. Clone this repo.
2. Go into airflow folder `cd etl-assessment/airflow`
3. run `docker-compose up`
4. visit [localhost:8080](http://localhost:8080/)
5. from the list of DAGs trigger turn on `orders_ETL`, `shipments_ETL`, `reviews_ETL`
