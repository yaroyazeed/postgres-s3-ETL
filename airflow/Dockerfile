FROM apache/airflow:2.2.0

COPY requirements.txt .

RUN pip install -r requirements.txt \
	&& pip install psycopg2-binary