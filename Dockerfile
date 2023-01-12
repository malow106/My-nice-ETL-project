FROM apache/airflow:2.4.3
COPY requirements.txt /requirements.txt
RUN pip install --user --upgrade pip
RUN pip install --no-cache-dir --user -r /requirements.txt
# ENV AIRFLOW_CONN_PG_MY_DB: $(.env)
