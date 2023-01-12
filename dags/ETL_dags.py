from airflow import DAG
from airflow.operators.python import PythonOperator
from datetime import timedelta, datetime
# from airflow.utils.dates import days_ago
from faker import Faker
# import faker
import pandas as pd
import random
from sqlalchemy import create_engine
from sqlalchemy_utils import database_exists, create_database
import pandas as pd


def create_db():

    engine_dwh = create_engine('postgresql://root:root@postgresdb:5432/DWH')

    if not database_exists(engine_dwh.url):
        create_database(engine_dwh.url)

    engine_sas = create_engine('postgresql://root:root@postgresdb:5432/SAS')

    if not database_exists(engine_sas.url):
        create_database(engine_sas.url)

    # print(database_exists(engine.url))


def extract_to_sas():

    # Connect to the SAS database
    engine_sas = create_engine('postgresql://root:root@postgresdb:5432/SAS')

    # Create a dataframe from faker data
    df_faker = pd.DataFrame(create_rows_faker(random.randint(10, 100)))

    # Insert the dataframe into the 'users' table
    df_faker.to_sql('raw_data_people', engine_sas, if_exists='append')


def create_rows_faker(num=1):

    fake = Faker("fr_FR")

    output = [{
        "id": fake.bban(),
        "Nom": fake.last_name(),
        "Prenom": fake.first_name(),
        "Age": random.randint(18, 63),
        "Date_recensement": fake.date_of_birth(),
        "Married": random.choice([True, False]),
        "Dep_naissance":fake.department_name(),
        "Region":fake.region(),
        "Living_location":fake.local_latlng(country_code='FR', coords_only=True),
        "Job":fake.job()}
        for x in range(num)]
    return output


def transform_load():

    engine_dwh = create_engine('postgresql://root:root@postgresdb:5432/DWH')
    engine_sas = create_engine('postgresql://root:root@postgresdb:5432/SAS')

    df = pd.read_sql("SELECT * FROM raw_data_people", engine_sas)

    df_person = df[["index", "Nom", "Prenom", "Age", "Job", "Married"]]
    df_location = df[["index", "Date_recensement", "Dep_naissance", "Region"]]

    df_person.to_sql('person', engine_dwh, if_exists='append')
    df_location.to_sql('location', engine_dwh, if_exists='append')


default_args = {
    'owner': 'Malo PARIS',
    # 'start_date': datetime(2023, 1, 1),
    # 'end_date': datetime(),
    'depends_on_past': False,
    'retries': 1,
    'retry_delay': timedelta(minutes=5),
}


dag_ETL = DAG(
    dag_id="ETL_Project_v1",
    default_args=default_args,
    schedule_interval='*/5 * * * *',
    # schedule_interval='@once',
    dagrun_timeout=timedelta(minutes=5),
    description='Airflow ETL project',
    start_date=datetime(2023, 1, 11),
    catchup=False)


python_task_create_db = PythonOperator(
    task_id='Create-database', python_callable=create_db, dag=dag_ETL)

python_task_extract = PythonOperator(
    task_id='extract-raw-data', python_callable=extract_to_sas, dag=dag_ETL)

python_task_load = PythonOperator(
    task_id='transform-load-dwh', python_callable=transform_load, dag=dag_ETL)

python_task_create_db >> python_task_extract >> python_task_load
