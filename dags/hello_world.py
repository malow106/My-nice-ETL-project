from datetime import datetime
from airflow import DAG
from airflow.operators.python import PythonOperator
from datetime import datetime, timedelta


default_args = {
    'owner': 'Malo PARIS',
    'retries': 5,
    'retry_delay': timedelta(minutes=5)
}


def print_hello():

    from sqlalchemy import create_engine
    import pandas as pd

    # Connect to the database
    engine = create_engine('postgresql://root:root@postgresdb:5432/mydb')

    # Create a dataframe
    data = {'name': ['John', 'Jane', 'Mike'], 'age': [28, 34, 40]}
    df = pd.DataFrame(data)

    # Insert the dataframe into the 'users' table
    df.to_sql('users', engine, if_exists='append')
    return 'Hello world from first Airflow DAG!'


dag = DAG('hello_world', description='Hello World DAG',
          schedule_interval='*/1 * * * *',
          start_date=datetime(2022, 1, 1), catchup=False)

hello_operator = PythonOperator(
    task_id='hello_task', python_callable=print_hello, dag=dag)
