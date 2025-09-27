from airflow import DAG
from airflow.operators.python import PythonOperator
from datetime import datetime, timedelta

def python_function1():
    print('HELLO WORLD')  # caps to see it easier in log

def location_and_date(location, today):
    print(f'We are in {location} and today is {today}.')

def python_function2():
    print('OH DEAR, WHAT A SPLENDID APPLICATION!')

default_args = {
    'owner': 'airflow',
    'start_date': datetime(2024, 6, 4),
    'retries': 3,
    'retry_delay': timedelta(minutes=3)
}

with DAG(
    'exercise_python_operator',
    default_args = default_args,
    schedule_interval = '@daily'
) as dag:
    start = PythonOperator(
        task_id = 'start',
        python_callable = python_function1
    )
    location_and_date = PythonOperator(
        task_id = 'location_and_date',
        python_callable = location_and_date,
        op_kwargs = {'location': 'Stockholm', 'today': datetime.now().date()}
    )
    end = PythonOperator(
        task_id = 'end',
        python_callable = python_function2
    )

    start >> location_and_date >> end