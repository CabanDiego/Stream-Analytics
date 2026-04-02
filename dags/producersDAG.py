'''DAG for producing data with Faker'''

from airflow import DAG
from airflow.operators.bash import BashOperator
from datetime import datetime

start_date = datetime.today()

with DAG(
    dag_id='data_producers',
    start_date=(start_date),
    schedule_interval=None,
    catchup=False
) as dag:
    #Task 1: Create  transaction events using Faker
    task1 = BashOperator(
    task_id='generate_transaction_events_data',
    bash_command='python /opt/airflow/scripts/producers/transaction_events_producer.py --bootstrap-servers kafka:9092 --topic transaction_events --interval 2.0 --count 10'
)
    #Task 2: Create user events using Faker
    task2 = BashOperator(
    task_id='generate_user_events_data',
    bash_command='python /opt/airflow/scripts/producers/user_events_producer.py --bootstrap-servers kafka:9092 --topic user_events --interval 1.0 --count 10'
)

