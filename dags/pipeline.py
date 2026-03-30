from airflow import DAG
from airflow.operators.bash import BashOperator
from datetime import datetime

with DAG(
    dag_id='datastream_pipeline',
    start_date=(2026,3,30),
    schedule_interval=None,
    catchup=False
) as dag:
    #Task 1: Create  transaction events using Faker
    task1 = BashOperator(
        task_id='generate_transaction_events_data',
        bash_command='python /opt/airflow/scripts/producers/transaction_events_producer.py --bootstrap-servers localhost:9094 --topic transaction_events --interval 2.0'
    )

    #Task 2: Create user events using Faker
    task2 = BashOperator(
        task_id='generate_user_events_data',
        bash_command='python /opt/airflow/scripts/producers/user_events_producer.py --bootstrap-servers localhost:9094 --topic user_events --interval 1.0'
    )
    #Task 3: Ingest from producers to landing zone
    task3 = BashOperator(
        task_id='read_events_from_topics',
        bash_command='python /opt/airflow/jobs/ingest_kafka_to_landing.py'
    )

task1 >> task2 >> task3