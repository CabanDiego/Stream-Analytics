'''DAG for ingesting data from kafka producers'''

from airflow import DAG
from airflow.operators.bash import BashOperator
from datetime import datetime

with DAG(
    dag_id="kafka_landing_ingest",
    start_date=datetime(2026,3,30),
    schedule_interval='*/1 * * * *',  
    catchup=False
)as dag:
    
    #Task to run ingest consumer
    ingest_task = BashOperator(
        task_id='ingest_kafka_to_landing',
        bash_command=('python /opt/airflow/jobs/ingest_kafka_to_landing.py '
         '--topics transaction_events,user_events --duration 40 '
          '--output ./data/landing')
    )
ingest_task