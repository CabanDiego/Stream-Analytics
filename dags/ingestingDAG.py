'''DAG for ingesting data from kafka producers'''

from airflow import DAG
from airflow.operators.bash import BashOperator
from datetime import datetime

start_date = datetime.today()

with DAG(
    dag_id="kafka_landing_ingest",
    start_date=datetime(start_date),
    schedule_interval='*/3 * * * *',  
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