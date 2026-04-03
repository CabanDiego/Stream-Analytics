'''DAG for ingesting data from kafka producers'''

from airflow import DAG
from airflow.operators.bash import BashOperator
from datetime import datetime

start_date = datetime.today()

with DAG(
    dag_id="ingest_transform_stream_pipeline",
    start_date=(start_date),
    schedule_interval='*/3 * * * *',  
    catchup=False
)as dag:
    
    #Task to run ingest consumer
    ingest_task = BashOperator(
        task_id='ingest_kafka_to_landing',
        bash_command=('python /opt/spark-jobs/ingest_kafka_to_landing.py '
         '--topics transaction_events,user_events --duration 40 '
          '--output /opt/spark-data/landing')
    )

    #Task to run spark script
    transform_data_task = BashOperator(
        task_id='transform_ingested_data',
        bash_command=(
            'spark-submit --master local[*] /opt/airflow/scripts/transforming_data.py')
    )

    #Task to refresh/get data using streamlit from the gold layer
    streamlit_task = BashOperator(
        task_id='streamlit_data_refresh',
        bash_command='touch /opt/airflow/scripts/app.py'
    )


ingest_task >> transform_data_task >> streamlit_task