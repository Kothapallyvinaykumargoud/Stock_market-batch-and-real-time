import os
from airflow import DAG
from airflow.operators.python_operator import PythonOperator
from datetime import datetime, timedelta
from airflow.operators.bash_operator import BashOperator
from airflow.hooks.base import BaseHook
from scripts.batchdata import main as main_batchdata


defaultargs={
    'owner': 'vinay',
    'depends_on_past':False,
    'email_on_failure': False,
    'email_on_retry': False,
    'retries':1,
    'retry_delay':timedelta(minutes=1)
}

dag=DAG(
    "stock_batch_data",
    default_args=defaultargs,
    description="A simple stock market data batch DAG",
    schedule_interval="@daily",
    start_date=datetime(2025,12,27),
    catchup=False
)

fetch_hostorical_data=BashOperator(
    task_id="fetch_historical_data",
    bash_command="""
    python /opt/airflow/dags/scripts/batchstream.py
    """,
    dag=dag

)

def run_batchdata():
    # Load AWS credentials from Airflow Connection
    conn = BaseHook.get_connection('aws_s3')
    os.environ['AWS_ACCESS_KEY_ID'] = conn.login
    os.environ['AWS_SECRET_ACCESS_KEY'] = conn.password
    os.environ['AWS_DEFAULT_REGION'] = conn.extra_dejson.get('region_name', 'us-east-1')

    # Set the S3 bucket environment variable
    os.environ['s3_BUCKET'] = "stock-market-2025"
    main_batchdata()

consume_historical_data = PythonOperator(
    task_id="consume_historical_data",
    python_callable=run_batchdata,
    dag=dag
)


    
# consume_historical_data=BashOperator(
#     task_id="consume_historical_data",
#     bash_command="""
#     export s3_BUCKET=stock-market-2025
#     python /opt/airflow/dags/scripts/batchdata.py
#     """,
#     dag=dag
# )

fetch_hostorical_data>>consume_historical_data
