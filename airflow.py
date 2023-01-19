from airflow.models import DAG
from airflow.operators.bash_operator import BashOperator
from datetime import datetime
from datetime import timedelta

#sets default configurations
default_args = {
    'owner': 'Jayed',
    'depends_on_past': False,
    'email': ['jayed@live.co.uk'],
    'email_on_failure': False,
    'email_on_retry': False,
    'retries': 1,
    'start_date': datetime(2020, 1, 1),
    'retry_delay': timedelta(minutes=5),
    'end_date': datetime(2022, 1, 1),
  
}
#creates DAG, scheduled to run once a day at midnight
with DAG(dag_id='batch_consumer_dag',
         default_args=default_args,
         schedule_interval='0 0  * * *',
         catchup=False,
         tags=['batch_process']
         ) as dag:
    #runs the batch_consumer file. In if name == main block only has the functions dumping and cleaning the data

    run_producer_script = BashOperator(
        task_id='run_producer_file',
        bash_command='cd ~/pinterest_project && python3 batch_consumer.py',
        dag=dag)