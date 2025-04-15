from airflow import DAG
from datetime import datetime , timedelta
from airflow.operators.python import PythonOperator
from airflow.providers.google.cloud.operators.dataproc import DataprocSubmitJobOperator
from airflow.operators.bash import BashOperator
from airflow.models.param import Param

REGION = "us-central1"
PROJECT_ID = "inbound-bee-455509-t1"
CLUSTER_NAME = "cluster-386d"
PYSPARK_CODE_PATH = "gs://us-central1-airflow-project-986178bb-bucket/pyspark-code/spark-code-orders.py"

default_args = {
    'owner':'airflow',
    'depends_on_past':False,
    'email_on_failure':False,
    'retries':2,
    'retry_delay':timedelta(minutes=5)
}

JOB_DETAILS = {
    'placement':{"cluster_name" : CLUSTER_NAME},
    'pyspark_job':{
        "main_python_file_uri":PYSPARK_CODE_PATH,
        "args": ["--date={{ ti.xcom_pull(task_ids='execution_date_setting') }}"]
    }
}


dag = DAG(
    'Job_receiving_args',
    start_date=datetime(2025 , 4 , 13),
    description="This receives a date and process the file of that date",
    default_args=default_args,
    catchup=False,
    schedule_interval=None,
    params={
        'execution_date':Param(default = 'NA' , type = "string" , description = "Date on which you want the fiel to be processed for")
    }
    )

# This date is current execution date without dashes
def execution_date_setter(date , **kwargs):
    execution_date = kwargs['params'].get('execution_date' , 'NA')
    if execution_date == 'NA':
        execution_date = date
    return execution_date

p1 = PythonOperator(
    task_id = 'execution_date_setting',
    python_callable= execution_date_setter,
    provide_context = True,
    op_kwargs={'date' : '{{ ds_nodash }}'},
    dag = dag
)
     
p2 = DataprocSubmitJobOperator(
    task_id = 'submit_spark_job_on_cluster',
    job =  {
    'placement':{"cluster_name" : CLUSTER_NAME},
    'pyspark_job':{
        "main_python_file_uri":PYSPARK_CODE_PATH,
        "args": ["--date={{ ti.xcom_pull(task_ids='execution_date_setting') }}"]
    }
},
    region = REGION,
    project_id = PROJECT_ID,
    dag = dag
)

p1 >> p2