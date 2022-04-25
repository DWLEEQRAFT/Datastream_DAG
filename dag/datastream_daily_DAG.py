from airflow.operators.bash import BashOperator
from airflow.providers.docker.operators.docker import DockerOperator
from airflow.operators.python import PythonOperator
from airflow import DAG
import pendulum
from datetime import timedelta, datetime

__TASK__ = "daily_fred_dag"

KST = pendulum.timezone("Asia/Seoul")
image = "qraftaxe/fred_dag"  # qraftaxe/fred_dag

default_args = {
    'owner': 'airflow',
    'retries': 3,
    "retry_delay": timedelta(minutes=5),
    # "on_failure_callback": send_message
}

with DAG(
        dag_id=__TASK__,
        default_args=default_args,
        description=__TASK__,
        schedule_interval="@daily",
        start_date=datetime(2022, 4, 13, tzinfo=KST),
        tags=[__TASK__],
) as dag:
    run_batch = DockerOperator(
        task_id='Datastream_Batch_Bigquery',
        image='qraftaxe/fred_dag',
        container_name=image,
        api_version='auto',
        auto_remove=True,
        command="python -m datastream_batch",
        docker_url="unix://var/run/docker.sock",
        network_mode="bridge"
    )



    # Sample task
    t1 = BashOperator(task_id='print_date',
                      bash_command='date',
                      dag=dag)

    t1 >> run_batch
