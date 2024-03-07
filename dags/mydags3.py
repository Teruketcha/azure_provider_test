
import os
from datetime import datetime, timedelta
from typing import cast

from airflow.models import DAG
from airflow.models.xcom_arg import XComArg

# Ignore missing args provided by default_args
# mypy: disable-error-code="call-arg"

try:
    from airflow.operators.empty import EmptyOperator
except ModuleNotFoundError:
    from airflow.operators.dummy import DummyOperator as EmptyOperator  # type: ignore

from airflow.utils.edgemodifier import Label

from airflow.operators.python import PythonOperator
from airflow.operators.bash import BashOperator
from airflow.exceptions import AirflowException

def echo_pip_list():

    print('run dummy python')



with DAG(
    dag_id='test-normal-python-task',
    start_date=datetime(2021, 8, 13),
    schedule="@once",
    catchup=False,
    default_args={
        "retries": 0,
        "retry_delay": timedelta(minutes=3),
        "azure_data_factory_conn_id": "azure_data_factory",
        "factory_name": "sktdatalake-azure-adf-jihwan",  # This can also be specified in the ADF connection.
        "resource_group_name": "rg.datalake.dev.datafactory",  # This can also be specified in the ADF connection.
    },
    default_view="graph",
) as dag:
    begin = EmptyOperator(task_id="begin")

    end = EmptyOperator(task_id="end")


    # [START howto_operator_adf_run_pipeline]
    run_bash_func = BashOperator(task_id='print_pip_list_with_bash', bash_command='pip list')
    run_bash_check_disk = BashOperator(task_id='run_bash_check_disk', bash_command='df -h')
    run_bash_check_mem = BashOperator(task_id='run_bash_check_mem', bash_command='free -h')
    run_bash_check_dir = BashOperator(task_id='run_bash_check_dir', bash_command='pwd')
    run_python_func = PythonOperator(task_id="print_the_context", python_callable=echo_pip_list)
    # [END howto_operator_adf_run_pipeline]

    begin >> run_bash_func >> run_python_func >> end