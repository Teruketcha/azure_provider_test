
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

from airflow.providers.microsoft.azure.operators.data_factory import AzureDataFactoryRunPipelineOperator
from airflow.providers.microsoft.azure.sensors.data_factory import AzureDataFactoryPipelineRunStatusSensor
from airflow.utils.edgemodifier import Label

with DAG(
    dag_id='mydag1',
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
    run_pipeline1 = AzureDataFactoryRunPipelineOperator(
        task_id="run_pipeline1",
        pipeline_name="pipeline1",
    )
    # [END howto_operator_adf_run_pipeline]

    begin >> run_pipeline1 >> end