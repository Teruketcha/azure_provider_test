
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
from airflow.exceptions import AirflowException

def sempy_test():
    try:
        import sempy.fabric as fabric
    except ModuleNotFoundError:
        raise AirflowException('sempy fabric not found')
    
    client = fabric.FabricRestClient()

    ## url params
    workspace_id = '5a7fbbee-6458-4f57-8565-134d9966bee1'
    artifact_id = 'd11237a3-9a78-4063-ac99-2b3bd9a9c98b' #notebook
    ## body params
    lakehose_name = 'target_lh'
    workspace_pool_name = 'StarterPool'

    url = f'v1/workspaces/{workspace_id}/items/{artifact_id}/jobs/instances?jobType=RunNotebook'

    body = {
        "executionData": {
            # "parameters": {
            #     "parameterName": {
            #         "value": "new value",
            #         "type": "string"
            #     }
            # },
            "configuration": {
                # "conf": {
                #     "spark.conf1": "value"
                # },
                "defaultLakehouse": {
                    "name": lakehose_name
                    # "id": "<(optional) lakehouse-id>",
                    # "workspaceId": "<(optional) workspace-id-that-contains-the-lakehouse>"
                },
                "useStarterPool": False,
                "useWorkspacePool": workspace_pool_name
            }
        }
    }

    print(url)
    print(body)

    res = client.post(url, json=body)

    print(res.text)


with DAG(
    dag_id='test-sempy-lib',
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
    run_python_func = PythonOperator(task_id="print_the_context", python_callable=sempy_test)
    # [END howto_operator_adf_run_pipeline]

    begin >> run_python_func >> end