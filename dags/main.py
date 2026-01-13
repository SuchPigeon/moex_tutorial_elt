from airflow import DAG
from airflow.models import Variable

import pendulum
from datetime import datetime, timedelta
from airflow.operators.trigger_dagrun import TriggerDagRunOperator
from api.moex_stats import get_securities_info, save_json_data
from datawarehouse.dwh import staging_table, core_table
from dataquality.soda import moex_elt_data_quality

local_tz = pendulum.timezone("Europe/Moscow")

default_args = {
        "owner": "airflow",
        "depends_on_past": False,
        "email_on_failure": False,
        "email_on_retry": False,
        "max_active_runs": 1,
        "dagrun_timeout": timedelta(hours=1),
        "start_date": datetime(2025, 1, 1, tzinfo=local_tz)
}

staging_schema = "staging"
core_schema = "core"

# Dag step 1: security_info
with DAG(
        dag_id='security_info',
        default_args=default_args,
        description='DAG to get security info',
        schedule='0 14 * * *',
        catchup=False
) as dag_security:
    security_info = get_securities_info(Variable.get('security_name'), Variable.get('date_begin'), Variable.get('date_end'))
    json_data = save_json_data(security_info)

    trigger_update_db = TriggerDagRunOperator(
        task_id="trigger_update_db",
        trigger_dag_id="update_db"
    )

    security_info >> json_data >> trigger_update_db


# Dag step 2: update_db
with DAG(
        dag_id='update_db',
        default_args=default_args,
        description='Update db info',
        catchup=False
) as dag:
    update_staging = staging_table()
    update_core = core_table()

    trigger_data_quality = TriggerDagRunOperator(
        task_id="trigger_data_quality",
        trigger_dag_id="data_quality"
    )

    update_staging >> update_core >> trigger_data_quality


# Dag step 3: data_quality
with DAG(
        dag_id='data_quality',
        default_args=default_args,
        description='DAG to check the data quality on both layers in the db',
        catchup=False
) as dag:
    soda_validate_staging = moex_elt_data_quality(staging_schema)
    soda_validate_core    = moex_elt_data_quality(core_schema)

    soda_validate_staging >> soda_validate_core
