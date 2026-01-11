from airflow import DAG
from airflow.models import Variable

import pendulum
from datetime import datetime, timedelta
from api.moex_stats import get_securities_info, save_json_data
from datawarehouse.dwh import staging_table, core_table

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

with DAG(
        dag_id='security_info',
        default_args=default_args,
        description='DAG to get security info',
        schedule='0 14 * * *',
        catchup=False
) as dag:
    security_info = get_securities_info(Variable.get('security_name'), Variable.get('date_begin'), Variable.get('date_end'))
    json_data = save_json_data(security_info)

    security_info >> json_data

with DAG(
        dag_id='update_db',
        default_args=default_args,
        description='Update db info',
        schedule='0 15 * * *',
        catchup=False
) as dag:
    update_staging = staging_table()
    update_core = core_table()

    update_staging >> update_core
