from airflow import DAG
import pendulum
from datetime import datetime, timedelta
from api.moex_stats import get_securities_info, save_json_data

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
    security_info = get_securities_info('SBER', '2025-01-09')
    json_data = save_json_data(security_info)

    security_info >> json_data
