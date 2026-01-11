
from datawarehouse.data_utils import get_conn_cursor, close_conn_cursor, create_schema, create_table
from datawarehouse.data_loading import load_data
from datawarehouse.data_modification import insert_rows, update_rows, delete_rows
#from datawarehouse.data_transformation import transform_data

import logging
from airflow.decorators import task

logger = logging.getLogger(__name__)
table = "moex_api"

@task
def staging_table():
    schema = 'staging'

    conn, cur = None, None

    try:
        conn, cur = get_conn_cursor()

        moex_data = load_data()

        create_schema(schema)
        create_table(schema)

        for row in moex_data:
            insert_rows(cur,conn,schema,row)

        logger.info(f"{schema} table update completed")

    except Exception as e:
        logger.error(f"An error occurred during the update of {schema} table {e}")
        raise e

    finally:
        if conn and cur:
            close_conn_cursor(conn, cur)

@task
def core_table():
    schema = 'core'

    conn, cur = None, None

    try:
        conn, cur = get_conn_cursor()

        create_schema(schema)
        create_table(schema)

        current_moex_keys = set()

        cur.execute(f"SELECT * FROM staging.{table};")
        rows = cur.fetchall()

        for row in rows:
            current_moex_keys.add(row["security_name"])
            insert_rows(cur, conn, schema, row)

        logger.info(f"{schema} table update completed")

    except Exception as e:
        logger.error(f"An error occurred during the update of {schema} table {e}")
        raise e
    
    finally:
        if conn and cur:
            close_conn_cursor(conn, cur)
