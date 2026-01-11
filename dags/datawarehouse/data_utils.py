from airflow.providers.postgres.hooks.postgres import PostgresHook
from psycopg2.extras import RealDictCursor

table = "moex_api"

def get_conn_cursor():
    hook = PostgresHook(postgres_conn_id="postgres_db_moex_elt", database="elt_db")
    conn = hook.get_conn()
    cur = conn.cursor(cursor_factory=RealDictCursor)
    return conn, cur

def close_conn_cursor(conn,cur):
    cur.close()
    conn.close()

def create_schema(schema):
    conn, cur = get_conn_cursor()
    schema_sql = f"CREATE SCHEMA IF NOT EXISTS {schema};"

    cur.execute(schema_sql)
    conn.commit()

    close_conn_cursor(conn,cur)

def create_table(schema):
    conn, cur = get_conn_cursor()

    if schema == 'staging':
        table_sql = f"""
            CREATE TABLE IF NOT EXISTS {schema}.{table} (
                "security_name" varchar(10) not null,
                "open" numeric(21,5),
                "close" numeric(21,5),
                "high" numeric(21,5),
                "low" numeric(21,5),
                "value" numeric(21,0),
                "volume" numeric(21,0),
                "begin_timestamp" timestamp not null,
                "end_timestamp" timestamp not null,
                primary key ("security_name", "begin_timestamp")
            );"""
    else:
        table_sql = f"""
            CREATE TABLE IF NOT EXISTS {schema}.{table} (
                "security_name" varchar(10) not null,
                "open" numeric(21,5),
                "close" numeric(21,5),
                "high" numeric(21,5),
                "low" numeric(21,5),
                "value" numeric(21,0),
                "volume" numeric(21,0),
                "begin_timestamp" timestamptz not null,
                "end_timestamp" timestamptz not null,
                create_timestamp timestamptz not null default now(),
                last_update_timestamp timestamptz not null default now(),
                primary key ("security_name", "begin_timestamp")
            );"""

    cur.execute(table_sql)
    conn.commit()
    close_conn_cursor(conn,cur)
