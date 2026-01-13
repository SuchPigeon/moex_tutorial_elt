import requests
import pytest
import psycopg2

def test_moex_api_response(airflow_variable):
    security_name = airflow_variable("security_name")
    date_begin = airflow_variable("date_begin")
    date_end = airflow_variable("date_end")

    url = f'https://iss.moex.com/iss/engines/stock/markets/shares/securities/{security_name}/candles.json?from={date_begin}&till={date_end}&interval=24&start=0'

    try:
        response = requests.get(url)
        assert response.status_code == 200
    except:
        pytest.fail(f"Request to MOEX api failed: {e}")

def test_real_postgres_connection(real_postgres_connection):
    cursor = None

    try:
        cursor = real_postgres_connection.cursor()
        cursor.execute("SELECT 1;")
        result = cursor.fetchone()

        assert result[0] == 1

    except psycopg2.Error as e:
       pytest.fail(f"Database query failed: {e}")

    finally:
        if cursor is not None:
            cursor.close()
