import logging

logger = logging.getLogger(__name__)
table = "moex_api"

def insert_rows(cur,conn,schema,row):

    try:

        if schema == 'staging':

            cur.execute(
                f"""INSERT INTO {schema}.{table}("security_name", "open", "close", "high", "low", "value", "volume", "begin_timestamp", "end_timestamp")
                VALUES (%(security_name)s, %(open)s, %(close)s, %(high)s, %(low)s, %(value)s, %(volume)s, %(begin)s, %(end)s)
                ON CONFLICT DO NOTHING;
                """, row
            )

        else:
            cur.execute(
                f"""INSERT INTO {schema}.{table}("security_name", "open", "close", "high", "low", "value", "volume", "begin_timestamp", "end_timestamp")
                VALUES (%(security_name)s, %(open)s, %(close)s, %(high)s, %(low)s, %(value)s, %(volume)s, %(begin_timestamp)s, %(end_timestamp)s)
                ON CONFLICT DO NOTHING;
                """, row
            )

        conn.commit()

        logger.info(f"Inserted row") 

    except Exception as e:
        logger.error(f"Error inserting row with IDs")
        raise e

def update_rows(cur,conn,schema,row):

    try:

        if schema == 'staging':
            security_name = 'security_name'
            open_ = 'open'
            close = 'close'
            high = 'high'
            low = 'low'
            value = 'value'
            volume = 'volume'
            begin_timestamp = 'begin_timestamp'
            end_timestamp = 'end_timestamp'
        else:
            security_name = 'security_name'
            open_ = 'open'
            close = 'close'
            high = 'high'
            low = 'low'
            value = 'value'
            volume = 'volume'
            begin_timestamp = 'begin_timestamp'
            end_timestamp = 'end_timestamp'

        cur.execute(f"""
            UPDATE {schema}.{table}
            SET "open" = %({_open})s
            WHERE "security_name" = %({security_name})s AND "begin_timestamp" = %({begin_timestamp})s
            """, row)

    except Exception as e:
        logger.error(f"Error updating rows with IDs: {row[security_name]}")
        raise e

def delete_rows(cur,conn,schema,keys_to_delete):

    try:

        keys_to_delete = f"""({', '.join(f"'{key}'" for key in keys_to_delete)})"""

        cur.execute(
            f"""
            DELETE FROM {schema}.{table}
            WHERE "security_name" IN {keys_to_delete};
            """
        )

        conn.commit()
        logger.info(f"Delete rows with IDs: {keys_to_delete}")

    except Exception as e:
        logger.error(f"Error deleting rows with IDs: {keys_to_delete} - {e}")
        raise e
