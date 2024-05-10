from snowflake import connector
import logging

def connect_to_snowflake(user, password, account, warehouse, database, schema):
    try:
        conn = connector.connect(
            user=user,
            password=password,
            account=account,
            warehouse=warehouse,
            database=database,
            schema=schema
        )
        return conn
    except Exception as e:
        logging.error(f"Failed to connect to Snowflake: {e}")
        return None

def perform_query(connection, query):
    cursor = connection.cursor()
    try:
        cursor.execute(query)
        return cursor.fetchall(), None
    except Exception as e:
        return None, str(e)
    finally:
        cursor.close()

def close_connection(connection):
    if connection is not None:
        try:
            connection.close()
        except Exception as e:
            logging.error(f"Failed to close connection: {e}")

def get_tables_and_columns(connection, schema, database):
    query = f"""
    SELECT TABLE_NAME, COLUMN_NAME, DATA_TYPE
    FROM {database}.INFORMATION_SCHEMA.COLUMNS
    WHERE TABLE_SCHEMA = '{schema}'
    ORDER BY TABLE_NAME, ORDINAL_POSITION;
    """
    cursor = connection.cursor()
    try:
        cursor.execute(query)
        rows = cursor.fetchall()
        tables = {}
        for row in rows:
            table_name, column_name, data_type = row
            if table_name not in tables:
                tables[table_name] = []
            tables[table_name].append({'name': column_name, 'datatype': data_type})
        return tables, None
    except Exception as e:
        return None, str(e)
    finally:
        cursor.close()
