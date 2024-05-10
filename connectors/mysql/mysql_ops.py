import mysql.connector
from mysql.connector import Error

def connect_to_mysql(username, password, host, database):
    """Connect to a MySQL database"""
    print(f"{username}")
    print(f"{password}")
    print(f"{host}")
    print(f"{database}")
    try:
        conn = mysql.connector.connect(
            user=username,
            password=password,
            host=host,
            database=database
        )
        return conn
    except Error as e:
        print(f"Error connecting to MySQL: {e}")
        return None

def perform_query(connection, query):
    """Execute a SQL query and return the results"""
    cursor = connection.cursor(dictionary=True)
    try:
        cursor.execute(query)
        result = cursor.fetchall()
        return result, None
    except Error as e:
        return None, str(e)
    finally:
        cursor.close()

def close_connection(connection):
    """Close the MySQL connection"""
    if connection is not None:
        connection.close()

def get_tables_and_columns(connection, database):
    """Retrieve tables and column metadata from MySQL"""
    query = f"""
    SELECT TABLE_NAME, COLUMN_NAME, DATA_TYPE
    FROM INFORMATION_SCHEMA.COLUMNS
    WHERE TABLE_SCHEMA = '{database}'
    ORDER BY TABLE_NAME, ORDINAL_POSITION;
    """
    cursor = connection.cursor(dictionary=True)
    try:
        cursor.execute(query)
        rows = cursor.fetchall()
        tables = {}
        for row in rows:
            table_name = row['TABLE_NAME']
            column_name = row['COLUMN_NAME']
            data_type = row['DATA_TYPE']
            if table_name not in tables:
                tables[table_name] = []
            tables[table_name].append({'name': column_name, 'datatype': data_type})
        return tables, None
    except Error as e:
        return None, str(e)
    finally:
        cursor.close()
