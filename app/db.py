import pyodbc
import os

def get_mssql_connection():
    conn_str = (
        f"DRIVER={{ODBC Driver 17 for SQL Server}};"
        f"SERVER={os.getenv('MSSQL_SERVER')};"
        f"DATABASE={os.getenv('MSSQL_DB')};"
        f"UID={os.getenv('MSSQL_USER')};"
        f"PWD={os.getenv('MSSQL_PASSWORD')}"
    )
    return pyodbc.connect(conn_str)
