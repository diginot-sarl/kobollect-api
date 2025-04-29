import pyodbc
import os

# Set environment variables (for testing, these would be set in Docker)
# os.environ['MSSQL_SERVER'] = '195.26.250.58'
# os.environ['MSSQL_DB'] = 'eRecensement'
# os.environ['MSSQL_USER'] = 'sa'
# os.environ['MSSQL_PASSWORD'] = 'hids++--@@987654321#]*'

def get_mssql_connection():
    conn_str = (
        f"DRIVER={{ODBC Driver 17 for SQL Server}};"
        f"SERVER={os.getenv('MSSQL_SERVER')};"
        f"DATABASE={os.getenv('MSSQL_DB')};"
        f"UID={os.getenv('MSSQL_USER')};"
        f"PWD={os.getenv('MSSQL_PASSWORD')}"
    )
    return pyodbc.connect(conn_str)

try:
    conn = get_mssql_connection()
    print("Connection successful!")
    conn.close()
except Exception as e:
    print(f"Connection failed: {e}")