# Import necessary libraries
import pyodbc
import sqlalchemy
import pandas as pd
from sqlalchemy import create_engine
from sqlalchemy.engine.url import URL
import logging
from datetime import datetime

# Define the database connection string
DATABASE_CONNECTION_STRING = (
    "Driver={ODBC Driver 17 for SQL Server};"
    "Server=your_server_name;"
    "Database=your_database_name;"
    "Trusted_Connection=yes;"
)

# Set up logging
log_filename = f"log_{datetime.now().strftime('%Y%m%d_%H%M%S')}.txt"
logging.basicConfig(filename=log_filename, level=logging.INFO, 
                    format='%(asctime)s - %(levelname)s - %(message)s')

# Also set up logging to print to console
console_handler = logging.StreamHandler()
console_handler.setLevel(logging.INFO)
console_handler.setFormatter(logging.Formatter('%(asctime)s - %(levelname)s - %(message)s'))
logging.getLogger().addHandler(console_handler)

# Log the start of the script
logging.info("Script started.")

# Define functions for executing SQL queries, uploading data, and retrieving data

def execute_sql(query):
    """
    Execute SQL query using a hardcoded connection string.

    Parameters:
        query (str): SQL query to execute.

    Returns:
        None
    """
    try:
        logging.info("Attempting to connect to the database for executing SQL.")
        conn = pyodbc.connect(DATABASE_CONNECTION_STRING)
        cursor = conn.cursor()
        logging.info(f"Executing query: {query}")
        cursor.execute(query)
        conn.commit()
        logging.info("SQL query executed successfully.")
    except Exception as e:
        logging.error(f"Error executing query: {e}")
        print(f"Error executing query: {e}")
    finally:
        if cursor:
            cursor.close()
        conn.close()
        logging.info("Database connection closed after executing SQL.")

def upload_data(table, dataframe, upload_type):
    """
    Upload data to a specified table in the database.

    Parameters:
        table (str): Name of the table to upload data.
        dataframe (DataFrame): Pandas DataFrame containing data to upload.
        upload_type (str): Method of upload ('replace', 'append', etc.).

    Returns:
        None
    """
    try:
        logging.info("Attempting to connect to the database for uploading data.")
        engine = create_engine(f"mssql+pyodbc:///?odbc_connect={DATABASE_CONNECTION_STRING}")
        logging.info(f"Uploading data to table: {table}")
        dataframe.to_sql(table, engine, index=False, if_exists=upload_type, schema="dbo", chunksize=10000)
        logging.info(f"Data uploaded successfully to {table}.")
    except Exception as e:
        logging.error(f"Error uploading data: {e}")
        print(f"Error uploading data: {e}")

def retrieve_data(query):
    """
    Retrieve data from the database using SQL query.

    Parameters:
        query (str): SQL query to retrieve data.

    Returns:
        DataFrame: Pandas DataFrame containing retrieved data.
    """
    try:
        logging.info("Attempting to connect to the database for retrieving data.")
        engine = create_engine(f"mssql+pyodbc:///?odbc_connect={DATABASE_CONNECTION_STRING}")
        logging.info(f"Retrieving data with query: {query}")
        df = pd.read_sql(query, engine)
        logging.info("Data retrieved successfully.")
    except Exception as e:
        logging.error(f"Error retrieving data: {e}")
        print(f"Error retrieving data: {e}")
        df = pd.DataFrame()  # Return empty DataFrame in case of error
    return df

# Example usage
# Example SQL query for data retrieval
retrieval_query = """
SELECT *
FROM dbo.ExampleTable
WHERE some_condition = 'value'
"""
# Retrieve data from the database
try:
    data_frame = retrieve_data(retrieval_query)
    logging.info("Data retrieved successfully.")
    print("Data retrieved successfully:")
    print(data_frame.head())  # Display the first few rows of the retrieved data
except Exception as e:
    logging.error(f"Failed to retrieve data: {e}")
    print(f"Failed to retrieve data: {e}")

# Example data for uploading
data_to_upload = pd.DataFrame({
    'Column1': [1, 2, 3, 4],
    'Column2': ['A', 'B', 'C', 'D']
})

# Specify the table name and the upload type
table_name = "dbo.ExampleUploadTable"
upload_type = "append"  # Options: 'replace', 'append'

# Upload data to the database
try:
    upload_data(table_name, data_to_upload, upload_type)
    logging.info("Data uploaded successfully.")
    print("Data uploaded successfully.")
except Exception as e:
    logging.error(f"Failed to upload data: {e}")
    print(f"Failed to upload data: {e}")

# Example SQL query for data manipulation
update_query = """
UPDATE dbo.ExampleTable
SET Column1 = 'Updated value'
WHERE some_condition = 'value'
"""
# Execute an SQL query
try:
    execute_sql(update_query)
    logging.info("SQL query executed successfully.")
    print("SQL query executed successfully.")
except Exception as e:
    logging.error(f"Failed to execute SQL query: {e}")
    print(f"Failed to execute SQL query: {e}")

# Log the end of the script
logging.info("Script ended.")
