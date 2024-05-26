# Simplified template for SQL queries and data uploading

# Import necessary libraries
import pyodbc
import sqlalchemy
import pandas as pd

# Define functions for executing SQL queries and uploading data

def execute_sql(query, server, database):
    """
    Execute SQL query on specified server and database.

    Parameters:
        query (str): SQL query to execute.
        server (str): Server name/address.
        database (str): Name of the database.

    Returns:
        None
    """
    try:
        # Connect to database
        conn = pyodbc.connect(f'Driver={{SQL Server Native Client 11.0}};Server={server};Database={database};Trusted_Connection=yes;')
        
        # Create cursor
        cursor = conn.cursor()
        
        # Execute query
        cursor.execute(query)
        
        # Commit changes
        conn.commit()
        
    except Exception as e:
        print(f"Error executing query: {e}")
    finally:
        # Close cursor and connection
        cursor.close()
        conn.close()

def upload_data(server, database, table, dataframe, upload_type):
    """
    Upload data to specified table in the database.

    Parameters:
        server (str): Server name/address.
        database (str): Name of the database.
        table (str): Name of the table to upload data.
        dataframe (DataFrame): Pandas DataFrame containing data to upload.
        upload_type (str): Method of upload ('replace', 'append', etc.).

    Returns:
        None
    """
    try:
        # Create SQLAlchemy engine
        engine = sqlalchemy.create_engine(f'mssql+pyodbc://@{server}/{database}?driver=SQL+Server+Native+Client+11.0')

        # Upload data to table
        dataframe.to_sql(table, engine, index=False, if_exists=upload_type, schema="dbo", chunksize=10000)
        
    except Exception as e:
        print(f"Error uploading data: {e}")

def retrieve_data(query, server, database):
    """
    Retrieve data from specified server and database using SQL query.

    Parameters:
        query (str): SQL query to retrieve data.
        server (str): Server name/address.
        database (str): Name of the database.

    Returns:
        DataFrame: Pandas DataFrame containing retrieved data.
    """
    try:
        # Connect to database
        conn = pyodbc.connect(f'Driver={{SQL Server Native Client 11.0}};Server={server};Database={database};Trusted_Connection=yes;')
        
        # Read data using SQL query
        df = pd.read_sql(query, conn)
        
    except Exception as e:
        print(f"Error retrieving data: {e}")
        df = pd.DataFrame()  # Return empty DataFrame in case of error
    finally:
        # Close connection
        conn.close()
    
    return df

# Example usage:
# Define parameters
server = "your_server_name"
database = "your_database_name"

####################################################################################################################
# Example data for upload
# Here we create a sample DataFrame 'sample_df' to demonstrate how to upload data to the database.
# Replace the values in the DataFrame with your actual data.
sample_df = pd.DataFrame({"column1": [1, 2, 3], "column2": ["a", "b", "c"]})

# Example usage of upload_data function
# This line demonstrates how to use the 'upload_data' function to upload the 'sample_df' DataFrame to the specified table in the database.
# Ensure to replace 'server' and 'database' with your server and database names respectively.
# Replace 'Stg.sample_df' with the actual table name where you want to upload the data.
# The 'replace' parameter indicates that if the table already exists, it will be replaced with the new data.
upload_data(server, database, 'Stg.sample_df', sample_df, 'replace')

####################################################################################################################
# Example SQL query
# Here we provide an example of an UPDATE SQL query.
# Replace 'YOUR_TABLE' with the name of the table you want to update and specify the columns and values accordingly.
sql_query = """
UPDATE YOUR_TABLE
SET column1 = value1, column2 = value2
WHERE condition;
"""

# Example usage of execute_sql function
# This line demonstrates how to execute an SQL query using the 'execute_sql' function.
# Replace 'server' and 'database' with your server and database names respectively.
execute_sql(sql_query, server, database)

####################################################################################################################
# Example query
# This example demonstrates how to retrieve data from the database after uploading it.
# Replace 'YOUR_DATABASE' with your database name and 'Stg.sample_df' with the actual table name.
query = """
SELECT *
    FROM [YOUR_DATABASE].[dbo].[Stg.sample_df]
"""

# Retrieve data
# This line retrieves the data from the specified table using the 'retrieve_data' function.
# Replace 'server' and 'database' with your server and database names respectively.
df = retrieve_data(query, server, database)

# Display retrieved data
# This line prints the first few rows of the retrieved DataFrame.
print(df.head())

####################################################################################################################
# Note: This script is for development purposes only. 
# Please ensure that credentials are not hardcoded into production code 
# that is to be committed to Git repositories. We would be using environment variables 
# or secure credential management systems for production deployments.
