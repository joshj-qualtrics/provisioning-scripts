import pandas as pd
# Requires 'psycopg2' to be installed in the environment for actual database connection.
import psycopg2 
from typing import Optional, Dict, Any

class DataWarehouse:
    """
    A utility class to manage connections and data retrieval from an Amazon Redshift data warehouse.
    
    This class is configured for production use, expecting credentials (user, password, host, etc.) 
    to be passed in, typically sourced from secure environment variables.
    """
    
    def __init__(self, user: str, password: str, connection: str, host: Optional[str] = None, port: int = 5439, database: str = 'warehouse'):
        """
        Initializes the DataWarehouse connection parameters.
        
        Parameters:
            user (str): Database username (required).
            password (str): Database password (required).
            connection (str): Connection type (defaults to 'psycopg2').
            host (str): Redshift cluster host address (required, defaults to None).
            port (int): Redshift cluster port.
            database (str): Redshift database name.
        """
        self.user = user
        self.password = password
        # Use 'psycopg2' as a fallback if the environment variable returns None for connection type
        self.connection_type = connection if connection is not None else 'psycopg2'
        self.host = host
        self.port = port
        self.database = database

        # CRITICAL VALIDATION: Check for required parameters before proceeding
        missing_params = []
        if not self.user:
            missing_params.append('user (DB_USER)')
        if not self.password:
            missing_params.append('password (DB_PASSWORD)')
        if not self.host:
            # If host is missing or is the default 'redshift.example.com' (which you had issues with)
            missing_params.append('host (DB_HOST)')

        if missing_params:
            raise ValueError(
                f"Configuration Error: The following required database connection parameters were not provided (likely missing from .env or environment variables): {', '.join(missing_params)}"
            )
        
        # This is the expected connection string format for psycopg2:
        self.conn_details = f"dbname={self.database} user={self.user} password={self.password} host={self.host} port={self.port}"
        
        print(f"DataWarehouse connection details configured (Host: {self.host}, User: {self.user})")


    def _get_db_connection(self):
        """
        Establishes a connection to the Redshift database.
        
        Returns:
            psycopg2.connection: A live database connection object.
        """
        if self.connection_type == 'psycopg2':
            # Attempt to connect using the configured details
            return psycopg2.connect(self.conn_details)
        else:
            # This should now only happen if the connection_type was set to an unsupported value.
            raise ValueError(f"Unsupported connection type: {self.connection_type}. Only 'psycopg2' is supported.")


    def get_pandas_df(self, query: str) -> pd.DataFrame:
        """
        Executes a SQL query and returns the results as a pandas DataFrame.
        
        Parameters:
            query (str): The SQL query to execute.
            
        Returns:
            pd.DataFrame: DataFrame containing query results.
        """
        conn = None
        try:
            print("--- Establishing Redshift Connection and Executing Query ---")
            conn = self._get_db_connection()
            # pandas.read_sql handles the connection cursor and fetching data efficiently
            df = pd.read_sql(query, conn)
            print(f"Query executed successfully. Fetched {len(df)} records.")
            return df
        except Exception as e:
            # Provide more context on database failure
            print(f"CRITICAL DATABASE ERROR during query execution: {e}")
            raise
        finally:
            if conn:
                conn.close()
                print("Database connection closed.")
