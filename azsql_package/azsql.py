from msal import ConfidentialClientApplication;
import os;
import pyodbc;
import struct;
import time;
import logging;
import pandas as pd;

# Load environment variables

SERVER=os.environ["SERVER"];
DATABASE=os.environ["DATABASE"];
TENANT_ID=os.environ["TENANT_ID"];
CLIENT_ID=os.environ["NEXAIR_APP_CLIENT_ID"];
CLIENT_CREDENTIAL=os.environ["NEXAIR_APP_CLIENT_SECRET"];

# Set up logging
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)


# Class to handle SQL connections and queries
class AzSql (object):
    '''
    Class to handle SQL connections and queries using the pyodbc library.\n
    params:\n
    <b>server</b>: str, the server name\n
    <b>database</b>: str, the database name\n
    If no server or database is provided, the class will use the environment variables.\n
    '''
    
    # Initialize the class with the server and database
    def __init__(self, server=SERVER, database=DATABASE) -> None:
        '''
        This method initializes the class with the server and database parameters.\n
        params:\n
        <b>server</b>: str, the server name\n
        <b>database</b>: str, the database name\n
        '''
        self.server = server;
        self.database = database;


    # Configure the SQL connection string
    def config_sql(self) -> tuple:
        '''
        This method configures the SQL connection string using the MSAL library.\n
        No Params
        '''
        try:
            creds = ConfidentialClientApplication(
                client_id=CLIENT_ID, 
                authority=f'https://login.microsoftonline.com/{TENANT_ID}',
                client_credential=CLIENT_CREDENTIAL
            );
            token = creds.acquire_token_for_client(scopes=['https://database.windows.net//.default']);
            tokenb = bytes(token['access_token'], 'UTF-8');
            exptoken = b''.join(bytes({i}) + bytes(1) for i in tokenb)
            tokenstruct = struct.pack('=i', len(exptoken)) + exptoken;
            SQL_COPT_SS_ACCESS_TOKEN = 1256;
            connString = 'DRIVER={ODBC Driver 18 for SQL Server};' \
                        + f'SERVER={self.server};' \
                        + f'DATABASE={self.database};'\
                        + 'ENCRYPT=NO';
            return connString, tokenstruct, SQL_COPT_SS_ACCESS_TOKEN;
        except Exception as e:
            logger.error(f"Error configuring SQL connection: {e}");


    # Create a cursor to execute queries against the database
    def create_cursor(self, max_retries=3, delay=5) -> tuple:
        '''
        This method creates a cursor to execute queries against the database.\n
        params:\n
        <b>max_retries</b>: int, number of times to retry the connection\n
        <b>delay</b>: int, time to wait between retries\n
        '''
        connString, tokenstruct, SQL_COPT_SS_ACCESS_TOKEN = self.config_sql();
        attempt = 0;
        for attempt in range(max_retries):
            try:
                conn = pyodbc.connect(connString, attrs_before={SQL_COPT_SS_ACCESS_TOKEN: tokenstruct})
                cursor = conn.cursor()
                return conn, cursor
            except pyodbc.OperationalError as e:
                if "Login timeout expired" in str(e):
                    logger.warning(f"Attempt {attempt + 1} failed, retrying in {delay} seconds...")
                    time.sleep(delay)
                else:
                    logger.error(f"Operational error: {e}")
                    raise
        logger.error("Maximum retry attempts reached")
        raise pyodbc.OperationalError("Maximum retry attempts reached")

    # Perform a single step database operation (no transaction)
    def perform_db_operation(self, query, data_values=None, has_return=False, needs_commit=False) -> tuple:
        '''
        This method performs a database operation for a single step operation.\n
        params:\n
        <b>query</b>: str, the SQL query to execute\n
        <b>data_values</b>: list or tuple, the values to pass to the query\n
        <b>has_return</b>: bool, whether the operation returns a result\n
        <b>needs_commit</b>: bool, whether the operation needs to be committed\n
        '''
        try:
            conn, cursor = self.create_cursor();
            if data_values:
                if isinstance(data_values, list) and all(isinstance(i, tuple) for i in data_values):
                    cursor.executemany(query, data_values)
                else:
                    cursor.execute(query, data_values)
            else:
                cursor.execute(query);
            
            if has_return:
                result = cursor.fetchall()
                column_names = [column[0] for column in cursor.description]
                if needs_commit:
                    conn.commit();
                return result, column_names
            else:
                conn.commit();
                return "Success", "Operation successful";
        except Exception as e:
            logger.error(f"Error performing query: {e}")
            return "Error", f"Error performing query: {e}";
        finally:
            if 'cursor' in locals() and cursor is not None:
                cursor.close()
            if 'conn' in locals() and conn is not None:
                conn.close()
        
    # Perform a database operation for multiple steps in a transaction   
    def perform_atomic_db_operation(self, query, data_values=None, has_return=False, close_cursor=True, close_conn=True, conn=None, cursor=None, needs_commit=True) -> tuple:
        '''
        This method performs a database operation for multiple steps in a transaction.\n
        params:\n
        <b>query</b>: str, the SQL query to execute\n
        <b>data_values</b>: list or tuple, the values to pass to the query\n
        <b>has_return</b>: bool, whether the operation returns a result\n
        <b>close_cursor</b>: bool, whether to close the cursor\n
        <b>close_conn</b>: bool, whether to close the connection\n
        <b>conn</b>: pyodbc connection, the connection object\n
        <b>cursor</b>: pyodbc cursor, the cursor object\n
        <b>needs_commit</b>: bool, whether the operation needs to be committed\n        
        '''
        try:
            # Create connection and cursor if not provided
            if conn is None or cursor is None:
                conn, cursor = self.create_cursor()

            # Execute the query
            if data_values:
                if isinstance(data_values, list) and all(isinstance(i, tuple) for i in data_values):
                    cursor.executemany(query, data_values)
                else:
                    cursor.execute(query, data_values)
            else:
                cursor.execute(query)

            # Fetch results if needed
            if has_return:
                result = cursor.fetchall()
                column_names = [column[0] for column in cursor.description]
                if not close_cursor and not close_conn:
                    return result, column_names, conn, cursor
                return result, column_names

            if not close_cursor and not close_conn:
                return "Success", conn, cursor
            return "Success"

        except Exception as e:
            # Rollback the transaction on failure
            conn.rollback()
            logger.error(f"Error in atomic operation: {e}")
            raise e

        finally:
            if needs_commit:
                # Commit the transaction
                print("Committing transaction")
                conn.commit()
                
            # Manage cursor and connection lifecycle
            if close_cursor and 'cursor' in locals() and cursor is not None:
                cursor.close()
            if close_conn and 'conn' in locals() and conn is not None:
                conn.close()

    # Create a DataFrame from the cursor return  
    def create_data_frame(self, cursor_return, column_names) -> pd.DataFrame:
        '''
        This method creates a DataFrame from the cursor return and column names.\n
        params:\n
        <b>cursor_return</b>: list, the return from the cursor\n
        <b>column_names</b>: list, the column names\n
        '''
        try:
            df = pd.DataFrame.from_records(cursor_return, columns=column_names)
            return df
        except Exception as e:
            logger.error(f"Error creating DataFrame: {e}")
            raise