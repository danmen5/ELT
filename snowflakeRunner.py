import snowflake.connector
import os

#Class that creates the connection with snowflake 
class snowflakeRunner:
    #initialices attributes
    def __init__(self, account, user, password, warehouse, database, schema):
        self.account = account
        self.user = user
        self.password = password
        self.warehouse = warehouse
        self.database = database
        self.schema = schema
        self.connection = None
        self.cursor = None

    #Creates the conection with the desired 
    def connect(self):
        try:
            self.connection = snowflake.connector.connect(
                user=self.user,
                password=self.password,
                account=self.account,
                warehouse=self.warehouse,
                database=self.database,
                schema=self.schema
            )
            self.cursor = self.connection.cursor()
            print("Connected to Snowflake successfully.")
        except Exception as e:
            print("Error connecting to Snowflake:", e)

    def disconnect(self):
        try:
            if self.cursor:
                self.cursor.close()
            if self.connection:
                self.connection.close()
            print("Disconnected from Snowflake.")
        except Exception as e:
            print("Error disconnecting from Snowflake:", e)

    def execute_query_from_file(self, file_path):
        try:
            if not os.path.isfile(file_path):
                print(f"File '{file_path}' not found.")
                return
            
            # Check if the connection is established
            if not self.connection:
                print("Error: Connection to Snowflake is not established.")
                return
            
            # Check if cursor is initialized
            if not self.cursor:
                self.cursor = self.connection.cursor()

            with open(file_path, 'r') as file:
                sql_query = file.read()
            
            self.cursor.execute(sql_query)
            print("Query executed successfully.")
        except Exception as e:
            print("Error executing query:", e)

# Example usage:
if __name__ == "__main__":
    # Initialize SnowflakeQueryExecutor
    snowflakeRunner = snowflakeRunner(
        account='mfb94343.us-east-1',
        user='DANMEN5',
        password='Efatnas10',
        warehouse='COMPUTE_WH',
        database='PROJECTS',
        schema='BRONZE'
    )

    # Connect to Snowflake
    snowflakeRunner.connect()

    # Execute SQL query from file
    query_file_path = 'TableExists.sql'  # Path to your SQL query file
    snowflakeRunner.execute_query_from_file(query_file_path)

    # Disconnect from Snowflake
    snowflakeRunner.disconnect()
