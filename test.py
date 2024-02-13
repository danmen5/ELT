from snowflakeRunner import snowflakeRunner

snowflakeConnector = snowflakeRunner(
    account='mfb94343.us-east-1',
    user='DANMEN5',
    password='Efatnas10',
    warehouse='COMPUTE_WH',
    database='PROJECTS',
    schema='BRONZE'
    )

snowflakeConnector.connect()

query_file_path = 'TableExists.sql' 

x=snowflakeConnector.execute_query_from_file(query_file_path)

print(x)

snowflakeConnector.disconnect()
