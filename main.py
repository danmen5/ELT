from s3 import S3Uploader
from snowflakeRunner import snowflakeRunner
from readTweet import readTweet
def main():
    try:
        #Creates the connection with tweeter
        consumer_key='******'
        consumer_key_secret='*******'
        access_token='*********'
        access_token_secret='********'
        reader = readTweet(consumer_key, consumer_key_secret, access_token, access_token_secret)
    except:
        pass

    #Creates the connection with s3
    access_key_id='AKIA3FLDXHL3GHXHBOH3'
    secret_access_key='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'
    bucket_name='snowflake-danmen'
    uploader = S3Uploader(access_key_id, secret_access_key, bucket_name)

    #Creates the connection with snowflake
    snowflakeConnector = snowflakeRunner(
        account='mfb94343.us-east-1',
        user='DANMEN5',
        password='Efatnas10',
        warehouse='COMPUTE_WH',
        database='PROJECTS',
        schema='BRONZE'
    )

    # Connect to Snowflake
    snowflakeConnector.connect()

    # Execute SQL query from file
    query_file_path = 'maxDate.sql'  
    start_date=snowflakeConnector.execute_query_from_file(query_file_path)
    print(start_date)

    # Disconnect from Snowflake
    snowflakeConnector.disconnect()

    #Extracts the tweets
    start_date = "2024-01-01"
    end_date = "2024-01-31"
    try:
        reader.read_tweets(start_date, end_date, count=5)
    except:
        pass

    #Uploads the tweets to s3
    tweet_file_path = 'tweets.csv'
    uploader.upload_file(tweet_file_path) 

    #Copies the tweet file into snowflake
    query_file_path = 'TableExists.sql'
    snowflakeConnector.execute_query_from_file(query_file_path)
    snowflakeConnector.disconnect()
     
if __name__=='__main__':
    main()
