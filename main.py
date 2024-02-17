from s3 import S3Uploader
from snowflakeRunner import snowflakeRunner
from readTweet import readTweet
from datetime import date
import os
def main():
    try:
        #Creates the connection with tweeter
        consumer_key=os.getenv('TwitterKEY')
        consumer_key_secret=os.getenv('TwitterSECRETKEY')
        access_token=os.getenv('TwitterTOKEN')
        access_token_secret=os.getenv('TwitterTOKENSECRET')
        reader = readTweet(consumer_key, consumer_key_secret, access_token, access_token_secret)
    except:
        pass

    #Creates the connection with s3
    access_key_id=os.getenv('S3KEY')
    secret_access_key=os.getenv('S3ACCESSKEY')
    bucket_name=os.getenv('BUCKET')
    uploader = S3Uploader(access_key_id, secret_access_key, bucket_name)

    #Creates the connection with snowflake
    snowflakeConnector = snowflakeRunner(
        account=os.getenv('SFACC'),
        user=os.getenv('SFUSER'),
        password=os.getenv('SFPW'),
        warehouse=os.getenv('SFWH'),
        database=os.getenv('SFDB'),
        schema=os.getenv('SFSC')
    )

    # Connect to Snowflake
    snowflakeConnector.connect()

    # Execute SQL query from file
    query_file_path = 'maxDate.sql'
    #Builds the start date to query tweets from the last tweet  
    start_date=snowflakeConnector.execute_query_from_file(query_file_path)
    #Builds today's date as end date
    end_date=date.today()

    # Disconnect from Snowflake
    snowflakeConnector.disconnect()

    #Extracts the tweets
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
