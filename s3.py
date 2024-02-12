import boto3

class S3Uploader:
    #Builds attributes for the class that connects to s3
    def __init__(self, aws_access_key_id, aws_secret_access_key, bucket_name):
        self.aws_access_key_id = aws_access_key_id
        self.aws_secret_access_key = aws_secret_access_key
        self.bucket_name = bucket_name

        # Create S3 client
        self.s3_client = boto3.client('s3',
                                      aws_access_key_id=self.aws_access_key_id,
                                      aws_secret_access_key=self.aws_secret_access_key)

    #uploads the file to s3
    def upload_file(self, file_path, object_name=None):
        """Upload a file to S3 bucket."""
        if object_name is None:
            object_name = file_path.split('/')[-1]

        try:
            self.s3_client.upload_file(file_path, self.bucket_name, object_name)
            print(f"File '{file_path}' uploaded successfully to S3 bucket '{self.bucket_name}' as '{object_name}'.")
        except Exception as e:
            print(f"Error uploading file '{file_path}' to S3 bucket '{self.bucket_name}': {e}")

# Example usage:
# Replace 'your_access_key_id', 'your_secret_access_key', and 'your_bucket_name' with your actual AWS credentials and bucket name
access_key_id='AKIA3FLDXHL3GHXHBOH3'
secret_access_key='R7367oJO5yaOQkBCSUHWz6FgtKcR4n8mTxagzn8m'
bucket_name='snowflake-danmen'
uploader = S3Uploader(access_key_id, secret_access_key, bucket_name)
# Assuming readTweet generates a CSV file called 'tweets.csv'
tweet_file_path = 'tweets.csv'
uploader.upload_file(tweet_file_path)
