BEGIN 
-- Creates the stage with s3 in case it doesn't exist
CREATE OR REPLACE STAGE PROJECTS.STAGES.S3
  URL = 's3://snowflake-danmen'  -- Replace with the URL of your S3 bucket and path
  CREDENTIALS = (AWS_KEY_ID='AKIA3FLDXHL3GHXHBOH3' AWS_SECRET_KEY='R7367oJO5yaOQkBCSUHWz6FgtKcR4n8mTxagzn8m');

-- checks if the table exists. If it does, it inserts the data from s3. If it doesn't, it creates the table and then inserts
IF (EXISTS(SELECT * FROM PROJECTS.INFORMATION_SCHEMA.TABLES WHERE TABLE_NAME='TWEETS'))
THEN 
  COPY INTO PROJECTS.BRONZE.TWEETS
  FROM @PROJECTS.STAGES.S3/tweets.csv  -- Replace with the path to your CSV file in your stage
  FILE_FORMAT = (TYPE = 'CSV' SKIP_HEADER = 1);
  RETURN 'INSERTED';
ELSE 
    CREATE TABLE PROJECTS.BRONZE.TWEETS (
    created_at datetime,
    tweet VARCHAR(300)
  );
  COPY INTO PROJECTS.BRONZE.TWEETS
  FROM @PROJECTS.STAGES.S3/tweets.csv  -- Replace with the path to your CSV file in your stage
  FILE_FORMAT = (TYPE = 'CSV' SKIP_HEADER = 1);
  RETURN 'CREATED';
END IF;
END;
