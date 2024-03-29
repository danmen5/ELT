import tweepy
import csv

class readTweet:
    #Creates attrributes fo the twitter conection
    def __init__(self, consumer_key, consumer_secret, access_token, access_token_secret):
        self.consumer_key = consumer_key
        self.consumer_secret = consumer_secret
        self.access_token = access_token
        self.access_token_secret = access_token_secret
        
        # Authenticate with Twitter
        self.auth = tweepy.OAuth1UserHandler(consumer_key, consumer_secret, access_token, access_token_secret)
        self.api = tweepy.API(self.auth)

    #Reads the tweets between specific dates    
    def read_tweets(self, start_date, end_date, count=10):
        """Read tweets from the user's timeline between specified dates"""
        try:
            # Retrieve tweets from the user's timeline between specified dates
            tweets = tweepy.Cursor(self.api.home_timeline, since=start_date, until=end_date).items(count)
            tweet_data = [[tweet.created_at, tweet.text] for tweet in tweets]
            
            # Save tweets to a CSV file
            with open('tweets.csv', 'w', newline='', encoding='utf-8') as csvfile:
                writer = csv.writer(csvfile)
                writer.writerow(['CREATED_AT', 'TWEET'])
                writer.writerows(tweet_data)
            
            print("Tweets retrieved and saved to 'tweets.csv' successfully.")
        except tweepy.TweepyException as e:
            print("Error: Unable to retrieve tweets -", e)