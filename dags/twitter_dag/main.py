from airflow.models.dag import dag
from airflow.operators.python import task
from airflow.utils.dates import days_ago

from dotenv import load_dotenv
import json
import os
import pandas as pd
import tweepy

@dag(
    schedule_interval=None,
    start_date=days_ago(1),
    catchup=False
)
def twitter_etl():
    dag_path = "/opt/airflow/dags/twitter_dag"

    @task()
    def fetch() -> str:
        """
        Fetch tweets from Twitter API and save on disk as JSON.
        """
        load_dotenv()
        bearer_token=os.getenv("TWITTER_BEARER_TOKEN")
        client = tweepy.Client(bearer_token)

        tweets = client.search_recent_tweets("Shopify", tweet_fields=["lang", "public_metrics"], max_results=100)
        tweets_path = f"{dag_path}/tweets.json"
        out_file = open(tweets_path, 'w')
        tweets_json = [tweet.data for tweet in tweets.data]
        json.dump(tweets_json, out_file, indent=4)
        return tweets_path

    @task()
    def count_per_language(path: str) -> str:
        """
        Count the number of tweets per language.
        Save csv on disk and return file's path.
        """
        output_directory = f"{dag_path}/per_language"
        output_path = f"{output_directory}/per_language.csv"

        df = pd.read_json(path)
        per_language = df.groupby('lang').size()

        if not os.path.exists(output_directory):
            os.makedirs(output_directory)
        per_language.to_csv(output_path)

        return output_path

    @task()
    def shopify_retweets(path: str):
        """
        Count the number of total retweets of the hashtag "Shopify"
        Save csv on disk and return file's path.
        """
        output_directory = f"{dag_path}/retweets"
        output_path = f"{output_directory}/retweets.csv"

        df = pd.read_json(path)
        total_retweets = df.loc[df.text.str.contains('#shopify', case=False)] \
                         ["public_metrics"] \
                         .map(lambda o: o["retweet_count"]) \
                         .sum()

        if not os.path.exists(output_directory):
            os.makedirs(output_directory)
        df = pd.DataFrame([total_retweets], columns=["total_retweets"])
        df.to_csv(output_path)

        return output_path

    @task()
    def all_hashtags(path: str):
        """
        Find all hashtags in the tweets.
        Save csv on disk and return file's path.
        """
        output_directory = f"{dag_path}/hashtags"
        output_path = f"{output_directory}/hashtags.csv"

        df = pd.read_json(path)
        nested_hashtags = df.text.str.findall(r'#.*?(?=\s|$)').values.reshape(-1,).tolist()
        
        hashtags = []
        for x in nested_hashtags:
            hashtags += x

        if not os.path.exists(output_directory):
            os.makedirs(output_directory)
        df = pd.DataFrame(hashtags)
        df.to_csv(output_path)

    tweets_path = fetch()

    count_per_language(tweets_path)
    shopify_retweets(tweets_path)
    all_hashtags(tweets_path)

twitter_dag = twitter_etl()
