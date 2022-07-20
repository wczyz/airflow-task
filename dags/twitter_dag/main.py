from airflow.models.dag import dag
from airflow.decorators import task
from airflow.utils.dates import days_ago

from dotenv import load_dotenv
import json
import os
import pandas as pd
import psycopg2
import tweepy

@dag(
    schedule_interval=None,
    start_date=days_ago(1),
    catchup=False
)
def twitter_etl():
    dag_path = "/opt/airflow/dags/twitter_dag"

    @task
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

    @task
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

    @task
    def shopify_retweets(path: str) -> str:
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
        df = pd.DataFrame([total_retweets], columns=["retweets"])
        df.to_csv(output_path, index=False)

        return output_path

    @task
    def all_hashtags(path: str) -> str:
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
        return output_path

    def table_exists(db_cursor, table_name: str) -> bool:
        db_cursor.execute("SELECT EXISTS(SELECT * FROM information_schema.tables WHERE table_name=%s)", (table_name,))
        return db_cursor.fetchone()[0]

    @task
    def load_per_language(path: str, db_cursor):
        """
        Load csv with per language information into Postgres database.
        """
        contents = open(path, 'r')
        table_name = "per_language"
        if not table_exists(db_cursor, table_name):
            db_cursor.execute(f"CREATE TABLE {table_name} (lang varchar PRIMARY KEY, count integer);")
        else:
            db_cursor.execute(f"TRUNCATE {table_name}")
        db_cursor.copy_from(contents, table_name, columns=("lang", "count"), sep=",")

    @task
    def load_retweets(path: str, db_cursor):
        """
        Load csv with shopify retweets count into Postgres database.
        """
        contents = open(path, 'r')
        table_name = "shopify_retweets"
        if not table_exists(db_cursor, table_name):
            db_cursor.execute(f"CREATE TABLE {table_name} (retweets integer PRIMARY KEY);")
        else:
            db_cursor.execute(f"TRUNCATE {table_name}")
        db_cursor.copy_expert(f"COPY {table_name}(retweets) FROM STDIN WITH HEADER CSV", contents)

    @task
    def load_hashtags(path: str, db_cursor):
        """
        Load csv with hashtags into Postgres database.
        """
        contents = open(path, 'r')
        table_name = "hashtags"
        if not table_exists(db_cursor, table_name):
            db_cursor.execute(f"CREATE TABLE {table_name} (id integer PRIMARY KEY, hashtag varchar);")
        else:
            db_cursor.execute(f"TRUNCATE {table_name}")
        db_cursor.copy_expert(f"COPY {table_name}(id, hashtag) FROM STDIN WITH HEADER CSV", contents)

    tweets_path = fetch()

    per_language_path = count_per_language(tweets_path)
    retweets_path = shopify_retweets(tweets_path)
    hashtags_path = all_hashtags(tweets_path)

    host = "postgres"
    port = "5432"
    dbname = "airflow"
    user = "airflow"
    password = "airflow"
    db_conn = psycopg2.connect(host=host, port=port, dbname=dbname, user=user, password=password)
    db_conn.autocommit = True
    db_cursor = db_conn.cursor()

    load_per_language(per_language_path, db_cursor)
    load_retweets(retweets_path, db_cursor)
    load_hashtags(hashtags_path, db_cursor)

twitter_dag = twitter_etl()
