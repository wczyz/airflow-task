# Twitter ETL Pipeline

## Setup

1. copy `.env.sample` to `.env`
2. Fill `TWITTER_BEARER_TOKEN` with value obtained from Twitter Developer Account
3. If using Linux, set `AIRFLOW_UID` to host's user id:
```
echo -e "AIRFLOW_UID=$(id -u)" > .env
```
