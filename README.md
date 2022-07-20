# Twitter ETL Pipeline

## Setup

If using Linux:
1. copy `.env.sample` to `.env` and set `AIRFLOW_UID` to host's user id:
```
echo -e "AIRFLOW_UID=$(id -u)" > .env
```
