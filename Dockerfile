FROM apache/airflow
RUN pip install poetry
COPY pyproject.toml .env .
RUN poetry config virtualenvs.create false --local && poetry install --no-dev
