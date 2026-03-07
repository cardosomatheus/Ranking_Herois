FROM apache/airflow:3.1.7
USER root

ENV POETRY_HOME=/opt/poetry \
    POETRY_VIRTUALENVS_CREATE=false \
    PATH="/opt/poetry/bin:$PATH"

RUN apt-get update \
  && apt-get install -y --no-install-recommends \
      curl \       
      vim \
  && curl -sSL https://install.python-poetry.org | python3 - \
  && apt-get autoremove -yqq --purge \
  && apt-get clean \
  && rm -rf /var/lib/apt/lists/*

RUN apt-get update \
  && apt-get install -y --no-install-recommends \
         openjdk-17-jdk \
         curl \
         vim \
  && apt-get autoremove -yqq --purge \
  && apt-get clean \
  && rm -rf /var/lib/apt/lists/*

ENV JAVA_HOME=/usr/lib/jvm/java-17-openjdk-amd64
RUN export JAVA_HOME

USER airflow
COPY pyproject.toml poetry.lock logging.yaml .env ./
RUN poetry install --no-root --no-interaction --no-ansi