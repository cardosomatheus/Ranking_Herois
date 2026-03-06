FROM python:3.12-slim

WORKDIR /movies

ENV POETRY_HOME=/opt/poetry \
    POETRY_VIRTUALENVS_CREATE=false \
    PATH="/opt/poetry/bin:/usr/lib/jvm/java-21-openjdk-amd64/bin:$PATH"

RUN apt-get update \
    && apt-get install -y --no-install-recommends \
        curl \
        openjdk-21-jre-headless \
    && curl -sSL https://install.python-poetry.org | python3 - \
    && apt-get purge -y curl \
    && apt-get autoremove -y \
    && apt-get clean \
    && rm -rf /var/lib/apt/lists/*

COPY pyproject.toml poetry.lock /movies/

RUN poetry install --no-root --no-interaction --no-ansi

COPY source/ /movies/source/
COPY logging.yaml .env /movies/

CMD ["python3", "-m", "source.main"]