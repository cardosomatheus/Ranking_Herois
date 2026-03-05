FROM python:3.12-slim

# Configura o ambiente de trabalho.
WORKDIR /movies

# Instala o curl para baixar o Poetry e o Java para o Spark.
RUN apt-get update && apt-get install -y --no-install-recommends curl 

# Variaveis do poetry e java
ENV POETRY_HOME=/opt/poetry
ENV POETRY_VIRTUALENVS_CREATE=false
ENV PATH="$POETRY_HOME/bin:$PATH"
ENV JAVA_HOME=/usr/lib/jvm/default-java


# Instala o Poetry e as dependências do projeto
RUN curl -sSL https://install.python-poetry.org | python3 -
RUN poetry install --no-root

# Instala o Java necessário para o Spark
RUN apt-get update \
    && apt-get install -y --no-install-recommends \
       default-jre-headless \
    && apt-get autoremove -yqq --purge \
    && apt-get clean \
    && rm -rf /var/lib/apt/lists/*


COPY logging.yaml /movies/logging.yaml
COPY source/ /movies/source/
COPY poetry.lock pyproject.toml /movies/
COPY teste.py /movies/

CMD [ "python3", "teste.py" ]