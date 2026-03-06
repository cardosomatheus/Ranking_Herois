from airflow.sdk import dag, task
from datetime import datetime, timezone, timedelta
# Testando o comportamento do airflow via docker


@dag(start_date=datetime.now(timezone.utc),
     dag_id="Ranking_Herois",
     schedule=timedelta(minutes=30),
     description="Pontuação de herois da marvel.",
     catchup=False,
     tags=['Herois', 'Ranking', 'ETL'])
def ranking_heroes_dag():

    @task()
    def tarefa1():
        print("hello")

    @task()
    def tarefa2():
        print("word")

    tarefa1() >> tarefa2()


ranking_heroes_dag()
