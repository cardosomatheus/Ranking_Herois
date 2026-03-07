from airflow.sdk import dag, task
from datetime import datetime, timezone, timedelta
# Testando o comportamento do airflow via docker


import logging
from source.extracao.gera_usuarios import GeradorDeUsuario
# from source.load.salva_parquet_load import SalvaParquetLoad
from source.transformacao.associacao_usuarios_herois import (
    AssociacaoUsuariosHerois
)

logger = logging.getLogger(__name__)


@dag(start_date=datetime.now(timezone.utc),
     dag_id="Ranking_Herois",
     schedule=timedelta(minutes=30),
     description="Pontuação de herois da marvel.",
     catchup=False,
     default_args={
         'retries': 2,
         'retry_delay': timedelta(minutes=5)
        },
     tags=['Herois', 'Ranking', 'ETL'])
def ranking_heroes_dag():

    @task()
    def iniciando_pipeline():
        start_banner = """
        ###########################################################
        #                                                         #
        #   ____  _____   _    ____  _____                        #
        #  / ___||_   _| / \  |  _ \|_   _|                       #
        #  \___ \  | |  / _ \ | |_) | | |                         #
        #   ___) | | | / ___ \|  _ <  | |                         #
        #  |____/  |_|/_/   \_\_| \_\ |_|                         #
        #                                                         #
        ###########################################################
        """
        logger.info(start_banner)

    @task()
    def pontuacao_usuario_sobre_herois():
        GeradorDeUsuario().cria_usuarios_fakes(100)

    @task()
    def concatena_pontuacao_herois():
        associacao = AssociacaoUsuariosHerois()
        df_herois_e_usuarios = associacao.associar_herois_e_usuarios()
        df_herois_e_usuarios.head(2)
        return df_herois_e_usuarios

    iniciando_pipeline() \
        >> pontuacao_usuario_sobre_herois() \
        >> concatena_pontuacao_herois()


ranking_heroes_dag()
