from airflow.sdk import dag, task, chain
from datetime import datetime, timezone, timedelta
from pyspark.sql import SparkSession
import logging
from source.load.pipeline_gold import GoldAtualizaRelatorios
from source.extracao.usuarios_bronze import BronzeUsuario
from source.transformacao.transformacao_usuarios_herois import (
    TransformacaoUsuariosHerois
)


logger = logging.getLogger(__name__)
default_args = {
    'retries': 2,
    'retry_delay': timedelta(minutes=2)
}
spark = SparkSession.\
    builder.\
    appName('ranking de herois').\
    getOrCreate()


@dag(start_date=datetime.now(timezone.utc),
     dag_id="Ranking_Herois",
     schedule=timedelta(hours=1),
     description="Pontuação de herois da marvel.",
     catchup=False,
     default_args=default_args,
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
    def gera_pontuacao_usuario_sobre_herois_bronze():
        camada_bronze = BronzeUsuario()
        camada_bronze.cria_usuarios_fakes(1000)

    @task()
    def transforma_dados_gerados_silver():
        camada_silver = TransformacaoUsuariosHerois()
        camada_silver.executa_pipeline()

    @task()
    def gera_relatorios_camada_gold():
        camada_gold = GoldAtualizaRelatorios()
        camada_gold.executa_pipeline()

    @task()
    def finalizando_pipeline():
        end_banner = """
        ###########################################################
        #                                                         #
        #   _____  _   _  ____                                    #
        #  | ____|| \ | ||  _ \                                   #
        #  |  _|  |  \| || | | |                                  #
        #  | |___ | |\  || |_| |                                  #
        #  |_____||_| \_||____/                                   #
        #                                                         #
        ###########################################################
        """
        logger.info(end_banner)

    inicio = iniciando_pipeline()
    bronze = gera_pontuacao_usuario_sobre_herois_bronze()
    silver = transforma_dados_gerados_silver()
    gold = gera_relatorios_camada_gold()
    fim = finalizando_pipeline()
    chain(inicio, bronze, silver, gold, fim)


ranking_heroes_dag()
