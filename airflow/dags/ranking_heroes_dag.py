from airflow.sdk import dag, task, chain
from datetime import datetime, timezone, timedelta

import logging
from source.extracao.gera_usuarios import GeradorDeUsuario
from source.load.salva_parquet_load import SalvaParquetLoad
from source.transformacao.associacao_usuarios_herois import (
    AssociacaoUsuariosHerois
)


logger = logging.getLogger(__name__)
default_args = {
    'retries': 2,
    'retry_delay': timedelta(minutes=2)
}


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
    def gera_pontuacao_usuario_sobre_herois():
        GeradorDeUsuario().cria_usuarios_fakes(100)

    @task()
    def concatena_bases_salvando_em_parquet():
        associacao = AssociacaoUsuariosHerois()
        df_herois_e_usuarios = associacao.associar_herois_e_usuarios()
        df_herois_e_usuarios.show(5)

        # Salvando em parquet
        classe_load = SalvaParquetLoad(associacao.spark)
        classe_load.load_dataframe_to_parquet(df_herois_e_usuarios)

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
    pontuacao = gera_pontuacao_usuario_sobre_herois()
    df_herois = concatena_bases_salvando_em_parquet()
    fim = finalizando_pipeline()
    chain(inicio, pontuacao, df_herois, fim)


ranking_heroes_dag()
