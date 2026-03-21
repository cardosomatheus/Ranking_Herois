import os
from dotenv import load_dotenv
import logging
from pyspark.sql import SparkSession, DataFrame
from pyspark.sql import functions as f

logger = logging.getLogger(__name__)


class GoldTop10HeroisMasculinos:
    load_dotenv()
    SILVER_PATH_USUARIOS_HEROIS = os.getenv('SILVER_PATH_USUARIOS_HEROIS')
    GOLD_PATH_TOP10_HEROIS_MASCULINOS = os.getenv(
        'GOLD_PATH_TOP10_HEROIS_MASCULINOS'
    )

    def __init__(self):
        self.spark = SparkSession.\
            builder.\
            appName('ranking de herois').\
            getOrCreate()

    def ler_parquet_silver_herois_usuarios(self):
        "Leitura do parquet de herois x usuarios da camada SILVER"
        return self.spark.read.parquet(self.SILVER_PATH_USUARIOS_HEROIS)

    def busca_top10_herois_masculinos(self, dataframe: DataFrame):
        """ Retorna top 10 herois masculinos com maior media de pontuacão."""
        dataframe = dataframe\
            .filter("genero_heroi = 'Male'")\
            .groupBy(["id_heroi", "nome_heroi"])\
            .agg(f.round(f.avg("pontuacao_ranking"), 2).alias('media_heroi'))\
            .orderBy(f.desc('media_heroi'))\
            .limit(10)

        return dataframe

    def salva_top10_herois_masculinos_em_formato_parquet(
        self, dataframe: DataFrame
    ):
        """ Salva o dataframe na camada GOLD"""
        dataframe.write.parquet(
            path=self.GOLD_PATH_TOP10_HEROIS_MASCULINOS,
            mode='overwrite',
            compression='snappy'
        )

    def executa_pipeline(self):
        """ Executa processo. """
        logger.info('Atualiza top10 herois masculinos na camada GOLD.')

        dataframe = self.ler_parquet_silver_herois_usuarios()
        dataframe = self.busca_top10_herois_masculinos(dataframe=dataframe)
        self.salva_top10_herois_masculinos_em_formato_parquet(
            dataframe=dataframe
        )


if __name__ == '__main__':
    top_10_herois = GoldTop10HeroisMasculinos()
    top_10_herois.executa_pipeline()
