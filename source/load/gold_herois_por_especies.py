import os
from dotenv import load_dotenv
import logging
from pyspark.sql import SparkSession, DataFrame
from pyspark.sql import functions as f

logger = logging.getLogger(__name__)


class GoldHeroisPorEspecies:
    load_dotenv()
    SILVER_PATH_USUARIOS_HEROIS = os.getenv('SILVER_PATH_USUARIOS_HEROIS')
    GOLD_PATH_HEROIS_X_ESPECIES = os.getenv('GOLD_PATH_HEROIS_X_ESPECIES')

    def __init__(self):
        self.spark = SparkSession.\
            builder.\
            appName('ranking de herois').\
            getOrCreate()

    def ler_parquet_silver_herois_usuarios(self):
        "Leitura do parquet de herois x usuarios da camada SILVER"
        return self.spark.read.parquet(self.SILVER_PATH_USUARIOS_HEROIS)

    def busca_herois_por_especies(self, dataframe: DataFrame):
        """ Retorna a qtd de herois por cada especie."""
        dataframe = dataframe\
            .filter("especie_heroi <> ''")\
            .groupBy("especie_heroi")\
            .agg(f.count("pontuacao_ranking").alias('qtd_especie'))\
            .orderBy(f.desc('qtd_especie'))

        return dataframe

    def salva_herois_por_especies_em_formato_parquet(
        self, dataframe: DataFrame
    ):
        """ Salva o dataframe na camada GOLD"""
        dataframe.show()
        dataframe.write.parquet(
            path=self.GOLD_PATH_HEROIS_X_ESPECIES,
            mode='overwrite',
            compression='snappy'
        )

    def executa_pipeline(self):
        """ Executa processo. """
        logger.info('Atualiza Herois por especie na camada GOLD.')
        dataframe = self.ler_parquet_silver_herois_usuarios()
        dataframe = self.busca_herois_por_especies(dataframe=dataframe)
        self.salva_herois_por_especies_em_formato_parquet(
            dataframe=dataframe
        )


if __name__ == '__main__':
    herois_por_especie = GoldHeroisPorEspecies()
    herois_por_especie.executa_pipeline()
