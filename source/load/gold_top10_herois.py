import os
from dotenv import load_dotenv
from pyspark.sql import SparkSession, DataFrame
from pyspark.sql import functions as f


class GoldTop10Herois:
    load_dotenv()
    SILVER_PATH_USUARIOS_HEROIS = os.getenv('SILVER_PATH_USUARIOS_HEROIS')
    GOLD_PATH_TOP10_HEROIS = os.getenv('GOLD_PATH_TOP10_HEROIS')

    def __init__(self):
        self.spark = SparkSession.\
            builder.\
            appName('ranking de herois').\
            getOrCreate()

    def ler_parquet_silver_herois_usuarios(self):
        "Leitura do parquet de herois x usuarios na camada SILVER"
        return self.spark.read.parquet(self.SILVER_PATH_USUARIOS_HEROIS)

    def busca_top10_herois(self, dataframe: DataFrame):
        """ Retorna top 10 herois com maior media de pontuacao."""
        dataframe = dataframe.\
            groupBy(["id_heroi", "nome_heroi"])\
            .agg(f.round(f.avg("pontuacao_ranking"), 2).alias('media_heroi'))\
            .orderBy(f.desc('media_heroi'))\
            .limit(10)

        return dataframe

    def salva_top10_herois_em_formato_parquet(self, dataframe: DataFrame):
        """ Salva o dataframe na camada GOLD"""
        dataframe.write.parquet(
            path=self.GOLD_PATH_TOP10_HEROIS,
            mode='overwrite',
            compression='snappy'
        )

    def execute_pipeline(self):
        """ Executa processo. """
        dataframe = self.ler_parquet_silver_herois_usuarios()
        dataframe = self.busca_top10_herois(dataframe=dataframe)
        self.salva_top10_herois_em_formato_parquet(dataframe=dataframe)


if __name__ == '__main__':
    top_10_herois = GoldTop10Herois()
    top_10_herois.execute_pipeline()
