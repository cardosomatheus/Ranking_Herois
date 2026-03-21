import logging
import os
from dotenv import load_dotenv
from pyspark.sql import SparkSession, DataFrame
from pyspark.sql import functions as f
from pyspark.sql.types import (
    StructType,
    StructField,
    StringType,
    IntegerType,
    FloatType
)

logger = logging.getLogger(__name__)


class TransformacaoHerois:
    load_dotenv()
    BRONZE_PATH_HEROI = os.getenv('BRONZE_PATH_HEROI')
    SILVER_PATH_HEROI = os.getenv('SILVER_PATH_HEROI')
    schema_herois = StructType([
                StructField("heroi_id", IntegerType(), False),
                StructField("name", StringType(), True),
                StructField("Gender", StringType(), True),
                StructField("Eye_color", StringType(), True),
                StructField("Race", StringType(), True),
                StructField("Hair_color", StringType(), True),
                StructField("Height", FloatType(), True),
                StructField("Publisher", StringType(), True),
                StructField("Skin_color", StringType(), True),
                StructField("Alignment", StringType(), True),
                StructField("Weight", FloatType(), True)
            ])

    def __init__(self):
        """
        Inicializa a classe TranformacaoHerois.
        Cria uma sessão Spark.
        """
        self.spark = SparkSession.\
            builder.\
            appName('ranking de herois').\
            getOrCreate()

    def ler_csv_heroi(self) -> DataFrame:
        logger.info('Lendo CSV heróis na camada BRONZE.')
        if not self.BRONZE_PATH_HEROI:
            raise ValueError('O PATH do arquivo CSV de Herois não definido.')

        dataframe = self.spark.read.csv(
            self.BRONZE_PATH_HEROI,
            header=True,
            schema=self.schema_herois
        )

        return dataframe

    def padroniza_dataframe_heroi(self, dataframe: DataFrame) -> DataFrame:
        dataframe = dataframe.withColumn(
            "Race",
            f.regexp_replace(f.col("Race"), '-', "")
        )
        return dataframe

    def salva_csv_heroi_em_formato_parquet(self, dataframe: DataFrame):
        logger.info('Salvando Parquet heróis na camada SILVER.')
        dataframe.show(10)
        dataframe.write.parquet(
            path=self.SILVER_PATH_HEROI,
            mode='overwrite',
            compression='snappy'
        )

        logger.info('Parquet heroi salvo na camada SILVER')

    def executa_pipeline(self):
        dataframe = self.ler_csv_heroi()
        dataframe = self.padroniza_dataframe_heroi(dataframe=dataframe)
        self.salva_csv_heroi_em_formato_parquet(dataframe)


if __name__ == "__main__":
    transformacao_herois = TransformacaoHerois()
    df_herois = transformacao_herois.executa_pipeline()
