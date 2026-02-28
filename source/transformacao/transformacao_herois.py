import logging
import os
from dotenv import load_dotenv
from pyspark.sql import SparkSession, DataFrame
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
    PATH_FILE_HEROI_CSV = os.getenv('PATH_FILE_HEROI_CSV')
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

    def __init__(self, spark_session: SparkSession):
        """
        Inicializa a classe TranformacaoHerois.
        Cria uma sessão Spark.
        """
        self.spark = spark_session

    def extrair_dados_herois(self) -> DataFrame:
        """
        Extrai os dados dos heróis do arquivo CSV e retorna um DataFrame.
        """
        logger.info('Extração dos dados de heróis.')
        if self.PATH_FILE_HEROI_CSV is None:
            raise ValueError('O PATH do arquivo CSV de heróis não definido.')

        df_herois = self.spark.read.csv(
            self.PATH_FILE_HEROI_CSV,
            header=True,
            schema=self.schema_herois
        )
        if len(df_herois.take(1)) == 0:
            msg = f'O arquivo de herois está vazio. {self.PATH_FILE_HEROI_CSV}'
            logger.error(msg)
            raise ValueError(msg)
        return df_herois

    def executa_pipeline(self) -> DataFrame:
        """
        Executa a transformação dos dados dos heróis e retorna um DataFrame.
        """
        df_herois = self.extrair_dados_herois()
        return df_herois


if __name__ == "__main__":
    transformacao_herois = TransformacaoHerois()
    df_herois = transformacao_herois.executa_pipeline()
    print(df_herois.show())
