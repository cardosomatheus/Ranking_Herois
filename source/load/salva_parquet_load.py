from pyspark.sql import DataFrame
from pyspark.sql import SparkSession
from dotenv import load_dotenv
import os
import logging

logger = logging.getLogger(__name__)


class SalvaParquetLoad:
    """
    A classe utiliza a biblioteca PySpark para realizar escrita do Parquet.
    Classe responsável por:
        Ler o arquivo parquet já transformado na camada SILVER.
        Adicionar as novas informação no arquivo parquet na camada GOLD.
     """
    load_dotenv()
    SILVER_PATH_FILE = os.getenv('SILVER_PATH_FILE')
    GOLD_PATH_FILE = os.getenv('GOLD_PATH_FILE')

    def __init__(self, spark: SparkSession):
        """Inicializa a classe com uma instância do SparkSession."""
        self.spark = spark
        self.name_file = 'file_processed_load.parquet'

    def executa_pipeline(self):
        """ Executa a parte do load em GOLD PATH"""
        dataframe_gold = self.read_datafrane_parquet()
        self.salva_dataframe_parquet_gold(dataframe_gold)

    def read_datafrane_parquet(self) -> DataFrame:
        """Leitura do arquivo parquet no SILVER PATH."""
        logger.info('Lendo DataFrame da SILVER PATH.')

        if not self.SILVER_PATH_FILE:
            msg = 'Silver path não encontrado no load final.'
            logger.error(msg)
            raise ValueError(msg)

        dataframe = self.spark.read.parquet(self.SILVER_PATH_FILE)
        dataframe.show(5)
        return dataframe

    def salva_dataframe_parquet_gold(self, dataframe: DataFrame) -> None:
        """Salva o DataFrame em formato Parquet no GOLD PATH."""
        logger.info('Salvando DataFrame na GOLD PATH.')
        if not self.GOLD_PATH_FILE:
            msg = 'Gold path não encontrado'
            logger.error(msg)
            raise ValueError(msg)

        if dataframe.take(1) == []:
            msg = f'O DF está vazio em SILVER PATH: {self.SILVER_PATH_FILE}'
            logger.info(msg)
            return

        dataframe.write.parquet(
            path=f'{self.GOLD_PATH_FILE}/{self.name_file}',
            mode='append',
            compression='snappy'
        )
        logger.info(f"DataFrame salvo com sucesso em {self.GOLD_PATH_FILE}")


if __name__ == "__main__":
    spark = SparkSession.\
            builder.\
            appName('usuarios_e_herois').\
            getOrCreate()

    classe_load = SalvaParquetLoad(spark=spark)
    classe_load.executa_pipeline()
