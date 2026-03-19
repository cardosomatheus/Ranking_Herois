import logging
import os
from dotenv import load_dotenv
from pyspark.sql import SparkSession, DataFrame
from pyspark.sql.functions import col, regexp_replace
from pyspark.sql.types import (
    StructType,
    StructField,
    StringType,
    IntegerType,
    TimestampType,
    LongType
)

logger = logging.getLogger(__name__)


class TranformacaoUsuarios:
    load_dotenv()
    BRONZE_PATH_USUARIO = os.getenv('BRONZE_PATH_USUARIO')
    SILVER_PATH_USUARIOS = os.getenv('SILVER_PATH_USUARIOS')
    schema_usuarios = StructType([
        StructField("nome", StringType(), True),
        StructField("email", StringType(), True),
        StructField("telefone", StringType(), True),
        StructField("cpf", StringType(), True),
        StructField("ip_execucao", StringType(), True),
        StructField("heroi_id", IntegerType(), False),
        StructField("nota", IntegerType(), False),
        StructField("data_execucao", TimestampType(), True)
    ])

    def __init__(self):
        """
        Inicializa a classe TranformacaoUsuarios.
        Cria uma sessão Spark.
        """
        self.spark = SparkSession.\
            builder.\
            appName('ranking de herois').\
            getOrCreate()

    def ler_txt_usuarios(self):
        """Leitura dos dados de usuários."""
        logger.info('Leitura dos dados de usuários.')
        if not self.BRONZE_PATH_USUARIO:
            raise ValueError('O PATH do arquivo TXT de usuários não definido.')

        dataframe = self.spark.read.csv(
            self.BRONZE_PATH_USUARIO,
            header=True,
            schema=self.schema_usuarios
        )

        return dataframe

    def padroniza_dataframe_usuario(self, dataframe: DataFrame) -> DataFrame:
        """Padroniza o DataFrame de usuários."""
        logger.info('Padronização do DataFrame de usuários.')

        dataframe = dataframe.withColumn(
            'cpf_numerico',
            regexp_replace(col('cpf'), r'\D', '').cast(LongType())
        )

        dataframe = dataframe.withColumn(
            'telefone_numerico',
            regexp_replace(col('telefone'), r'\D', '').cast(LongType())
        )

        dataframe.dropDuplicates(['heroi_id', 'cpf_numerico'])
        return dataframe

    def salva_txt_usuario_em_formato_parquet(self, dataframe: DataFrame):
        logger.info('Salvando Parquet usuario na camada SILVER.')
        dataframe.write.parquet(
            path=self.SILVER_PATH_USUARIOS,
            mode='overwrite',
            compression='snappy'
        )

        logger.info('Parquet usuario salvo na camada com sucesso.')

    def executa_pipeline(self) -> DataFrame:
        """Executa o pipeline de transformação dos dados de usuários."""
        dataframe = self.ler_txt_usuarios()
        dataframe = self.padroniza_dataframe_usuario(dataframe=dataframe)
        self.salva_txt_usuario_em_formato_parquet(dataframe=dataframe)


if __name__ == "__main__":
    transformacao_usuarios = TranformacaoUsuarios()
    dataframe = transformacao_usuarios.executa_pipeline()
