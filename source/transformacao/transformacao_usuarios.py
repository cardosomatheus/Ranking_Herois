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
    TimestampType
)

logger = logging.getLogger(__name__)


class TranformacaoUsuarios:
    load_dotenv()
    PATH_FILE_USUARIO_TXT = os.getenv('PATH_FILE_USUARIO_TXT')
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

    def __init__(self, spark_session: SparkSession):
        """
        Inicializa a classe TranformacaoUsuarios.
        Cria uma sessão Spark.
        """
        self.spark = spark_session

    def extrair_dados_usuarios(self):
        """Extração dos dados de usuários."""
        logger.info('Extração dos dados de usuários.')
        if self.PATH_FILE_USUARIO_TXT is None:
            raise ValueError('O PATH do arquivo TXT de usuários não definido.')

        df_usuarios = self.spark.read.csv(
            self.PATH_FILE_USUARIO_TXT,
            header=True,
            schema=self.schema_usuarios
        )
        if len(df_usuarios.take(1)) == 0:
            msg = f'O arquivo de USER está vazio. {self.PATH_FILE_USUARIO_TXT}'
            logger.error(msg)
            raise ValueError(msg)
        return df_usuarios

    def padroniza_dataframe_usuario(self, df_usuarios: DataFrame) -> DataFrame:
        """Padroniza o DataFrame de usuários."""
        logger.info('Padronização do DataFrame de usuários.')
        df_usuarios = df_usuarios.withColumn(
            'cpf_numerico', regexp_replace(col('cpf'), r'\D', '')
        )

        df_usuarios = df_usuarios.withColumn(
            'telefone_numerico', regexp_replace(col('telefone'), r'\D', '')
        )

        return df_usuarios

    def executa_pipeline(self) -> DataFrame:
        """Executa o pipeline de transformação dos dados de usuários."""
        df_usuarios = self.extrair_dados_usuarios()
        df_usuarios_padronizado = self.padroniza_dataframe_usuario(df_usuarios)
        return df_usuarios_padronizado


if __name__ == "__main__":
    transformacao_usuarios = TranformacaoUsuarios()
    df_usuarios = transformacao_usuarios.executa_pipeline()
    print(df_usuarios.show())
