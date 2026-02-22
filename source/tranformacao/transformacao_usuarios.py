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


class TranformacaoUsuarios:
    load_dotenv()
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
        Cria uma sessão Spark e defini o caminho do arquivo CSV de usuários.
        """
        self.spark = SparkSession.builder.appName('usuarios').getOrCreate()
        self.PATH_FILE_USUARIOS_CSV = os.getenv('PATH_FILE_USUARIOS_CSV')

    def extrair_dados_usuarios(self):
        if self.PATH_FILE_USUARIOS_CSV is None:
            raise ValueError('O PATH do arquivo CSV de usuários não definido.')
        df_usuarios = self.spark.read.csv(
            self.PATH_FILE_USUARIOS_CSV,
            header=True,
            schema=self.schema_usuarios
        )
        if len(df_usuarios.take(1)) == 0:
            raise ValueError('O arquivo de usuarios está vazio.')
        return df_usuarios

    def padroniza_dataframe_usuario(self, df_usuarios: DataFrame) -> DataFrame:
        """Padroniza o DataFrame de usuários."""
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
