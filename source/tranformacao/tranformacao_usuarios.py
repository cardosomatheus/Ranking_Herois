from pyspark.sql import SparkSession
from pyspark.sql.types import StructType,StructField, StringType, IntegerType
import os
from dotenv import load_dotenv


class TranformacaoUsuarios:
    load_dotenv()

    def __init__(self):
        self.PATH_FILE_HEROIX_CSV = os.getenv('PATH_FILE_HEROIX_CSV')
        self.PATH_FILE_TXT = os.getenv('PATH_FILE_TXT')

    def extrair_dados_herois(self):
        spark = SparkSession.builder.appName('herois').getOrCreate()

        df_herois = spark.read.csv(self.PATH_FILE_HEROIX_CSV, header=True, inferSchema=True)
        return df_herois.show()

    def extrair_dados_usuarios(self):
        spark = SparkSession.builder.appName('usuarios').getOrCreate()
        schema = StructType([
            StructField("nome", StringType(), True),
            StructField("email", StringType(), True),
            StructField("cpf", StringType(), True),
            StructField("telefone", StringType(), True),
            StructField("ip_execucao", StringType(), True),
            StructField("heroi_id", IntegerType(), False),
            StructField("nota", IntegerType(), False)
        ])

        df_usuarios = spark.read.csv(
            self.PATH_FILE_TXT, header=True, schema=schema
        )
        if df_usuarios.rdd.isEmpty():
            raise ValueError('O arquivo de usuários está vazio.')
        return df_usuarios.show()


if __name__ == "__main__":
    extracao = TranformacaoUsuarios()
    # extracao.extrair_dados_herois()
    print(extracao.extrair_dados_usuarios())