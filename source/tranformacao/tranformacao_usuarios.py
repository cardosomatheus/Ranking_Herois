from pyspark.sql import SparkSession, DataFrame
from pyspark.sql.types import StructType, StructField, StringType, IntegerType, FloatType
import os
from dotenv import load_dotenv
import re

class TranformacaoHerois:
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
        self.spark = SparkSession.builder.appName('herois').getOrCreate()
        self.PATH_FILE_HEROIX_CSV = os.getenv('PATH_FILE_HEROIX_CSV')

    def extrair_dados_herois(self):
        df_herois = self.spark.read.csv(
            self.PATH_FILE_HEROIX_CSV,
            header=True,
            schema=self.schema_herois
        )
        if len(df_herois.take(1)) == 0:
            raise ValueError('O arquivo de herois está vazio.')
        return df_herois

    def padronizar_dataframe_herois(self, df_herois: DataFrame) -> DataFrame:
        # Implementação de padronização.
        ...

    def _retorna_cpf_numerico(self, cpf: str) -> int:
        """Recebe um CPF no formato string e retorna os números"""
        cpf_numerico = re.sub(r'\D', '', cpf)
        return int(cpf_numerico)


class TranformacaoUsuarios:
    load_dotenv()
    schema_usuarios = StructType([
        StructField("nome", StringType(), True),
        StructField("email", StringType(), True),
        StructField("telefone", StringType(), True),
        StructField("cpf", StringType(), True),
        StructField("ip_execucao", StringType(), True),
        StructField("heroi_id", IntegerType(), False),
        StructField("nota", IntegerType(), False)
    ])

    def __init__(self):
        self.spark = SparkSession.builder.appName('usuarios').getOrCreate()
        self.PATH_FILE_TXT = os.getenv('PATH_FILE_TXT')

    def extrair_dados_usuarios(self):
        df_usuarios = self.spark.read.csv(
            self.PATH_FILE_TXT,
            header=True,
            schema=self.schema_usuarios
        )
        if len(df_usuarios.take(1)) == 0:
            raise ValueError('O arquivo de usuários está vazio.')
        return df_usuarios

    def associar_herois_e_usuarios(
        self, df_herois: DataFrame, df_usuarios: DataFrame
    ) -> DataFrame:
        if len(df_usuarios.take(1)) == 0:
            raise ValueError('Assoc. não feita, DataFrame usuários vazio')

        if len(df_herois.take(1)) == 0:
            raise ValueError('Assoc. não feita, DataFrame herois vazio')

        df_herois_e_usuarios = df_usuarios.join(
            other=df_herois,
            on='heroi_id',
            how='inner'
        )

        df_herois_e_usuarios = df_herois_e_usuarios.select(
            'nome',
            'email',
            'telefone',
            'cpf',
            'ip_execucao',
            'nota',
            'heroi_id',
            'name',
            'Gender',
            'Eye_color',
            'Race',
            'Hair_color',
            'Height',
            'Alignment',
            'Weight'
        )
        return df_herois_e_usuarios


if __name__ == "__main__":
    transformacao_usuarios = TranformacaoUsuarios()
    df_usuarios = transformacao_usuarios.extrair_dados_usuarios()
    print(df_usuarios.show())
    # df_final = extracao.associar_herois_e_usuarios(herois, usuarios)
    # df_final.show()