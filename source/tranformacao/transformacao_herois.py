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


class TranformacaoHerois:
    load_dotenv()
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
        Cria uma sessão Spark e defini o caminho do arquivo CSV de heróis.
        """
        self.spark = SparkSession.builder.appName('herois').getOrCreate()
        self.PATH_FILE_HEROIX_CSV = os.getenv('PATH_FILE_HEROIX_CSV')

    def extrair_dados_herois(self) -> DataFrame:
        """
        Extrai os dados dos heróis do arquivo CSV e retorna um DataFrame.
        """
        if self.PATH_FILE_HEROIX_CSV is None:
            raise ValueError('O PATH do arquivo CSV de heróis não definido.')

        df_herois = self.spark.read.csv(
            self.PATH_FILE_HEROIX_CSV,
            header=True,
            schema=self.schema_herois
        )
        if len(df_herois.take(1)) == 0:
            raise ValueError('O arquivo de herois está vazio.')
        return df_herois


if __name__ == "__main__":
    transformacao_herois = TranformacaoHerois()
    df_herois = transformacao_herois.extrair_dados_herois()
    print(df_herois.show())
