from pyspark.sql import DataFrame
from pyspark.sql import SparkSession
from dotenv import load_dotenv
import os


class SalvaParquetLoad:
    """
    Classe responsável por salvar um DataFrame em formato Parquet.
    A classe utiliza a biblioteca PySpark para realizar escrita do Parquet.
    """
    load_dotenv()
    output_path = os.getenv("PATH_FILE_OUTPUT_PARQUET")

    def __init__(self, spark: SparkSession):
        """Inicializa a classe com uma instância do SparkSession."""
        self.spark = spark

    def load_dataframe_to_parquet(self, dataframe: DataFrame) -> None:
        """Salva o DataFrame em formato Parquet no caminho especificado."""
        if dataframe.take(1) == []:
            raise ValueError('DataFrame vazio, não é possível salvar')

        dataframe.write.parquet(
            path=self.output_path,
            mode='overwrite',
            compression='snappy'
        )
        print(f"DataFrame salvo com sucesso em {self.output_path}")


if __name__ == "__main__":
    from source.transformacao.associacao_usuarios_herois import (
        AssociacaoUsuariosHerois
    )

    associacao = AssociacaoUsuariosHerois()
    df_herois_e_usuarios = associacao.associar_herois_e_usuarios()

    classe_load = SalvaParquetLoad(associacao.spark)
    classe_load.load_dataframe_to_parquet(df_herois_e_usuarios)
