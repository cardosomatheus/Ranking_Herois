from pyspark.sql import SparkSession, DataFrame
from pyspark.sql.functions import col
from source.transformacao.transformacao_usuarios import TranformacaoUsuarios
from source.transformacao.transformacao_herois import TransformacaoHerois
import logging
from dotenv import load_dotenv
from os import getenv

logger = logging.getLogger(__name__)


class AssociacaoUsuariosHerois:
    load_dotenv()
    SILVER_PATH_FILE = getenv('SILVER_PATH_FILE')

    def __init__(self, spark_session: SparkSession):
        """
        Inicializa a classe AssociacaoUsuariosHerois e Cria uma sessão Spark.
        """
        self.spark = spark_session
        self.transformacao_usuarios = TranformacaoUsuarios(self.spark)
        self.transformacao_herois = TransformacaoHerois(self.spark)

    def executa_pipeline(self):
        df_herois_e_usuarios = self.associar_herois_e_usuarios()
        self.salva_dataframe_parquet(df_herois_e_usuarios)

    def salva_dataframe_parquet(self, df_herois_e_usuarios: DataFrame):
        if not self.SILVER_PATH_FILE:
            raise ValueError('Caminho silver path não encontrado')

        df_herois_e_usuarios.write.parquet(
            path=self.SILVER_PATH_FILE,
            mode='overwrite',
            compression='snappy'
        )

        msg = f'DataFrame silver salvo em: "{self.SILVER_PATH_FILE}."'
        logger.info(msg)

    def associar_herois_e_usuarios(self) -> DataFrame:
        """
        Faz a associação (INNE JOIN) entre os DataFrames de usuários e heróis.
        Retorna um DataFrame resultante da associação.
        """
        logger.info('Processo de Associação entre usuários e heróis.')
        df_usuarios = self.transformacao_usuarios.executa_pipeline()
        df_herois = self.transformacao_herois.executa_pipeline()

        logger.info('Inciando a associação entre os user e heróis.')
        df_herois_e_usuarios = df_usuarios.join(
            other=df_herois,
            on='heroi_id',
            how='inner'
        )

        df_herois_e_usuarios = df_herois_e_usuarios.select(
            col('nome').alias('nome_completo'),
            'email',
            'telefone',
            'telefone_numerico',
            'cpf',
            'cpf_numerico',
            'ip_execucao',
            'nota',
            'data_execucao',
            'heroi_id',
            col('name').alias('nome_heroi'),
            col('Gender').alias('genero_heroi'),
            col('Eye_color').alias('cor_olho_heroi'),
            col('Race').alias('especie_heroi'),
            col('Hair_color').alias('cor_cabelo_heroi'),
            col('Height').alias('altura_cm_heroi'),
            col('Alignment').alias('Alinhamento_heroi'),
            col('Weight').alias('peso_heroi')
        )
        df_herois_e_usuarios.show(10)
        logger.info('Associação Concluída com sucesso.')
        return df_herois_e_usuarios


if __name__ == "__main__":
    spark = SparkSession.\
            builder.\
            appName('usuarios_e_herois').\
            getOrCreate()
    associacao = AssociacaoUsuariosHerois(spark_session=spark)
    associacao.executa_pipeline()
