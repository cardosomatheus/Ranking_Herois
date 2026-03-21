from source.transformacao.transformacao_usuarios import TranformacaoUsuarios
from source.transformacao.transformacao_herois import TransformacaoHerois
import logging
from dotenv import load_dotenv
import os
from pyspark.sql import SparkSession, DataFrame
from pyspark.sql.functions import col
from pyspark.errors import AnalysisException
from pyspark.sql.types import (
    StructField,
    StructType,
    StringType,
    IntegerType,
    FloatType,
    TimestampType,
    LongType
)

logger = logging.getLogger(__name__)


class TransformacaoUsuariosHerois:
    load_dotenv()
    SILVER_PATH_USUARIOS_HEROIS = os.getenv('SILVER_PATH_USUARIOS_HEROIS')
    SILVER_PATH_USUARIOS = os.getenv('SILVER_PATH_USUARIOS')
    SILVER_PATH_HEROI = os.getenv('SILVER_PATH_HEROI')
    schema_usuarios_herois = StructType([
        # Dados do Usuário (Padronizados)
        StructField('nome_completo', StringType(), True),
        StructField('email_usuario', StringType(), True),
        StructField('telefone_usuario', StringType(), True),
        StructField('telefone_usuario_numerico', LongType(), True),
        StructField('cpf_usuario', StringType(), True),
        StructField('cpf_usuario_numerico', LongType(), True),
        StructField('ip_origem', StringType(), True),
        StructField('pontuacao_ranking', IntegerType(), True),
        StructField('data_processamento', TimestampType(), True),
        StructField('id_heroi', IntegerType(), True),
        # Dados do Herói
        StructField('nome_heroi', StringType(), True),
        StructField('genero_heroi', StringType(), True),
        StructField('cor_olho_heroi', StringType(), True),
        StructField('especie_heroi', StringType(), True),
        StructField('cor_cabelo_heroi', StringType(), True),
        StructField('altura_cm_heroi', FloatType(), True),
        StructField('alinhamento_heroi', StringType(), True),
        StructField('peso_heroi', FloatType(), True)
    ])

    def __init__(self):
        """
        Inicializa a classe AssociacaoUsuariosHerois e Cria uma sessão Spark.
        """
        self.spark = SparkSession.\
            builder.\
            appName('ranking de herois').\
            getOrCreate()

    def gera_parquet_silver_herois(self) -> None:
        logger.info(f'Executando {self.gera_parquet_silver_herois.__name__}')
        TransformacaoHerois().executa_pipeline()

    def gera_parquet_silver_usuarios(self) -> None:
        logger.info(f'Executando {self.gera_parquet_silver_usuarios.__name__}')
        TranformacaoUsuarios().executa_pipeline()

    def ler_parquet_silver_herois(self) -> DataFrame:
        logger.info(f'Executando {self.ler_parquet_silver_herois.__name__}')

        if not self.SILVER_PATH_USUARIOS:
            msg = 'PATH de Herois na silver path não encontrado.'
            logger.error(msg)
            raise ValueError(msg)

        return self.spark.read.parquet(self.SILVER_PATH_HEROI)

    def ler_parquet_silver_usuarios(self) -> DataFrame:
        logger.info(f'Executando {self.ler_parquet_silver_usuarios.__name__}')

        if not self.SILVER_PATH_USUARIOS:
            msg = 'PATH de Usuarios na silver path não encontrado.'
            logger.error(msg)
            raise ValueError(msg)

        return self.spark.read.parquet(self.SILVER_PATH_USUARIOS)

    def _juntando_tabelas_herois_usuarios(
        self, df_herois: DataFrame, df_usuarios: DataFrame
    ) -> DataFrame:
        logger.info('Inciando join entre usuarios e heróis.')
        df_herois_e_usuarios = df_usuarios.join(
            other=df_herois,
            on='heroi_id',
            how='inner'
        )

        df_herois_usuarios = df_herois_e_usuarios.select(
            col('nome').alias('nome_completo'),
            col('email').alias('email_usuario'),
            col('telefone').alias('telefone_usuario'),
            col('telefone_numerico').alias('telefone_usuario_numerico'),
            col('cpf').alias('cpf_usuario'),
            col('cpf_numerico').alias('cpf_usuario_numerico'),
            col('ip_execucao').alias('ip_origem'),
            col('nota').alias('pontuacao_ranking'),
            col('data_execucao').alias('data_processamento'),
            col('heroi_id').alias('id_heroi'),
            col('name').alias('nome_heroi'),
            col('Gender').alias('genero_heroi'),
            col('Eye_color').alias('cor_olho_heroi'),
            col('Race').alias('especie_heroi'),
            col('Hair_color').alias('cor_cabelo_heroi'),
            col('Height').alias('altura_cm_heroi'),
            col('Alignment').alias('alinhamento_heroi'),
            col('Weight').alias('peso_heroi')
        )

        logger.info('Associação Concluída com sucesso.')
        return df_herois_usuarios

    def __evita_duplicidade_no_df_herois_silver(
        self, dataframe: DataFrame
    ) -> DataFrame:
        try:
            dataframe_old = self.spark\
                            .read\
                            .parquet(self.SILVER_PATH_USUARIOS_HEROIS)
            dataframe = dataframe.join(
                other=dataframe_old,
                on=['id_heroi', 'cpf_usuario_numerico'],
                how='left_anti'
            )
        except AnalysisException as e:
            print("Aviso: parquet antiga não encontrado. Criando nova carga.")
            print(f"messagem: {e}")

        finally:
            return dataframe

    def salva_dataframe_juntado_silver_parquet(
        self, df_herois_usuarios: DataFrame
    ):
        if not self.SILVER_PATH_USUARIOS_HEROIS:
            msg = 'PATH Herois x Usuarios em silver path não encontrado'
            logger.error(msg)
            raise ValueError(msg)

        if df_herois_usuarios.schema != self.schema_usuarios_herois:
            msg = 'O Schema Juntado está diferente do esperado.'
            logger.error(msg)
            raise ValueError(msg)

        df_herois_usuarios = self.__evita_duplicidade_no_df_herois_silver(
            dataframe=df_herois_usuarios
        )

        if not df_herois_usuarios.isEmpty():
            df_herois_usuarios.write.parquet(
                path=self.SILVER_PATH_USUARIOS_HEROIS,
                mode='append',
                compression='snappy'
            )

        logger.info('DF Herois x Usuarios salvo em parquet na camada SILVER.')

    def executa_pipeline(self):
        self.gera_parquet_silver_herois()
        self.gera_parquet_silver_usuarios()
        df_herois = self.ler_parquet_silver_herois()
        df_usuarios = self.ler_parquet_silver_usuarios()
        df_herois_usuarios = self._juntando_tabelas_herois_usuarios(
            df_herois, df_usuarios
        )
        self.salva_dataframe_juntado_silver_parquet(df_herois_usuarios)


if __name__ == "__main__":
    associacao = TransformacaoUsuariosHerois()
    associacao.executa_pipeline()
