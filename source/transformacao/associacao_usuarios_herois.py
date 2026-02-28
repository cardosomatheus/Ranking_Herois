from pyspark.sql import SparkSession, DataFrame
from source.transformacao.transformacao_usuarios import TranformacaoUsuarios
from source.transformacao.transformacao_herois import TransformacaoHerois
import logging

logger = logging.getLogger(__name__)


class AssociacaoUsuariosHerois:

    def __init__(self):
        """
        Inicializa a classe AssociacaoUsuariosHerois e Cria uma sessão Spark.
        """
        self.spark = SparkSession.\
            builder.\
            appName('usuarios_e_herois').\
            getOrCreate()
        self.spark.sparkContext.setLogLevel("ERROR") 
        self.transformacao_usuarios = TranformacaoUsuarios(self.spark)
        self.transformacao_herois = TransformacaoHerois(self.spark)

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
            'nome',
            'email',
            'telefone',
            'telefone_numerico',
            'cpf',
            'cpf_numerico',
            'ip_execucao',
            'nota',
            'data_execucao',
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

        logger.info('Associação Concluída com sucesso.')
        return df_herois_e_usuarios


if __name__ == "__main__":
    associacao = AssociacaoUsuariosHerois()
    df_herois_e_usuarios = associacao.associar_herois_e_usuarios()
    df_herois_e_usuarios.show()
