from pyspark.sql import SparkSession, DataFrame
from source.transformacao.transformacao_herois import TransformacaoHerois
from source.transformacao.transformacao_usuarios import TransformacaoUsuarios


class AssociaUsuariosHerois:
    transformacao_herois = TransformacaoHerois()
    transformacao_usuarios = TransformacaoUsuarios()

    def __init__(self):
        """Inicializa a classe AssociaUsuariosHerois."""
        self.spark = SparkSession.\
            builder.\
            appName('usuarios_e_herois').\
            getOrCreate()

    def associar_herois_e_usuarios(self) -> DataFrame:
        """
        Associa (inner join ) dados de usuários e heróis pelo campo 'heroi_id'.
        retorna um DataFrame com as colunas selecionadas.
        """
        df_usuarios = self.transformacao_herois.executa_pipeline()
        df_herois = self.transformacao_usuarios.executa_pipeline()

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
        return df_herois_e_usuarios


if __name__ == "__main__":
    transformacao_herois = AssociaUsuariosHerois()
    df_herois = transformacao_herois.associar_herois_e_usuarios()
    print(df_herois.show())
