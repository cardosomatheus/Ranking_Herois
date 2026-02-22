                                    # EM DESENVOLVIMENTO
    def __init__(self):
        self.spark = SparkSession.builder.appName('usuarios').getOrCreate()
        self.PATH_FILE_TXT = os.getenv('PATH_FILE_TXT')

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

    def extrair_dados_usuarios(self):
        df_usuarios = self.spark.read.csv(
            self.PATH_FILE_TXT,
            header=True,
            schema=self.schema_usuarios
        )
        if len(df_usuarios.take(1)) == 0:
            raise ValueError('O arquivo de usuários está vazio.')
        return df_usuarios