from source.extracao.gera_usuarios import GeradorDeUsuario
from source.load.salva_parquet_load import SalvaParquetLoad
from source.transformacao.associacao_usuarios_herois import (
    AssociacaoUsuariosHerois
)

if __name__ == "__main__":
    # Gerar os dados dos usuários
    gerador = GeradorDeUsuario()
    gerador.cria_usuarios_fakes()

    # Criar a sessão Spark e associar os dados dos usuários e heróis
    associacao = AssociacaoUsuariosHerois()
    df_herois_e_usuarios = associacao.associar_herois_e_usuarios()

    # Salvar o DataFrame resultante em formato Parquet
    classe_load = SalvaParquetLoad(associacao.spark)
    classe_load.load_dataframe_to_parquet(df_herois_e_usuarios)
