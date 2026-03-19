import logging
import logging.config
import yaml
import os


def configura_setup_logging(file_path: str):
    if not os.path.exists(file_path):
        raise FileNotFoundError(f"Arquivo: {file_path} não encontrado:")

    with open(file_path, 'rt') as f:
        config = yaml.safe_load(f.read())
        logging.config.dictConfig(config=config)


configura_setup_logging('logging.yaml')
if __name__ == "__main__":
    from source.extracao.usuarios_bronze import BronzeUsuario
    from source.transformacao.transformacao_usuarios_herois import (
        TransformacaoUsuariosHerois
    )
    # from source.load.salva_parquet_load import SalvaParquetLoad

    start_banner = """
    ###########################################################
    #                                                         #
    #   ____  _____   _    ____  _____                        #
    #  / ___||_   _| / \  |  _ \|_   _|                       #
    #  \___ \  | |  / _ \ | |_) | | |                         #
    #   ___) | | | / ___ \|  _ <  | |                         #
    #  |____/  |_|/_/   \_\_| \_\ |_|                         #
    #                                                         #
    ###########################################################
    """

    logger = logging.getLogger(__name__)
    logger.info(start_banner)

    # CAMADA BRONZE
    gerador = BronzeUsuario()
    gerador.cria_usuarios_fakes(1000)

    # CAMADA SILVER
    transformacao = TransformacaoUsuariosHerois()
    transformacao.executa_pipeline()

    # CAMADA GOLD
#    classe_load = SalvaParquetLoad(spark)
#    classe_load.executa_pipeline()

    end_banner = """
    ###########################################################
    #                                                         #
    #   _____  _   _  ____                                    #
    #  | ____|| \ | ||  _ \                                   #
    #  |  _|  |  \| || | | |                                  #
    #  | |___ | |\  || |_| |                                  #
    #  |_____||_| \_||____/                                   #
    #                                                         #
    ###########################################################
    """
    logger.info(end_banner)
