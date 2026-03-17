from dotenv import load_dotenv
import csv
import os
import logging

logger = logging.getLogger(__name__)


class GeraHeroisBronze:
    load_dotenv()
    BRONZE_PATH_HEROI = os.getenv('BRONZE_PATH_HEROI')

    def __init__(self):
        pass

    def buscar_ultimo_id_heroi_csv(self) -> int:
        """Obtém o último ID de herói presente no arquivo CSV."""
        with open(self.BRONZE_PATH_HEROI, 'r', encoding='utf-8') as file:
            leitor = csv.reader(file)
            linhas = list(leitor)

            if len(linhas) <= 1:
                msg = 'O arquivo CSV de heróis está vazio.'
                logger.error(msg)
                raise ValueError(msg)

            return int(linhas[-1][0])
