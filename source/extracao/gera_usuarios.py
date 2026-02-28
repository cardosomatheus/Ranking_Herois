from faker import Faker
from dotenv import load_dotenv
import csv
import os
from random import randint
from datetime import datetime, timedelta
import logging

logger = logging.getLogger(__name__)


class GeradorDeUsuario:
    load_dotenv()
    PATH_FILE_USUARIO_TXT = os.getenv('PATH_FILE_USUARIO_TXT')
    PATH_FILE_HEROI_CSV = os.getenv('PATH_FILE_HEROI_CSV')

    def __init__(self):
        self.faker = Faker('pt_BR')

    def cria_usuarios_fakes(self, num_records: int = 10) -> list:
        """
        Gera dados Fakes e os salva em um arquivo .txt
        Os registros gerados são: nome, email, telefone, cpf e ip de execução.
        num_records (int): O número de registros a serem gerados.
        """
        logger.info(f"Iniciando a geração de {num_records} user fakes.")

        if not isinstance(num_records, int):
            msg = f"Tipo inválido em num_records: {num_records}. Deve ser INT."
            logger.error(msg)
            raise TypeError(msg)

        if num_records <= 0:
            msg = f"Valor de num_records: {num_records} deve ser maior que 0."
            logger.error(msg)
            raise ValueError(msg)

        with open(self.PATH_FILE_USUARIO_TXT, 'w+') as file:
            cabecalho = "nome,email,telefone,cpf,ip_execucao,heroi_id,nota\n"
            file.write(cabecalho)
            last_heroi_id = self.obter_ultimo_heroi_id()
            for i in range(num_records):
                record = {
                    'nome': self.faker.name(),
                    'email': self.faker.email(),
                    'telefone': self.faker.cellphone_number(),
                    'cpf': self.faker.ssn(),
                    'ip_execucao': self.faker.ipv4(),
                    'heroi_id': randint(1, last_heroi_id),
                    'nota': randint(1, 10),
                    'data_execucao': self.random_date()
                }
                row = ",".join(f'"{value}"' for value in record.values())+"\n"
                file.write(row)
        logger.info(f"Finalizada a geração de {num_records} user fakes.")

    def obter_ultimo_heroi_id(self) -> int:
        """Obtém o último ID de herói presente no arquivo CSV."""
        with open(self.PATH_FILE_HEROI_CSV, 'r', encoding='utf-8') as file:
            leitor = csv.reader(file)
            linhas = list(leitor)

            if len(linhas) <= 1:
                # O arquivo tem só o cabeçalho ou está vazio.
                raise ValueError('O arquivo CSV de heróis está vazio.')

            return int(linhas[-1][0])

    def random_date(
        self,
        start: datetime = datetime.now() - timedelta(hours=1),
        end: datetime = datetime.now()
    ) -> datetime:
        """Gera uma data aleatória entre o intervalo de start e end."""
        return self.faker.date_time_between(
            start_date=start,
            end_date=end
        )


if __name__ == "__main__":
    gerador = GeradorDeUsuario()
    print(gerador.PATH_FILE_HEROI_CSV)
    print(gerador.PATH_FILE_USUARIO_TXT)
    gerador.cria_usuarios_fakes(num_records=10)
