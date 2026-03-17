from source.extracao.herois_bronze import GeraHeroisBronze
from faker import Faker
from dotenv import load_dotenv
import os
from random import randint
from datetime import datetime, timedelta
import logging

logger = logging.getLogger(__name__)


class GeradorDeUsuario:
    load_dotenv()
    BRONZE_PATH_USUARIO = os.getenv('BRONZE_PATH_USUARIO')

    def __init__(self):
        self.faker = Faker('pt_BR')
        self.herois_bronze = GeraHeroisBronze()

    def cria_usuarios_fakes(self, num_records: int = 1000) -> list:
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

        with open(self.BRONZE_PATH_USUARIO, 'w+') as file:
            cabecalho = "nome,email,telefone,cpf,ip_execucao,heroi_id,nota\n"
            file.write(cabecalho)
            ultimo_heroi_id = self.herois_bronze.buscar_ultimo_id_heroi_csv()

            for i in range(num_records):
                record = {
                    'nome': self.faker.name(),
                    'email': self.faker.email(),
                    'telefone': self.faker.cellphone_number(),
                    'cpf': self.faker.ssn(),
                    'ip_execucao': self.faker.ipv4(),
                    'heroi_id': randint(1, ultimo_heroi_id),
                    'nota': randint(1, 10),
                    'data_execucao': self.obter_horario_entre_intervalo()
                }
                row = ",".join(f'"{value}"' for value in record.values())+"\n"
                file.write(row)
        logger.info(f"Finalizada a geração de {num_records} user fakes.")

    def obter_horario_entre_intervalo(
        self,
        start: datetime = datetime.now() - timedelta(hours=1),
        end: datetime = datetime.now()
    ) -> datetime:
        """
        Obtem um horario aleatorio entre entre o intervalo de start e end.
        Por padrão o intervalo é de 1 hora.
        """
        return self.faker.date_time_between(
            start_date=start,
            end_date=end
        )


if __name__ == "__main__":
    gerador = GeradorDeUsuario()
    gerador.cria_usuarios_fakes(num_records=10000)
