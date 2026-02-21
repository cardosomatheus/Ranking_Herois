from faker import Faker
from dotenv import load_dotenv
import os
from random import choice

def ramdom_name_movie() -> str:
    """
    Retorna um nome de filme baseado na lista de filmes
    Retorna: str: Um nome de filme.
    """
    movies = [
        "Homem de Ferro",
        "O Incrível Hulk",
        "Homem de Ferro 2",
        "Thor",
        "Capitão América: O Primeiro Vingador",
        "Os Vingadores",
        "Homem de Ferro 3",
        "Thor: O Mundo Sombrio",
        "Capitão América: O Soldado Invernal",
        "Guardiões da Galáxia",
        "Vingadores: Era de Ultron",
        "Homem-Formiga",
        "Capitão América: Guerra Civil",
        "Doutor Estranho",
        "Guardiões da Galáxia Vol. 2",
        "Homem-Aranha: De Volta ao Lar",
        "Thor: Ragnarok",
        "Pantera Negra",
        "Vingadores: Guerra Infinita",
        "Homem-Formiga e a Vespa"
    ]
    return choice(movies)


def create_fake_data(num_records: int) -> list:
    """
    Gera dados Fakes e os salva em um arquivo .txt
    Os registros gerados são: nome, email, telefone, cpf e ip de execução.
    num_records (int): O número de registros a serem gerados.
    """
    if not isinstance(num_records, int):
        raise TypeError('O Valor precisa ser inteiro. Exempplo: 10')

    if num_records <= 0:
        raise ValueError('O valor precisa ser maior que zero.')

    faker = Faker('pt_BR')
    load_dotenv()
    PATH_FILE_TXT = os.getenv('PATH_FILE_TXT')
    with open(PATH_FILE_TXT, 'w+') as file:
        for i in range(num_records):
            record = {
                'nome': faker.name(),
                'email': faker.email(),
                'telefone': faker.phone_number(),
                'cpf': faker.ssn(),
                'ip_execucao': faker.ipv4(),
                'nome_filme': ramdom_name_movie()
            }
            row = f'"{record["nome"]}", "{record["email"]}", "{record["telefone"]}", "{record["cpf"]}", "{record["ip_execucao"]}", "{record["nome_filme"]}" \n'
            file.write(row)



if __name__ == "__main__":
    create_fake_data(10)
    #print(ramdom_name_movie())
