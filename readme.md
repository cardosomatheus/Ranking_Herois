# Hero Analytics Pipeline

Este projeto consiste em um pipeline de dados para processar avaliações de heróis da marvel enviadas por usuários.
Arquitetura e Fluxo de Dados.

```bash
    Ingestão: Coleta de arquivos brutos contendo as pontuações dos usuários(Dados fakes).
    Processamento (PySpark): Limpeza, normalização e tratamento dos dados brutos.
    Armazenamento: Conversão dos dados processados para o formato Parquet (otimizando performance e custo).
    Orquestração: Gerenciamento de todo o fluxo e usando Apache Airflow.

Stack Técnica.
    Controle: Poetry
    Linguagem: Python
    Processamento: PySpark
    Orquestração: Apache Airflow
    Formato de Saída: Parquet
```
Pré-requisitos:
Antes de iniciar, certifique-se de ter instalado em sua máquina:
JDK 21
Python 3.10+
Poetry
Docker e Docker Compose

1. Configurar o JAVA_HOME
Para que o projeto funcione corretamente, a variável de ambiente JAVA_HOME deve apontar para a instalação do JDK 21.
```bash
export JAVA_HOME=/caminho/para/seu/jdk-21
export PATH=$JAVA_HOME/bin:$PATH
```

2. Instalar Dependências (Poetry)
O projeto utiliza o Poetry para gerenciar as dependências do Python. Com o Poetry instalado, execute:
```bash
# Instala as dependências listadas no pyproject.toml
poetry install

# Ativa o ambiente virtual
poetry shell
```

3. Executar com Docker Compose
Para subir os serviços necessários (como bancos de dados ou ferramentas de mensageria), utilize o comando:
```bash
docker-compose up -d
```

Após a subida, a Dag contida no path airflow/dags Estara disponivel para execução no link:
```bash
http://localhost:8080/
```


Camada GOLD VAI BUSCAR O PARQUET UNIFICADO DE HEROIS E USUARIOS.
Montar relatórios para a camada GOLD:
    - top 10 herois com maiores pontuações.
    - top 10 herois femininas com maiores pontuações.
    - top 10 herois masculinos com maiores pontuações.
    - Qtd de cada herois por raca.

configurar um usuario e senha admin para airflow.