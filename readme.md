```bash
. Gerar dados de usuarios sobre um heroi e sua nota. (ok)
. extrair dados dos 2 arquivo e juntar-los. (ok)
. fazer as devidas tranformações.   (ok)
. fazer a carga do arquivo tratado para parquet(ok)
. Definir Logs no processo.(pesquisar sobre) (OK)
. Fazer a carga(merge) D-1 para o banco e apagar o arquivo processado. 
. orquestrar processo com airflow
. Jogar todo processo em conteineres dockers.
```
# Hero Analytics Pipeline

Este projeto consiste em um pipeline de dados para processar e analisar avaliações de heróis enviadas por usuários.
Arquitetura e Fluxo de Dados
```bash
    Ingestão: Coleta de arquivos brutos contendo as pontuações dos usuários(Dados fakes).
    Processamento (PySpark): Limpeza, normalização e tratamento dos dados brutos.
    Armazenamento: Conversão dos dados processados para o formato Parquet (otimizando performance e custo).
    Carga (D+1): Carregamento automatizado dos dados para o banco de dados com um dia de atraso (D+1).
    Orquestração: Gerenciamento de todo o fluxo e usando Apache Airflow.
    Análise: Criação de Views SQL para extração de analsie descritivas sobre heróis.

Stack Técnica.
    Linguagem: Python
    Processamento: PySpark
    Orquestração: Apache Airflow
    Formato de Saída: Parquet
    Destino: Banco de Dados (SQL)
```
