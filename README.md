# Desafio MW

Este projeto foi desenvolvido como parte do processo seletivo da empresa MW Soluções. Ele permite a importação de dados a partir de um arquivo CSV para o banco de dados QuestDB, utilizando um script em Python e Docker para a orquestração dos serviços.

## Estrutura do Projeto

- `conversor.py` - Script Python para processar e importar dados do CSV para o QuestDB.
- `questdb-usuarios-dataset.csv` - Arquivo CSV contendo os dados a serem importados.
- `01_grafana.yml` - Configuração do container Docker para o Grafana.
- `02_questdb.yml` - Configuração do container Docker para o QuestDB.
- `03_python.yml` - Configuração do container Docker para o script Python.

