import pandas as pd
import logging
import requests

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger("ImportadorCSV")

QUESTDB_HOST = "questdb"
QUESTDB_PORT = 9000
QUESTDB_TABLE = "usuarios"

def detectar_tipo(coluna: str, valor) -> str:
    tipos_personalizados = {
        "ipConcentrador": "STRING",
        "nomeConcentrador": "STRING",
        "latitudeCliente": "STRING",
        "longitudeCliente": "STRING",
        "motivoDesconexao": "STRING",
        "popCliente": "STRING",
        "bairroCliente": "STRING",
        "cidadeCliente": "STRING",
        "planoContrato": "STRING",
        "conexaoInicial": "TIMESTAMP",
        "conexaoFinal": "TIMESTAMP",
        "tempoConectado": "LONG",
        "consumoDownload": "LONG",
        "consumoUpload": "LONG",
        "statusInternet": "LONG",
        "statusCliente": "INT",
        "valorPlano": "DOUBLE",
    }
    return tipos_personalizados.get(coluna, "SYMBOL")

def processar_csv(arquivo_csv: str) -> (pd.DataFrame, dict):
    logger.info(f"Processando o CSV: {arquivo_csv}")
    df = pd.read_csv(arquivo_csv, dtype=str).fillna("")
    
    tipos_colunas = {col: detectar_tipo(col, df[col].iloc[0] if not df[col].empty else "") for col in df.columns}
    return df, tipos_colunas

def criar_tabela_questdb(nome_tabela: str, colunas: dict):
    query = f"CREATE TABLE IF NOT EXISTS {nome_tabela} ("
    query += ", ".join([f"{col} {tipo}" for col, tipo in colunas.items()])
    query += ");"
    
    response = requests.get(f"http://{QUESTDB_HOST}:{QUESTDB_PORT}/exec?query={requests.utils.quote(query)}")
    if response.status_code == 200:
        logger.info(f"Tabela {nome_tabela} criada.")
    else:
        logger.error(f"Erro ao criar tabela: {response.text}")

def inserir_via_csv_import(df: pd.DataFrame):
    temp_csv_file = "/tmp/temp_import.csv"
    df.to_csv(temp_csv_file, index=False)
    
    with open(temp_csv_file, 'rb') as f:
        response = requests.post(f"http://{QUESTDB_HOST}:{QUESTDB_PORT}/imp", 
                                 params={'name': QUESTDB_TABLE, 'overwrite': 'true', 'durable': 'true'},
                                 files={'data': f})
    if response.status_code == 200:
        logger.info("Dados enviados para o QuestDB.")
    else:
        logger.error(f"Erro ao enviar dados: {response.text}")

def main():
    arquivo_csv = "questdb-usuarios-dataset.csv"
    df, tipos_colunas = processar_csv(arquivo_csv)
    if not df.empty:
        criar_tabela_questdb(QUESTDB_TABLE, tipos_colunas)
        inserir_via_csv_import(df)

if __name__ == "__main__":
    main()
