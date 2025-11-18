import logging
import time
import pandas as pd
import traceback
from urllib.parse import quote_plus
from datetime import datetime
from conexoes import CONEXOES
from consultas_definidas import consultas
from criador_dataframe import CriadorDataFrame
import sqlalchemy
import os

# Configuração do logger
log_folder = "logs"
data_str = datetime.now().strftime("%Y-%m-%d")
log_file = f"{log_folder}/execucao_{data_str}.log"

# Cria pasta de logs se não existir
if not os.path.exists(log_folder):
    os.makedirs(log_folder)

# Configura logger principal
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(message)s",
    handlers=[
        logging.FileHandler(log_file, encoding="utf-8"),
        logging.StreamHandler()
    ]
)
logger = logging.getLogger("logger_financa")

def funcao_conexao(nome_conexao: str):
    """
    Retorna uma conexão SQLAlchemy com base nas informações da conexão especificada.
    Suporta conexões do tipo: 'sql', 'azure_sql' e 'olap'.
    """
    info = CONEXOES[nome_conexao]

    if info["tipo"] == "sql":
        servidor = info["servidor"]
        banco = info["banco"]
        driver = info["driver"]
        trusted = info.get("trusted_connection", False)
        trusted_str = "Trusted_Connection=yes;" if trusted else ""

        odbc_str = (
            f"DRIVER={driver};"
            f"SERVER={servidor};"
            f"DATABASE={banco};"
            f"{trusted_str}"
        )

        string_conexao = f"mssql+pyodbc:///?odbc_connect={quote_plus(odbc_str)}"
        return sqlalchemy.create_engine(string_conexao).connect()

    elif info["tipo"] == "azure_sql":
        servidor = info["servidor"]
        banco = info.get("banco", "")
        driver = info["driver"]
        authentication = info["authentication"]
        usuario = info.get("usuario")
        senha = info.get("senha")

        odbc_str = (
            f"DRIVER={driver};"
            f"SERVER={servidor},1433;"
            f"DATABASE={banco};"
            f"Authentication={authentication};"
        )

        if usuario and senha:
            odbc_str += f"UID={usuario};PWD={senha};"

        string_conexao = f"mssql+pyodbc:///?odbc_connect={quote_plus(odbc_str)}"
        return sqlalchemy.create_engine(string_conexao).connect()

    elif info["tipo"] == "olap":
        return info["str_conexao"]

    else:
        raise ValueError("Tipo de conexão não suportado.")

def selecionar_consulta_por_nome(titulo: str):
    """
    Executa a consulta pelo nome e retorna um DataFrame.
    Loga desempenho, linhas, colunas e uso de memória.
    """
    inicio = time.perf_counter()
    logger.info(f"⛔️ Iniciando execução da consulta: '{titulo}'")

    try:
        # Busca a consulta diretamente no dicionário pela chave (título)
        consulta_encontrada = consultas.get(titulo)
        
        if not consulta_encontrada:
            # Se não encontrar, levanta o erro
            raise ValueError(f"Consulta '{titulo}' não reconhecida.")

        df = CriadorDataFrame(
            funcao_conexao,
            consulta_encontrada.conexao,
            consulta_encontrada.sql,
            consulta_encontrada.tipo
        ).executar()

        fim = time.perf_counter()
        tempo = fim - inicio

        # Apenas loga se o dataframe tiver dados
        if not df.empty:
            linhas, colunas = df.shape
            memoria_mb = df.memory_usage(deep=True).sum() / 1024**2
            logger.info(f"✅ Consulta '{titulo}' finalizada em {tempo:.2f} segundos.")
            logger.info(f"📊 Linhas: {linhas} | Colunas: {colunas} | Memória: {memoria_mb:.2f} MB")
        else:
            logger.warning(f"⚠️ A consulta '{titulo}' foi executada mas não retornou dados.")

        return df

    except Exception as e:
        logger.error(f"❌ Erro na consulta '{titulo}': {str(e)}")
        logger.error(traceback.format_exc())
        return pd.DataFrame()

def salvar_no_financa(df: pd.DataFrame, table_name: str):
    """
    Salva DataFrame no SQL Server. Loga tempo, tamanho e falhas.
    """
    if df.empty:
        logger.warning(f"⚠️ DataFrame está vazio. Nada será salvo na tabela '{table_name}'.")
        return

    try:
        logger.info(f"📀 Iniciando salvamento na tabela '{table_name}'...")
        inicio = time.perf_counter()

        # Chama a função 'funcao_conexao' que já está neste arquivo
        engine = funcao_conexao("SPSVSQL39")
        df.to_sql(name=table_name, con=engine, if_exists='replace', index=False)
        return True

        fim = time.perf_counter()
        tempo = fim - inicio

        logger.info(f"✅ Salvamento concluído na tabela '{table_name}' em {tempo:.2f} segundos.")
    except Exception as e:
        logger.error(f"❌ Erro ao salvar no SQL: {str(e)}")
        logger.error(traceback.format_exc())
