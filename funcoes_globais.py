<<<<<<< HEAD
# funcoes_globais.py (versão com transação por chunk)

import logging
import time
import pandas as pd
import os
import pyodbc
from urllib.parse import quote_plus
from sqlalchemy import create_engine, text
import numpy as np
import math

logger = logging.getLogger("logger_financa")

# Suas importações personalizadas
from conexoes import CONEXOES
from consultas_definidas import consultas
from criador_dataframe import CriadorDataFrame

def funcao_conexao(nome_conexao: str, tentativas: int = 3, delay_segundos: int = 10) -> create_engine:
    """
    Cria uma engine com retry, timeout de query e reciclagem de conexão.
    """
    for tentativa in range(tentativas):
        try:
            info = CONEXOES[nome_conexao]
            tipo_conexao = info.get("tipo")
            
            odbc_str = ""
            if tipo_conexao == "sql":
                servidor = info["servidor"]
                banco = info["banco"]
                driver = info["driver"].replace('+', ' ')
                trusted_str = "Trusted_Connection=yes;" if info.get("trusted_connection", False) else ""
                odbc_str = f"DRIVER={{{driver}}};SERVER={servidor};DATABASE={banco};{trusted_str};timeout=600"
            
            elif tipo_conexao == "olap":
                return info["str_conexao"]
            else:
                raise ValueError(f"Tipo de conexão '{tipo_conexao}' não suportado.")

            string_conexao = f"mssql+pyodbc:///?odbc_connect={quote_plus(odbc_str)}"
            
            engine = create_engine(
                string_conexao, 
                pool_pre_ping=True, 
                pool_recycle=300
            )
            
            with engine.connect() as connection:
                logger.info(f"✅ Conexão com '{nome_conexao}' estabelecida (pool_recycle=300s).")
                return engine

        except pyodbc.OperationalError as e:
            if '08S01' in str(e) and tentativa < tentativas - 1:
                logger.warning(f"⚠️ Falha de comunicação ao conectar. Tentando novamente em {delay_segundos}s...")
                time.sleep(delay_segundos)
            else:
                logger.error(f"Erro final de conexão na tentativa {tentativa + 1}.", exc_info=True)
                raise e
    raise ConnectionError(f"Não foi possível conectar a '{nome_conexao}' após {tentativas} tentativas.")


def selecionar_consulta_por_nome(titulo: str) -> pd.DataFrame:
    """Executa a consulta pelo nome e retorna um DataFrame."""
    # (Esta função permanece a mesma da versão anterior)
    logger.info(f"▶️ Executando a consulta: '{titulo}'...")
    inicio = time.perf_counter()
    try:
        consulta_encontrada = consultas.get(titulo)
        if not consulta_encontrada:
            raise ValueError(f"Consulta '{titulo}' não encontrada nas definições.")

        tipo_correto = "mdx" if consulta_encontrada.tipo == "olap" else consulta_encontrada.tipo
        
        df = CriadorDataFrame(
            funcao_conexao,
            consulta_encontrada.conexao,
            consulta_encontrada.sql,
            tipo_correto
        ).executar()
        
        fim = time.perf_counter()
        tempo = fim - inicio
        if not df.empty:
            logger.info(f"✅ Consulta '{titulo}' finalizada em {tempo:.2f} segundos ({len(df)} linhas).")
        else:
            logger.warning(f"⚠️ Consulta '{titulo}' finalizada, mas não retornou nenhuma linha.")
        return df
        
    except Exception as e:
        logger.error(f"❌ Erro fatal ao executar 'selecionar_consulta_por_nome': {e}", exc_info=True)
        return pd.DataFrame()


def salvar_no_financa(df: pd.DataFrame, table_name: str):
    """
    Salva um DataFrame no SQL Server usando uma transação por chunk para 
    máxima resiliência contra timeouts de rede.
    """
    if df.empty:
        logger.warning(f"⚠️ DataFrame está vazio. Nada será salvo na tabela '{table_name}'.")
        return

    logger.info(f"📀 Iniciando processo de salvamento para a tabela '{table_name}'.")
    inicio = time.perf_counter()
    engine = None
    try:
        engine = funcao_conexao("SPSVSQL39")
        
        SQL_SERVER_PARAM_LIMIT = 2100
        num_colunas = len(df.columns)
        chunksize = (SQL_SERVER_PARAM_LIMIT // num_colunas) if num_colunas > 0 else 1000
        
        logger.info(f"⚙️ Tabela com {num_colunas} colunas. Chunksize dinâmico calculado: {chunksize} linhas por bloco.")
        
        total_rows = len(df)
        num_chunks = math.ceil(total_rows / chunksize)
        
        # Primeiro, apaga a tabela em uma transação separada.
        with engine.begin() as connection:
            logger.info(f"🗑️ Removendo a tabela antiga '{table_name}' (se existir)...")
            connection.execute(text(f'DROP TABLE IF EXISTS "{table_name}"'))
            logger.info(f"✅ Tabela removida.")
            
        logger.info(f"💾 Salvando {total_rows} linhas em {num_chunks} blocos...")
        chunks = np.array_split(df, num_chunks)

        # --- MUDANÇA PRINCIPAL: TRANSAÇÃO DENTRO DO LOOP ---
        for i, chunk_df in enumerate(chunks):
            bloco_atual = i + 1
            # Para cada bloco, abrimos uma nova transação.
            # Isso força o pool a nos dar uma conexão "fresca" ou testada.
            with engine.begin() as connection:
                logger.info(f"  -> Salvando bloco {bloco_atual}/{num_chunks} ({len(chunk_df)} linhas)...")
                chunk_df.to_sql(name=table_name, con=connection, if_exists='append', index=False, method='multi')
        # ---------------------------------------------------
            
        fim = time.perf_counter()
        tempo = fim - inicio
        logger.info(f"🎉 Sucesso! {total_rows} linhas salvas na tabela '{table_name}' em {tempo:.2f} segundos.")

    except Exception as e:
        logger.error(f"❌ Erro ao salvar no SQL para a tabela '{table_name}'.", exc_info=True)
        raise e
    finally:
        if engine:
            engine.dispose()
=======
import logging
import time
import pandas as pd
import traceback
import os
from urllib.parse import quote_plus
from datetime import datetime
from sqlalchemy import create_engine, text

# --- CORREÇÃO ---
# Importando as definições REAIS dos seus outros arquivos
from conexoes import CONEXOES
from consultas_definidas import consultas
from criador_dataframe import CriadorDataFrame
# ----------------

# Configuração do logger
log_folder = "logs"
data_str = datetime.now().strftime("%Y-%m-%d")
log_file = os.path.join(log_folder, f"execucao_{data_str}.log")

# Cria pasta de logs se não existir
if not os.path.exists(log_folder):
    os.makedirs(log_folder)

# Configura logger principal apenas se não estiver configurado
if not logging.getLogger("logger_financa").handlers:
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
    Retorna uma engine SQLAlchemy com base nas informações da conexão.
    """
    info = CONEXOES[nome_conexao]
    tipo_conexao = info.get("tipo")

    if tipo_conexao == "sql":
        servidor = info["servidor"]
        banco = info["banco"]
        driver = info["driver"].replace('+', ' ') # Garante que o driver esteja no formato correto
        trusted = info.get("trusted_connection", False)
        trusted_str = "Trusted_Connection=yes;" if trusted else ""
        odbc_str = (
            f"DRIVER={{{driver}}};"  # Adicionado chaves para nomes de driver com espaços
            f"SERVER={servidor};"
            f"DATABASE={banco};"
            f"{trusted_str}"
        )
        string_conexao = f"mssql+pyodbc:///?odbc_connect={quote_plus(odbc_str)}"
        return create_engine(string_conexao)

    elif tipo_conexao == "azure_sql":
        servidor = info["servidor"]
        banco = info.get("banco", "")
        driver = info["driver"].replace('+', ' ')
        authentication = info["authentication"]
        usuario = info.get("usuario")
        senha = info.get("senha")
        odbc_str = (
            f"DRIVER={{{driver}}};"
            f"SERVER={servidor},1433;"
            f"DATABASE={banco};"
            f"Authentication={authentication};"
        )
        if usuario and senha:
            odbc_str += f"UID={usuario};PWD={senha};"
        string_conexao = f"mssql+pyodbc:///?odbc_connect={quote_plus(odbc_str)}"
        return create_engine(string_conexao)

    elif tipo_conexao == "olap":
        return info["str_conexao"]

    else:
        raise ValueError(f"Tipo de conexão '{tipo_conexao}' não suportado.")


def selecionar_consulta_por_nome(titulo: str) -> pd.DataFrame:
    """
    Executa a consulta pelo nome e retorna um DataFrame.
    Loga desempenho, linhas, colunas e uso de memória.
    """
    # Linha de DEBUG (pode ser removida após confirmar que funciona)
    logger.info(f"DEBUG: Chaves disponíveis no dicionário 'consultas': {list(consultas.keys())}")

    inicio = time.perf_counter()
    logger.info(f"⛔️ Iniciando execução da consulta: '{titulo}'")
    try:
        consulta_encontrada = consultas.get(titulo)
        
        if not consulta_encontrada:
            raise ValueError(f"Consulta '{titulo}' não reconhecida.")

        # O tipo da consulta é MDX, não OLAP.
        tipo_correto = "mdx" if consulta_encontrada.tipo == "olap" else consulta_encontrada.tipo

        df = CriadorDataFrame(
            funcao_conexao,
            consulta_encontrada.conexao,
            consulta_encontrada.sql,
            tipo_correto
        ).executar()

        fim = time.perf_counter()
        tempo = fim - inicio

        if not df.empty:
            linhas, colunas = df.shape
            memoria_mb = df.memory_usage(deep=True).sum() / 1024**2
            logger.info(f"✅ Consulta '{titulo}' finalizada em {tempo:.2f} segundos.")
            logger.info(f"📊 Linhas: {linhas} | Colunas: {colunas} | Memória: {memoria_mb:.2f} MB")
        else:
            # Não loga como aviso, pois o erro já foi logado dentro do CriadorDataFrame
            pass 
            
        return df

    except Exception as e:
        logger.error(f"❌ Erro na consulta '{titulo}': {str(e)}")
        logger.error(traceback.format_exc())
        return pd.DataFrame()


def salvar_no_financa(df: pd.DataFrame, table_name: str):
    """
    Salva um DataFrame no SQL Server de forma otimizada e robusta.

    - Calcula dinamicamente o 'chunksize' para respeitar o limite de 2100
      parâmetros do SQL Server, evitando erros com 'method=multi'.
    """
    if df.empty:
        logger.warning(f"⚠️ DataFrame está vazio. Nada será salvo na tabela '{table_name}'.")
        return

    logger.info(f"📀 Iniciando processo de salvamento para a tabela '{table_name}'.")
    inicio = time.perf_counter()

    try:
        engine = funcao_conexao("SPSVSQL39")

        # --- LÓGICA INTELIGENTE PARA O CHUNKSIZE ---
        # Limite de parâmetros do SQL Server
        SQL_SERVER_PARAM_LIMIT = 2100
        
        # Número de colunas no DataFrame
        num_colunas = len(df.columns)
        
        # Calcula o chunksize seguro, arredondando para baixo.
        # Garante que (chunksize * num_colunas) nunca exceda 2100.
        if num_colunas > 0:
            chunksize = SQL_SERVER_PARAM_LIMIT // num_colunas
        else:
            chunksize = 1000 # Um padrão caso o DF não tenha colunas

        logger.info(f"⚙️ Tabela com {num_colunas} colunas. Chunksize dinâmico calculado: {chunksize} linhas por bloco.")
        # -------------------------------------------

        with engine.begin() as connection:
            total_rows = len(df)
            logger.info(f"Total de {total_rows} linhas a serem salvas.")

            logger.info(f"🗑️  Removendo a tabela '{table_name}' (se existir)...")
            connection.execute(text(f'DROP TABLE IF EXISTS "{table_name}"'))
            logger.info(f"✅ Tabela '{table_name}' removida com sucesso.")

            logger.info(f"💾 Salvando dados em blocos de {chunksize} linhas...")
            
            df.to_sql(
                name=table_name,
                schema='dbo',
                con=connection,
                if_exists='append',
                index=False,
                chunksize=chunksize, # Usa o valor seguro calculado
                method='multi'
            )

        fim = time.perf_counter()
        tempo = fim - inicio
        logger.info(f"✅ Sucesso! {total_rows} linhas salvas na tabela '{table_name}' em {tempo:.2f} segundos.")

    except Exception as e:
        logger.error(f"❌ Erro ao salvar no SQL para a tabela '{table_name}': {e}")
        logger.error(traceback.format_exc())
>>>>>>> 61599e95081f947a071ebfc3cb911548d55a28fa
