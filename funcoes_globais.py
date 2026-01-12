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
