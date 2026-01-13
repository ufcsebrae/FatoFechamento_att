# funcoes_globais.py (versão final com retry de blocos)

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
    # Esta função não precisa mais de lógica de retry, pois a causa do erro foi identificada.
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


def salvar_no_financa(df: pd.DataFrame, table_name: str, retries_por_chunk: int = 2):
    """
    Salva o DataFrame de forma resiliente. Tenta salvar cada bloco e, se falhar,
    guarda para uma segunda rodada de tentativas no final.
    """
    if df.empty:
        logger.warning(f"⚠️ DataFrame está vazio. Nada será salvo.")
        return

    logger.info(f"📀 Iniciando processo de salvamento para a tabela '{table_name}'.")
    inicio_total = time.perf_counter()
    engine = None
    blocos_falhos = [] # Lista para guardar os blocos que falharam

    try:
        engine = funcao_conexao("SPSVSQL39")
        
        SQL_SERVER_PARAM_LIMIT = 2100
        num_colunas = len(df.columns)
        chunksize = (SQL_SERVER_PARAM_LIMIT // num_colunas) if num_colunas > 0 else 1000
        
        total_rows = len(df)
        num_chunks = math.ceil(total_rows / chunksize)
        
        with engine.begin() as connection:
            logger.info(f"🗑️ Removendo a tabela antiga '{table_name}'...")
            connection.execute(text(f'DROP TABLE IF EXISTS "{table_name}"'))
            logger.info(f"✅ Tabela removida.")
            
        logger.info(f"💾 Iniciando 1ª rodada: salvando {total_rows} linhas em {num_chunks} blocos...")
        chunks = np.array_split(df, num_chunks)

        for i, chunk_df in enumerate(chunks):
            bloco_atual = i + 1
            try:
                with engine.begin() as connection:
                    logger.info(f"  -> 1ª Tentativa: Salvando bloco {bloco_atual}/{num_chunks}...")
                    chunk_df.to_sql(name=table_name, con=connection, if_exists='append', index=False, method='multi')
            except pyodbc.OperationalError as e:
                if '08S01' in str(e):
                    logger.warning(f"  ⚠️ Falha de comunicação no bloco {bloco_atual}. Adicionando à lista para retentativa.")
                    blocos_falhos.append((bloco_atual, chunk_df))
                else:
                    logger.error(f"  ❌ Erro de banco de dados não recuperável no bloco {bloco_atual}.", exc_info=True)
                    raise e # Falha imediatamente se o erro não for de comunicação
        
        # --- SEGUNDA RODADA: TENTA SALVAR NOVAMENTE OS BLOCOS QUE FALHARAM ---
        if blocos_falhos:
            logger.warning(f"--- Iniciando 2ª rodada para {len(blocos_falhos)} blocos que falharam ---")
            blocos_com_falha_permanente = []
            
            for bloco_num, chunk_df in blocos_falhos:
                try:
                    with engine.begin() as connection:
                        logger.info(f"  -> 2ª Tentativa: Salvando bloco {bloco_num}...")
                        chunk_df.to_sql(name=table_name, con=connection, if_exists='append', index=False, method='multi')
                    logger.info(f"  ✅ Sucesso na 2ª tentativa para o bloco {bloco_num}.")
                except Exception as e:
                    logger.error(f"  ❌ FALHA PERMANENTE no bloco {bloco_num} mesmo na 2ª tentativa.", exc_info=True)
                    blocos_com_falha_permanente.append(bloco_num)

            if blocos_com_falha_permanente:
                # Se ainda houver falhas, levanta uma exceção para que a 'main' saiba que o processo não foi 100%
                raise RuntimeError(f"Não foi possível salvar os seguintes blocos: {blocos_com_falha_permanente}")

        fim_total = time.perf_counter()
        tempo_total = fim_total - inicio_total
        
        if blocos_falhos and not blocos_com_falha_permanente:
             logger.info(f"🎉 Sucesso! Todos os {total_rows} foram salvos, com algumas retentativas, em {tempo_total:.2f} segundos.")
        else:
             logger.info(f"🎉 Sucesso! Todos os {total_rows} foram salvos na primeira rodada em {tempo_total:.2f} segundos.")

    except Exception as e:
        logger.error(f"❌ O processo de salvamento falhou. Causa: {e}", exc_info=True)
        raise e
    finally:
        if engine:
            engine.dispose()
