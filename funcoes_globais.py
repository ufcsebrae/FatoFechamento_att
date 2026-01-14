# funcoes_globais.py (versão final com retry de blocos em duas rodadas)

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
    Cria uma engine com retry para falhas de conexão inicial,
    timeout de query longo e reciclagem de conexão.
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
                return info["str_conexao"] # Conexões OLAP podem ter um tratamento diferente
            else:
                raise ValueError(f"Tipo de conexão '{tipo_conexao}' não suportado.")

            string_conexao = f"mssql+pyodbc:///?odbc_connect={quote_plus(odbc_str)}"
            
            engine = create_engine(
                string_conexao, 
                pool_pre_ping=True, 
                pool_recycle=300 # Recicla conexões mais antigas que 5 minutos
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
                raise e # Levanta o erro na última tentativa ou se não for erro de comunicação
        except Exception as e:
            logger.error(f"Erro inesperado ao criar engine de conexão: {e}", exc_info=True)
            raise e
    raise ConnectionError(f"Não foi possível conectar a '{nome_conexao}' após {tentativas} tentativas.")


def selecionar_consulta_por_nome(titulo: str) -> pd.DataFrame:
    """Executa a consulta pelo nome e retorna um DataFrame."""
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


def salvar_no_financa(df: pd.DataFrame, table_name: str, retries_per_chunk: int = 3):
    """
    Salva o DataFrame no SQL Server usando duas rodadas de tentativas para blocos.
    Se um bloco falhar na primeira rodada (mesmo após retries internos),
    ele é adicionado a uma lista para uma segunda rodada de tentativas.
    O processo não é interrompido por falhas individuais de blocos.
    """
    if df.empty:
        logger.warning(f"⚠️ DataFrame está vazio. Nada será salvo.")
        return

    logger.info(f"📀 Iniciando processo de salvamento para a tabela '{table_name}'.")
    inicio_total = time.perf_counter()
    engine = None
    
    # Lista para guardar os blocos que falharam persistentemente na 1ª rodada
    blocos_com_falha_persistente_primeira_rodada = [] 

    try:
        engine = funcao_conexao("SPSVSQL39")
        
        SQL_SERVER_PARAM_LIMIT = 2100
        num_colunas = len(df.columns)
        chunksize = (SQL_SERVER_PARAM_LIMIT // num_colunas) if num_colunas > 0 else 1000
        
        total_rows = len(df)
        num_chunks = math.ceil(total_rows / chunksize)
        
        # --- ETAPA 1: Remover a tabela antiga ---
        with engine.begin() as connection:
            logger.info(f"🗑️ Removendo a tabela antiga '{table_name}' (se existir)...")
            connection.execute(text(f'DROP TABLE IF EXISTS "{table_name}"'))
            logger.info(f"✅ Tabela removida.")
            
        logger.info(f"💾 Iniciando 1ª RODADA: Salvando {total_rows} linhas em {num_chunks} blocos.")
        chunks = np.array_split(df, num_chunks)

        # --- ETAPA 2: Primeira tentativa de salvamento de todos os blocos ---
        for i, chunk_df in enumerate(chunks):
            bloco_atual = i + 1
            tentativas_chunk_atual = 0
            sucesso_chunk_atual = False
            
            while not sucesso_chunk_atual and tentativas_chunk_atual < retries_per_chunk:
                try:
                    with engine.begin() as connection:
                        if tentativas_chunk_atual > 0:
                             logger.info(f"  -> Retentando bloco {bloco_atual}/{num_chunks} (Tentativa {tentativas_chunk_atual + 1}/{retries_per_chunk} interna)...")
                        else:
                             logger.info(f"  -> Salvando bloco {bloco_atual}/{num_chunks} ({len(chunk_df)} linhas)...")
                        
                        chunk_df.to_sql(name=table_name, con=connection, if_exists='append', index=False, method='multi')
                        sucesso_chunk_atual = True
                
                except pyodbc.OperationalError as e:
                    tentativas_chunk_atual += 1
                    if '08S01' in str(e): # Apenas para falha de comunicação
                        logger.warning(f"  ⚠️ Falha de comunicação no bloco {bloco_atual}. Tentativa {tentativas_chunk_atual}/{retries_per_chunk}.")
                        if tentativas_chunk_atual < retries_per_chunk:
                            time.sleep(5) # Pequena pausa antes da retentativa interna
                        else:
                            logger.error(f"  ❌ Bloco {bloco_atual} falhou após {retries_per_chunk} retentativas internas.")
                            # Adiciona à lista de falhas para a 2ª rodada e sai do loop while
                            blocos_com_falha_persistente_primeira_rodada.append((bloco_atual, chunk_df))
                            break 
                    else:
                        # Se for outro erro operacional, re-raise imediatamente
                        logger.error(f"  ❌ Erro operacional não recuperável no bloco {bloco_atual}.", exc_info=True)
                        raise e
                except Exception as e: # Captura outros erros inesperados para este chunk
                    logger.error(f"  ❌ Erro inesperado ao salvar bloco {bloco_atual}.", exc_info=True)
                    raise e

        # --- ETAPA 3: Segunda rodada (retry) para blocos que falharam persistentemente na 1ª ---
        if blocos_com_falha_persistente_primeira_rodada:
            logger.warning(f"--- Iniciando 2ª RODADA para {len(blocos_com_falha_persistente_primeira_rodada)} blocos que falharam persistentemente na 1ª Rodada ---")
            blocos_com_falha_final = []
            
            for bloco_num, chunk_df_failed in blocos_com_falha_persistente_primeira_rodada:
                try:
                    with engine.begin() as connection:
                        logger.info(f"  -> 2ª Rodada: Retentando bloco {bloco_num}...")
                        chunk_df_failed.to_sql(name=table_name, con=connection, if_exists='append', index=False, method='multi')
                    logger.info(f"  ✅ Sucesso na 2ª Rodada para o bloco {bloco_num}.")
                except Exception as e:
                    logger.error(f"  ❌ FALHA FINAL no bloco {bloco_num} mesmo após a 2ª Rodada.", exc_info=True)
                    blocos_com_falha_final.append(bloco_num)

            if blocos_com_falha_final:
                # Se ainda houver falhas, levanta uma exceção para que a 'main' reporte o status parcial.
                raise RuntimeError(f"Não foi possível salvar os seguintes blocos mesmo após a 2ª Rodada: {blocos_com_falha_final}")

        fim_total = time.perf_counter()
        tempo_total = fim_total - inicio_total
        
        if blocos_com_falha_persistente_primeira_rodada and not blocos_com_falha_final:
             logger.info(f"🎉 Sucesso total! Todos os {total_rows} foram salvos, com retentativas na 1ª e 2ª rodadas, em {tempo_total:.2f} segundos.")
        else:
             logger.info(f"🎉 Sucesso! Todos os {total_rows} foram salvos na 1ª Rodada em {tempo_total:.2f} segundos.")

    except Exception as e:
        logger.error(f"❌ O processo de salvamento falhou. Causa: {e}", exc_info=True)
        raise e # Re-raise para que a função main possa capturar e enviar o e-mail de falha.
    finally:
        if engine:
            engine.dispose()

