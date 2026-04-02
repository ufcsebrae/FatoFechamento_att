# funcoes_globais.py
import logging
import time
import pandas as pd
import os
import pyodbc
from urllib.parse import quote_plus
from sqlalchemy import create_engine, text, engine
import numpy as np
import math
from typing import Optional

# Suas importações personalizadas
from conexoes import CONEXOES
from consultas_definidas import consultas
from criador_dataframe import CriadorDataFrame

logger = logging.getLogger("logger_financa")


def funcao_conexao(
    nome_conexao: str, tentativas: int = 3, delay_segundos: int = 10
) -> Optional[engine.Engine]:
    """
    Cria uma engine SQLAlchemy com lógica de retry, configuração de segurança e otimização de escrita.
    """
    info = CONEXOES.get(nome_conexao)
    if not info:
        logger.error(f"Conexão '{nome_conexao}' não encontrada nas definições.")
        return None

    tipo_conexao = info.get("tipo")
    if tipo_conexao == "olap":
        return info.get("str_conexao")
    
    if tipo_conexao != "sql":
        raise ValueError(f"Tipo de conexão '{tipo_conexao}' não suportado.")

    driver = info["driver"].replace('+', ' ')
    servidor = info["servidor"]
    banco = info["banco"]

    params = {
        "DRIVER": f"{{{driver}}}",
        "SERVER": servidor,
        "DATABASE": banco,
        "timeout": "600",
        "Encrypt": "yes",
        "TrustServerCertificate": "yes"
    }

    if info.get("trusted_connection", False):
        params["Trusted_Connection"] = "yes"
    
    odbc_str = ";".join(f"{key}={value}" for key, value in params.items())
    string_conexao_url = f"mssql+pyodbc:///?odbc_connect={quote_plus(odbc_str)}"

    for tentativa in range(tentativas):
        try:
            # --- ALTERAÇÃO 1: Adicionado fast_executemany=True para otimizar a performance. ---
            engine_instance = create_engine(
                string_conexao_url,
                pool_pre_ping=True,
                pool_recycle=300,
                fast_executemany=True  # Otimização chave para bulk insert no SQL Server
            )
            
            with engine_instance.connect():
                logger.info(f"✅ Conexão com '{nome_conexao}' estabelecida (fast_executemany=True, pool_recycle=300s).")
                return engine_instance

        except pyodbc.OperationalError as e:
            if '08S01' in str(e) and tentativa < tentativas - 1:
                logger.warning(
                    f"⚠️ Falha de comunicação ao conectar com '{nome_conexao}'. "
                    f"Tentativa {tentativa + 1}/{tentativas}. "
                    f"Nova tentativa em {delay_segundos}s..."
                )
                time.sleep(delay_segundos)
            else:
                logger.error(f"Erro final de conexão na tentativa {tentativa + 1}.", exc_info=True)
                raise e
        except Exception as e:
            logger.error(f"Erro inesperado ao criar a engine para '{nome_conexao}'.", exc_info=True)
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
    Salva o DataFrame no SQL Server usando um método otimizado (fast_executemany)
    e uma lógica robusta de retries em blocos (chunks).
    """
    if df.empty:
        logger.warning(f"⚠️ DataFrame está vazio. Nada será salvo.")
        return

    logger.info(f"📀 Iniciando processo de salvamento para a tabela '{table_name}'.")
    inicio_total = time.perf_counter()
    engine = None
    
    blocos_com_falha_persistente_primeira_rodada = [] 

    try:
        engine = funcao_conexao("SPSVSQL39")
        
        # --- ALTERAÇÃO 2: Ajuste do chunksize para um valor maior e mais eficiente. ---
        chunksize = 10000
        
        total_rows = len(df)
        num_chunks = math.ceil(total_rows / chunksize) if chunksize > 0 else 1
        
        with engine.begin() as connection:
            logger.info(f"🗑️ Removendo a tabela antiga '{table_name}' (se existir)...")
            connection.execute(text(f'DROP TABLE IF EXISTS "{table_name}"'))
            logger.info(f"✅ Tabela removida.")
            
        logger.info(f"💾 Iniciando 1ª RODADA: Salvando {total_rows} linhas em {num_chunks} blocos de ~{chunksize} linhas.")
        chunks = np.array_split(df, num_chunks) if num_chunks > 1 else [df]

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
                        
                        # Removido 'method=multi' para deixar o fast_executemany atuar
                        chunk_df.to_sql(name=table_name, con=connection, if_exists='append', index=False, schema='dbo')
                        sucesso_chunk_atual = True
                
                except pyodbc.OperationalError as e:
                    tentativas_chunk_atual += 1
                    if '08S01' in str(e):
                        logger.warning(f"  ⚠️ Falha de comunicação no bloco {bloco_atual}. Tentativa {tentativas_chunk_atual}/{retries_per_chunk}.")
                        if tentativas_chunk_atual < retries_per_chunk:
                            time.sleep(5)
                        else:
                            logger.error(f"  ❌ Bloco {bloco_atual} falhou após {retries_per_chunk} retentativas internas.")
                            blocos_com_falha_persistente_primeira_rodada.append((bloco_atual, chunk_df))
                            break 
                    else:
                        logger.error(f"  ❌ Erro operacional não recuperável no bloco {bloco_atual}.", exc_info=True)
                        raise e
                except Exception as e: 
                    logger.error(f"  ❌ Erro inesperado ao salvar bloco {bloco_atual}.", exc_info=True)
                    # Adiciona à lista de falhas e sai do loop while para não tentar este chunk novamente
                    blocos_com_falha_persistente_primeira_rodada.append((bloco_atual, chunk_df))
                    break

        if blocos_com_falha_persistente_primeira_rodada:
            logger.warning(f"--- Iniciando 2ª RODADA para {len(blocos_com_falha_persistente_primeira_rodada)} blocos que falharam ---")
            blocos_com_falha_final = []
            
            for bloco_num, chunk_df_failed in blocos_com_falha_persistente_primeira_rodada:
                try:
                    with engine.begin() as connection:
                        logger.info(f"  -> 2ª Rodada: Retentando bloco {bloco_num}...")
                        chunk_df_failed.to_sql(name=table_name, con=connection, if_exists='append', index=False)
                    logger.info(f"  ✅ Sucesso na 2ª Rodada para o bloco {bloco_num}.")
                except Exception as e:
                    logger.error(f"  ❌ FALHA FINAL no bloco {bloco_num} mesmo após a 2ª Rodada.", exc_info=True)
                    blocos_com_falha_final.append(bloco_num)

            if blocos_com_falha_final:
                raise RuntimeError(f"Não foi possível salvar os seguintes blocos: {blocos_com_falha_final}")

        fim_total = time.perf_counter()
        tempo_total = fim_total - inicio_total
        
        if not blocos_com_falha_persistente_primeira_rodada:
             logger.info(f"🎉 Sucesso! Todos os {total_rows} foram salvos em {tempo_total:.2f} segundos.")
        elif not blocos_com_falha_final:
             logger.info(f"🎉 Sucesso total! Todos os {total_rows} foram salvos, com retentativas, em {tempo_total:.2f} segundos.")

    except Exception as e:
        logger.error(f"❌ O processo de salvamento falhou. Causa: {e}", exc_info=True)
        raise e
    finally:
        if engine:
            engine.dispose()
