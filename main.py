# main.py

import configura_mdx
import logging
import os
from datetime import datetime

# --- CONFIGURAÇÃO CENTRAL DE LOGGING ---
# Esta configuração garante que o logger seja inicializado antes de qualquer
# outra parte do código e que o arquivo de log seja criado imediatamente.
log_folder = "logs"
if not os.path.exists(log_folder):
    os.makedirs(log_folder)

data_str = datetime.now().strftime("%Y-%m-%d")
log_file_path = os.path.join(log_folder, f"execucao_{data_str}.log")

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] - %(message)s",
    handlers=[
        logging.FileHandler(log_file_path, encoding="utf-8"),
        logging.StreamHandler()
    ],
    force=True  # Garante que esta configuração sobrescreva qualquer outra.
)

# Obtém a instância do logger que será usada em todo o projeto.
logger = logging.getLogger("logger_financa")

# --- Importações do projeto (após a configuração do log) ---
from funcoes_globais import selecionar_consulta_por_nome, salvar_no_financa
from notificacoes import enviar_email_status

def main():
    """
    Função principal que orquestra a execução do script:
    1. Executa a consulta para obter os dados.
    2. Salva os dados no banco de dados.
    3. Envia um e-mail com o status final e o log completo.
    """
    status_final = "SUCESSO"
    query = "FatoFechamento"
    tabela_destino = "FatoFechamento_v2"
    
    try:
        logger.info(f"--- INÍCIO DA EXECUÇÃO DO SCRIPT: {query} ---")
        
        # 1. Obter os dados
        df_fato_fechamento = selecionar_consulta_por_nome(query)
        
        if df_fato_fechamento.empty:
            # Esta exceção será levantada se a consulta falhar ou não retornar linhas.
            raise ValueError("A consulta não retornou dados. Verifique o log de erros para a causa raiz.")
            
        logger.info("Consulta executada com sucesso. Visualizando as primeiras linhas:")
        logger.info(f"\n{df_fato_fechamento.head().to_string()}")
        
        # 2. Salvar os dados
        logger.info(f"Iniciando o salvamento dos dados na tabela: {tabela_destino}...")
        salvar_no_financa(df_fato_fechamento, tabela_destino)
        
        logger.info(f"--- PROCESSO FINALIZADO COM SUCESSO ---")
        
    except Exception as e:
        status_final = "FALHA"
        # Loga o erro completo usando exc_info=True, que captura o traceback.
        logger.error(f"A execução principal falhou catastroficamente: {e}", exc_info=True)
        
    finally:
        logger.info("Preparando notificação por e-mail...")
        log_content = ""
        try:
            # Garante que o buffer de log seja escrito no arquivo antes de lê-lo.
            for handler in logging.getLogger().handlers:
                handler.flush()
            
            with open(log_file_path, 'r', encoding='utf-8') as f:
                log_content = f.read()
            logger.info(f"Arquivo de log '{log_file_path}' lido com sucesso.")
        except Exception as log_e:
            log_content = f"FALHA AO LER O ARQUIVO DE LOG.\nErro: {log_e}"
            logger.error(log_content)
            
        # O restante da sua lógica de envio de e-mail permanece aqui...
        assunto = f"Relatório de Execução do Script FatoFechamento - {status_final}"
        # enviar_email_status(subject=assunto, body=log_content)
        logger.info(f"Simulação: E-mail de status '{status_final}' enviado.")


if __name__ == "__main__":
    main()

