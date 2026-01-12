<<<<<<< HEAD
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

=======
# GARANTE QUE A CONFIGURAÇÃO DA DLL SEJA A PRIMEIRA COISA A SER EXECUTADA
import configura_mdx

import logging
import traceback
import os
from datetime import datetime

# Importa as funções reais do seu projeto
from funcoes_globais import selecionar_consulta_por_nome, salvar_no_financa
from notificacoes import enviar_email_status

def main():
    """
    Função principal que executa a consulta, salva os dados
    e envia um e-mail de status com o log completo da execução anexado.
    """
    # Define o path do arquivo de log que será usado nesta execução.
    # Esta informação é necessária no início para ser usada no bloco 'finally'.
    log_folder = "logs"
    data_str = datetime.now().strftime("%Y-%m-%d")
    log_file = os.path.join(log_folder, f"execucao_{data_str}.log")

    # Obtém a instância do logger configurada no seu outro arquivo.
    # É uma boa prática referenciar o logger pelo nome para garantir que você está
    # usando a mesma instância configurada.
    logger = logging.getLogger("logger_financa")

    status_final = "SUCESSO"
    detalhes_erro = ""
    
    try:
        query = "FatoFechamento"
        logger.info(f"Iniciando a consulta: {query}...")
        df_fato_fechamento = selecionar_consulta_por_nome(query)
        
        if df_fato_fechamento.empty:
            # Se a consulta falhar (conforme a lógica em 'selecionar_consulta_por_nome'),
            # ela retorna um DataFrame vazio e um erro já foi logado.
            raise ValueError("A consulta não retornou dados. Verifique o log de erros para a causa raiz.")

        logger.info("Consulta executada com sucesso. Visualizando as primeiras linhas:")
        # Usar to_string() garante uma formatação legível no arquivo de log.
        logger.info(f"\n{df_fato_fechamento.head().to_string()}")

        tabela_destino = "FatoFechamento_v2"
        logger.info(f"Salvando dados na tabela: {tabela_destino}...")
        # A função 'salvar_no_financa' já possui logs detalhados.
        salvar_no_financa(df_fato_fechamento, tabela_destino)
        logger.info("Processo de salvamento finalizado.")

    except Exception as e:
        status_final = "FALHA"
        # O traceback completo será logado, ideal para depuração.
        detalhes_erro = f"Ocorreu um erro inesperado:\n\n{traceback.format_exc()}"
        logger.error(detalhes_erro)

    finally:
        logger.info("Preparando notificação por e-mail...")
        
        # Tenta ler o conteúdo do log para anexar ao e-mail.
        log_content = ""
        try:
            # Garante que todos os logs em buffer sejam escritos no arquivo antes da leitura.
            for handler in logging.getLogger("logger_financa").handlers:
                handler.flush()
            
            with open(log_file, 'r', encoding='utf-8') as f:
                log_content = f.read()
            logger.info(f"Arquivo de log '{log_file}' lido com sucesso para anexo no e-mail.")
        except Exception as log_e:
            log_content = f"FALHA AO LER O ARQUIVO DE LOG.\nErro: {log_e}"
            logger.error(log_content)

        # Monta o corpo do e-mail.
        assunto = f"Relatório de Execução do Script FatoFechamento - {status_final}"
        
        if status_final == "SUCESSO":
            corpo_resumo = (
                "O script de atualização do FatoFechamento foi executado com sucesso.\n\n"
                f"- Consulta: {query}\n"
                f"- Tabela de Destino: {tabela_destino}"
            )
        else:
            corpo_resumo = (
                "O script de atualização do FatoFechamento falhou.\n\n"
                "Por favor, revise o log abaixo para identificar a causa do erro."
            )
            
        corpo_final = (
            f"{corpo_resumo}\n\n"
            f"{'='*40}\n"
            f"LOG DE EXECUÇÃO COMPLETO\n"
            f"{'='*40}\n\n"
            f"{log_content}"
        )
            
        enviar_email_status(subject=assunto, body=corpo_final)
        logger.info("Notificação por e-mail enviada.")

if __name__ == "__main__":
    main()
>>>>>>> 61599e95081f947a071ebfc3cb911548d55a28fa
