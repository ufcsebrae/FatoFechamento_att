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
