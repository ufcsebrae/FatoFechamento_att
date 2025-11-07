# GARANTE QUE A CONFIGURAÇÃO DA DLL SEJA A PRIMEIRA COISA A SER EXECUTADA
import configura_mdx

from funcoes_globais import selecionar_consulta_por_nome, salvar_no_financa
from notificacoes import enviar_email_status
import traceback

def main():
    """
    Função principal que executa a consulta e salva os dados.
    Envia um e-mail de status no final.
    """
    status_final = "SUCESSO"
    detalhes_erro = ""
    
    try:
        query = "FatoFechamento"
        print(f"Iniciando a consulta: {query}...")
        df_fato_fechamento = selecionar_consulta_por_nome(query)
        
        if df_fato_fechamento.empty:
            # Este erro acontece se a consulta falhar e retornar um DataFrame vazio
            raise ValueError("A consulta não retornou dados. Verifique o log de erros para a causa raiz.")

        print("Consulta executada com sucesso. Visualizando as primeiras linhas:")
        print(df_fato_fechamento.head())

        tabela_destino = "FatoFechamento_v2"
        print(f"Salvando dados na tabela: {tabela_destino}...")
        salvar_no_financa(df_fato_fechamento, tabela_destino)
        print("Dados salvos com sucesso.")

    except Exception as e:
        status_final = "FALHA"
        detalhes_erro = f"Ocorreu um erro inesperado:\n\n{traceback.format_exc()}"
        print(detalhes_erro)

    finally:
        print("Enviando notificação por e-mail...")
        assunto = f"Relatório de Execução do Script - {status_final}"
        
        if status_final == "SUCESSO":
            corpo = "O script de atualização foi executado com sucesso.\n\n- Consulta: FatoFechamento\n- Tabela Salva: FatoFechamento_v2"
        else:
            corpo = f"O script de atualização falhou.\n\n{detalhes_erro}"
            
        enviar_email_status(subject=assunto, body=corpo)

if __name__ == "__main__":
    main()
