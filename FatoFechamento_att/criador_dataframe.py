import pandas as pd
import logging

# Obtém o logger configurado em main.py
logger = logging.getLogger("logger_financa")

class CriadorDataFrame:
    def __init__(self, funcao_conexao, conexao_nome, consulta, tipo):
        self.funcao_conexao = funcao_conexao
        self.conexao_nome = conexao_nome
        self.consulta = consulta
        self.tipo = tipo

    def executar(self) -> pd.DataFrame:
        """
        Executa a consulta de leitura (SELECT) e retorna um DataFrame.
        """
        engine = None
        try:
            # Obtém uma engine de conexão
            engine = self.funcao_conexao(self.conexao_nome)
            if not engine:
                 raise ConnectionError(f"A função de conexão não retornou uma engine para '{self.conexao_nome}'.")
            
            if self.tipo == "sql":
                return pd.read_sql_query(self.consulta, engine)
            else:
                raise NotImplementedError(f"O tipo de consulta '{self.tipo}' não está implementado.")
                
        except Exception as e:
            logger.error(f"Erro final dentro do CriadorDataFrame ao ler a consulta SQL.", exc_info=True)
            raise e
        finally:
             if engine:
                  engine.dispose()
