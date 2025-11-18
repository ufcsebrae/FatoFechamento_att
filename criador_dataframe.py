import pandas as pd
from pyadomd import Pyadomd
import logging

logger = logging.getLogger("logger_financa")

class CriadorDataFrame:
    def __init__(self, funcao_conexao, conexao: str, consulta: str, tipo: str = "sql"):
        self.funcao_conexao = funcao_conexao
        self.conexao_nome = conexao
        self.consulta = consulta
        self.tipo = tipo.lower()

    def executar(self) -> pd.DataFrame:
        try:
            # A engine/string de conexão é obtida pela função passada
            info_conexao = self.funcao_conexao(self.conexao_nome)

            if self.tipo in ("sql", "azure_sql"):
                # info_conexao aqui é uma engine SQLAlchemy
                return pd.read_sql_query(self.consulta, info_conexao)

            elif self.tipo == "mdx":
                # info_conexao aqui é a string de conexão OLAP
                with Pyadomd(info_conexao) as conexao:
                    with conexao.cursor() as cursor:
                        cursor.execute(self.consulta)
                        dados = cursor.fetchall()
                        colunas = [col.name for col in cursor.description]
                        return pd.DataFrame(dados, columns=colunas)

            else:
                raise ValueError(f"Tipo de consulta '{self.tipo}' não suportado.")

        except Exception as erro:
            logger.error(f"Erro ao executar a consulta (Tipo: {self.tipo}, Conexão: {self.conexao_nome}): {erro}")
            logger.error(traceback.format_exc())
            return pd.DataFrame()

