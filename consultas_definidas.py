from typing import Dict
from utils import carregar_sql
from conexoes import CONEXOES

class Consulta:
    def __init__(self, titulo: str, sql: str, tipo: str, conexao: str):
        self.titulo = titulo
        self.tipo = tipo
        self.sql = sql
        self.conexao = conexao

        if conexao not in CONEXOES:
            raise ValueError(f"Conexão '{conexao}' não está definida em CONEXOES.py")

        self.info_conexao = CONEXOES[conexao]

# --- CORREÇÃO DEFINITIVA ---
# A chave aqui deve ser "FatoFechamento" para corresponder à chamada no main.py
consultas: Dict[str, Consulta] = {
    "FatoFechamento": Consulta(
        titulo="FatoFechamento",
        tipo="sql",
        sql=carregar_sql("FatoFechamento.sql"),
        conexao="SPSVSQL39"
    )
}
