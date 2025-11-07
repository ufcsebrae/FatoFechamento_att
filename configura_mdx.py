import os
import sys
import clr

# --- Caminho da DLL conforme a imagem ---
# O 'r' antes da string garante que as barras invertidas sejam lidas corretamente.
dll_path = r"C:\Arquivos de Programas\On-premises data gateway\Microsoft.AnalysisServices.AdomdClient.dll"

# 1. Verifica se o arquivo da DLL realmente existe no caminho fornecido
if not os.path.exists(dll_path):
    # Se não encontrar, levanta um erro claro para o usuário.
    raise FileNotFoundError(
        f"ERRO: A DLL não foi encontrada no caminho especificado: '{dll_path}'.\n"
        "Por favor, confirme se o 'On-premises data gateway' está instalado neste local."
    )

# 2. Tenta carregar a DLL usando o caminho completo
try:
    clr.AddReference(dll_path)
    print(f"SUCESSO: DLL 'Microsoft.AnalysisServices.AdomdClient.dll' carregada de:\n{dll_path}")
except Exception as e:
    print(f"FALHA: Não foi possível carregar a DLL do caminho '{dll_path}'.\nErro: {e}")
    sys.exit(1) # Encerra o script se não conseguir carregar a DLL

# 3. Agora que a DLL foi carregada, a importação do Pyadomd deve funcionar
try:
    from pyadomd import Pyadomd
    print("SUCESSO: Módulo 'pyadomd' importado corretamente.")
except ImportError as e:
    print(f"ERRO: Falha ao importar 'pyadomd' mesmo após carregar a DLL.\nErro: {e}")
    sys.exit(1)

# Pronto para usar!
