import win32com.client as win32
import os

# O e-mail será enviado para o destinatário especificado abaixo
RECIPIENT_EMAIL = "cesargl@sebraesp.com.br"

def enviar_email_status(subject: str, body: str):
    """
    Envia um e-mail usando o cliente Outlook local, aproveitando a autenticação do Windows.
    Não requer senha no código.
    """
    try:
        print(f"Tentando enviar e-mail para '{RECIPIENT_EMAIL}' via Outlook...")
        
        # Conecta-se ao aplicativo Outlook
        outlook = win32.Dispatch('outlook.application')
        
        # Cria um novo item de e-mail
        mail = outlook.CreateItem(0)
        
        # Define os campos do e-mail
        mail.To = RECIPIENT_EMAIL
        mail.Subject = subject
        mail.Body = body
        
        # Envia o e-mail
        mail.Send()
        
        print(f"SUCESSO: E-mail de status enviado para {RECIPIENT_EMAIL} através do Outlook.")

    except Exception as e:
        print("\n" + "="*50)
        print("ERRO: FALHA AO ENVIAR O E-MAIL VIA OUTLOOK.")
        print("Possíveis causas:")
        print("1. O Microsoft Outlook não está instalado neste computador.")
        print("2. O Outlook não está configurado com uma conta de e-mail.")
        print("3. O Outlook exibiu um aviso de segurança que precisa ser aceito manualmente.")
        print(f"Detalhes do erro: {e}")
        print("="*50 + "\n")

