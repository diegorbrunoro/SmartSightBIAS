import functions_framework
import requests
import os
import time
from typing import Dict, Any, Tuple

@functions_framework.http
def vendas_dados_download_farm(request) -> Tuple[str, int]:
    """
    Função que orquestra o download de dados de vendas para múltiplas filiais.

    Args:
        request: Objeto de requisição HTTP

    Returns:
        Tuple[str, int]: Resposta e código HTTP
    """
    print("Iniciando execução da função vendas_dados_download_farm.")

    # URL da função de download real
    DOWNLOAD_URL = os.getenv('DOWNLOAD_FUNCTION_URL',
                             "https://southamerica-east1-quick-woodland-453702-g2.cloudfunctions.net/download-dados-farm")
    print(f"Usando DOWNLOAD_URL: {DOWNLOAD_URL}")

    # Obtém parâmetros
    request_json = request.get_json(silent=True)
    if request_json:
        modulo = request_json.get('modulo', 'venda')
        primeiro_registro = request_json.get('primeiroRegistro', 0)
        print(f"Parâmetros recebidos via JSON: modulo={modulo}, primeiro_registro={primeiro_registro}")
    else:
        modulo = request.args.get('modulo', 'venda')
        try:
            primeiro_registro = int(request.args.get('primeiroRegistro', '0'))
            print(f"Parâmetros recebidos via query: modulo={modulo}, primeiro_registro={primeiro_registro}")
        except ValueError:
            print("Erro: primeiroRegistro deve ser um número inteiro")
            return "primeiroRegistro deve ser um número inteiro", 400

    # Valida módulo
    if modulo != 'venda':
        print(f"Erro: Módulo inválido. Esperado 'venda', recebido '{modulo}'")
        return "Módulo deve ser 'venda' para esta função", 400

    # Valida primeiro_registro
    if primeiro_registro < 0:
        print("Erro: primeiroRegistro não pode ser negativo")
        return "primeiroRegistro não pode ser negativo", 400

    # Lista de filiais e tokens (pode ser migrado para Secret Manager ou env vars futuramente)
    filiais = [
        {
            "filial": "01",
            "token": "eyJhbGciOiJIUzI1NiJ9.eyJjb2RfZmlsaWFsIjoiMSIsInNjb3BlIjpbImRyb2dhcmlhIl0sInRva2VuX2ludGVncmFjYW8iOiJ0cnVlIiwiY29kX2Zhcm1hY2lhIjoiMjA4OTAiLCJleHAiOjQwNzA5MTk2MDAsImlhdCI6MTY5MDgyNTk2NiwianRpIjoiOThiZGY0OTUtNDUxNy00NGEzLTg1ODktMzNkYzI3NjJiMmE5IiwiY29kX3VzdWFyaW8iOiI5IiwiYXV0aG9yaXRpZXMiOlsiQVBJX0lOVEVHUkFDQU8iXX0.7CnITyJuUhAZbKO-uothoZkHWidKv9lvtlN_d-ZLJ7k"
        },
        {
            "filial": "02",
            "token": "eyJhbGciOiJIUzI1NiJ9.eyJjb2RfZmlsaWFsIjoiMiIsInNjb3BlIjpbImRyb2dhcmlhIl0sInRva2VuX2ludGVncmFjYW8iOiJ0cnVlIiwiY29kX2Zhcm1hY2lhIjoiMjgyMDQiLCJleHAiOjQxMDI0NTU2MDAsImlhdCI6MTc0NTk1NDU1OSwianRpIjoiMDI4NzU1NmQtMjBlMC00Y2RiLWEyYTUtNWI5OTYwMDUxMzZjIiwiY29kX3VzdWFyaW8iOiIxNCIsImF1dGhvcml0aWVzIjpbIkFQSV9JTlRFR1JBQ0FPIl19.EyKhGJBgaAw3UoRFnHT1a2JlZ2U7BvqgsdG0WnLh1s4"
        }
    ]

    # Payload com múltiplas filiais
    payload = {
        "modulo": modulo,
        "primeiroRegistro": primeiro_registro,
        "filiais": filiais
    }
    print(f"Enviando payload: {payload}")

    # Faz a requisição com retries para erro 429
    max_retries = 5
    for attempt in range(max_retries):
        try:
            print(f"Tentativa {attempt + 1}/{max_retries}: Enviando requisição POST para {DOWNLOAD_URL}")
            response = requests.post(DOWNLOAD_URL, json=payload, timeout=60)
            print(f"Resposta recebida: status={response.status_code}, texto={response.text[:200]}")
            response.raise_for_status()
            return response.text, 200
        except requests.exceptions.HTTPError as e:
            if response.status_code == 429:
                wait_time = 2 ** attempt * 5  # Backoff exponencial: 5s, 10s, 20s, 40s, 80s
                print(f"Erro 429 (Too Many Requests) na tentativa {attempt + 1}. Aguardando {wait_time} segundos...")
                time.sleep(wait_time)
                if attempt == max_retries - 1:
                    print("Todas as tentativas falharam com erro 429")
                    return f"Erro ao chamar download-dados-farm: {str(e)}", 429
            else:
                print(f"Erro HTTP ao chamar download-dados-farm: {str(e)}")
                return f"Erro ao chamar download-dados-farm: {str(e)}", 500
        except requests.Timeout:
            print("Timeout ocorrido após 60 segundos")
            return "Timeout ao chamar download-dados-farm", 504
        except requests.RequestException as e:
            print(f"Erro detalhado ao chamar download-dados-farm: {str(e)}")
            return f"Erro ao chamar download-dados-farm: {str(e)}", 500
