import functions_framework
import requests
import os
from typing import Dict, Any, Tuple

@functions_framework.http
def produtos_dados_download_farm(request) -> Tuple[str, int]:
    """
    Função que orquestra o download de dados de produtos para múltiplas filiais.

    Args:
        request: Objeto de requisição HTTP

    Returns:
        Tuple[str, int]: Resposta e código HTTP
    """
    print("Iniciando execução da função produtos_dados_download_farm.")

    # URL da função de download real
    DOWNLOAD_URL = os.getenv('DOWNLOAD_FUNCTION_URL',
                             "https://southamerica-east1-quick-woodland-453702-g2.cloudfunctions.net/download-dados-farm")

    # Obtem parâmetros
    request_json = request.get_json(silent=True)
    if request_json:
        modulo = request_json.get('modulo', 'produto')
        primeiro_registro = request_json.get('primeiroRegistro', 0)
    else:
        modulo = request.args.get('modulo', 'produto')
        try:
            primeiro_registro = int(request.args.get('primeiroRegistro', '0'))
        except ValueError:
            return "primeiroRegistro deve ser um número inteiro", 400

    if primeiro_registro < 0:
        return "primeiroRegistro não pode ser negativo", 400

    print(f"Recebido: modulo={modulo}, primeiro_registro={primeiro_registro}")

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

    try:
        response = requests.post(DOWNLOAD_URL, json=payload, timeout=60)
        response.raise_for_status()
        return response.text, 200
    except requests.Timeout:
        return "Timeout ao chamar download-dados-farm", 504
    except requests.RequestException as e:
        print(f"Erro ao chamar download-dados-farm: {str(e)}")
        return f"Erro ao chamar download-dados-farm: {str(e)}", 500