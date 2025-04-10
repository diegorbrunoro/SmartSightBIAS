import functions_framework
import requests
import os
from typing import Dict, Any, Tuple

@functions_framework.http
def fornecedor_download_farm(request) -> Tuple[str, int]:
    """
    Função que faz download de dados de farmácia.
    
    Args:
        request: Objeto de requisição HTTP
        
    Returns:
        Tuple[str, int]: Resposta e código HTTP
        
    Raises:
        ValueError: Se os parâmetros forem inválidos
    """
    print("Iniciando execução da função fornecedor_download_farm.")
    
    # Configuração via variável de ambiente
    DOWNLOAD_URL = os.getenv('DOWNLOAD_FUNCTION_URL', 
                           "https://southamerica-east1-quick-woodland-453702-g2.cloudfunctions.net/download-dados-farm")
    
    # Obtém parâmetros
    request_json = request.get_json(silent=True)
    if request_json:
        modulo = request_json.get('modulo', 'fornecedor')
        primeiro_registro = request_json.get('primeiroRegistro', 0)
    else:
        modulo = request.args.get('modulo', 'fornecedor')
        try:
            primeiro_registro = int(request.args.get('primeiroRegistro', '0'))
        except ValueError:
            return "primeiroRegistro deve ser um número inteiro", 400

    # Validação básica
    if primeiro_registro < 0:
        return "primeiroRegistro não pode ser negativo", 400
        
    print(f"Recebido: modulo={modulo}, primeiro_registro={primeiro_registro}")

    # Chama a função download-dados-farm
    payload = {"modulo": modulo, "primeiroRegistro": primeiro_registro}
    try:
        response = requests.post(DOWNLOAD_URL, json=payload, timeout=30)
        response.raise_for_status()
        return response.text, 200
    except requests.Timeout:
        return "Timeout ao chamar download-dados-farm", 504
    except requests.RequestException as e:
        print(f"Erro ao chamar download-dados-farm: {str(e)}")
        return f"Erro ao chamar download-dados-farm: {str(e)}", 500