import functions_framework
import requests

# Supondo que essa função já existe com a lógica completa
def download_dados_farm(modulo, primeiro_registro=0):
    print(f"Processando download para modulo={modulo}, primeiro_registro={primeiro_registro}")
    # Aqui vai toda a lógica anterior: requisições, salvar no bucket, retries, etc.
    # Ela já usa 'modulo' dinamicamente em URLs, caminhos de arquivo e consolidação
    return "Download concluído", 200

@functions_framework.http
def produtos_dados_download_farm(request):
    print("Iniciando execução da função produtos_dados_download_farm.")
    request_json = request.get_json(silent=True)
    modulo = request_json.get('modulo', 'produto') if request_json else 'produto'  # Default 'produto'
    primeiro_registro = request_json.get('primeiroRegistro', 0) if request_json else 0
    print(f"Recebido: modulo={modulo}, primeiro_registro={primeiro_registro}")
    return download_dados_farm(modulo, primeiro_registro)