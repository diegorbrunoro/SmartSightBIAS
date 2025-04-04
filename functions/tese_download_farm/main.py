import functions_framework
import requests

@functions_framework.http
def tese_download_farm(request):
    print("Iniciando execução da função tese_download_farm.")
    request_json = request.get_json(silent=True)
    modulo = request_json.get('modulo', 'produto') if request_json else 'produto'
    primeiro_registro = request_json.get('primeiroRegistro', 0) if request_json else 0
    print(f"Recebido: modulo={modulo}, primeiro_registro={primeiro_registro}")

    # Chama a função download-dados-farm via HTTP
    url = "https://southamerica-east1-quick-woodland-453702-g2.cloudfunctions.net/download-dados-farm"
    payload = {"modulo": modulo, "primeiroRegistro": primeiro_registro}
    try:
        response = requests.post(url, json=payload)
        response.raise_for_status()
        return response.text, 200
    except requests.RequestException as e:
        print(f"Erro ao chamar download-dados-farm: {str(e)}")
        return f"Erro ao chamar download-dados-farm: {str(e)}", 500