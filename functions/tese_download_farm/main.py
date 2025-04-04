# teste_download_farm.py
import functions_framework
import logging
from utils import download_dados_farm  # Importa a função genérica de utils.py

# Configurar logging
logging.basicConfig(level=logging.INFO)

# Função de teste
@functions_framework.http
def teste_download_farm(request):
    # Inicia a função
    modulo = 'fornecedor'  # Armazena o nome do módulo

    # Chama a função genérica download_dados_farm
    result = download_dados_farm(
        request,
        modulo=modulo,
        reinvoke_url="https://southamerica-east1-quick-woodland-453702-g2.cloudfunctions.net/teste-download-farm",
        consolidate_output_file="fornecedor/consolidado/fornecedores_inicial.parquet"
    )

    # Informa em log que chamou download_dados_farm e qual o módulo
    logging.info(f"Função download_dados_farm chamada para o módulo: {modulo}")

    # Encerra a função retornando o resultado
    return result

if __name__ == "__main__":
    import os
    port = int(os.getenv("PORT", 8080))
    logging.info(f"Iniciando o servidor na porta: {port}")
    try:
        # Rodar a função teste_download_farm localmente
        functions_framework.run_http(teste_download_farm, host="0.0.0.0", port=port)
        logging.info("Servidor iniciado com sucesso!")
    except Exception as e:
        logging.error(f"Erro ao iniciar o servidor: {str(e)}")
        raise