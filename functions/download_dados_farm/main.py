# main.py
import functions_framework
import requests
import pandas as pd
import pyarrow as pa
import pyarrow.parquet as pq
import io
import time
from google.cloud import storage
import logging

# Configurar logging
logging.basicConfig(level=logging.INFO)

# Token (mude para variável de ambiente ou Secret Manager)
TOKEN = 'eyJhbGciOiJIUzI1NiJ9.eyJjb2RfZmlsaWFsIjoiMSIsInNjb3BlIjpbImRyb2dhcmlhIl0sInRva2VuX2ludGVncmFjYW8iOiJ0cnVlIiwiY29kX2Zhcm1hY2lhIjoiMjA4OTAiLCJleHAiOjQwNzA5MTk2MDAsImlhdCI6MTY5MDgyNTk2NiwianRpIjoiOThiZGY0OTUtNDUxNy00NGEzLTg1ODktMzNkYzI3NjJiMmE5IiwiY29kX3VzdWFyaW8iOiI5IiwiYXV0aG9yaXRpZXMiOlsiQVBJX0lOVEVHUkFDQU8iXX0.7CnITyJuUhAZbKO-uothoZkHWidKv9lvtlN_d-ZLJ7k'

@functions_framework.http
def download_dados_farm(request):
    """
    Função genérica para baixar dados de uma API, paginar os resultados e salvá-los no Google Cloud Storage.
    
    Args:
        request: Requisição HTTP recebida pela Cloud Function.
                 Espera um JSON com os seguintes campos:
                 - modulo (str): Módulo da API a ser consultado (ex.: 'venda', 'produto', 'fornecedor').
                 - reinvoke_url (str): URL para reinvocar a função em caso de pausa.
                 - consolidate_output_file (str): Caminho do arquivo consolidado no bucket.
                 - primeiroRegistro (int, opcional): Registro inicial para a paginação (padrão: 0).
    
    Returns:
        tuple: (mensagem, status_code) indicando o resultado da execução.
    """
    # Extrair parâmetros do request
    request_json = request.get_json(silent=True)
    if not request_json:
        logging.error("Nenhum JSON fornecido na requisição.")
        return "Requisição deve conter um JSON com 'modulo', 'reinvoke_url' e 'consolidate_output_file'.", 400

    modulo = request_json.get('modulo')
    reinvoke_url = request_json.get('reinvoke_url')
    consolidate_output_file = request_json.get('consolidate_output_file')

    # Validação dos parâmetros obrigatórios
    if not modulo:
        logging.error("Parâmetro 'modulo' não fornecido.")
        return "Parâmetro 'modulo' é obrigatório.", 400
    if not reinvoke_url:
        logging.error("Parâmetro 'reinvoke_url' não fornecido.")
        return "Parâmetro 'reinvoke_url' é obrigatório.", 400
    if not consolidate_output_file:
        logging.error("Parâmetro 'consolidate_output_file' não fornecido.")
        return "Parâmetro 'consolidate_output_file' é obrigatório.", 400

    logging.info(f"Iniciando execução da função para módulo: {modulo}.")
    
    # Função interna para fazer requisições com retries
    def make_request_with_retries(url, headers, max_retries=10, timeout=20, delay=60):
        for attempt in range(max_retries + 1):
            try:
                logging.info(f"Fazendo requisição para URL: {url}")
                response = requests.get(url, headers=headers, timeout=timeout)
                response.raise_for_status()
                logging.info(f"Requisição bem-sucedida. Status: {response.status_code}, Resposta: {response.text[:100]}...")
                return response
            except (requests.exceptions.RequestException, requests.exceptions.Timeout) as e:
                if attempt < max_retries:
                    logging.warning(f'Timeout na tentativa {attempt + 1:02d}. Erro: {str(e)}. Tentando novamente em {delay} segundos...')
                    time.sleep(delay)
                else:
                    logging.error(f'Todas as tentativas ({max_retries}) falharam. Último erro: {str(e)}')
                    raise

    # Validação de entrada para primeiro_registro
    try:
        primeiro_registro = int(request_json.get('primeiroRegistro', 0))
        if primeiro_registro < 0:
            raise ValueError("primeiroRegistro deve ser maior ou igual a 0")
    except (ValueError, TypeError) as e:
        logging.error(f"Erro nos parâmetros: {str(e)}")
        return f"Parâmetro inválido: {str(e)}", 400

    # Definir constantes dentro da função
    QTD_REGISTROS = 999  # Máximo permitido pela API
    MAX_PAGINAS = 1000
    DELAY_ENTRE_REQUISICOES = 1
    MAX_ITERACOES_POR_EXECUCAO = 15

    logging.info(f"Parâmetros recebidos: primeiro_registro={primeiro_registro}, qtd_registros={QTD_REGISTROS}")

    storage_client = storage.Client()
    bucket = storage_client.bucket("farmacia-data-bucket-001")
    iteracao_inicial = primeiro_registro // QTD_REGISTROS

    while True:
        iteracao = primeiro_registro // QTD_REGISTROS
        iteracao_str = f"{iteracao:03d}"
        logging.info(f"Iteração: {iteracao_str}")

        if iteracao >= MAX_PAGINAS:
            logging.info(f"Limite máximo de {MAX_PAGINAS} iterações atingido em {primeiro_registro}.")
            consolidate_url = "https://southamerica-east1-quick-woodland-453702-g2.cloudfunctions.net/consolidate-dados-farm"
            try:
                response = requests.post(consolidate_url, json={
                    "module": modulo,
                    "output_file": consolidate_output_file
                })
                response.raise_for_status()
                logging.info(f"Consolidação iniciada com sucesso: {response.status_code}")
            except requests.exceptions.RequestException as e:
                logging.error(f"Erro ao chamar consolidate-dados-farm: {str(e)}")
                return f"Erro ao iniciar consolidação: {str(e)}", 500
            return f"Limite máximo de {MAX_PAGINAS} iterações atingido.", 200

        if (iteracao - iteracao_inicial) >= MAX_ITERACOES_POR_EXECUCAO:
            logging.info(f"Limite de {MAX_ITERACOES_POR_EXECUCAO} iterações por execução atingido. Reiniciando a partir de primeiro_registro={primeiro_registro}.")
            try:
                response = requests.post(reinvoke_url, json={"primeiroRegistro": primeiro_registro})
                response.raise_for_status()
                logging.info(f"Função reinvocada com sucesso: {response.status_code}")
            except requests.exceptions.RequestException as e:
                logging.error(f"Erro ao reinvocar a função: {str(e)}")
                return f"Erro ao reinvocar a função: {str(e)}", 500
            return f"Processamento pausado após {MAX_ITERACOES_POR_EXECUCAO} iterações. Reiniciando a partir de {primeiro_registro}.", 200

        blob_name = f"{modulo}/In_{iteracao_str}_{modulo}_pg_{primeiro_registro}_a_{primeiro_registro + QTD_REGISTROS - 1}.parquet"
        blob = bucket.blob(blob_name)
        if blob.exists():
            logging.info(f"Arquivo {blob_name} já existe, pulando iteração {iteracao_str}.")
            primeiro_registro += QTD_REGISTROS
            continue

        url = f'https://api-sgf-gateway.triersistemas.com.br/sgfpod1/rest/integracao/{modulo}/obter-todos-v1?primeiroRegistro={primeiro_registro}&quantidadeRegistros={QTD_REGISTROS}'
        headers = {'Authorization': f'Bearer {TOKEN}'}

        try:
            response = make_request_with_retries(url, headers)
            data = response.json()
            logging.info(f"Dados recebidos da API: {len(data)} registros.")
        except Exception as e:
            logging.error(f"Erro ao buscar dados da API: {str(e)}")
            return f"Erro ao buscar dados: {str(e)}", 500

        if not data:
            logging.info(f"Nenhum dado retornado para iteração {iteracao_str} (página {primeiro_registro}). Processamento concluído.")
            consolidate_url = "https://southamerica-east1-quick-woodland-453702-g2.cloudfunctions.net/consolidate-dados-farm"
            try:
                response = requests.post(consolidate_url, json={
                    "module": modulo,
                    "output_file": consolidate_output_file
                })
                response.raise_for_status()
                logging.info(f"Consolidação iniciada com sucesso: {response.status_code}")
            except requests.exceptions.RequestException as e:
                logging.error(f"Erro ao chamar consolidate-dados-farm: {str(e)}")
                return f"Erro ao iniciar consolidação: {str(e)}", 500
            return "Nenhum dado retornado. Processamento concluído.", 200

        num_registros_retornados = len(data)
        if num_registros_retornados != 999:
            logging.warning(f"Aviso: Número de registros retornados na iteração {iteracao_str} é diferente de 999. Foram retornados {num_registros_retornados} registros.")

        ultimo_registro = primeiro_registro + num_registros_retornados - 1
        blob_name = f"{modulo}/In_{iteracao_str}_{modulo}_pg_{primeiro_registro}_a_{ultimo_registro}.parquet"

        try:
            df = pd.DataFrame(data)
            buffer = io.BytesIO()
            table = pa.Table.from_pandas(df, preserve_index=False)
            pq.write_table(table, buffer)
            buffer.seek(0)
            blob = bucket.blob(blob_name)
            blob.upload_from_file(buffer, content_type='application/octet-stream')
            logging.info(f"Página processada: {blob_name}, Registros: {num_registros_retornados:03d}")
        except Exception as e:
            logging.error(f"Erro ao salvar no bucket: {str(e)}")
            return f"Erro ao salvar no bucket: {str(e)}", 500

        if num_registros_retornados < QTD_REGISTROS:
            logging.info(f"Última iteração concluída. Retornados {num_registros_retornados} registros, menos que {QTD_REGISTROS}.")
            consolidate_url = "https://southamerica-east1-quick-woodland-453702-g2.cloudfunctions.net/consolidate-dados-farm"
            try:
                response = requests.post(consolidate_url, json={
                    "module": modulo,
                    "output_file": consolidate_output_file
                })
                response.raise_for_status()
                logging.info(f"Consolidação iniciada com sucesso: {response.status_code}")
            except requests.exceptions.RequestException as e:
                logging.error(f"Erro ao chamar consolidate-dados-farm: {str(e)}")
                return f"Erro ao iniciar consolidação: {str(e)}", 500
            return f"Iteração {iteracao_str} salva com sucesso! Processamento concluído. Registros: {num_registros_retornados}", 200

        primeiro_registro += QTD_REGISTROS
        logging.info(f"Preparando próxima iteração a partir de primeiro_registro={primeiro_registro}")
        logging.info(f"Aguardando {DELAY_ENTRE_REQUISICOES} segundos antes da próxima requisição...")
        time.sleep(DELAY_ENTRE_REQUISICOES)

    return "Processamento concluído de forma inesperada.", 200