import functions_framework
import requests
import pandas as pd
import pyarrow as pa
import pyarrow.parquet as pq
import io
import time
from google.cloud import storage

TOKEN = 'eyJhbGciOiJIUzI1NiJ9.eyJjb2RfZmlsaWFsIjoiMSIsInNjb3BlIjpbImRyb2dhcmlhIl0sInRva2VuX2ludGVncmFjYW8iOiJ0cnVlIiwiY29kX2Zhcm1hY2lhIjoiMjA4OTAiLCJleHAiOjQwNzA5MTk2MDAsImlhdCI6MTY5MDgyNTk2NiwianRpIjoiOThiZGY0OTUtNDUxNy00NGEzLTg1ODktMzNkYzI3NjJiMmE5IiwiY29kX3VzdWFyaW8iOiI5IiwiYXV0aG9yaXRpZXMiOlsiQVBJX0lOVEVHUkFDQU8iXX0.7CnITyJuUhAZbKO-uothoZkHWidKv9lvtlN_d-ZLJ7k'

def make_request_with_retries(url, headers, max_retries=10, timeout=20, delay=60):
    for attempt in range(max_retries + 1):
        try:
            print(f"Fazendo requisição para URL: {url}")
            response = requests.get(url, headers=headers, timeout=timeout)
            response.raise_for_status()
            print(f"Requisição bem-sucedida. Status: {response.status_code}, Resposta: {response.text[:100]}...")
            return response
        except (requests.exceptions.RequestException, requests.exceptions.Timeout) as e:
            if attempt < max_retries:
                print(f'Timeout na tentativa {attempt + 1:02d}. Erro: {str(e)}. Tentando novamente em {delay} segundos...')
                time.sleep(delay)
            else:
                print(f'Todas as tentativas ({max_retries}) falharam. Último erro: {str(e)}')
                raise

@functions_framework.http
def download_dados_farm(request):
    print("Iniciando execução da função download_dados_farm.")
    request_json = request.get_json(silent=True)
    modulo = request_json.get('modulo', 'produto') if request_json else 'produto'
    primeiro_registro = request_json.get('primeiroRegistro', 0) if request_json else 0
    print(f"Recebido: modulo={modulo}, primeiro_registro={primeiro_registro}")

    qtd_registros = 999
    url = f'https://api-sgf-gateway.triersistemas.com.br/sgfpod1/rest/integracao/{modulo}/obter-todos-v1?primeiroRegistro={primeiro_registro}&quantidadeRegistros={qtd_registros}'
    headers = {'Authorization': f'Bearer {TOKEN}'}

    try:
        response = make_request_with_retries(url, headers)
        data = response.json()
        print(f"Dados recebidos da API: {len(data)} registros.")
    except Exception as e:
        print(f"Erro ao buscar dados da API: {str(e)}")
        return f"Erro ao buscar dados: {str(e)}", 500

    if not data:
        print("Nenhum dado retornado.")
        return "Nenhum dado retornado.", 200

    num_registros_retornados = len(data)
    storage_client = storage.Client()
    bucket = storage_client.bucket("farmacia-data-bucket-001")
    iteracao = primeiro_registro // qtd_registros
    iteracao_str = f"{iteracao:03d}"
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
        print(f"Página processada: {blob_name}, Registros: {num_registros_retornados:03d}")
        return "Processamento concluído", 200
    except Exception as e:
        print(f"Erro ao salvar no bucket: {str(e)}")
        return