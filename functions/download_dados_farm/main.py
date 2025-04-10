import functions_framework
import requests
import pandas as pd
import pyarrow as pa
import pyarrow.parquet as pq
import io
import time
from google.cloud import storage
from google.cloud import bigquery
import os
from flask import jsonify

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

def process_page(modulo, primeiro_registro, qtd_registros, storage_client, bucket):
    url = f'https://api-sgf-gateway.triersistemas.com.br/sgfpod1/rest/integracao/{modulo}/obter-todos-v1?primeiroRegistro={primeiro_registro}&quantidadeRegistros={qtd_registros}'
    headers = {'Authorization': f'Bearer {TOKEN}'}

    try:
        response = make_request_with_retries(url, headers)
        data = response.json()
        print(f"Dados recebidos da API: {len(data)} registros.")
    except Exception as e:
        print(f"Erro ao buscar dados da API: {str(e)}")
        return None, f"Erro ao buscar dados: {str(e)}"

    if not data:
        print("Nenhum dado retornado.")
        return None, "Nenhum dado retornado."

    num_registros_retornados = len(data)
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
        print(f"Página processada e salva no Cloud Storage: {blob_name}, Registros: {num_registros_retornados:03d}")

        return df, None
    except Exception as e:
        print(f"Erro ao processar dados: {str(e)}")
        return None, f"Erro ao processar dados: {str(e)}"

@functions_framework.http
def download_dados_farm(request):
    print("Iniciando execução da função download_dados_farm.")

    # Teste simples para saber se está vivo
    if request.method == "GET":
        return "API online. Envie um POST com JSON {'modulo': 'nome_modulo'}", 200

    request_json = request.get_json(silent=True)

    if not request_json or 'modulo' not in request_json:
        return "Parâmetro 'modulo' é obrigatório no corpo da requisição.", 400

    modulo = request_json['modulo']
    primeiro_registro = request_json.get('primeiroRegistro', 0)
    print(f"Recebido: modulo={modulo}, primeiro_registro={primeiro_registro}")

    qtd_registros = 999
    storage_client = storage.Client()
    bucket = storage_client.bucket("farmacia-data-bucket-001")

    all_dfs = []
    current_registro = primeiro_registro
    has_more_data = True

    while has_more_data:
        df, error = process_page(modulo, current_registro, qtd_registros, storage_client, bucket)

        if error:
            return error, 500

        if df is None or len(df) == 0:
            has_more_data = False
        else:
            all_dfs.append(df)
            current_registro += len(df)

            if len(df) < qtd_registros:
                has_more_data = False

    if not all_dfs:
        return "Nenhum dado processado", 200

    consolidated_df = pd.concat(all_dfs, ignore_index=True)
    consolidated_blob_name = f"{modulo}/consolidado/{modulo}_consolidado.parquet"

    try:
        buffer = io.BytesIO()
        table = pa.Table.from_pandas(consolidated_df, preserve_index=False)
        pq.write_table(table, buffer)
        buffer.seek(0)
        blob = bucket.blob(consolidated_blob_name)
        blob.upload_from_file(buffer, content_type='application/octet-stream')
        print(f"Arquivo consolidado salvo: {consolidated_blob_name}")
    except Exception as e:
        return f"Erro ao salvar arquivo consolidado: {str(e)}", 500

    try:
        client = bigquery.Client()
        dataset_id = 'farmacia_data'
        table_id = f'{modulo}_raw'
        table_ref = client.dataset(dataset_id).table(table_id)

        job_config = bigquery.LoadJobConfig(
            write_disposition=bigquery.WriteDisposition.WRITE_APPEND,
            source_format=bigquery.SourceFormat.PARQUET,
        )

        job = client.load_table_from_dataframe(
            consolidated_df, table_ref, job_config=job_config
        )
        job.result()

        print(f"Dados carregados com sucesso no BigQuery: {dataset_id}.{table_id}")
        return jsonify({
            "mensagem": "Processamento concluído",
            "registros_processados": len(consolidated_df)
        }), 200
    except Exception as e:
        print(f"Erro ao enviar para BigQuery: {str(e)}")
        return f"Erro ao enviar para BigQuery: {str(e)}", 500
