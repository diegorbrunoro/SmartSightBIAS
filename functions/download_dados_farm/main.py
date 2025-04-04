import requests
import pandas as pd
import pyarrow as pa
import pyarrow.parquet as pq
import io
import time
from google.cloud import storage

# Token original mantido
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

def download_dados_farm(event, context):
    # Extrair parâmetros do evento
    data = event.get('data', {})
    modulo = data.get('modulo', 'produto')
    primeiro_registro = data.get('primeiroRegistro', 0)

    print(f"Iniciando download para módulo: {modulo}")
    qtd_registros = 999
    max_paginas = 1000
    delay_entre_requisicoes = 1
    max_iteracoes_por_execucao = 15

    storage_client = storage.Client()
    bucket = storage_client.bucket("farmacia-data-bucket-001")
    iteracao_inicial = primeiro_registro // qtd_registros

    while True:
        iteracao = primeiro_registro // qtd_registros
        iteracao_str = f"{iteracao:03d}"
        print(f"Iteração: {iteracao_str}")

        if iteracao >= max_paginas:
            consolidate_url = "https://southamerica-east1-quick-woodland-453702-g2.cloudfunctions.net/consolidate-dados-farm"
            output_file_path = f"{modulo}/consolidado/{modulo}_inicial.parquet"
            try:
                response = requests.post(consolidate_url, json={"module": modulo, "output_file": output_file_path})
                response.raise_for_status()
                print(f"Consolidação solicitada com sucesso para {output_file_path}")
            except requests.RequestException as e:
                print(f"Erro ao chamar consolidação: {str(e)}")
                return f"Erro na consolidação: {str(e)}", 500
            return f"Limite máximo de {max_paginas} iterações atingido.", 200

        if (iteracao - iteracao_inicial) >= max_iteracoes_por_execucao:
            reinvoke_url = "https://southamerica-east1-quick-woodland-453702-g2.cloudfunctions.net/produtos-dados-download-farm"
            try:
                response = requests.post(reinvoke_url, json={"primeiroRegistro": primeiro_registro, "modulo": modulo})
                response.raise_for_status()
                print(f"Reinvocação bem-sucedida para primeiro_registro={primeiro_registro}, modulo={modulo}")
            except requests.RequestException as e:
                print(f"Erro ao reinvocar a função: {str(e)}")
                return f"Erro ao reinvocar: {str(e)}", 500
            return f"Processamento pausado após {max_iteracoes_por_execucao} iterações. Reiniciando a partir de {primeiro_registro}.", 200

        blob_name = f"{modulo}/In_{iteracao_str}_{modulo}_pg_{primeiro_registro}_a_{primeiro_registro + qtd_registros - 1}.parquet"
        blob = bucket.blob(blob_name)
        if blob.exists():
            print(f"Arquivo {blob_name} já existe, pulando iteração {iteracao_str}.")
            primeiro_registro += qtd_registros
            continue

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
            consolidate_url = "https://southamerica-east1-quick-woodland-453702-g2.cloudfunctions.net/consolidate-dados-farm"
            output_file_path = f"{modulo}/consolidado/{modulo}_inicial.parquet"
            try:
                response = requests.post(consolidate_url, json={"module": modulo, "output_file": output_file_path})
                response.raise_for_status()
                print(f"Consolidação solicitada com sucesso para {output_file_path}")
            except requests.RequestException as e:
                print(f"Erro ao chamar consolidação: {str(e)}")
                return f"Erro na consolidação: {str(e)}", 500
            return "Nenhum dado retornado. Processamento concluído.", 200

        num_registros_retornados = len(data)
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
        except Exception as e:
            print(f"Erro ao salvar no bucket: {str(e)}")
            return f"Erro ao salvar no bucket: {str(e)}", 500

        if num_registros_retornados < qtd_registros:
            consolidate_url = "https://southamerica-east1-quick-woodland-453702-g2.cloudfunctions.net/consolidate-dados-farm"
            output_file_path = f"{modulo}/consolidado/{modulo}_inicial.parquet"
            try:
                response = requests.post(consolidate_url, json={"module": modulo, "output_file": output_file_path})
                response.raise_for_status()
                print(f"Consolidação solicitada com sucesso para {output_file_path}")
            except requests.RequestException as e:
                print(f"Erro ao chamar consolidação: {str(e)}")
                return f"Erro na consolidação: {str(e)}", 500
            return f"Iteração {iteracao_str} salva com sucesso! Processamento concluído. Registros: {num_registros_retornados}", 200

        primeiro_registro += qtd_registros
        print(f"Preparando próxima iteração a partir de primeiro_registro={primeiro_registro}")
        time.sleep(delay_entre_requisicoes)

    return "Processamento concluído de forma inesperada.", 200