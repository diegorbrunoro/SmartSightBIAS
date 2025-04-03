import functions_framework
import requests
import pandas as pd
import pyarrow as pa
import pyarrow.parquet as pq
import io
import time
from google.cloud import storage
from datetime import datetime, timedelta

# Token hardcoded
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
def vendas_dados_download_farm(request):
    print("Iniciando execução da função vendas_dados_download_farm.")
    request_json = request.get_json(silent=True)
    primeiro_registro = request_json.get('primeiroRegistro', 0) if request_json else 0
    qtd_registros = 999  # Máximo permitido pela API
    modulo = 'venda'
    max_paginas = 1000
    delay_entre_requisicoes = 1
    max_iteracoes_por_execucao = 15

    print(f"Parâmetros recebidos: primeiro_registro={primeiro_registro}, qtd_registros={qtd_registros}")

    storage_client = storage.Client()
    bucket = storage_client.bucket("farmacia-data-bucket-001")
    iteracao_inicial = primeiro_registro // qtd_registros

    while True:
        iteracao = primeiro_registro // qtd_registros
        iteracao_str = f"{iteracao:03d}"
        print(f"Iteração: {iteracao_str}")

        if iteracao >= max_paginas:
            print(f"Limite máximo de {max_paginas} iterações atingido em {primeiro_registro}.")
            consolidate_url = "https://southamerica-east1-quick-woodland-453702-g2.cloudfunctions.net/consolidate-dados-farm"
            requests.post(consolidate_url, json={
                "module": "venda",
                "output_file": "venda/consolidado/vendas_inicial.parquet"
            })
            return f"Limite máximo de {max_paginas} iterações atingido.", 200

        if (iteracao - iteracao_inicial) >= max_iteracoes_por_execucao:
            print(f"Limite de {max_iteracoes_por_execucao} iterações por execução atingido. Reiniciando a partir de primeiro_registro={primeiro_registro}.")
            reinvoke_url = "https://southamerica-east1-quick-woodland-453702-g2.cloudfunctions.net/vendas-dados-download-farm"
            requests.post(reinvoke_url, json={"primeiroRegistro": primeiro_registro})
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
            print(f"Nenhum dado retornado para iteração {iteracao_str} (página {primeiro_registro}). Processamento concluído.")
            consolidate_url = "https://southamerica-east1-quick-woodland-453702-g2.cloudfunctions.net/consolidate-dados-farm"
            requests.post(consolidate_url, json={
                "module": "venda",
                "output_file": "venda/consolidado/vendas_inicial.parquet"
            })
            return "Nenhum dado retornado. Processamento concluído.", 200

        num_registros_retornados = len(data)
        if num_registros_retornados != 999:
            print(f"Aviso: Número de registros retornados na iteração {iteracao_str} é diferente de 999. Foram retornados {num_registros_retornados} registros.")

        ultimo_registro = primeiro_registro + num_registros_retornados - 1
        blob_name = f"{modulo}/In_{iteracao_str}_{modulo}_pg_{primeiro_registro}_a_{ultimo_registro}.parquet"

        try:
            blob = bucket.blob(blob_name)
            # Converter JSON pra Parquet
            df = pd.DataFrame(data)
            buffer = io.BytesIO()
            table = pa.Table.from_pandas(df, preserve_index=False)
            pq.write_table(table, buffer)
            buffer.seek(0)
            blob.upload_from_file(buffer, content_type='application/octet-stream')
            print(f"Página processada: {blob_name}, Registros: {num_registros_retornados:03d}")
        except Exception as e:
            print(f"Erro ao salvar no bucket: {str(e)}")
            return f"Erro ao salvar no bucket: {str(e)}", 500

        if num_registros_retornados < qtd_registros:
            print(f"Última iteração concluída. Retornados {num_registros_retornados} registros, menos que {qtd_registros}.")
            consolidate_url = "https://southamerica-east1-quick-woodland-453702-g2.cloudfunctions.net/consolidate-dados-farm"
            requests.post(consolidate_url, json={
                "module": "venda",
                "output_file": "venda/consolidado/vendas_inicial.parquet"
            })
            return f"Iteração {iteracao_str} salva com sucesso! Processamento concluído. Registros: {num_registros_retornados}", 200

        primeiro_registro += qtd_registros
        print(f"Preparando próxima iteração a partir de primeiro_registro={primeiro_registro}")
        
        print(f"Aguardando {delay_entre_requisicoes} segundos antes da próxima requisição...")
        time.sleep(delay_entre_requisicoes)

    return "Processamento concluído de forma inesperada.", 200

if __name__ == "__main__":
    import os
    port = int(os.getenv("PORT", 8080))
    print(f"Iniciando o servidor na porta: {port}")
    try:
        functions_framework.run_http(vendas_dados_download_farm, host="0.0.0.0", port=port)
        print("Servidor iniciado com sucesso!")
    except Exception as e:
        print(f"Erro ao iniciar o servidor: {str(e)}")
        raise