import functions_framework
from google.cloud import storage
from google.cloud import bigquery
import pandas as pd
import pyarrow as pa
import pyarrow.parquet as pq
import io
import requests

@functions_framework.http
def consolidate_dados_farm(request):
    print("Iniciando execução da função consolidate_dados_farm.")
    request_json = request.get_json(silent=True)
    
    # Parâmetro module é obrigatório
    module = request_json.get('module') if request_json else None
    if not module:
        print("Erro: Parâmetro 'module' não fornecido no request.")
        return "Erro: Parâmetro 'module' é obrigatório.", 400

    # Configurações baseadas no module
    output_file = request_json.get('output_file', f"{module}/consolidado/{module}_inicial.parquet")
    table_name = request_json.get('table_name', f"quick-woodland-453702-g2.farmacia_data.{module}")
    deduplication_key = request_json.get('deduplication_key', None)  # Desativar deduplicação por padrão
    start_index = request_json.get('start_index', 0)
    max_files_per_execution = 100  # Processar 100 arquivos por vez

    bucket_name = "farmacia-data-bucket-001"
    source_prefix = f"{module}/"

    # Carregar e consolidar os novos dados do bucket
    storage_client = storage.Client()
    bucket = storage_client.bucket(bucket_name)
    blobs = list(storage_client.list_blobs(bucket_name, prefix=source_prefix))
    
    resultados = []
    schema = None  # Schema base inferido do primeiro arquivo
    blobs_found = False
    processed_files = 0
    total_processed = 0

    for i, blob in enumerate(blobs[start_index:]):
        if blob.name.endswith(".parquet"):
            blobs_found = True
            print(f"Processando arquivo: {blob.name}")
            try:
                buffer = io.BytesIO(blob.download_as_bytes())
                table = pq.read_table(buffer)
                
                # Definir schema base na primeira iteração
                if schema is None:
                    schema = table.schema
                else:
                    # Ajustar schema do arquivo atual pro schema base
                    table = table.cast(schema, safe=False)
                
                df = table.to_pandas()
                if deduplication_key:
                    df = df.drop_duplicates(subset=deduplication_key)
                resultados.append(df)
                processed_files += 1
                total_processed += len(df)
                print(f"Arquivo {blob.name} processado com {len(df)} registros.")
            except Exception as e:
                print(f"Erro ao processar {blob.name}: {str(e)}")
                continue

            if processed_files >= max_files_per_execution:
                print(f"Processados {processed_files} arquivos. Reiniciando a partir do índice {start_index + processed_files}...")
                reinvoke_url = "https://southamerica-east1-quick-woodland-453702-g2.cloudfunctions.net/consolidate-dados-farm"
                requests.post(reinvoke_url, json={
                    "module": module,
                    "output_file": output_file,
                    "table_name": table_name,
                    "deduplication_key": deduplication_key,
                    "start_index": start_index + processed_files
                })
                return f"Processamento pausado após {processed_files} arquivos. Reiniciando a partir do índice {start_index + processed_files}.", 200

    if not blobs_found:
        print(f"Nenhum arquivo .parquet encontrado com o prefixo {source_prefix} a partir do índice {start_index}.")
        return "Nenhum dado encontrado para consolidar.", 200

    if not resultados:
        print(f"Nenhum dado adicionado. Total de registros processados: {total_processed}.")
        return "Nenhum dado encontrado para consolidar.", 200

    # Consolidar os dados
    consolidated_df = pd.concat(resultados, ignore_index=True)
    buffer = io.BytesIO()
    table = pa.Table.from_pandas(consolidated_df, schema=schema)  # Usa o schema base
    pq.write_table(table, buffer)
    buffer.seek(0)

    destination_blob = bucket.blob(output_file)
    destination_blob.upload_from_file(buffer, content_type="application/octet-stream")
    print(f"Arquivo consolidado salvo em: {output_file}, Total de registros: {len(consolidated_df)}.")

    # Carregar no Big Query
    try:
        bigquery_client = bigquery.Client()
        job_config = bigquery.LoadJobConfig(
            source_format=bigquery.SourceFormat.PARQUET,
            write_disposition=bigquery.WriteDisposition.WRITE_TRUNCATE,
        )
        uri = f"gs://{bucket_name}/{output_file}"
        load_job = bigquery_client.load_table_from_uri(uri, table_name, job_config=job_config)
        load_job.result()
        print(f"Carregamento concluído para {uri} na tabela {table_name}")
    except Exception as e:
        print(f"Erro ao carregar dados no Big Query: {str(e)}")
        return f"Erro ao carregar dados no Big Query: {str(e)}", 500

    return f"Consolidação e carregamento concluídos! Total de registros: {len(consolidated_df)}.", 200