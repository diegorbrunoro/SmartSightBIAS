import functions_framework
import requests
import pandas as pd
import pyarrow as pa
import pyarrow.parquet as pq
import io
import time
import re
from google.cloud import storage, bigquery
from flask import jsonify

def make_request_with_retries(url, headers, max_retries=5, delay=5, timeout=30):
    for attempt in range(max_retries):
        try:
            print(f"[{attempt + 1}/{max_retries}] Requisitando: {url}")
            response = requests.get(url, headers=headers, timeout=timeout)
            response.raise_for_status()
            return response.json()
        except requests.exceptions.HTTPError as e:
            print(f"Erro HTTP ({response.status_code}): {e}")
            if response.status_code == 429:
                wait = 2 ** attempt * delay
                print(f"Rate limit: aguardando {wait} segundos...")
                time.sleep(wait)
            else:
                raise
        except Exception as e:
            print(f"Erro geral: {str(e)}")
            time.sleep(delay)
    raise Exception("Todas as tentativas falharam.")

def deve_reprocessar_blob(blob_path, qtd_registros):
    match = re.search(r'pg_(\d+)_a_(\d+)_r_(\d+)', blob_path)
    if match:
        inicio = int(match.group(1))
        fim = int(match.group(2))
        registros_salvos = int(match.group(3))
        registros_no_range = fim - inicio + 1
        if registros_no_range != registros_salvos or registros_salvos < qtd_registros:
            print(f"⚠️ Arquivo possivelmente incompleto ({registros_salvos} de {qtd_registros}): {blob_path} — reprocessando")
            return True
        else:
            print(f"✅ Arquivo completo: {blob_path} — pulando")
            return False
    else:
        print(f"⚠️ Nome de arquivo inesperado: {blob_path} — reprocessando por segurança")
        return True

def carregar_parquets_filial(bucket, prefixo):
    blobs = list(bucket.list_blobs(prefix=prefixo))
    dfs = []
    for blob in blobs:
        if blob.name.endswith(".parquet"):
            print(f"📅 Lendo arquivo existente: {blob.name}")
            buffer = io.BytesIO()
            blob.download_to_file(buffer)
            buffer.seek(0)
            table = pq.read_table(buffer)
            df = table.to_pandas()
            dfs.append(df)
    return dfs

@functions_framework.http
def download_dados_farm(request):
    print("🔄 Iniciando download_dados_farm")

    if request.method == "GET":
        return "✅ API online. Use POST com {'modulo': '...', 'filiais': [...]} ", 200

    request_json = request.get_json(silent=True)
    if not request_json or 'modulo' not in request_json or 'filiais' not in request_json:
        return jsonify({"mensagem": "Parâmetros obrigatórios: 'modulo' e 'filiais'"}), 400

    modulo = request_json['modulo'].strip()
    filiais = request_json['filiais']
    primeiro_registro = int(request_json.get('primeiroRegistro', 0))
    qtd_registros = 999

    print(f"🗓 Início do módulo '{modulo}' com {len(filiais)} filiais")

    storage_client = storage.Client()
    bucket = storage_client.bucket("farmacia-data-bucket-001")
    bq_client = bigquery.Client()
    dataset_id = "farmacia_data"

    resultados = []

    for filial in filiais:
        cod_filial = filial.get("filial")
        token = filial.get("token")

        print(f"▶️ Processando filial {cod_filial}...")

        current_registro = primeiro_registro
        total_registros = 0
        pagina = 0
        arquivos_gerados = []
        dfs = []
        continuar = True

        while continuar:
            pagina += 1
            iteracao_str = f"{pagina:03d}"
            start_idx = current_registro
            end_idx = current_registro + qtd_registros - 1

            blob_path_check = f"{modulo}/filial_{cod_filial}/In_{iteracao_str}_{modulo}_pg_{start_idx}_a_{end_idx}_r_{qtd_registros}.parquet"
            blob_check = bucket.blob(blob_path_check)

            if blob_check.exists():
                if not deve_reprocessar_blob(blob_path_check, qtd_registros):
                    print(f"⏩ Pulando página já processada: {blob_path_check}")
                    current_registro += qtd_registros
                    total_registros += qtd_registros
                    continue
                else:
                    print(f"♻️ Reprocessando página incompleta: {blob_path_check}")

            url = f"https://api-sgf-gateway.triersistemas.com.br/sgfpod1/rest/integracao/{modulo}/obter-todos-v1?primeiroRegistro={current_registro}&quantidadeRegistros={qtd_registros}"
            headers = {'Authorization': f'Bearer {token}'}

            try:
                data = make_request_with_retries(url, headers)
            except Exception as e:
                print(f"❌ Erro na página {pagina} da filial {cod_filial}: {e}")
                break

            if not isinstance(data, list) or not data:
                print(f"⚠️ Nenhum dado na página {pagina} da filial {cod_filial}")
                break

            df = pd.DataFrame(data)
            if df.empty:
                print(f"⚠️ DataFrame vazio na página {pagina} da filial {cod_filial}")
                break

            dfs.append(df)

            blob_path = f"{modulo}/filial_{cod_filial}/In_{iteracao_str}_{modulo}_pg_{start_idx}_a_{end_idx}_r_{len(df)}.parquet"
            try:
                buffer = io.BytesIO()
                table = pa.Table.from_pandas(df, preserve_index=False)
                pq.write_table(table, buffer)
                buffer.seek(0)
                blob = bucket.blob(blob_path)
                blob.upload_from_file(buffer, content_type='application/octet-stream')
                print(f"✅ Página {pagina} salva: {blob_path}")
                arquivos_gerados.append(blob_path)
            except Exception as e:
                print(f"❌ Erro ao salvar blob: {e}")
                break

            current_registro += len(df)
            total_registros += len(df)

            if len(df) < qtd_registros:
                print("📦 Última página detectada.")
                continuar = False

        prefixo_filial = f"{modulo}/filial_{cod_filial}/"
        dfs_existentes = carregar_parquets_filial(bucket, prefixo_filial)

        if dfs_existentes:
            df_consolidado = pd.concat(dfs_existentes, ignore_index=True)
            consolidated_blob_path = f"{modulo}/consolidado/{modulo}_consolidado_filial{cod_filial}.parquet"
            consolidated_blob = bucket.blob(consolidated_blob_path)

            try:
                buffer = io.BytesIO()
                table = pa.Table.from_pandas(df_consolidado, preserve_index=False)
                pq.write_table(table, buffer)
                buffer.seek(0)
                consolidated_blob.upload_from_file(buffer, content_type='application/octet-stream')
                print(f"📁 Consolidado salvo (sobrescrito): {consolidated_blob_path}")
            except Exception as e:
                print(f"❌ Erro ao salvar consolidado: {e}")
                continue

            try:
                table_id = f"{modulo}_raw_{cod_filial}"
                table_ref = bq_client.dataset(dataset_id).table(table_id)
                job_config = bigquery.LoadJobConfig(
                    write_disposition=bigquery.WriteDisposition.WRITE_TRUNCATE,
                    source_format=bigquery.SourceFormat.PARQUET,
                    autodetect=True,
                )
                buffer.seek(0)
                job = bq_client.load_table_from_file(buffer, table_ref, job_config=job_config)
                job.result()
                print(f"🚀 Dados enviados ao BigQuery: {dataset_id}.{table_id}")
            except Exception as e:
                print(f"❌ Erro ao enviar para BigQuery: {e}")

        resultados.append({
            "filial": cod_filial,
            "arquivos_individuais": arquivos_gerados,
            "registros_processados": total_registros
        })

    return jsonify({
        "mensagem": "Download, consolidação e carga concluídos",
        "resultados": resultados
    }), 200
