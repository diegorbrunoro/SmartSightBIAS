import functions_framework
import requests
import pandas as pd
import pyarrow as pa
import pyarrow.parquet as pq
import io
import time
import re
import random
from datetime import datetime, timezone
from concurrent.futures import ThreadPoolExecutor
from google.cloud import storage, bigquery
from flask import jsonify

def make_request_with_retries(url, headers, max_retries=5, delay=5, timeout=(10, 30)):
    session = requests.Session()
    adapter = requests.adapters.HTTPAdapter(max_retries=0)
    session.mount("https://", adapter)
    session.mount("http://", adapter)

    for attempt in range(max_retries):
        try:
            print(f"[{attempt + 1}/{max_retries}] 👝️ Requisitando: {url}")
            response = session.get(url, headers=headers, timeout=timeout)
            response.raise_for_status()
            return response.json()
        except requests.exceptions.HTTPError as e:
            print(f"❌ Erro HTTP ({response.status_code}): {e}")
            if response.status_code == 429 or response.status_code >= 500:
                wait = 2 ** attempt * delay
                print(f"🕵️‍♂️ Backoff: aguardando {wait} segundos antes de tentar novamente...")
                time.sleep(wait)
            else:
                raise
        except Exception as e:
            print(f"❌ Erro geral: {str(e)}")
            wait = 2 ** attempt * delay
            print(f"🕵️‍♂️ Backoff: aguardando {wait} segundos antes de tentar novamente...")
            time.sleep(wait)
    raise Exception("Todas as tentativas falharam.")

def carregar_parquets_filial(bucket, prefixo):
    blobs = list(bucket.list_blobs(prefix=prefixo))
    dfs = []
    for blob in blobs:
        if blob.name.endswith(".parquet"):
            print(f"🗅️ Lendo arquivo existente: {blob.name}")
            buffer = io.BytesIO()
            blob.download_to_file(buffer)
            buffer.seek(0)
            table = pq.read_table(buffer)
            df = table.to_pandas()
            dfs.append(df)
    return dfs

def identificar_ultimo_registro(bucket, prefixo, hoje):
    maior_final = None
    for blob in bucket.list_blobs(prefix=prefixo):
        if blob.name.endswith(".parquet") and blob.updated.date() == hoje:
            match = re.search(r"_pg_\d+_a_(\d+)_r_\d+", blob.name)
            if match:
                registro_final = int(match.group(1))
                if maior_final is None or registro_final > maior_final:
                    maior_final = registro_final
    return maior_final + 1 if maior_final is not None else 0

def processar_filial(filial, modulo, primeiro_registro, qtd_registros, bucket, bq_client, dataset_id, max_pages_per_filial=None):
    cod_filial = filial.get("filial")
    token = filial.get("token")

    print(f"▶️ Processando filial {cod_filial}...")

    prefixo_filial = f"{modulo}/filial_{cod_filial}/"
    hoje = datetime.now(timezone.utc).date()

    print(f"🧹 Verificando arquivos antigos em {prefixo_filial}...")
    blobs = list(bucket.list_blobs(prefix=prefixo_filial))
    blobs_do_dia = []
    for blob in blobs:
        if blob.name.endswith(".parquet"):
            data_modificacao = blob.updated.date()
            if data_modificacao < hoje:
                print(f"🗑️ Excluindo {blob.name} (modificado em {data_modificacao})")
                blob.delete()
            else:
                print(f"📦 Mantendo {blob.name} (modificado em {data_modificacao})")
                blobs_do_dia.append(blob.name)

    current_registro = identificar_ultimo_registro(bucket, prefixo_filial, hoje)
    print(f"📍 Iniciando em registro: {current_registro}")

    total_registros = 0
    pagina = 0
    arquivos_gerados = []
    paginas_falhadas = []
    continuar = True

    while continuar:
        pagina += 1
        inicio_pagina = time.time()
        iteracao_str = f"{pagina:03d}"
        start_idx = current_registro

        # Limite de páginas para teste
        if max_pages_per_filial and pagina > max_pages_per_filial:
            print(f"🛑 Limite de teste atingido: máximo {max_pages_per_filial} páginas por filial")
            break

        blob_path_prefix = f"{modulo}/filial_{cod_filial}/In_{iteracao_str}_{modulo}_pg_{start_idx}_a_"
        if any(blob.startswith(blob_path_prefix) for blob in blobs_do_dia):
            print(f"⏩ Página {pagina} já existe para hoje, pulando...")
            current_registro += qtd_registros
            continue

        url = f"https://api-sgf-gateway.triersistemas.com.br/sgfpod1/rest/integracao/{modulo}/obter-todos-v1?primeiroRegistro={current_registro}&quantidadeRegistros={qtd_registros}"
        headers = {'Authorization': f'Bearer {token}'}

        try:
            data = make_request_with_retries(url, headers)
        except Exception as e:
            print(f"❌ Erro na página {pagina} da filial {cod_filial}: {e} — URL: {url}")
            paginas_falhadas.append({
                "pagina": pagina,
                "registro_inicio": current_registro,
                "url": url,
                "erro": str(e)
            })
            break

        if not isinstance(data, list) or not data:
            print(f"⚠️ Nenhum dado na página {pagina} da filial {cod_filial}")
            break

        df = pd.DataFrame(data)
        if df.empty:
            print(f"⚠️ DataFrame vazio na página {pagina} da filial {cod_filial}")
            break

        end_idx = start_idx + len(df) - 1
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
            blobs_do_dia.append(blob_path)
        except Exception as e:
            print(f"❌ Erro ao salvar blob: {e}")
            break

        # Liberar memória imediatamente após salvar
        del df
        del table
        del buffer
        import gc
        gc.collect()

        current_registro += len(data)
        total_registros += len(data)

        if len(data) < qtd_registros:
            print("📦 Última página detectada.")
            continuar = False

        tempo_execucao = time.time() - inicio_pagina
        print(f"🕵️ Página {pagina} processada em {tempo_execucao:.2f} segundos")
        sleep = random.uniform(1.2, 3.5)
        print(f"🕵️ Aguardando {sleep:.2f}s antes da próxima página...")
        time.sleep(sleep)

    # Consolidar dados usando streaming para economizar memória
    print(f"🔄 Consolidando dados da filial {cod_filial}...")
    try:
        # Carregar e consolidar em lotes para economizar memória
        blobs_parquet = [blob for blob in bucket.list_blobs(prefix=prefixo_filial) if blob.name.endswith('.parquet')]
        
        if blobs_parquet:
            # Usar PyArrow para consolidar sem carregar tudo na memória
            tables = []
            for i, blob in enumerate(blobs_parquet):
                buffer = io.BytesIO()
                blob.download_to_file(buffer)
                buffer.seek(0)
                table = pq.read_table(buffer)
                
                # Converter para pandas para normalizar tipos
                df = table.to_pandas()
                
                # Adicionar coluna com nome do arquivo de origem
                df['arquivo_origem'] = blob.name
                
                # Normalizar tipos problemáticos
                if 'numeroNotaFiscal' in df.columns:
                    # Converter para string para evitar conflitos de tipo
                    df['numeroNotaFiscal'] = df['numeroNotaFiscal'].astype(str)
                
                if 'codigoCliente' in df.columns:
                    # Converter para string para evitar conflitos de tipo
                    df['codigoCliente'] = df['codigoCliente'].astype(str)
                
                # Converter de volta para PyArrow
                table = pa.Table.from_pandas(df, preserve_index=False)
                tables.append(table)
                
                # Liberar memória
                buffer.close()
                del buffer
                del df
                gc.collect()
            
            # Concatenar tabelas PyArrow (mais eficiente que pandas)
            consolidated_table = pa.concat_tables(tables)
            
            # Consolidar usando streaming
            consolidated_blob_path = f"{modulo}/consolidado/{modulo}_consolidado_filial{cod_filial}.parquet"
            consolidated_blob = bucket.blob(consolidated_blob_path)
            
            # Salvar consolidado
            buffer = io.BytesIO()
            pq.write_table(consolidated_table, buffer)
            buffer.seek(0)
            consolidated_blob.upload_from_file(buffer, content_type='application/octet-stream')
            print(f"📁 Consolidado salvo: {consolidated_blob_path}")
            
            # Liberar memória
            del tables
            del consolidated_table
            buffer.close()
            del buffer
            gc.collect()
            
            # Carregar para BigQuery
            try:
                table_id = f"{modulo}_{cod_filial}" # Aqui adiciono o nome que vai aparecer no arquivo que vai ser carregado no bigquery no 0_landing Exemplo: compra_01 e etc
                table_ref = bq_client.dataset(dataset_id).table(table_id)
                job_config = bigquery.LoadJobConfig(
                    write_disposition=bigquery.WriteDisposition.WRITE_TRUNCATE,
                    source_format=bigquery.SourceFormat.PARQUET,
                    autodetect=True,
                )
                
                # Recarregar buffer para BigQuery
                buffer = io.BytesIO()
                consolidated_blob.download_to_file(buffer)
                buffer.seek(0)
                job = bq_client.load_table_from_file(buffer, table_ref, job_config=job_config)
                job.result()
                print(f"🚀 Dados enviados ao BigQuery: {dataset_id}.{table_id}")
                buffer.close()
                del buffer
                gc.collect()
                
            except Exception as e:
                print(f"❌ Erro ao enviar para BigQuery: {e}")
                
    except Exception as e:
        print(f"❌ Erro ao consolidar dados: {e}")
        import traceback
        print(f"Detalhes do erro: {traceback.format_exc()}")

    return {
        "filial": cod_filial,
        "arquivos_individuais": arquivos_gerados,
        "registros_processados": total_registros,
        "paginas_falhadas": paginas_falhadas
    }

@functions_framework.http
def download_dados_farm(request):
    print("🔄 Iniciando download_dados_farm")

    # Configurações configuráveis
    CONFIG = {
        "bucket_name": "smartsight-biaas-data",  # Bucket para armazenar arquivos
        "project_id": "smartsight-biaas",        # Projeto BigQuery
        "dataset_id": "0_landing",               # Dataset BigQuery
        "max_pages_per_filial": None,            # Limite de páginas para teste (None = sem limite) ou 2 para teste
        "records_per_page": 999                  # Registros por página
    }

    if request.method == "GET":
        return "✅ API online. Use POST com {'modulo': '...', 'filiais': [...]} ", 200

    request_json = request.get_json(silent=True)
    if not request_json or 'modulo' not in request_json or 'filiais' not in request_json:
        return jsonify({"mensagem": "Parâmetros obrigatórios: 'modulo' e 'filiais'"}), 400

    modulo = request_json['modulo'].strip()
    filiais = request_json['filiais']
    primeiro_registro = int(request_json.get('primeiroRegistro', 0))
    qtd_registros = CONFIG["records_per_page"]

    storage_client = storage.Client()
    bucket = storage_client.bucket(CONFIG["bucket_name"])
    bq_client = bigquery.Client(project=CONFIG["project_id"])
    dataset_id = CONFIG["dataset_id"]

    resultados = []
    with ThreadPoolExecutor() as executor:
        futures = [
            executor.submit(
                processar_filial,
                filial, modulo, primeiro_registro, qtd_registros,
                bucket, bq_client, dataset_id, CONFIG["max_pages_per_filial"]
            ) for filial in filiais
        ]
        for future in futures:
            resultados.append(future.result())

    return jsonify({
        "mensagem": "Download, consolidação e carga concluídos",
        "resultados": resultados
    }), 200
