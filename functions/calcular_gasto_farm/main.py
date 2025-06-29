import json
from google.cloud import bigquery
from datetime import datetime

def calcular_gasto(request):
    try:
        request_json = request.get_json(silent=True)
        cpu_segundos = request_json.get("cpu_segundos", 200000)
        memoria_gib_segundos = request_json.get("memoria_gib_segundos", 300000)
        requisicoes = request_json.get("requisicoes", 2500000)

        preco_cpu = max(cpu_segundos - 180000, 0) * 0.000024
        preco_memoria = max(memoria_gib_segundos - 360000, 0) * 0.0000025
        preco_req = max(requisicoes - 2000000, 0) / 1_000_000 * 0.40

        total_usd = preco_cpu + preco_memoria + preco_req

        # Envia para BigQuery
        client = bigquery.Client()
        tabela = "quick-woodland-453702-g2.farmacia_data.custos_cloud_run"
        row = [{
            "data_execucao": datetime.utcnow().isoformat(),
            "cpu_segundos": cpu_segundos,
            "memoria_gib_segundos": memoria_gib_segundos,
            "requisicoes": requisicoes,
            "custo_cpu_usd": round(preco_cpu, 4),
            "custo_memoria_usd": round(preco_memoria, 4),
            "custo_requisicoes_usd": round(preco_req, 4),
            "total_estimado_usd": round(total_usd, 4),
        }]
        print("Enviando para BigQuery:", row)
        client.insert_rows_json(tabela, row)

        return json.dumps(row[0]), 200, {'Content-Type': 'application/json'}

    except Exception as e:
        print("Erro ao processar requisição:", str(e))
        return json.dumps({"erro": str(e)}), 500, {'Content-Type': 'application/json'}
