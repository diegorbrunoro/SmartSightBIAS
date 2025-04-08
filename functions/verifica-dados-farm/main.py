from google.cloud import bigquery
import functions_framework
import os

# Função principal da Cloud Function (com hífen)
@functions_framework.http
def verifica_dados_farm(request):
    # Verificar se é uma requisição POST
    if request.method != "POST":
        return {"status": "error", "message": "Método não permitido"}, 405

    # Obter o payload da requisição
    request_json = request.get_json(silent=True)
    if not request_json or "modulo" not in request_json:
        return {"status": "error", "message": "Módulo não especificado"}, 400

    modulo = request_json["modulo"]
    primeiro_registro = request_json.get("primeiroRegistro", 0)

    # Configurar o cliente do BigQuery
    client = bigquery.Client()

    # Consulta SQL para o módulo "compras"
    if modulo == "compras":
        query = """
        SELECT * 
        FROM `quick-woodland-453702-g2.farmacia_data.compras_view_bronze` 
        WHERE dataEntrada IS NOT NULL 
        ORDER BY dataEntrada DESC 
        LIMIT 3
        """
        try:
            # Executar a consulta
            query_job = client.query(query)
            results = query_job.result()

            # Converter os resultados para uma lista de dicionários
            data = [dict(row) for row in results]

            return {
                "status": "success",
                "data": data
            }, 200, {"Content-Type": "application/json"}

        except Exception as e:
            return {
                "status": "error",
                "message": str(e)
            }, 500, {"Content-Type": "application/json"}
    
    # Para outros módulos, retornar uma mensagem genérica
    else:
        return {
            "status": "success",
            "message": f"Módulo {modulo} processado (funcionalidade não implementada ainda)"
        }, 200, {"Content-Type": "application/json"}

# Definir a porta explicitamente (necessário para Cloud Run)
if __name__ == "__main__":
    port = int(os.getenv("PORT", 8080))  # Usa a porta 8080 por padrão
    functions_framework.run_http(verifica_dados_farm, host="0.0.0.0", port=port)