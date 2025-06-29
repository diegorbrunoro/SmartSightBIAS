SELECT
  'estoque_raw_01' AS origem_raw,
  SAFE_CAST(codigoProduto AS INT64) AS codigoProduto,
  SAFE_CAST(quantidadeEstoque AS INT64) AS quantidadeEstoque,
  SAFE_CAST(valorCustoMedio AS FLOAT64) AS valorCustoMedio,
  COALESCE(NULLIF(dataUltimaEntrada, ''), 'Sem informação') AS dataUltimaEntrada,
  SAFE_CAST(valorUltimaEntrada AS FLOAT64) AS valorUltimaEntrada
FROM `quick-woodland-453702-g2.0_landing.estoque_01`

UNION ALL

SELECT
  'estoque_raw_02' AS origem_raw,
  SAFE_CAST(codigoProduto AS INT64) AS codigoProduto,
  SAFE_CAST(quantidadeEstoque AS INT64) AS quantidadeEstoque,
  SAFE_CAST(valorCustoMedio AS FLOAT64) AS valorCustoMedio,
  COALESCE(NULLIF(dataUltimaEntrada, ''), 'Sem informação') AS dataUltimaEntrada,
  SAFE_CAST(valorUltimaEntrada AS FLOAT64) AS valorUltimaEntrada
FROM `quick-woodland-453702-g2.0_landing.estoque_02`