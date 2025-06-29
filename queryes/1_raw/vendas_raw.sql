SELECT
  origem_raw,
  CONCAT(CAST(numeroNota AS STRING), '-', CAST(base.codFilial AS STRING)) AS chave_composta,
  SAFE_CAST(numeroNota AS INT64) AS numeroNota,
  DATE(dataEmissao) AS dataEmissao,
  horaEmissao,
  SAFE_CAST(codigoVendedor AS INT64) AS codigoVendedor,
  SAFE_CAST(codigoCliente AS FLOAT64) AS codigoCliente,
  SAFE_CAST(entrega AS BOOL) AS entrega,
  condicaoPagamento.codigo AS condicaoPagamento_codigo,
  condicaoPagamento.nome AS condicaoPagamento_nome,
  SAFE_CAST(numeroCupomFiscal AS INT64) AS numeroCupomFiscal,
  SAFE_CAST(numeroNotaFiscal AS FLOAT64) AS numeroNotaFiscal,
  SAFE_CAST(base.codFilial AS INT64) AS codFilial,
  item.element.codigoProduto AS codigoProduto,
  item.element.codigoVendedor AS item_codigoVendedor,
  item.element.parceiro AS item_parceiro,
  item.element.quantidadeProdutos AS quantidadeProdutos,
  item.element.valorTotalBruto AS valorTotalBruto,
  item.element.valorTotalCusto AS valorTotalCusto,
  item.element.valorTotalLiquido AS valorTotalLiquido
FROM (
  SELECT *, 'venda_raw_01' AS origem_raw
  FROM `quick-woodland-453702-g2.0_landing.venda_01`
  UNION ALL
  SELECT *, 'venda_raw_02' AS origem_raw
  FROM `quick-woodland-453702-g2.0_landing.venda_02`
) AS base,
UNNEST(base.itens.list) AS item