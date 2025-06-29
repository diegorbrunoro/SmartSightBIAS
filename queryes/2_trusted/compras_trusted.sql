SELECT
  origem_raw,

  -- Identificador composto único por nota e fornecedor
  CONCAT(CAST(numeroNotaFiscal AS STRING), '-', CAST(codigoFornecedor AS STRING)) AS chave_composta,

  -- Campos principais
  SAFE_CAST(numeroNotaFiscal AS INT64) AS numeroNotaFiscal,
  PARSE_DATE('%Y-%m-%d', dataEntrada) AS dataEntrada,
  SAFE_CAST(codigoFornecedor AS INT64) AS codigoFornecedor,
  SAFE_CAST(valorTotalNota AS FLOAT64) AS valorTotalNota,
  SAFE_CAST(valorTotalProdutos AS FLOAT64) AS valorTotalProdutos,
  SAFE_CAST(quantidadeItens AS INT64) AS quantidadeItens,
  chaveAcessoNfe,

  -- Dados do item (expandido via UNNEST)
  item.element.codigoProduto AS codigoProduto,
  item.element.fatorCompra AS fatorCompra,
  item.element.quantidadeProdutos AS quantidadeProdutos,
  item.element.valorCusto AS valorCusto,
  item.element.valorST AS valorST,
  item.element.valorUnitario AS valorUnitario,
  item.element.valorUnitarioLiquido AS valorUnitarioLiquido

FROM (
  SELECT *, 'compra_raw_01' AS origem_raw
  FROM `quick-woodland-453702-g2.1_raw.compra_raw_01`
  UNION ALL
  SELECT *, 'compra_raw_02' AS origem_raw
  FROM `quick-woodland-453702-g2.1_raw.compra_raw_02`
) AS base,
UNNEST(base.itens.list) AS item