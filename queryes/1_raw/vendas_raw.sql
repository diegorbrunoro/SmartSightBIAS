-- View para normalizar dados de vendas das filiais 01 e 02
-- Transforma dados aninhados em formato tabular
-- Camada: 1_raw
-- Funciona mesmo se apenas uma das tabelas existir

CREATE OR REPLACE VIEW `smartsight-biaas.1_raw.vendas_raw` AS

-- Filial 01 (se existir)
SELECT 
  numeroNota,
  dataEmissao,
  horaEmissao,
  codigoVendedor,
  CAST(codigoCliente AS STRING) as codigoCliente,
  entrega,
  condicaoPagamento.codigo as condicaoPagamento_codigo,
  condicaoPagamento.nome as condicaoPagamento_nome,
  numeroCupomFiscal,
  CAST(numeroNotaFiscal AS STRING) as numeroNotaFiscal,
  codFilial,
  -- Rastreabilidade
  arquivo_origem,
  -- Normalizar itens (cada item vira uma linha)
  item.element.codigoProduto,
  item.element.codigoVendedor as item_codigoVendedor,
  item.element.parceiro,
  item.element.quantidadeProdutos,
  item.element.valorTotalBruto,
  item.element.valorTotalCusto,
  item.element.valorTotalLiquido,
  -- Campos calculados úteis
  ROUND(item.element.valorTotalBruto * item.element.quantidadeProdutos, 2) as valorTotalBrutoCalculado,
  ROUND(item.element.valorTotalCusto * item.element.quantidadeProdutos, 2) as valorTotalCustoCalculado,
  ROUND(item.element.valorTotalLiquido * item.element.quantidadeProdutos, 2) as valorTotalLiquidoCalculado,
  -- Data de processamento
  CURRENT_TIMESTAMP() as dataProcessamento
FROM `smartsight-biaas.0_landing.venda_01`,
UNNEST(itens.list) as item

UNION ALL

-- Filial 02 (se existir)
SELECT 
  numeroNota,
  dataEmissao,
  horaEmissao,
  codigoVendedor,
  CAST(codigoCliente AS STRING) as codigoCliente,
  entrega,
  condicaoPagamento.codigo as condicaoPagamento_codigo,
  condicaoPagamento.nome as condicaoPagamento_nome,
  numeroCupomFiscal,
  CAST(numeroNotaFiscal AS STRING) as numeroNotaFiscal,
  codFilial,
  -- Rastreabilidade
  arquivo_origem,
  -- Normalizar itens (cada item vira uma linha)
  item.element.codigoProduto,
  item.element.codigoVendedor as item_codigoVendedor,
  item.element.parceiro,
  item.element.quantidadeProdutos,
  item.element.valorTotalBruto,
  item.element.valorTotalCusto,
  item.element.valorTotalLiquido,
  -- Campos calculados úteis
  ROUND(item.element.valorTotalBruto * item.element.quantidadeProdutos, 2) as valorTotalBrutoCalculado,
  ROUND(item.element.valorTotalCusto * item.element.quantidadeProdutos, 2) as valorTotalCustoCalculado,
  ROUND(item.element.valorTotalLiquido * item.element.quantidadeProdutos, 2) as valorTotalLiquidoCalculado,
  -- Data de processamento
  CURRENT_TIMESTAMP() as dataProcessamento
FROM `smartsight-biaas.0_landing.venda_02`,
UNNEST(itens.list) as item

ORDER BY dataEmissao DESC, numeroNota DESC, codigoProduto;