SELECT
  'fornecedor_raw_01' AS origem_raw,
  SAFE_CAST(codigo AS INT64) AS codigo,
  COALESCE(NULLIF(nomeFantasia, ''), 'Sem informação') AS nomeFantasia,
  COALESCE(NULLIF(razaoSocial, ''), 'Sem informação') AS razaoSocial,
  COALESCE(NULLIF(numeroCnpj, ''), 'Sem informação') AS numeroCnpj,
  COALESCE(NULLIF(nomeCidade, ''), 'Sem informação') AS nomeCidade,
  COALESCE(NULLIF(email, ''), 'Sem informação') AS email,
  SAFE_CAST(ativo AS BOOL) AS ativo
FROM `quick-woodland-453702-g2.farmacia_data.fornecedor_raw_01`

UNION ALL

SELECT
  'fornecedor_raw_02' AS origem_raw,
  SAFE_CAST(codigo AS INT64) AS codigo,
  COALESCE(NULLIF(nomeFantasia, ''), 'Sem informação') AS nomeFantasia,
  COALESCE(NULLIF(razaoSocial, ''), 'Sem informação') AS razaoSocial,
  COALESCE(NULLIF(numeroCnpj, ''), 'Sem informação') AS numeroCnpj,
  COALESCE(NULLIF(nomeCidade, ''), 'Sem informação') AS nomeCidade,
  COALESCE(NULLIF(email, ''), 'Sem informação') AS email,
  SAFE_CAST(ativo AS BOOL) AS ativo
FROM `quick-woodland-453702-g2.farmacia_data.fornecedor_raw_02`