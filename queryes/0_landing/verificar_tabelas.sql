-- Script para verificar tabelas existentes no dataset 0_landing
-- Execute este script no BigQuery para ver o que já foi criado

-- Verificar datasets existentes
SELECT 
  dataset_id,
  location,
  created,
  last_modified
FROM `smartsight-biaas.__TABLES__`
WHERE dataset_id LIKE '%landing%'
ORDER BY dataset_id;

-- Verificar tabelas no dataset 0_landing
SELECT 
  table_id,
  creation_time,
  last_modified_time,
  row_count,
  size_bytes
FROM `smartsight-biaas.0_landing.__TABLES__`
ORDER BY table_id;

-- Verificar se as tabelas de vendas existem
SELECT 
  CASE 
    WHEN EXISTS(SELECT 1 FROM `smartsight-biaas.0_landing.__TABLES__` WHERE table_id = 'vendas_01') 
    THEN 'EXISTE' 
    ELSE 'NÃO EXISTE' 
  END as vendas_01_status,
  
  CASE 
    WHEN EXISTS(SELECT 1 FROM `smartsight-biaas.0_landing.__TABLES__` WHERE table_id = 'vendas_02') 
    THEN 'EXISTE' 
    ELSE 'NÃO EXISTE' 
  END as vendas_02_status; 