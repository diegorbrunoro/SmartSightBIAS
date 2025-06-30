-- Script para criar o dataset 0_landing se não existir
-- Execute este script no BigQuery primeiro

-- Criar dataset 0_landing
CREATE SCHEMA IF NOT EXISTS `smartsight-biaas.0_landing`
OPTIONS(
  location="southamerica-east1",
  description="Dados brutos da fonte (landing zone)"
);

-- Verificar se foi criado
SELECT 
  schema_name,
  location,
  created
FROM `smartsight-biaas.INFORMATION_SCHEMA.SCHEMATA`
WHERE schema_name = '0_landing'; 