-- Databricks notebook source
CREATE SCHEMA IF NOT EXISTS databricksestudos.olist_governanca
  COMMENT 'Projeto Olist — funções de governança (row filters e column masks)';

-- COMMAND ----------

GRANT USE CATALOG ON CATALOG databricksestudos TO `olist_analistas`;
GRANT USE SCHEMA, SELECT ON SCHEMA databricksestudos.olist_gold TO `olist_analistas`;

-- COMMAND ----------

SHOW GRANTS ON CATALOG databricksestudos;
SHOW GRANTS ON SCHEMA databricksestudos.olist_gold;

-- COMMAND ----------

CREATE OR REPLACE FUNCTION databricksestudos.olist_governanca.filtro_uf(uf STRING)
RETURNS BOOLEAN
COMMENT 'Row filter por UF: admin e analistas veem tudo; analistas_sp só SP'
RETURN CASE
  WHEN is_account_group_member('olist_admin')        THEN TRUE
  WHEN is_account_group_member('olist_analistas_sp') THEN uf='SP'
  WHEN is_account_group_member('olist_analistas')    THEN TRUE
  ELSE FALSE
END;

-- COMMAND ----------

CREATE OR REPLACE FUNCTION databricksestudos.olist_governanca.mascara_cep(cep STRING)
RETURNS STRING
COMMENT 'Column mask do CEP: olist_admin vê completo; demais veem só os 2 primeiros dígitos'
RETURN CASE
  WHEN is_account_group_member('olist_admin') THEN cep
  ELSE concat(left(cep, 2), '***')
END;