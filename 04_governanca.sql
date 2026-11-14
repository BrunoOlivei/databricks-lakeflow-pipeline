-- Databricks notebook source
-- Cria um schema de governança para o projeto Olist para armazenar funções de governança

CREATE SCHEMA IF NOT EXISTS databricksestudos.olist_governanca
  COMMENT 'Projeto Olist — funções de governança (row filters e column masks)';

-- COMMAND ----------

GRANT USE CATALOG ON CATALOG databricksestudos TO `olist_analistas`; -- Concede acesso ao catálogo para o grupo `olist_analistas`
GRANT USE SCHEMA, SELECT ON SCHEMA databricksestudos.olist_gold TO `olist_analistas`; -- Concede acesso ao schema e a permissão de SELECT para o schema `olist_gold`

-- COMMAND ----------

SHOW GRANTS ON CATALOG databricksestudos; -- Verifica os privilégios concedidos ao catálogo
SHOW GRANTS ON SCHEMA databricksestudos.olist_gold; -- Verifica os privilégios concedidos ao schema

-- COMMAND ----------

-- Cria uma função de row filter para o schema `olist_gold` filtrando por UF
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

-- Cria uma função de column mask para o schema `olist_silver.customers_current mascarando o CEP
CREATE OR REPLACE FUNCTION databricksestudos.olist_governanca.mascara_cep(cep STRING)
RETURNS STRING
COMMENT 'Column mask do CEP: olist_admin vê completo; demais veem só os 2 primeiros dígitos'
RETURN CASE
  WHEN is_account_group_member('olist_admin') THEN cep
  ELSE concat(left(cep, 2), '***')
END;

-- COMMAND ----------

-- REVOKE SELECT ON TABLE databricksestudos.olist_gold.receita_diaria_uf FROM `olist_analistas` -- Revoga permissão de SELECT na tabela `receita_diaria_uf` não funciona pois a permissão de SELECT é herdada do schema `olist_gold`

-- COMMAND ----------

-- REVOKE SELECT ON SCHEMA databricksestudos.olist_gold FROM `olist_analistas` -- Revoga permissão de SELECT no schema `olist_gold` funciona pois a permissão de SELECT é herdada do schema `olist_gold

-- COMMAND ----------

-- SHOW GRANTS ON CATALOG databricksestudos; -- Verifica os privilégios concedidos ao catálogo
-- SHOW GRANTS ON SCHEMA databricksestudos.olist_gold; -- Verifica os privilégios concedidos ao schema após revogar a permissão de SELECT ON SCHEMA databricksestudos.olist_gold