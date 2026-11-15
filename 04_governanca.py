# Databricks notebook source
# /// script
# [tool.databricks.environment]
# environment_version = "6"
# ///
dbutils.widgets.text("catalog", "databricksestudos")
CATALOG = dbutils.widgets.get("catalog")
print(f"Catálogo alvo: {CATALOG}")

# COMMAND ----------

# Cria um schema de governança para o projeto Olist para armazenar funções de governança
query = f"""
CREATE SCHEMA IF NOT EXISTS {CATALOG}.olist_governanca
  COMMENT 'Projeto Olist — funções de governança (row filters e column masks)';
"""
print(query)
spark.sql(query)

# COMMAND ----------

# Concede acesso ao catálogo para o grupo `olist_analistas`
query = f"GRANT USE CATALOG ON CATALOG {CATALOG} TO `olist_analistas`;"
print(query)
spark.sql(query)

# COMMAND ----------

# Concede acesso ao schema e a permissão de SELECT para o schema `olist_gold`
query = f"GRANT USE SCHEMA, SELECT ON SCHEMA {CATALOG}.olist_gold TO `olist_analistas`;"
print(query)
spark.sql(query)

# COMMAND ----------

# Verifica os privilégios concedidos ao catálogo
query = f"SHOW GRANTS ON CATALOG {CATALOG};"
print(query)
display(spark.sql(query))

# COMMAND ----------

# Verifica os privilégios concedidos ao schema
query = f"SHOW GRANTS ON SCHEMA {CATALOG}.olist_gold;"
print(query)
display(spark.sql(query))

# COMMAND ----------

# Cria uma função de row filter para o schema `olist_gold` filtrando por UF
query = f"""
CREATE OR REPLACE FUNCTION {CATALOG}.olist_governanca.filtro_uf(uf STRING)
RETURNS BOOLEAN
COMMENT 'Row filter por UF: admin e analistas veem tudo; analistas_sp só SP'
RETURN CASE
  WHEN is_account_group_member('olist_admin')        THEN TRUE
  WHEN is_account_group_member('olist_analistas_sp') THEN uf='SP'
  WHEN is_account_group_member('olist_analistas')    THEN TRUE
  ELSE FALSE
END;
"""
print(query)
spark.sql(query)

# COMMAND ----------

# Cria uma função de column mask para o schema `olist_silver.customers_current` mascarando o CEP
query = f"""
CREATE OR REPLACE FUNCTION {CATALOG}.olist_governanca.mascara_cep(cep STRING)
RETURNS STRING
COMMENT 'Column mask do CEP: olist_admin vê completo; demais veem só os 2 primeiros dígitos'
RETURN CASE
  WHEN is_account_group_member('olist_admin') THEN cep
  ELSE concat(left(cep, 2), '***')
END;
"""
print(query)
spark.sql(query) 

# COMMAND ----------

# MAGIC %sql
# MAGIC -- REVOKE SELECT ON TABLE databricksestudos.olist_gold.receita_diaria_uf FROM `olist_analistas` -- Revoga permissão de SELECT na tabela `receita_diaria_uf` não funciona pois a permissão de SELECT é herdada do schema `olist_gold`

# COMMAND ----------

# MAGIC %sql
# MAGIC -- REVOKE SELECT ON SCHEMA databricksestudos.olist_gold FROM `olist_analistas` -- Revoga permissão de SELECT no schema `olist_gold` funciona pois a permissão de SELECT é herdada do schema `olist_gold

# COMMAND ----------

# MAGIC %sql
# MAGIC -- SHOW GRANTS ON CATALOG databricksestudos; -- Verifica os privilégios concedidos ao catálogo
# MAGIC -- SHOW GRANTS ON SCHEMA databricksestudos.olist_gold; -- Verifica os privilégios concedidos ao schema após revogar a permissão de SELECT ON SCHEMA databricksestudos.olist_gold