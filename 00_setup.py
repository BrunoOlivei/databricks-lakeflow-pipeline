# Databricks notebook source
# /// script
# [tool.databricks.environment]
# environment_version = "6"
# ///
# MAGIC %md
# MAGIC # 00 · Setup do projeto Olist
# MAGIC
# MAGIC Cria os schemas e o Volume de landing no catálogo `databricksestudos`. Rode uma vez, em SQL warehouse ou em compute com Unity Catalog.
# MAGIC
# MAGIC Depois de rodar: faça upload dos CSVs do Kaggle (descompactados) para `/Volumes/{CATALOGO}/olist_raw/landing/source/` pela UI (Catalog → olist_raw → landing → Upload to this volume).

# COMMAND ----------

dbutils.widgets.text("catalog", "databricksestudos")
CATALOG = dbutils.widgets.get("catalog")
print(f"Catálogo alvo: {CATALOG}")

# COMMAND ----------

query = f"""
    CREATE SCHEMA IF NOT EXISTS {CATALOG}.olist_raw
    COMMENT 'Projeto Olist — arquivos brutos e Volume de landing';
    """
print(query)
spark.sql(query)

# COMMAND ----------

query = f"""
CREATE VOLUME IF NOT EXISTS {CATALOG}.olist_raw.landing
  COMMENT 'CSVs do Kaggle em source/ e JSON gerados pelo notebook 01_gerador_olist';
"""
print(query)
spark.sql(query)

# COMMAND ----------

# Camadas medallion publicadas pela pipeline declarativa

query = f"CREATE SCHEMA IF NOT EXISTS {CATALOG}.olist_bronze COMMENT 'Projeto Olist — bronze (ingestão crua com Auto Loader)';"
print(query)
spark.sql(query)

# COMMAND ----------

query = f"CREATE SCHEMA IF NOT EXISTS {CATALOG}.olist_silver COMMENT 'Projeto Olist — silver (limpeza, expectations, CDC)';"
print(query)
spark.sql(query)

# COMMAND ----------

query = f"CREATE SCHEMA IF NOT EXISTS {CATALOG}.olist_gold   COMMENT 'Projeto Olist — gold (materialized views de negócio)';"
print(query)
spark.sql(query)

# COMMAND ----------

dbutils.fs.ls(f"/Volumes/{CATALOG}/olist_raw/landing/source/")