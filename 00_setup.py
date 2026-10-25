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
# MAGIC Depois de rodar: faça upload dos CSVs do Kaggle (descompactados) para `/Volumes/databricksestudos/olist_raw/landing/source/` pela UI (Catalog → olist_raw → landing → Upload to this volume).

# COMMAND ----------

# MAGIC %sql
# MAGIC USE CATALOG databricksestudos;

# COMMAND ----------

# MAGIC %sql
# MAGIC CREATE SCHEMA IF NOT EXISTS olist_raw
# MAGIC   COMMENT 'Projeto Olist — arquivos brutos e Volume de landing';

# COMMAND ----------

# MAGIC %sql
# MAGIC CREATE VOLUME IF NOT EXISTS olist_raw.landing
# MAGIC   COMMENT 'CSVs do Kaggle em source/ e JSON gerados pelo notebook 01_gerador_olist';

# COMMAND ----------

# MAGIC %sql
# MAGIC -- Camadas medallion publicadas pela pipeline declarativa
# MAGIC CREATE SCHEMA IF NOT EXISTS olist_bronze COMMENT 'Projeto Olist — bronze (ingestão crua com Auto Loader)';
# MAGIC CREATE SCHEMA IF NOT EXISTS olist_silver COMMENT 'Projeto Olist — silver (limpeza, expectations, CDC)';
# MAGIC CREATE SCHEMA IF NOT EXISTS olist_gold   COMMENT 'Projeto Olist — gold (materialized views de negócio)';

# COMMAND ----------

# MAGIC %sql
# MAGIC LIST '/Volumes/databricksestudos/olist_raw/landing/source/';