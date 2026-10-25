# Databricks notebook source
# MAGIC %md
# MAGIC # 02 · Checagem da gold (task do job)
# MAGIC Roda depois da task da pipeline. Conta os pedidos do dia mais recente da gold e publica como **task value** (`pedidos_ultimo_dia`), que a task If/else seguinte usa na condição:
# MAGIC
# MAGIC ```
# MAGIC {{tasks.checagem_gold.values.pedidos_ultimo_dia}} > 0
# MAGIC ```
# MAGIC
# MAGIC Lembrete (notas 010/011): task value só é definido por notebook Python; `>` compara número, `==` compara texto.

# COMMAND ----------

dbutils.widgets.text("catalog", "databricksestudos")
CATALOG = dbutils.widgets.get("catalog")

# COMMAND ----------

linha = spark.sql(f"""
    SELECT
      max(data_compra) AS ultima_data,
      coalesce(sum(CASE WHEN data_compra = (SELECT max(data_compra) FROM {CATALOG}.olist_gold.receita_diaria_uf)
                        THEN pedidos END), 0) AS pedidos_ultimo_dia
    FROM {CATALOG}.olist_gold.receita_diaria_uf
""").first()
 
ultima_data = str(linha["ultima_data"])
pedidos = int(linha["pedidos_ultimo_dia"])
 
dbutils.jobs.taskValues.set(key="ultima_data", value=ultima_data)
dbutils.jobs.taskValues.set(key="pedidos_ultimo_dia", value=pedidos)
 
print(f"Última data na gold: {ultima_data} · pedidos: {pedidos}")
 