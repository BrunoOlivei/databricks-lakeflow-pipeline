# Databricks notebook source
# MAGIC  %md
# MAGIC # 03 · Alerta (task do job)
# MAGIC
# MAGIC Notebook genérico de alerta, usado em dois lugares do job:
# MAGIC
# MAGIC | Task | Como dispara | Parâmetro `mensagem` |
# MAGIC |---|---|---|
# MAGIC | `alerta_sem_pedidos` | Ramo **false** do If/else (`pedidos_ultimo_dia > 0` falso) | `Gold sem pedidos no último dia ({{tasks.checagem_gold.values.ultima_data}})` |
# MAGIC | `alerta_falha` | **Run if = At least one failed**, dependendo da pipeline e da checagem | `Falha no job {{job.name}}, execução {{job.run_id}}` |
# MAGIC
# MAGIC Num projeto real, aqui entraria um webhook (Teams/Slack). Para estudo, ele só registra e, se `falhar = sim`, termina com erro para você ver a task vermelha no DAG.

# COMMAND ----------

dbutils.widgets.text("mensagem", "Alerta sem mensagem")
dbutils.widgets.dropdown("falhar", "nao", ["nao", "sim"])
 
mensagem = dbutils.widgets.get("mensagem")
print(f"[ALERTA] {mensagem}")
 
if dbutils.widgets.get("falhar") == "sim":
    raise RuntimeError(mensagem)