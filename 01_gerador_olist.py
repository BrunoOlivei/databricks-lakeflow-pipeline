# Databricks notebook source
# /// script
# [tool.databricks.environment]
# environment_version = "6"
# ///
# MAGIC %md
# MAGIC # 01 · Gerador de arquivos Olist (simula chegada diária)
# MAGIC
# MAGIC Lê os CSVs originais do Olist em `landing/source/` e, a cada execução, solta **N dias** de dados como JSON (uma linha por registro) no Volume:
# MAGIC
# MAGIC | Pasta no Volume | Conteúdo | Uso na pipeline |
# MAGIC |---|---|---|
# MAGIC | `order_events/` | Eventos de status do pedido (created → approved → shipped → delivered, ou status final) | Fonte de CDC (`AUTO CDC`, SCD 1 e SCD 2) |
# MAGIC | `order_items/` | Itens dos pedidos comprados no dia | Stream-static join com `dim_products` |
# MAGIC | `payments/` | Pagamentos dos pedidos comprados no dia | Join na gold |
# MAGIC | `reviews/` | Avaliações criadas no dia | Expectations (nota 1–5) |
# MAGIC | `customers/` | Snapshot do cliente de cada pedido do dia | CDC SCD 1 |
# MAGIC | `dims/` | Produtos (com categoria traduzida) e vendedores — escrito uma vez | Tabelas estáticas (MV) |
# MAGIC
# MAGIC O estado (próximo dia a gerar) fica em `landing/_gerador/estado.json`. Arquivos são escritos com prefixo `_tmp_` e renomeados no fim, para o Auto Loader não ler arquivo pela metade (nomes começando com `_` são ignorados na listagem).
# MAGIC
# MAGIC **Injeção de registros ruins** (widget `injetar`), para testar as expectations:
# MAGIC - `warn_drop`: evento sem `order_id` (DROP), item com preço negativo (DROP), avaliação com nota 7 (WARN) e um pedido de teste criado e depois **deletado** (testa `APPLY AS DELETE WHEN`).
# MAGIC - `fail`: evento com `event_ts` nulo → a expectation `ON VIOLATION FAIL UPDATE` derruba o fluxo (bom para treinar repair run).

# COMMAND ----------

dbutils.widgets.text("catalog", "databricksestudos")
dbutils.widgets.text("schema", "olist_raw")
dbutils.widgets.text("volume", "landing")
dbutils.widgets.text("data_inicial", "2017-01-01")
dbutils.widgets.text("dias_por_execucao", "1")
dbutils.widgets.dropdown("injetar", "nenhum", ["nenhum", "warn_drop", "fail"])
dbutils.widgets.dropdown("reiniciar", "nao", ["nao", "sim"])

CATALOG = dbutils.widgets.get("catalog")
SCHEMA = dbutils.widgets.get("schema")
VOLUME = dbutils.widgets.get("volume")
DATA_INICIAL = dbutils.widgets.get("data_inicial")
DIAS = int(dbutils.widgets.get("dias_por_execucao"))
INJETAR = dbutils.widgets.get("injetar")
REINICIAR = dbutils.widgets.get("reiniciar") == "sim"

BASE = f"/Volumes/{CATALOG}/{SCHEMA}/{VOLUME}"
SRC = f"{BASE}/source"
ESTADO = f"{BASE}/_gerador/estado.json"
ENTIDADES = ["order_events", "order_items", "payments", "reviews", "customers"]

# COMMAND ----------

import json
import os
from datetime import date, timedelta

import pandas as pd

TS_FMT = "%Y-%m-%d %H:%M:%S"


def ler_csv(nome: str) -> pd.DataFrame:
    # dtype=str preserva os valores como vieram; a tipagem acontece na silver.
    # utf-8-sig remove o BOM que alguns CSVs do Olist têm no cabeçalho.
    return pd.read_csv(f"{SRC}/{nome}", dtype=str, encoding="utf-8-sig")


def gravar_jsonl(df: pd.DataFrame, pasta: str, nome: str) -> int:
    """Grava JSON lines com escrita atômica (tmp + rename). Retorna nº de linhas."""
    if df.empty:
        return 0
    destino = f"{BASE}/{pasta}"
    os.makedirs(destino, exist_ok=True)
    tmp = f"{destino}/_tmp_{nome}"
    final = f"{destino}/{nome}"
    df.to_json(tmp, orient="records", lines=True, force_ascii=False)
    os.replace(tmp, final)
    return len(df)


def fmt_ts(serie: pd.Series) -> pd.Series:
    return serie.dt.strftime(TS_FMT).where(serie.notna(), None)

# COMMAND ----------

# MAGIC %md ## Reinício (opcional) e estado

# COMMAND ----------

if REINICIAR:
    for pasta in ENTIDADES + ["dims", "_gerador"]:
        dbutils.fs.rm(f"{BASE}/{pasta}", recurse=True)
    print("Arquivos gerados apagados. Faça um FULL REFRESH da pipeline antes de continuar.")

if os.path.exists(ESTADO):
    with open(ESTADO) as f:
        estado = json.load(f)
else:
    estado = {"data_inicial": DATA_INICIAL, "proximo_dia": DATA_INICIAL}

inicio_global = pd.Timestamp(estado["data_inicial"])
proximo_dia = date.fromisoformat(estado["proximo_dia"])
print(f"Gerando {DIAS} dia(s) a partir de {proximo_dia} (início global {inicio_global.date()})")

# COMMAND ----------

# MAGIC %md ## Leitura dos CSVs

# COMMAND ----------

orders = ler_csv("olist_orders_dataset.csv")
items = ler_csv("olist_order_items_dataset.csv")
payments = ler_csv("olist_order_payments_dataset.csv")
reviews = ler_csv("olist_order_reviews_dataset.csv")
customers = ler_csv("olist_customers_dataset.csv")

ts_cols = [
    "order_purchase_timestamp",
    "order_approved_at",
    "order_delivered_carrier_date",
    "order_delivered_customer_date",
    "order_estimated_delivery_date",
]
for c in ts_cols:
    orders[c] = pd.to_datetime(orders[c], errors="coerce")

# Só pedidos comprados a partir do início global: eventos posteriores
# nunca chegam para um pedido que não foi "criado" no landing.
orders = orders[orders["order_purchase_timestamp"] >= inicio_global].copy()
orders["dia_compra"] = orders["order_purchase_timestamp"].dt.date

# COMMAND ----------

# MAGIC %md ## Dimensões (uma vez)

# COMMAND ----------

if not os.path.exists(f"{BASE}/dims/products/products.json"):
    products = ler_csv("olist_products_dataset.csv")
    traducao = ler_csv("product_category_name_translation.csv")
    dim_products = products.merge(traducao, on="product_category_name", how="left")
    sellers = ler_csv("olist_sellers_dataset.csv")
    n_p = gravar_jsonl(dim_products, "dims/products", "products.json")
    n_s = gravar_jsonl(sellers, "dims/sellers", "sellers.json")
    print(f"Dimensões gravadas: {n_p} produtos, {n_s} vendedores")

# COMMAND ----------

# MAGIC %md ## Eventos de status (fonte do CDC)
# MAGIC
# MAGIC Cada pedido vira até 5 eventos, um por etapa com data conhecida. Cada evento carrega **todos os campos conhecidos até aquele momento** (o SCD 1 sobrescreve a linha inteira; campo nulo sobrescreve, a não ser com `IGNORE NULL UPDATES`).
# MAGIC
# MAGIC `event_ts` é forçado a ser estritamente crescente por pedido (há datas inconsistentes no Olist, ex.: entrega antes do despacho). Assim o `SEQUENCE BY event_ts` nunca empata.

# COMMAND ----------

etapas = [
    # (status do evento, coluna de data, rank)
    ("created", "order_purchase_timestamp", 0),
    ("approved", "order_approved_at", 1),
    ("shipped", "order_delivered_carrier_date", 2),
    ("delivered", "order_delivered_customer_date", 3),
]
campos_por_rank = {
    "approved_ts": 1,
    "carrier_ts": 2,
    "delivered_ts": 3,
}

partes = []
for status, col, rank in etapas:
    p = orders[orders[col].notna()][
        ["order_id", "customer_id", "order_status", "order_purchase_timestamp",
         "order_approved_at", "order_delivered_carrier_date",
         "order_delivered_customer_date", "order_estimated_delivery_date"]
    ].copy()
    p["event_status"] = status
    p["raw_ts"] = p[col]
    p["rank"] = rank
    partes.append(p)

# Status final que não é uma das etapas (canceled, unavailable, invoiced, processing…)
ultima_data = orders[ts_cols[:4]].max(axis=1)
finais = orders[~orders["order_status"].isin([e[0] for e in etapas])].copy()
finais = finais.assign(
    event_status=finais["order_status"],
    raw_ts=ultima_data.loc[finais.index],
    rank=4,
)
partes.append(finais[partes[0].columns])

eventos = pd.concat(partes, ignore_index=True).sort_values(["order_id", "rank"])

# Estritamente crescente por pedido: cummax da data + rank segundos
eventos["event_ts"] = (
    eventos.groupby("order_id")["raw_ts"].cummax()
    + pd.to_timedelta(eventos["rank"], unit="s")
)

# Campos conhecidos até a etapa do evento
eventos["purchase_ts"] = eventos["order_purchase_timestamp"]
eventos["approved_ts"] = eventos["order_approved_at"].where(eventos["rank"] >= 1)
eventos["carrier_ts"] = eventos["order_delivered_carrier_date"].where(eventos["rank"] >= 2)
eventos["delivered_ts"] = eventos["order_delivered_customer_date"].where(eventos["rank"] >= 3)
eventos["estimated_delivery_ts"] = eventos["order_estimated_delivery_date"]
eventos["order_status"] = eventos["event_status"]
eventos["operation"] = "UPSERT"
eventos["dia_evento"] = eventos["event_ts"].dt.date

eventos = eventos[
    ["order_id", "customer_id", "order_status", "event_ts", "purchase_ts",
     "approved_ts", "carrier_ts", "delivered_ts", "estimated_delivery_ts",
     "operation", "dia_evento"]
]
for c in ["event_ts", "purchase_ts", "approved_ts", "carrier_ts", "delivered_ts", "estimated_delivery_ts"]:
    eventos[c] = fmt_ts(eventos[c])

print(f"{len(eventos):,} eventos para {orders['order_id'].nunique():,} pedidos")

# COMMAND ----------

# MAGIC %md ## Fatos por dia de compra / criação

# COMMAND ----------

pedidos_dia = orders[["order_id", "customer_id", "dia_compra", "order_purchase_timestamp"]]

items_d = items.merge(pedidos_dia[["order_id", "dia_compra"]], on="order_id")
payments_d = payments.merge(pedidos_dia[["order_id", "dia_compra"]], on="order_id")

reviews["review_creation_date"] = pd.to_datetime(reviews["review_creation_date"], errors="coerce")
reviews_d = reviews[reviews["order_id"].isin(orders["order_id"])].copy()
reviews_d["dia_review"] = reviews_d["review_creation_date"].dt.date
reviews_d["review_creation_date"] = fmt_ts(reviews_d["review_creation_date"])

customers_d = customers.merge(pedidos_dia, on="customer_id")
customers_d["snapshot_ts"] = fmt_ts(customers_d["order_purchase_timestamp"])
customers_d = customers_d.drop(columns=["order_id", "order_purchase_timestamp"])

# COMMAND ----------

# MAGIC %md ## Registros ruins (opcional)

# COMMAND ----------

def ruins(dia: date) -> dict:
    """Linhas extras por entidade para testar expectations e delete."""
    if INJETAR == "nenhum":
        return {}
    ts = f"{dia} 23:00:00"
    base_evento = {c: None for c in eventos.columns if c != "dia_evento"}
    if INJETAR == "warn_drop":
        teste_id = f"TESTE-{dia}"
        return {
            "order_events": [
                {**base_evento, "order_id": None, "order_status": "created", "event_ts": ts, "operation": "UPSERT"},
                {**base_evento, "order_id": teste_id, "customer_id": "TESTE", "order_status": "created",
                 "event_ts": ts, "purchase_ts": ts, "operation": "UPSERT"},
                {**base_evento, "order_id": teste_id, "customer_id": "TESTE", "order_status": "created",
                 "event_ts": f"{dia} 23:30:00", "purchase_ts": ts, "operation": "DELETE"},
            ],
            "order_items": [
                {"order_id": teste_id, "order_item_id": "1", "product_id": "inexistente", "seller_id": "inexistente",
                 "shipping_limit_date": ts, "price": "-10.00", "freight_value": "0"},
            ],
            "reviews": [
                {"review_id": f"TESTE-{dia}", "order_id": teste_id, "review_score": "7",
                 "review_comment_title": None, "review_comment_message": "nota inválida de teste",
                 "review_creation_date": ts, "review_answer_timestamp": ts},
            ],
        }
    if INJETAR == "fail":
        alvo = eventos["order_id"].iloc[0]
        return {
            "order_events": [
                {**base_evento, "order_id": alvo, "order_status": "approved", "event_ts": None, "operation": "UPSERT"},
            ]
        }
    return {}

# COMMAND ----------

# MAGIC %md ## Geração

# COMMAND ----------

resumo = []
for i in range(DIAS):
    dia = proximo_dia + timedelta(days=i)
    extras = ruins(dia)

    # order_events por último: é a pasta vigiada pelo trigger de file arrival
    # do job, então quando ela recebe arquivo as outras já estão completas.
    lotes = {
        "customers": customers_d[customers_d["dia_compra"] == dia].drop(columns=["dia_compra"]),
        "order_items": items_d[items_d["dia_compra"] == dia].drop(columns=["dia_compra"]),
        "payments": payments_d[payments_d["dia_compra"] == dia].drop(columns=["dia_compra"]),
        "reviews": reviews_d[reviews_d["dia_review"] == dia].drop(columns=["dia_review"]),
        "order_events": eventos[eventos["dia_evento"] == dia].drop(columns=["dia_evento"]),
    }
    contagem = {"dia": str(dia)}
    for entidade, df in lotes.items():
        if entidade in extras:
            df = pd.concat([df, pd.DataFrame(extras[entidade])], ignore_index=True)
        contagem[entidade] = gravar_jsonl(df, entidade, f"{entidade}_{dia}.json")
    resumo.append(contagem)

estado["proximo_dia"] = str(proximo_dia + timedelta(days=DIAS))
os.makedirs(os.path.dirname(ESTADO), exist_ok=True)
with open(ESTADO, "w") as f:
    json.dump(estado, f)

display(pd.DataFrame(resumo))
print(f"Próximo dia a gerar: {estado['proximo_dia']}")