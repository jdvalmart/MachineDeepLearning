"""
=============================================================
LABORATORIO 21 — Integración de Bases de Datos en IA y Big Data
Talento Tech · Sesión 21
=============================================================
NOTA: Ejercicios 2, 3 y 4 requieren Kafka y Elasticsearch
corriendo. Aquí se ejecutan con SIMULADORES locales para
que puedas correr el código sin infraestructura adicional.
En producción, solo cambia el bootstrap_servers / Elasticsearch URL.
=============================================================
"""

import sqlite3
import pandas as pd
import json
import time
import random
from datetime import datetime

# EJERCICIO 1: Prepocesamiento de Datos con SQLite

print("=" * 60)
print("EJERCICIO 1: Prepocesamiento de Datos con SQLite")
print("=" * 60)

# Crear una base de datos SQLite y una tabla
conn = sqlite3.connect(
    ":memory:"
)  # En memoria para el lab; en producción: 'big_data_lab.db'
cursor = conn.cursor()

cursor.execute("""
CREATE TABLE IF NOT EXISTS ventas (
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    producto TEXT,
    cantidad INTEGER,
    precio FLOAT,
    fecha TEXT
)""")

# Insertar datos de ejemplo
datos_ventas = [
    ("Laptop", 1, 1200.00, "2024-06-01"),
    ("Smartphone", 2, 800.00, "2024-06-02"),
    ("Tablet", 3, 300.00, "2024-06-03"),
    ("Monitor", 2, 1200.00, "2024-06-04"),
]
cursor.executemany(
    "INSERT INTO ventas (producto, cantidad, precio, fecha) VALUES (?, ?, ?, ?)",
    datos_ventas,
)
conn.commit()
print("Datos insertados en SQLite.")

# Leer y procesar los datos con pandas
df = pd.read_sql_query("SELECT * FROM ventas", conn)
print("\n 📊 DataFrame original:")
print(df.to_string(index=False))

print("\n🔍 Valores faltantes por columna:")
print(df.isnull().sum().to_string())

# Rellenar valores faltantes (si hubiera) con la mediana
mediana_cantidad = df["cantidad"].median()
df["cantidad"].fillna(mediana_cantidad)

print(f"\n✅ 'cantidad' faltante rellenada con la mediana: {mediana_cantidad}")
print("\n📊 DataFrame después del preprocesamiento:")
print(df.to_string(index=False))

# Guardar en una tabla nueva
df.to_sql("ventas_preprocesadas", conn, if_exists="replace", index=False)
print("\n✅ Datos preprocesados guardados en tabla 'ventas_preprocesadas'.")

# Verificación
df_check = pd.read_sql_query("SELECT * FROM ventas_preprocesadas", conn)
print("\n📊 Verificación — tabla 'ventas_preprocesadas':")
print(df_check.to_string(index=False))

print("\n" + "=" * 60)
print("EJERCICIO 2: Ingesta de Datos en Streaming con Kafka")
print("(SIMULADO — sin broker real)")
print("=" * 60)


# Simulador de Kafka con una cola en memoria
class KafkaTopicSimulado:
    """Simula un Topic de Kafka en memoria para el lab."""

    def __init__(self):
        self._topics: dict[str, list] = {}

    def send(self, topic: str, value: dict):
        if topic not in self._topics:
            self._topics[topic] = []
        self._topics[topic].append(json.dumps(value).encode("utf-8"))
        print(f"  📨 [Producer] → topic '{topic}': {value}")

    def consume(self, topic: str, group_id: str = "grupo_default"):
        mensajes = self._topics.get(topic, [])
        print(
            f"\n  📥 [Consumer group='{group_id}'] ← topic '{topic}': {len(mensajes)} mensajes"
        )
        for msg in mensajes:
            dato = json.loads(msg.decode("utf-8"))
            print(f"     → {dato}")
        return mensajes


kafka_broker = KafkaTopicSimulado()

# Productor
print("\n[PRODUCTOR] Enviando eventos al topic 'eventos_tiempo_real':")
mensajes = [
    {
        "id": 1,
        "evento": "inicio_sesion",
        "usuario": "usuario1",
        "ts": datetime.now().isoformat(),
    },
    {
        "id": 2,
        "evento": "compra",
        "usuario": "usuario2",
        "ts": datetime.now().isoformat(),
    },
    {
        "id": 3,
        "evento": "salida",
        "usuario": "usuario1",
        "ts": datetime.now().isoformat(),
    },
]
for m in mensajes:
    kafka_broker.send("eventos_tiempo_real", value=m)

print("\n✅ Mensajes enviados al topic 'eventos_tiempo_real'.")

# Consumidor
kafka_broker.consume("eventos_tiempo_real", group_id="grupo1")

# ─────────────────────────────────────────────────────────────────
# EJERCICIO 3: Análisis de Datos con Elasticsearch (SIMULADO)
# ─────────────────────────────────────────────────────────────────
print("\n" + "=" * 60)
print("EJERCICIO 3: Análisis de Datos con Elasticsearch")
print("(SIMULADO — sin servidor real)")
print("=" * 60)


class ElasticsearchSimulado:
    """
    Simula Elasticsearch en memoria.
    En producción: es = Elasticsearch("http://localhost:9200")
    """

    def __init__(self):
        self._indices: dict[str, list] = {}

    def index(self, index: str, id: int, document: dict):
        if index not in self._indices:
            self._indices[index] = []
        document["_id"] = id
        self._indices[index].append(document)
        print(f"  📌 Indexado en '{index}' [id={id}]: {document}")

    def search(self, index: str, query: dict) -> dict:
        docs = self._indices.get(index, [])
        campo, valor = list(query["match"].items())[0]
        hits = [d for d in docs if d.get(campo) == valor]
        return {"hits": {"hits": [{"_source": h} for h in hits], "total": len(hits)}}


es = ElasticsearchSimulado()

print("\n[ELASTICSEARCH] Indexando datos de eventos:")
datos_es = [
    {
        "usuario": "usuario1",
        "evento": "inicio_sesion",
        "timestamp": "2024-12-07T10:00:00",
    },
    {"usuario": "usuario2", "evento": "compra", "timestamp": "2024-12-07T10:05:00"},
    {"usuario": "usuario1", "evento": "compra", "timestamp": "2024-12-07T10:10:00"},
    {"usuario": "usuario3", "evento": "salida", "timestamp": "2024-12-07T10:15:00"},
]
for i, dato in enumerate(datos_es):
    es.index(index="eventos", id=i, document=dato)

print("\n✅ Datos indexados en Elasticsearch.")

# Búsqueda
print("\n[BÚSQUEDA] Eventos de 'usuario1':")
resultado = es.search(index="eventos", query={"match": {"usuario": "usuario1"}})
print(f"  Total encontrados: {resultado['hits']['total']}")
for hit in resultado["hits"]["hits"]:
    print(f"  → {hit['_source']}")

# ─────────────────────────────────────────────────────────────────
# EJERCICIO 4: Estrategias de Escalabilidad
# ─────────────────────────────────────────────────────────────────
print("\n" + "=" * 60)
print("EJERCICIO 4: Estrategias de Escalabilidad")
print("(Demostración de distribución por group_id)")
print("=" * 60)

# Simular múltiples consumer groups recibiendo mensajes distintos
print("\n[ESCALABILIDAD] Distribuyendo mensajes entre múltiples consumers:")

mensajes_para_distribuir = [
    {
        "id": i,
        "evento": random.choice(["compra", "visita", "click"]),
        "usuario": f"user{i}",
    }
    for i in range(1, 9)
]

# Enviar todos al mismo topic
for m in mensajes_para_distribuir:
    kafka_broker.send("eventos_escala", value=m)

print("\n[CONSUMER GROUP 'grupo1'] → procesa mensajes impares (partición 0):")
for m in mensajes_para_distribuir[::2]:
    print(f"  → Consumer-A procesó: {m}")

print("\n[CONSUMER GROUP 'grupo1'] → procesa mensajes pares (partición 1):")
for m in mensajes_para_distribuir[1::2]:
    print(f"  → Consumer-B procesó: {m}")

print("\n✅ Distribución de carga demostrada. En Kafka real, esto se gestiona")
print("   automáticamente con particiones y group_id.")

# ─────────────────────────────────────────────────────────────────
# RETO FINAL: Pipeline Integrado SQL → Kafka → Elasticsearch
# ─────────────────────────────────────────────────────────────────
print("\n" + "=" * 60)
print("RETO FINAL: Pipeline Integrado")
print("SQL → Kafka Producer → Consumer → Elasticsearch → Consulta")
print("=" * 60)


def pipeline_integrado():
    """
    Pipeline completo:
    1. Leer eventos de SQL (tabla ventas_preprocesadas)
    2. Producir cada venta a un topic Kafka
    3. Consumer indexa en Elasticsearch
    4. Consultar Elasticsearch por producto
    """

    print("\n[PASO 1] Leyendo datos de SQL (ventas_preprocesadas)...")
    df_ventas = pd.read_sql_query("SELECT * FROM ventas_preprocesadas", conn)
    print(df_ventas.to_string(index=False))

    print("\n[PASO 2] Produciendo eventos a Kafka topic 'ventas_pipeline'...")
    broker = KafkaTopicSimulado()
    for _, row in df_ventas.iterrows():
        evento = {
            "id": int(row["id"]),
            "producto": row["producto"],
            "cantidad": int(row["cantidad"]),
            "precio": float(row["precio"]),
            "fecha": row["fecha"],
            "ts": datetime.now().isoformat(),
        }
        broker.send("ventas_pipeline", value=evento)

    print("\n[PASO 3] Consumer indexando en Elasticsearch...")
    es2 = ElasticsearchSimulado()
    mensajes_consumidos = broker.consume("ventas_pipeline", group_id="pipeline-group")
    for msg in mensajes_consumidos:
        doc = json.loads(msg.decode("utf-8"))
        es2.index(index="ventas_index", id=doc["id"], document=doc)

    print("\n[PASO 4] Consultando Elasticsearch — buscar 'Laptop'...")
    resultado = es2.search(
        index="ventas_index", query={"match": {"producto": "Laptop"}}
    )
    print(f"  Resultados para 'Laptop': {resultado['hits']['total']}")
    for hit in resultado["hits"]["hits"]:
        print(f"  → {hit['_source']}")

    print("\n✅ Pipeline integrado completado exitosamente.")
    return resultado


resultado_reto = pipeline_integrado()

print("\n" + "=" * 60)
print("LABORATORIO 21 COMPLETADO ✅")
print("=" * 60)
print("\nNOTA PARA PRODUCCIÓN:")
print("  - Ejercicio 1: ya funciona con PostgreSQL real (cambiar connect)")
print("  - Ejercicio 2: reemplaza KafkaTopicSimulado con KafkaProducer/Consumer")
print("    real de kafka-python (bootstrap_servers='localhost:9092')")
print("  - Ejercicio 3: reemplaza ElasticsearchSimulado con")
print("    Elasticsearch('http://localhost:9200') de la lib elasticsearch-py")
print("  - Ejercicio 4: configura particiones en el topic y múltiples")
print("    consumers con el mismo group_id para distribución automática")
