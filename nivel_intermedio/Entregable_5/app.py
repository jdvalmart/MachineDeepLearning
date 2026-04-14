"""
app.py — Microservicio de Inferencia
Entregable 5 · Talento Tech · Sesión 21

Endpoints:
  POST /predict   → Inferencia con modelo entrenado (+ caché Redis)
  GET  /health    → Health check del servicio

Uso:
  curl -X POST http://localhost:5000/predict \
       -H "Content-Type: application/json" \
       -d '{"features": [5.1, 3.5, 1.4, 0.2]}'
"""

from flask import Flask, request, jsonify
import numpy as np
import pickle
import json
import os
import time
from sklearn.datasets import load_iris
from sklearn.ensemble import RandomForestClassifier
from sklearn.model_selection import train_test_split

app = Flask(__name__)

# ──────────────────────────────────────────
# Cargar o entrenar modelo al iniciar
# ──────────────────────────────────────────
MODEL_PATH = os.environ.get("MODEL_PATH", "model.pkl")
iris = load_iris()


def entrenar_y_guardar_modelo():
    X_train, _, y_train, _ = train_test_split(
        iris.data, iris.target, test_size=0.2, random_state=42
    )
    clf = RandomForestClassifier(n_estimators=50, random_state=42)
    clf.fit(X_train, y_train)
    with open(MODEL_PATH, "wb") as f:
        pickle.dump(clf, f)
    print(f"[app] Modelo guardado en {MODEL_PATH}")
    return clf


if os.path.exists(MODEL_PATH):
    with open(MODEL_PATH, "rb") as f:
        modelo = pickle.load(f)
    print(f"[app] Modelo cargado desde {MODEL_PATH}")
else:
    modelo = entrenar_y_guardar_modelo()

# ──────────────────────────────────────────
# Caché Redis (con fallback en memoria)
# ──────────────────────────────────────────
try:
    import redis

    cache = redis.Redis(
        host=os.environ.get("REDIS_HOST", "localhost"),
        port=int(os.environ.get("REDIS_PORT", 6379)),
        decode_responses=True,
    )
    cache.ping()
    print("[app] Redis conectado")
    USE_REDIS = True
except Exception:
    print("[app] Redis no disponible — usando caché en memoria")
    USE_REDIS = False
    _mem_cache = {}


def cache_get(key: str):
    if USE_REDIS:
        return cache.get(key)
    return _mem_cache.get(key)


def cache_set(key: str, value: str, ex: int = 300):
    if USE_REDIS:
        cache.set(key, value, ex=ex)
    else:
        _mem_cache[key] = value


# ──────────────────────────────────────────
# Endpoints
# ──────────────────────────────────────────
@app.route("/health", methods=["GET"])
def health():
    return jsonify(
        {
            "status": "ok",
            "model": MODEL_PATH,
            "redis": USE_REDIS,
            "timestamp": time.strftime("%Y-%m-%dT%H:%M:%S"),
        }
    )


@app.route("/predict", methods=["POST"])
def predict():
    t0 = time.perf_counter()
    data = request.get_json()

    # Validación
    if not data or "features" not in data:
        return (
            jsonify({"error": "Se requiere campo 'features' (lista de 4 floats)"}),
            400,
        )

    features = data["features"]
    if len(features) != 4:
        return jsonify({"error": "Se requieren exactamente 4 features para Iris"}), 400

    # Verificar caché
    cache_key = f"pred:{hash(tuple(features))}"
    cached = cache_get(cache_key)
    if cached:
        result = json.loads(cached)
        result["cache"] = "HIT"
        result["latencia_ms"] = round((time.perf_counter() - t0) * 1000, 2)
        return jsonify(result)

    # Inferencia
    X = np.array(features).reshape(1, -1)
    pred = modelo.predict(X)[0]
    proba = modelo.predict_proba(X)[0]

    result = {
        "prediccion": int(pred),
        "especie": iris.target_names[pred],
        "confianza": round(float(max(proba)), 4),
        "probabilidades": {
            name: round(float(p), 4) for name, p in zip(iris.target_names, proba)
        },
        "cache": "MISS",
    }

    # Guardar en caché (TTL 5 min)
    cache_set(cache_key, json.dumps(result), ex=300)
    result["latencia_ms"] = round((time.perf_counter() - t0) * 1000, 2)
    return jsonify(result)


if __name__ == "__main__":
    port = int(os.environ.get("PORT", 5000))
    print(f"[app] Iniciando servidor en puerto {port}")
    app.run(host="0.0.0.0", port=port, debug=False)
