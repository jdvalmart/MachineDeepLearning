import redis
import pandas as pd
import time

# Conexión a Redis
client = redis.StrictRedis(host="localhost", port=6379, decode_responses=True)

# Cargar dataset
df = pd.read_csv("movies.csv")  # Asegúrate de tener el archivo


# Función para cargar datos en Redis (cada movieId -> title)
def cache_data():
    for _, row in df.iterrows():
        client.set(str(row["movieId"]), row["title"], ex=10)
    print("Datos cargados en Redis")


# Función para obtener título con tiempo de medición
def get_movie_title(movie_id):
    start = time.time()
    title = client.get(str(movie_id))
    elapsed = time.time() - start
    if title:
        print(f"Cache: {title} (tiempo: {elapsed:.6f}s)")
    else:
        print(f"No encontrado en cache, consultando CSV...")
        # Simular consulta a base de datos
        start = time.time()
        row = df[df["movieId"] == movie_id]
        elapsed_db = time.time() - start
        if not row.empty:
            print(f"Desde CSV: {row.iloc[0]['title']} (tiempo: {elapsed_db:.6f}s)")
        else:
            print("No existe")


if __name__ == "__main__":
    cache_data()  # llenar cache
    # Probar múltiples búsquedas
    for movie_id in [1, 2, 3, 1, 2, 3]:
        get_movie_title(movie_id)
