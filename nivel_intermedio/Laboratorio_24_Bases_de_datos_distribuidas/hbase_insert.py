import happybase
import pandas as pd

# Conexión a Thrift
connection = happybase.Connection("localhost", port=9090)
table = connection.table("movies")

# Cargar dataset (asegúrate de tener movies.csv en el directorio)
df = pd.read_csv("movies.csv")

for i, row in df.iterrows():
    table.put(
        str(row["movieId"]),
        {
            b"details:title": row["title"].encode("utf-8"),
            b"details:genres": row["genres"].encode("utf-8"),
        },
    )
    if i % 100 == 0:
        print(f"Insertadas {i} películas")

print("Inserción completa")
connection.close()
