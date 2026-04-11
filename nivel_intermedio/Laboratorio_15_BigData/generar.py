import random

palabras = [
    "hadoop",
    "mapreduce",
    "data",
    "python",
    "big",
    "data",
    "cloud",
    "computing",
    "streaming",
    "file",
    "analysis",
    "word",
    "count",
    "example",
    "input",
    "output",
    "system",
    "network",
    "process",
    "distributed",
    "parallel",
    "task",
    "job",
    "reduce",
]

texto_generado = " ".join(random.choices(palabras, k=10000))

with open("archivo_palabras.txt", "w") as archivo:
    archivo.write(texto_generado)

print("Archivo generado")
