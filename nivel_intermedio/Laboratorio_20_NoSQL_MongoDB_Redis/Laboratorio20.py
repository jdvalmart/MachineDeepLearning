"""
Laboratoreio    20: NoSQL, MongoDB y Redis
Autor:          Juan David Valencia
Descripción:    Laboratorio para practicar el uso de bases de datos NoSQL, específicamente MongoDB y Redis.
"""

from pymongo import MongoClient
import redis
import uuid
from datetime import datetime, time, timezone

SEP = "-" * 55

# --------------------------------------
# ejercicio 1: CRUD Básico con MongoDB
# --------------------------------------

print(f"\n{SEP}")
print("Ejercicio 1: CRUD Básico con MongoDB")
print(SEP)

# Conexión a MongoDB
mongo = MongoClient("mongodb://localhost:27017/")
db = mongo["estudiantes"]
colletion = db["informacion"]
colletion.drop()  # Limpiar la colección para el ejercicio

# Create

estudiante = {
    "nombre": "Laura",
    "edad": 23,
    "carrera": "Ingenieria de software",
    "promedio": 4.3,
}
colletion.insert_one(estudiante)
print("Estudiante insertado: Laura")

estudiantes = [
    {"nombre": "Juan", "edad": 22, "carrera": "Ingeniería", "promedio": 3.8},
    {"nombre": "María", "edad": 24, "carrera": "Medicina", "promedio": 4.5},
    {"nombre": "Carlos", "edad": 21, "carrera": "Derecho", "promedio": 3.2},
]
colletion.insert_many(estudiantes)
print("Estudiantes insertados:")
for estudiante in colletion.find():
    print(estudiante)

# Read

print("\n---- Todos los estudiantes ----")
for estudiante in colletion.find({}, {"_id": 0}):
    print(" ", estudiante)

# Update
colletion.update_one({"nombre": "Juan"}, {"$set": {"promedio": 4.0}})
actualizado = colletion.find_one({"nombre": "Juan"}, {"_id": 0})
print(f"\n✔ Promedio de Juan actualizado → {actualizado}")

# Delete
colletion.delete_one({"nombre": "Carlos"})
print("\n✔ Estudiante Carlos eliminado")
print("\n---- Estudiantes restantes ----")
for estudiante in colletion.find({}, {"_id": 0}):
    print(" ", estudiante)

# --------------------------------------
# Ejercicio 2: Operaciones básicas con Redis
# --------------------------------------

print(f"\n{SEP}")
print("Ejercicio 2: Operaciones básicas con Redis")
print(SEP)

# Conexión a Redis
r = redis.StrictRedis(host="localhost", port=6379, decode_responses=True)
r.flushdb()  # Limpiar la base de datos para el ejercicio

# SET / GET
r.set("usuario:1001", "Laura Sánchez")
print(f"✔ clave almacenada -> usuario:1001")
print(f"Valor de usuario:1001 -> {r.get('usuario:1001')}")

# TTL (token de sesión)
r.setex("token:123", 300, "activo")
print(f"\n✔ Token con TTL=300s → token:123 = {r.get('token:123')}")
print(f"  TTL restante       → {r.ttl('token:123')} segundos")

# LISTA de tareas
r.delete("tareas")
r.rpush("tareas", "Tarea1", "Tarea2", "Tarea3")
print(f"\n✔ Lista 'tareas'    → {r.lrange('tareas', 0, -1)}")

# ─────────────────────────────────────────────
# EJERCICIO 3: Escalabilidad y TTL en Redis
# ─────────────────────────────────────────────
print(f"\n{SEP}")
print("EJERCICIO 3: Escalabilidad y TTL en Redis")
print(SEP)

for i in range(5):
    r.setex(f"clave:{i}", 10 * (i + 1), f"valor_{i}")
    print(f"✔ clave:{i}  creada con TTL = {10*(i+1)}s")

print("\n── Verificación de TTLs ──")
for i in range(5):
    print(f"  clave:{i}  TTL restante → {r.ttl(f'clave:{i}')}s")

# ─────────────────────────────────────────────
# EJERCICIO 4: Datos jerárquicos en MongoDB
# ─────────────────────────────────────────────
print(f"\n{SEP}")
print("EJERCICIO 4: Datos Jerárquicos en MongoDB")
print(SEP)

db2 = mongo["jerarquia"]
col_org = db2["organizacion"]
col_org.drop()

organizacion = {
    "departamento": "Tecnología",
    "jefe": "Laura Sánchez",
    "empleados": [
        {"nombre": "Jorge Morales", "rol": "Desarrollador"},
        {"nombre": "Liliana Gómez", "rol": "Analista de Datos"},
        {"nombre": "Carlos Ruiz", "rol": "Soporte Técnico"},
    ],
}
res = col_org.insert_one(organizacion)
print(f"✔ Documento jerárquico insertado → ID: {res.inserted_id}")

# Consultar subdocumento
emp = col_org.find_one(
    {"empleados.nombre": "Liliana Gómez"}, {"empleados.$": 1, "_id": 0}
)
print(f"\n✔ Subdocumento (Liliana Gómez) → {emp}")

# Actualizar subdocumento
col_org.update_one(
    {"empleados.nombre": "Carlos Ruiz"},
    {"$set": {"empleados.$.rol": "Administrador de Sistemas"}},
)
print("✔ Rol de Carlos Ruiz actualizado → Administrador de Sistemas")

# Eliminar subdocumento
col_org.update_one(
    {"departamento": "Tecnología"},
    {"$pull": {"empleados": {"nombre": "Jorge Morales"}}},
)
print("✔ Jorge Morales eliminado de la lista")

doc_final = col_org.find_one({"departamento": "Tecnología"}, {"_id": 0})
print(f"\n  Documento final:\n  {doc_final}")

# ─────────────────────────────────────────────
# RETO FINAL: Gestión de Usuarios (MongoDB + Redis)
# ─────────────────────────────────────────────
print(f"\n{SEP}")
print("RETO FINAL: Gestión de Usuarios (MongoDB + Redis)")
print(SEP)

db3 = mongo["app_usuarios"]
usuarios_col = db3["perfiles"]
usuarios_col.drop()


def registrar_usuario(nombre, email, rol):
    """Inserta perfil en MongoDB y genera token de sesión en Redis."""
    perfil = {
        "nombre": nombre,
        "email": email,
        "rol": rol,
        "creado_en": datetime.now(timezone.utc).strftime("%Y-%m-%d %H:%M:%S"),
    }
    result = usuarios_col.insert_one(perfil)
    user_id = str(result.inserted_id)

    token = str(uuid.uuid4())
    r.setex(f"session:{user_id}", 600, token)  # sesión válida 10 min
    return user_id, token


def obtener_sesion(user_id):
    """Recupera el token activo y el perfil del usuario."""
    token = r.get(f"session:{user_id}")
    ttl = r.ttl(f"session:{user_id}")
    from bson import ObjectId

    perfil = usuarios_col.find_one({"_id": ObjectId(user_id)}, {"_id": 0})
    return perfil, token, ttl


# Registrar 3 usuarios
usuarios_demo = [
    ("Ana Torres", "ana@mail.com", "admin"),
    ("Luis Pérez", "luis@mail.com", "editor"),
    ("María Castro", "maria@mail.com", "viewer"),
]

ids = []
print("── Registro de usuarios ──")
for nombre, email, rol in usuarios_demo:
    uid, tok = registrar_usuario(nombre, email, rol)
    ids.append(uid)
    print(f"  ✔ {nombre:15} | ID: {uid} | token: {tok[:8]}...")

print("\n── Recuperación de sesiones activas ──")
for uid in ids:
    perfil, token, ttl = obtener_sesion(uid)
    print(
        f"  📄 {perfil['nombre']:15} | rol: {perfil['rol']:7} "
        f"| token activo: {str(token)[:8]}... | TTL: {ttl}s"
    )

print(f"\n✔ Total perfiles en MongoDB: {usuarios_col.count_documents({})}")
print(f"✔ Total sesiones en Redis:   {len(r.keys('session:*'))}")

print(f"\n{SEP}")
print("LAB 20 COMPLETADO ✅")
print(SEP)
