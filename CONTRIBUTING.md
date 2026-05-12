# Contributing — Guía para laboratorios

Cómo trabajar en este repositorio sin romper nada.

---

## 🚀 Arranque rápido

```bash
cd ~/proyectos/bootcamp-ia-mintic
source venv_tf/bin/activate    # ← SIEMPRE primero
```

Si no ves `(venv_tf)` al inicio de tu terminal, no has activado el entorno.

---

## 📓 Cómo usar Jupyter Notebook correctamente

### El problema

Si abres Jupyter desde la raíz del proyecto:

```bash
cd ~/proyectos/bootcamp-ia-mintic
jupyter notebook   # ❌ CUIDADO
```

Jupyter toma como directorio de trabajo la raíz. Si tu notebook está en
`nivel_intermedio/` y haces `pd.read_csv('datasets/archivo.csv')`, Python
busca en `bootcamp-ia-mintic/datasets/` (¡no existe!) en vez de
`bootcamp-ia-mintic/nivel_intermedio/datasets/`.

### La solución

**Abre Jupyter DENTRO del nivel donde vas a trabajar:**

```bash
# Para labs de nivel básico
cd ~/proyectos/bootcamp-ia-mintic/nivel_basico
source ../venv_tf/bin/activate
jupyter notebook

# Para labs de nivel intermedio
cd ~/proyectos/bootcamp-ia-mintic/nivel_intermedio
source ../venv_tf/bin/activate
jupyter notebook
```

De esta forma:
- `pd.read_csv('datasets/archivo.csv')` → busca en `nivel_intermedio/datasets/` ✅
- El kernel de Jupyter se llama **"Python 3.10 (venv_tf)"** — selecciónalo siempre.

---

## 🧪 Checklist: laboratorio nuevo

### Antes de escribir código

- [ ] `source venv_tf/bin/activate` (desde la raíz del proyecto)
- [ ] `cd nivel_basico` o `cd nivel_intermedio` (según corresponda)
- [ ] `jupyter notebook`
- [ ] Kernel correcto seleccionado: **Python 3.10 (venv_tf)**

### Durante el laboratorio

- [ ] Notebook guardado en la carpeta del nivel correcto
- [ ] Nombre del archivo: `Laboratorio_XX_Tema_del_lab.ipynb` (guiones bajos, sin espacios)
- [ ] Estructura del notebook: **markdown → código → markdown → código...**
- [ ] Cada bloque de código tiene su celda markdown arriba explicando QUÉ hace y POR QUÉ
- [ ] Datasets guardados en `datasets/` (no en la raíz del nivel)
- [ ] Rutas a archivos **relativas**, nunca absolutas (ej: `'datasets/mi_csv.csv'`)
- [ ] Si instalas un paquete nuevo → `pip freeze --local > ../requirements.txt`

### Estructura del notebook

| Celda | Tipo | Contenido |
|-------|------|-----------|
| 1 | `markdown` | `# Laboratorio XX — Tema` + objetivo del lab |
| 2 | `markdown` | `## 1. Nombre de sección` + explicación del concepto |
| 3 | `code` | Código de esa sección |
| 4 | `markdown` | `## 2. Nombre de sección` + explicación |
| 5 | `code` | Código |
| ... | ... | ... |
| Final | `markdown` | `## Conclusión` + resumen de lo aprendido |

### Antes de commitear

- [ ] Limpiar outputs: `jupyter nbconvert --clear-output --inplace ruta/notebook.ipynb`
- [ ] `git status` — verificar que no haya `.h5`, `.png`, `.db` en los cambios
- [ ] Commit con mensaje descriptivo: `feat(Laboratorio XX): descripción`

---

## 📝 Convención de commits

Usamos [Conventional Commits](https://www.conventionalcommits.org/):

| Prefijo | Cuándo usarlo |
|---------|--------------|
| `feat(Laboratorio XX):` | Laboratorio nuevo |
| `fix:` | Corrección de errores |
| `docs:` | Cambios en documentación |
| `refactor:` | Reorganización sin cambiar funcionalidad |
| `chore:` | Tareas de mantenimiento |
| `perf:` | Mejoras de rendimiento |

Ejemplos:
```
feat(Laboratorio 27): introduccion a transformers
fix: corregir path del dataset en Lab 6
docs: actualizar README con nuevo lab
```

---

## 📦 Dependencias

Si necesitas instalar un paquete nuevo:

```bash
source venv_tf/bin/activate
pip install nombre-paquete
pip freeze --local > requirements.txt   # actualiza la lista
```

**Nunca** uses `!pip install` dentro de una celda del notebook si tienes el
venv activado — el `!` de Jupyter puede instalar en el lugar equivocado.

---

## 🗂️ Organización de archivos

```
bootcamp-ia-mintic/
├── venv_tf/               ← entorno virtual (NO se commitea)
├── requirements.txt       ← dependencias
├── .gitignore             ← archivos a ignorar
├── README.md
├── CONTRIBUTING.md        ← este archivo
├── nivel_basico/
│   ├── datasets/          ← CSVs y JSONs de labs básicos
│   ├── Laboratorio_08_...
│   └── ...
├── nivel_intermedio/
│   ├── datasets/          ← CSVs de labs intermedios
│   ├── Laboratorio_01_...
│   └── Laboratorio_15_BigData/
└── entregables/           ← trabajos finales
    ├── Entregable_1/
    ├── Entregable_5/
    └── Entregable_6/
```
