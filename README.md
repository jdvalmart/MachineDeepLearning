# 🤖 Machine Deep Learning — Portafolio de IA

Repositorio de aprendizaje del Bootcamp de Inteligencia Artificial
(MINTIC / Talento Tech Valle / Universidad Libre) — Niveles Básico e Intermedio.

---

## ⚡ Inicio rápido

### Requisitos previos

- **Python 3.10** o superior (desarrollado con 3.10.12)
- **Git** instalado
- **pip** actualizado: `pip install --upgrade pip`
- 5 GB de espacio libre (TensorFlow + PyTorch son pesados)

### Instalación paso a paso

```bash
# 1. Clonar el repositorio
git clone https://github.com/jdvalmart/bootcamp-ia-mintic.git
cd bootcamp-ia-mintic

# 2. Crear entorno virtual
python3 -m venv venv_tf
source venv_tf/bin/activate        # Linux / macOS
# venv_tf\Scripts\activate         # Windows (PowerShell)
# venv_tf\Scripts\activate.bat     # Windows (CMD)

# 3. Instalar dependencias (~15 min la primera vez)
pip install --upgrade pip
pip install -r requirements.txt

# 4. Registrar el kernel en Jupyter (opcional, para VS Code)
python -m ipykernel install --user --name=venv_tf --display-name="Python 3.10 (venv_tf)"

# 5. Abrir Jupyter
jupyter notebook
```

### Abrir un laboratorio específico

```bash
# Desde la raíz del proyecto, con el venv activado:
jupyter notebook nivel_intermedio/Laboratorio_5_Redes_neuronales_convolucionales.ipynb
```

### Solución de problemas comunes

| Problema | Solución |
|----------|----------|
| `ModuleNotFoundError: No module named 'tensorflow'` | El venv no está activado. Ejecuta `source venv_tf/bin/activate` |
| Jupyter no encuentra el kernel | Instálalo manualmente con el paso 4 de arriba |
| Error de memoria con TensorFlow | Se usa solo CPU. Cierra otras aplicaciones pesadas |
| `CUDA error` o warnings de GPU | Ignorar — TensorFlow corre en CPU, los warnings son inofensivos |
| El notebook se ve sin outputs | Es normal — los outputs se limpiaron para mantener el repo ligero. Ejecuta las celdas para regenerarlos |

### Nota sobre GPU

Este proyecto está configurado para **CPU solamente**. Si tienes GPU NVIDIA y quieres acelerar
TensorFlow, instala en su lugar:

```bash
pip uninstall tensorflow
pip install tensorflow[and-cuda]
```

---

## 📚 Contenido

### 🟢 Nivel Básico

| # | Tema | Archivo |
|---|------|---------|
| 08 | IA en programación de computadores | `nivel_basico/Laboratorio_8_IA_en_programación_de_computadores.ipynb` |
| 09 | Librerías de Python para IA | `nivel_basico/Laboratorio_9_Librerias_de_python_para_la_IA.ipynb` |
| 10 | Scikit-learn y Seaborn | `nivel_basico/Laboratorio_10_librerias_Scikit-learn_y_Seaborn.ipynb` |
| 11 | Análisis descriptivo y diagnóstico | `nivel_basico/Laboratorio_11_Análisis_descriptivo_y_diagnóstico.ipynb` |
| 12 | Análisis predictivo | `nivel_basico/Laboratorio_12_Análisis_predictivo.ipynb` |
| 13 | Análisis de sentimiento | `nivel_basico/Laboratorio_13_Analisis_de_sentimiento.ipynb` |
| 14 | Aprendizaje automático | `nivel_basico/Laboratorio_14_Aprendizaje_automatico.ipynb` |
| 15 | Aprendizaje supervisado | `nivel_basico/Laboratorio_15_Aprendizaje_supervisado.ipynb` |
| 16 | Aprendizaje no supervisado | `nivel_basico/Laboratorio_16_Aprendizaje_no_supervisado.ipynb` |
| 17 | Aprendizaje inductivo | `nivel_basico/Laboratorio_17_Aprendizaje_inductivo.ipynb` |
| 18 | Aprendizaje por refuerzo | `nivel_basico/Laboratorio_18_Aprendizaje_por_refuezo.ipynb` |
| 19 | Reconocimiento de imágenes | `nivel_basico/Laboratorio_19_reconocimiento_de_imagenes.ipynb` |
| 25 | Asistente virtual (integrador) | `nivel_basico/Laboratorio_25_asistente_virtual.ipynb` |

### 🔵 Nivel Intermedio — Machine Learning

| # | Tema | Archivo |
|---|------|---------|
| 01 | Desarrollo en la IA: NumPy, pandas, scikit-learn | `nivel_intermedio/Laboratorio_1_Desarrollo_en_la_IA.ipynb` |
| 02 | Fundamentos de aprendizaje automático | `nivel_intermedio/Laboratorio_2_Fundamntos_de_aprendizaje_automatico.ipynb` |
| 03 | Redes neuronales artificiales | `nivel_intermedio/Laboratorio_3_redes_neuronales_artificiales.ipynb` |
| 04 | Aprendizaje y automatización de redes neuronales | `nivel_intermedio/Laboratorio_4_Aprendizaje_y_automatizacion_de_redes_neuronales.ipynb` |
| 05 | Redes neuronales convolucionales (CNN) | `nivel_intermedio/Laboratorio_5_Redes_neuronales_convolucionales.ipynb` |
| 06 | RNN y LSTM | `nivel_intermedio/Laboratorio_6_RNN_Y_LSTM.ipynb` |
| 07 | Deep Learning generativo (GANs) | `nivel_intermedio/Laboratorio_7_Deep_Learning_generativo_GANs.ipynb` |
| 08 | Introducción al PLN y preprocesamiento de textos | `nivel_intermedio/Laboratorio_8_Introduccion_al_PLN_y_Preprocesamieto_de_textos.ipynb` |
| 09 | Análisis de sentimiento y minería de opiniones | `nivel_intermedio/Laboratorio_9_Analisis_de_sentimiento_y_mineria_de_opiniones.ipynb` |
| 10 | Implementación y evaluación de NER | `nivel_intermedio/Laboratorio_10_Implementacion_y_evaluacion_de_NER.ipynb` |
| 11 | Clasificación de texto | `nivel_intermedio/Laboratorio_11_Clasficacion_de_texto.ipynb` |
| 12 | Interpretabilidad y explicabilidad (XAI) | `nivel_intermedio/Laboratorio_12_interpretabilidad_y_explicabilidad.ipynb` |
| 13 | Diseño en Lenguaje Computacional | `nivel_intermedio/Laboratorio_13_Diseño_en_Lenguaje_Computacional.ipynb` |
| 14 | Algoritmos probabilísticos | `nivel_intermedio/Laboratorio_14_Algoritmos_probalisticos.ipynb` |

### 🔵 Nivel Intermedio — Big Data

| # | Tema | Carpeta |
|---|------|---------|
| 15 | Arquitectura Hadoop y HDFS | `nivel_intermedio/Laboratorio_15_BigData/` |
| 17 | Procesamiento de datos con Dask y PySpark | `nivel_intermedio/Laboratorio_17_Procesamiento_de_datos_con_Dask_y_pySpark.ipynb` |

### 🔵 Nivel Intermedio — Bases de Datos

| # | Tema | Archivo/Carpeta |
|---|------|-----------------|
| 19 | Fundamentos de bases de datos | `nivel_intermedio/Laboratorio_19_Fundamentos_bases_de_datos.ipynb` |
| 20 | Bases de datos NoSQL: MongoDB y Redis | `nivel_intermedio/Laboratorio_20_NoSQL_MongoDB_Redis/` |
| 21 | Integración de bases de datos en IA y Big Data | `nivel_intermedio/Laboratorio_21_Integracion_bases_datos_en_IA_y_BigData.py` |

### 🔵 Nivel Intermedio — Sistemas Distribuidos

| # | Tema | Carpeta |
|---|------|---------|
| 22 | Escalabilidad y concurrencia en sistemas distribuidos | `nivel_intermedio/Laboratorio_22_Escalabilidad_y_concurrencia_en_sistemas_distribuidos/` |
| 23 | Configuración de un sistema distribuido con replicación activa (Kafka) | `nivel_intermedio/Laboratorio_23_configuracion_de_un_sistema_distribuido_con_replicacion_activa/` |
| 24 | Bases de datos distribuidas (Redis, HBase) | `nivel_intermedio/Laboratorio_24_Bases_de_datos_distribuidas/` |
| 26 | Integración de IA y computación en la niebla (Fog Computing) | `nivel_intermedio/Laboratorio_26_Integración_de_IA_y_Computación_en_la_Niebla.ipynb` |

### 📦 Entregables

| # | Tema | Archivo/Carpeta |
|---|------|-----------------|
| 01 | Entregable inicial | `entregables/Entregable_1/` |
| 05 | Bases de datos SQL y NoSQL | `entregables/Entregable_5/` |
| 06 | Aplicación de herramientas XAI (CNN + Flask) | `entregables/Entregable_6/` |

---

## 🛠️ Stack técnico

| Categoría | Herramientas |
|-----------|-------------|
| **Deep Learning** | TensorFlow 2.18, Keras 3, PyTorch 2.10 |
| **XAI / Interpretabilidad** | LIME, SHAP, Captum, Grad-CAM |
| **Machine Learning** | scikit-learn 1.7, XGBoost |
| **Procesamiento de datos** | pandas, NumPy, Dask, PySpark |
| **NLP** | NLTK, HuggingFace Transformers, spaCy |
| **Visión por computador** | OpenCV 4.13, scikit-image |
| **Bases de datos** | SQLite, MongoDB (pymongo), Redis, HBase (happybase) |
| **Sistemas distribuidos** | Apache Kafka (kafka-python), Hadoop |
| **Despliegue** | Flask 3.1, Docker, docker-compose |
| **Visualización** | matplotlib 3.10, seaborn, bokeh |
| **Entorno** | Python 3.10.12, Jupyter Notebook, venv |

---

## 👤 Autor

**Juan David Valencia Martínez**
- Ingeniero de Software Fullstack / AI Engineer
- Colombia
- GitHub: [@jdvalmart](https://github.com/jdvalmart)
