# Reporte — Entregable 6: Aplicación de Herramientas XAI
**Fecha de generación**: 2026-04-25 12:26  
**Dataset**: CIFAR-10  
**Modelo**: CNN (3 bloques convolucionales + BatchNorm + Dropout)

---

## 1. Rendimiento del Modelo

| Métrica | Valor |
|---------|-------|
| Loss (test) | 0.3973 |
| Accuracy (test) | 87.14% |
| Épocas entrenadas | 30 |

## 2. Herramientas XAI Aplicadas

### LIME
- Genera explicaciones locales por superpíxeles.
- Fracción de área explicativa promedio: **0.971**
- Resultado: regiones verdes = positivas; rojas = negativas.

### SHAP (DeepExplainer)
- Calcula contribución marginal promedio de cada píxel (valores de Shapley).
- Visualización por canal RGB; azul = impacto negativo, rojo = positivo.

### Grad-CAM
- Mapas de calor sobre la capa `conv2d_5` (última capa convolucional).
- Superposición con colormap JET sobre imagen original.

## 3. Métricas de Evaluación

| Métrica | Herramienta | Valor | Interpretación |
|---------|-------------|-------|----------------|
| Fidelidad (caída de prob.) | Grad-CAM | 0.565 | Alta |
| Comprensibilidad (fracción área) | Grad-CAM | 0.158 | Buena |
| Comprensibilidad (fracción superpíxeles) | LIME | 0.971 | Regular |
| Estabilidad (Pearson c/ ruido) | Grad-CAM | 0.7457 | Media |

## 4. Servicio Flask

Microservicio disponible en `app_flask.py`. Endpoints:
- `GET  /health` — verificación de estado
- `POST /predict` — clasificación (multipart/form-data: imagen)
- `POST /explain/gradcam` — mapa de calor Grad-CAM
- `POST /explain/lime` — explicación LIME

## 5. Archivos generados

| Archivo | Descripción |
|---------|-------------|
| `cnn_model_cifar10.h5` | Pesos del mejor modelo CNN |
| `training_history.png` | Curvas de loss y accuracy |
| `lime_explanations.png` | Visualizaciones LIME |
| `shap_explanations.png` | Visualizaciones SHAP |
| `gradcam_explanations.png` | Mapas Grad-CAM |
| `fidelity_evaluation.png` | Gráfica de fidelidad |
| `comprehensibility_evaluation.png` | Gráfica de comprensibilidad |
| `stability_evaluation.png` | Gráfica de estabilidad |
| `app_flask.py` | Microservicio REST Flask |
| `reporte_xai.md` | Este reporte |
