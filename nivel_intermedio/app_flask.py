"""
app_flask.py
============
Microservicio REST para clasificación CIFAR-10 con explicaciones XAI.

Uso:
    python app_flask.py

Endpoints:
    POST /predict          — retorna clase predicha y probabilidades
    POST /explain/gradcam  — retorna mapa de calor Grad-CAM (base64 PNG)
    POST /explain/lime     — retorna explicación LIME (base64 PNG)
"""

import io, base64, warnings
import numpy as np
import cv2
import matplotlib
matplotlib.use('Agg')   # backend sin pantalla
import matplotlib.pyplot as plt
import tensorflow as tf
from tensorflow.keras.models import load_model, Model
from flask import Flask, request, jsonify
from PIL import Image
import lime
import lime.lime_image
from skimage.segmentation import mark_boundaries

warnings.filterwarnings('ignore')

app   = Flask(__name__)
model = load_model('cnn_model_cifar10.h5')

CLASS_NAMES = ['avión','automóvil','pájaro','gato','ciervo',
               'perro','rana','caballo','barco','camión']


# ── Utilidades ────────────────────────────────────────────────────

def decode_image(file_bytes):
    """Decodifica los bytes de una imagen PNG/JPEG a np.array (32,32,3) float32."""
    img = Image.open(io.BytesIO(file_bytes)).convert('RGB').resize((32, 32))
    return np.array(img, dtype='float32') / 255.0


def fig_to_b64(fig):
    """Convierte una figura matplotlib a string base64 PNG."""
    buf = io.BytesIO()
    fig.savefig(buf, format='png', bbox_inches='tight', dpi=100)
    buf.seek(0)
    return base64.b64encode(buf.read()).decode('utf-8')


def grad_cam(image, layer_name='conv2d_5'):
    grad_model = Model(inputs=model.inputs,
                       outputs=[model.get_layer(layer_name).output, model.output])
    img_batch = tf.cast(image[np.newaxis], tf.float32)
    with tf.GradientTape() as tape:
        conv_out, preds = grad_model(img_batch)
        pred_cls = int(tf.argmax(preds[0]))
        loss = preds[:, pred_cls]
    grads = tape.gradient(loss, conv_out)[0]
    pooled = tf.reduce_mean(grads, axis=(0, 1))
    heatmap = tf.reduce_sum(pooled * conv_out[0], axis=-1).numpy()
    heatmap = np.maximum(heatmap, 0)
    if heatmap.max() > 0:
        heatmap /= heatmap.max()
    return cv2.resize(heatmap, (32, 32)), pred_cls, float(preds[0, pred_cls])


# ── Endpoints ─────────────────────────────────────────────────────

@app.route('/predict', methods=['POST'])
def predict():
    """Clasifica la imagen recibida."""
    if 'image' not in request.files:
        return jsonify({'error': 'No se recibió imagen'}), 400
    img   = decode_image(request.files['image'].read())
    probs = model.predict(img[np.newaxis], verbose=0)[0]
    return jsonify({
        'predicted_class': CLASS_NAMES[int(np.argmax(probs))],
        'confidence':      float(np.max(probs)),
        'probabilities':   {cls: float(p) for cls, p in zip(CLASS_NAMES, probs)}
    })


@app.route('/explain/gradcam', methods=['POST'])
def explain_gradcam():
    """Retorna imagen original + mapa de calor Grad-CAM en base64 PNG."""
    if 'image' not in request.files:
        return jsonify({'error': 'No se recibió imagen'}), 400
    img = decode_image(request.files['image'].read())
    heatmap, pred_cls, conf = grad_cam(img)

    fig, axes = plt.subplots(1, 3, figsize=(9, 3))
    axes[0].imshow(img);                           axes[0].set_title('Original');   axes[0].axis('off')
    axes[1].imshow(heatmap, cmap='jet');           axes[1].set_title('Heatmap');    axes[1].axis('off')
    overlay = 0.45 * cv2.cvtColor(
        cv2.applyColorMap(np.uint8(255*heatmap), cv2.COLORMAP_JET),
        cv2.COLOR_BGR2RGB) / 255.0 + 0.55 * img
    axes[2].imshow(overlay)
    axes[2].set_title(f'Predicción: {CLASS_NAMES[pred_cls]}\n({conf*100:.0f}%)')
    axes[2].axis('off')
    plt.tight_layout()

    return jsonify({
        'predicted_class': CLASS_NAMES[pred_cls],
        'confidence':      conf,
        'visualization':   fig_to_b64(fig)
    })


@app.route('/explain/lime', methods=['POST'])
def explain_lime():
    """Retorna explicación LIME en base64 PNG."""
    if 'image' not in request.files:
        return jsonify({'error': 'No se recibió imagen'}), 400
    img     = decode_image(request.files['image'].read())
    probs   = model.predict(img[np.newaxis], verbose=0)[0]
    pred_cls = int(np.argmax(probs))

    explainer   = lime.lime_image.LimeImageExplainer(random_state=42)
    explanation = explainer.explain_instance(
        img, model.predict, top_labels=3, hide_color=0, num_samples=300)
    temp, mask = explanation.get_image_and_mask(
        pred_cls, positive_only=True, num_features=5, hide_rest=False)

    fig, ax = plt.subplots(figsize=(4, 4))
    ax.imshow(mark_boundaries(temp, mask, color=(0, 1, 0)))
    ax.set_title(f'LIME — {CLASS_NAMES[pred_cls]} ({probs[pred_cls]*100:.0f}%)')
    ax.axis('off')
    plt.tight_layout()

    return jsonify({
        'predicted_class': CLASS_NAMES[pred_cls],
        'confidence':      float(probs[pred_cls]),
        'visualization':   fig_to_b64(fig)
    })


@app.route('/health', methods=['GET'])
def health():
    return jsonify({'status': 'ok', 'model': 'CNN CIFAR-10'})


if __name__ == '__main__':
    print('🚀 Servidor XAI iniciado en http://localhost:5000')
    app.run(debug=False, host='0.0.0.0', port=5000)