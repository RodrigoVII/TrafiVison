# -*- coding: utf-8 -*-
"""
YOLO análisis de tráfico (versión 2025)
--------------------------------------
Analiza todas las imágenes guardadas por el ETL de cámaras usando YOLOv8.
Cuenta los vehículos detectados y clasifica el tráfico en tres niveles:
- Bajo:    0–5 vehículos
- Medio:   6–15 vehículos
- Alto:    >15 vehículos

Salida:
- CSV con nombre de archivo, número de vehículos y nivel de tráfico.
  Guardado en: TrafiVison\csv\yolo_final.csv

Requiere:
- pip install ultralytics pandas
"""

import os
import pandas as pd
from ultralytics import YOLO

# =================== CONFIGURACIÓN ===================

# Carpeta base del proyecto
BASE_DIR = r"C:\Users\ditas\OneDrive\Escritorio\UE\2025-26 UE\Primer Cuatri\Proyecto De Computacion I\TrafiVison"

# Subcarpetas de imágenes y CSV
IMG_DIR = os.path.join(BASE_DIR, "imagenesTrafico")
CSV_DIR = os.path.join(BASE_DIR, "csv")
os.makedirs(CSV_DIR, exist_ok=True)
CSV_PATH = os.path.join(CSV_DIR, "yolo_final.csv")

# Cargar modelo preentrenado YOLOv8 (detecta coches, motos, buses, camiones, bicis…)
print("[INFO] Cargando modelo YOLOv8 (Nano)...")
model = YOLO("yolov8n.pt")

# =================== FUNCIONES ===================

def clasificar_trafico(num):
    """Clasifica el nivel de tráfico según número de vehículos detectados."""
    if num <= 5:
        return "Bajo"
    elif num <= 15:
        return "Medio"
    else:
        return "Alto"


def analizar_imagen(imagen):
    """
    Ejecuta YOLO sobre una imagen y devuelve el número de vehículos detectados.
    Clases COCO usadas: 1=bicycle, 2=car, 3=motorcycle, 5=bus, 7=truck
    """
    try:
        resultados = model.predict(imagen, imgsz=640, conf=0.25, verbose=False)
        det = resultados[0]
        clases = det.boxes.cls.tolist() if det.boxes is not None else []
        vehiculos_ids = {1, 2, 3, 5, 7}  # bici, coche, moto, bus, camión
        conteo = sum(1 for c in clases if int(c) in vehiculos_ids)
        return conteo
    except Exception as e:
        print(f"[ERROR] Fallo analizando {imagen}: {e}")
        return 0


def procesar_carpeta():
    """Analiza todas las imágenes de la carpeta y genera un CSV con resultados."""
    print("\n[INFO] Iniciando análisis YOLO de tráfico...\n")
    resultados = []

    # Listar imágenes válidas
    archivos = [f for f in os.listdir(IMG_DIR) if f.lower().endswith((".jpg", ".jpeg", ".png"))]
    archivos.sort()  # mantener orden temporal por nombre

    total = len(archivos)
    print(f"[INFO] Detectadas {total} imágenes en: {IMG_DIR}\n")

    for i, archivo in enumerate(archivos, start=1):
        ruta = os.path.join(IMG_DIR, archivo)
        num_vehiculos = analizar_imagen(ruta)
        nivel = clasificar_trafico(num_vehiculos)
        resultados.append({
            "foto": archivo,
            "num_vehiculos": int(num_vehiculos),
            "nivel_trafico": nivel
        })
        print(f"[{i}/{total}] {archivo}: {num_vehiculos} vehículos -> {nivel}")

    # Guardar CSV
    df = pd.DataFrame(resultados)
    df.to_csv(CSV_PATH, index=False, encoding="utf-8")
    print(f"\n✅ Análisis completado. Resultados guardados en:\n{CSV_PATH}")


# =================== EJECUCIÓN ===================

if __name__ == "__main__":
    procesar_carpeta()
#fin de codigo
