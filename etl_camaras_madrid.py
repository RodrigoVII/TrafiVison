# -*- coding: utf-8 -*-
"""
ETL cámaras tráfico Madrid (versión completa 2025)
--------------------------------------------------
Descarga imágenes de las cámaras públicas del Ayuntamiento de Madrid.
Guarda:
- nombre de la calle,
- fecha y hora actual,
- ruta local donde se guarda la imagen.

Cada ejecución captura las 10 cámaras y añade la información al CSV.
"""

import os
import time
import requests
import datetime as dt
import pandas as pd

# =================== CONFIGURACIÓN ===================

# Carpeta base del proyecto
BASE_DIR = r"C:\Users\ditas\OneDrive\Escritorio\UE\2025-26 UE\Primer Cuatri\Proyecto De Computacion I\TrafiVison"

# Subcarpetas para CSV e imágenes
CSV_DIR = os.path.join(BASE_DIR, "csv")
IMG_DIR = os.path.join(BASE_DIR, "imagenesTrafico")
os.makedirs(CSV_DIR, exist_ok=True)
os.makedirs(IMG_DIR, exist_ok=True)

CSV_PATH = os.path.join(CSV_DIR, "camaras_solo.csv")

# Lista de cámaras de Madrid (nombre -> URL)
CAMARAS = {
    "Princesa-Serrano Jover": "https://informo.madrid.es/cameras/Camara09316.jpg",
    "Alonso Martínez": "https://informo.madrid.es/cameras/Camara01321.jpg",
    "Cibeles": "https://informo.madrid.es/cameras/Camara01302.jpg",
    "Alcalá - Velázquez": "https://informo.madrid.es/cameras/Camara04308.jpg",
    "Castellana - S. Delgado": "https://informo.madrid.es/cameras/Camara06302.jpg",
    "Av. de América - Fco. Silvela": "https://informo.madrid.es/cameras/Camara04312.jpg",
    "Callao - Gran Vía": "https://informo.madrid.es/cameras/Camara01314.jpg",
    "Cuatro Caminos": "https://informo.madrid.es/cameras/Camara06308.jpg",
    "Paseo del Prado - Huertas": "https://informo.madrid.es/cameras/Camara01323.jpg",
    "Plaza Castilla (Norte)": "https://informo.madrid.es/cameras/Camara06303.jpg"
}

INTERVALO_MINUTOS = 15  # intervalo de ejecución automática (si lo activas más tarde)

# =================== FUNCIONES ===================

def guardar_imagen(url, calle):
    """Descarga una imagen y la guarda con nombre único."""
    ahora = dt.datetime.now()
    nombre_archivo = f"{calle.replace(' ', '_')}_{ahora.strftime('%Y-%m-%d_%H-%M-%S')}.jpg"
    ruta_local = os.path.join(IMG_DIR, nombre_archivo)

    # Descargar imagen
    try:
        r = requests.get(url, timeout=30)
        r.raise_for_status()
        with open(ruta_local, "wb") as f:
            f.write(r.content)
        return ruta_local, ahora
    except Exception as e:
        print(f"[ERR] No se pudo descargar {calle}: {e}")
        return None, None


def ciclo_captura():
    """Captura imágenes de todas las cámaras."""
    registros = []

    for calle, url in CAMARAS.items():
        ruta, momento = guardar_imagen(url, calle)
        if ruta and momento:
            registros.append({
                "fecha": momento.strftime("%Y-%m-%d"),
                "hora": momento.strftime("%H:%M"),
                "calle": calle,
                "ruta_imagen": ruta
            })
            print(f"[OK] {calle} -> {ruta}")

    # Guardar CSV
    if registros:
        df = pd.DataFrame(registros)
        if os.path.exists(CSV_PATH):
            df_prev = pd.read_csv(CSV_PATH)
            df = pd.concat([df_prev, df], ignore_index=True)
        df.to_csv(CSV_PATH, index=False, encoding="utf-8")
        print(f"\n[CSV actualizado]: {CSV_PATH}")
    else:
        print("\n⚠️ No se descargaron imágenes (sin conexión o error de cámara).")


def main():
    """Ejecuta una única captura manual."""
    print("\n[READY] ETL cámaras tráfico Madrid (10 cámaras activas)\n")
    ciclo_captura()
    print(f"[IMG dir]: {IMG_DIR}")


if __name__ == "__main__":
    main()
