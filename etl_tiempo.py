# -*- coding: utf-8 -*-
"""
ETL clima Madrid (Open-Meteo, versión estable 2025)
--------------------------------------------------
- Obtiene temperatura, precipitación y tipo de clima actuales.
- Clasifica la lluvia como No / Débil / Sí.
- Guarda fecha y hora por separado (para el merge posterior).
- Se ejecuta cada 15 minutos alineado a en punto, :15, :30 y :45.
- Compatible con el CSV de cámaras.

Autor: Maylen & Rodrigo
"""

import os
import time
import requests
import datetime as dt
import pandas as pd

# =================== CONFIGURACIÓN ===================

LAT, LON = 40.4168, -3.7038

# Carpeta base del proyecto
BASE_DIR = r"C:\Users\ditas\OneDrive\Escritorio\UE\2025-26 UE\Primer Cuatri\Proyecto De Computacion I\TrafiVison"

# Subcarpeta para guardar el CSV
CSV_DIR = os.path.join(BASE_DIR, "csv")
os.makedirs(CSV_DIR, exist_ok=True)
CSV_PATH = os.path.join(CSV_DIR, "tiempo_madrid.csv")

# Endpoint de Open-Meteo (API pública)
URL = (
    f"https://api.open-meteo.com/v1/forecast?"
    f"latitude={LAT}&longitude={LON}"
    "&current=temperature_2m,precipitation,weather_code"
    "&forecast_days=1"
    "&timezone=Europe/Madrid"
)

INTERVALO_MINUTOS = 15  # intervalo de captura (minutos)


# =================== FUNCIONES ===================

def clasifica_lluvia(litros):
    """Convierte la precipitación (L/m²) en categorías descriptivas."""
    if litros <= 0.1:
        return "No"
    elif litros <= 0.3:
        return "Débil"
    else:
        return "Sí"


def segundos_hasta_siguiente_bloque(intervalo=15):
    """
    Calcula cuántos segundos faltan hasta el siguiente bloque horario de :00, :15, :30 o :45.
    Esto garantiza que las capturas se sincronicen con las cámaras.
    """
    ahora = dt.datetime.now()
    minutos = (ahora.minute // intervalo + 1) * intervalo
    siguiente = ahora.replace(minute=0, second=0, microsecond=0) + dt.timedelta(minutes=minutos)
    return (siguiente - ahora).total_seconds()


def get_weather():
    """Llama a la API de Open-Meteo y guarda los datos en el CSV."""
    try:
        r = requests.get(URL, timeout=20)
        r.raise_for_status()
        data = r.json()

        # Bloque 'current' actualizado (modelo 2025)
        current = data.get("current", {})
        temperatura = current.get("temperature_2m")
        litros_m2 = current.get("precipitation", 0.0)
        weather_code = current.get("weather_code")

        # A veces Open-Meteo mete la data en "current_weather"
        if temperatura is None:
            cw = data.get("current_weather", {})
            temperatura = cw.get("temperature")
            litros_m2 = cw.get("precipitation", litros_m2)
            weather_code = cw.get("weather_code", weather_code)

        # Clasificar lluvia
        lluvia = clasifica_lluvia(float(litros_m2 or 0))

        # Fecha y hora actuales (solo HH:MM, sin fecha fantasma)
        ahora = dt.datetime.now()
        fecha = ahora.strftime("%Y-%m-%d")
        hora = ahora.strftime("%H:%M")

        # Registro nuevo
        registro = pd.DataFrame([{
            "fecha": fecha,
            "hora": hora,
            "temperatura": temperatura,
            "lluvia": lluvia,
            "litros_m2": round(float(litros_m2 or 0), 2)
        }])

        # Si existe, concatenar sin duplicar por fecha y hora
        if os.path.exists(CSV_PATH):
            viejo = pd.read_csv(CSV_PATH)
            combinado = pd.concat([viejo, registro], ignore_index=True)
            combinado.drop_duplicates(subset=["fecha", "hora"], inplace=True)
        else:
            combinado = registro

        combinado.to_csv(CSV_PATH, index=False, encoding="utf-8")

        print(f"[{hora}] {temperatura}°C | Lluvia={lluvia} | L/m²={round(float(litros_m2 or 0), 2)}")

    except Exception as e:
        print(f"[ERROR] No se pudo obtener el clima: {e}")


# =================== BUCLE PRINCIPAL ===================

def main():
    print("\n[READY] ETL clima Madrid (Open-Meteo v2 estable)")
    print(f"Guardando CSV en: {CSV_PATH}")
    print(f"Intervalo: cada {INTERVALO_MINUTOS} minutos (alineado a :00/:15/:30/:45)\n")

    # Ejecución inicial inmediata
    get_weather()

    while True:
        # Esperar hasta el siguiente bloque de 15 minutos
        esperar = segundos_hasta_siguiente_bloque(INTERVALO_MINUTOS)
        print(f"[SLEEP] Esperando {int(esperar // 60)} min {int(esperar % 60)} s para la siguiente medición...\n")
        time.sleep(esperar)
        get_weather()


if __name__ == "__main__":
    main()


