# -*- coding: utf-8 -*-
from pathlib import Path
import importlib.util
import pandas as pd
import pymysql

BASE_DIR = Path(__file__).resolve().parents[1]
CSV_FINAL = BASE_DIR / "csv" / "dataset_final.csv"

DB_CONFIG = {
    "host": "host.docker.internal",
    "port": 3306,
    "user": "root",
    "password": "",
    "database": "trafivision",
    "autocommit": True,
}

def cargar_modulo(nombre, ruta):
    spec = importlib.util.spec_from_file_location(nombre, ruta)
    modulo = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(modulo)
    return modulo

def conectar():
    return pymysql.connect(**DB_CONFIG)

def valor(v, defecto=None):
    if pd.isna(v):
        return defecto
    return v

def ejecutar_pipeline():
    # 1. Paso YOLO sobre las imágenes.
    print("[PIPELINE] Ejecutando YOLO...")
    yolo = cargar_modulo("yolo_final", BASE_DIR / "yolo_final.py")
    yolo.procesar_carpeta()

    # 2. Genero dataset_final.csv con tu merge real.
    print("[PIPELINE] Ejecutando merge...")
    cargar_modulo("merge_datasets_final", BASE_DIR / "merge_datasets_final.py")

    if not CSV_FINAL.exists():
        raise FileNotFoundError(f"No existe {CSV_FINAL}")

    df = pd.read_csv(CSV_FINAL)

    if df.empty:
        raise ValueError("dataset_final.csv está vacío")

    conn = conectar()
    cur = conn.cursor()

    insertados = 0
    saltados = 0

    for _, row in df.iterrows():
        try:
            calle = str(row["calle"]).strip()
            fecha = str(row["fecha"]).strip()
            hora = str(row["hora"]).strip()

            timestamp = f"{fecha} {hora}:00"
            ruta_imagen = str(row["ruta_imagen"]).strip()
            franja = str(row["franja_horaria"]).strip()

            laborable_txt = str(row["laborable"]).strip().lower()
            es_laborable = 1 if laborable_txt == "laborable" else 0

            num_vehiculos = int(valor(row["num_vehiculos"], 0))
            nivel = str(valor(row["nivel_trafico"], "Bajo")).strip().lower()
            temperatura = float(valor(row["temperatura"], 0))
            precipitacion = float(valor(row["litros_m2"], 0))

            # Cámara.
            cur.execute(
                """
                INSERT IGNORE INTO camara (codigo, distrito)
                VALUES (%s, %s);
                """,
                (calle, "Madrid"),
            )

            cur.execute(
                "SELECT id FROM camara WHERE codigo = %s LIMIT 1;",
                (calle,),
            )
            camara_row = cur.fetchone()

            if not camara_row:
                saltados += 1
                continue

            camara_id = camara_row[0]

            # Evito duplicados.
            cur.execute(
                """
                SELECT id
                FROM captura
                WHERE camara_id = %s AND timestamp = %s
                LIMIT 1;
                """,
                (camara_id, timestamp),
            )

            if cur.fetchone():
                saltados += 1
                continue

            # Captura.
            cur.execute(
                """
                INSERT INTO captura
                (camara_id, timestamp, ruta_imagen, franja_horaria, es_laborable)
                VALUES (%s, %s, %s, %s, %s);
                """,
                (camara_id, timestamp, ruta_imagen, franja, es_laborable),
            )

            captura_id = cur.lastrowid

            # Detección.
            cur.execute(
                """
                INSERT INTO deteccion (captura_id, num_vehiculos)
                VALUES (%s, %s);
                """,
                (captura_id, num_vehiculos),
            )

            # Tráfico.
            cur.execute(
                """
                INSERT INTO trafico (captura_id, nivel)
                VALUES (%s, %s);
                """,
                (captura_id, nivel),
            )

            # Meteo.
            cur.execute(
                """
                INSERT INTO meteo (captura_id, temperatura, precipitacion)
                VALUES (%s, %s, %s);
                """,
                (captura_id, temperatura, precipitacion),
            )

            insertados += 1

        except Exception as e:
            print(f"[ERROR FILA] {e}")
            saltados += 1

    cur.close()
    conn.close()

    return {
        "mensaje": "Pipeline completo: YOLO + merge + carga en MariaDB",
        "insertados": insertados,
        "saltados": saltados,
        "total_csv": len(df),
    }

if __name__ == "__main__":
    print(ejecutar_pipeline())