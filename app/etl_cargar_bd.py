import pandas as pd
import pymysql
from pathlib import Path

BASE_DIR = Path(__file__).resolve().parent
CSV_PATH = BASE_DIR / "csv" / "dataset_final.csv"

DB_CONFIG = {
    "host": "host.docker.internal",
    "user": "root",
    "password": "",
    "database": "trafivision",
    "autocommit": True
}

def conectar():
    return pymysql.connect(**DB_CONFIG)


def cargar_datos():
    conn = conectar()
    cursor = conn.cursor()

    df = pd.read_csv(CSV_PATH)

    registros = 0

    for _, row in df.iterrows():
        try:
            # 1️⃣ CAMARA (si no existe)
            cursor.execute("""
                INSERT IGNORE INTO camara (codigo, distrito)
                VALUES (%s, %s)
            """, (row["calle"], "Madrid"))

            # Obtener ID camara
            cursor.execute("SELECT id FROM camara WHERE codigo=%s", (row["calle"],))
            camara_id = cursor.fetchone()[0]

            # 2️⃣ CAPTURA
            cursor.execute("""
                INSERT INTO captura (camara_id, timestamp, franja_horaria, es_laborable)
                VALUES (%s, %s, %s, %s)
            """, (
                camara_id,
                f"{row['fecha']} {row['hora']}",
                row["franja_horaria"],
                1 if row["laborable"] == "Laborable" else 0
            ))

            captura_id = cursor.lastrowid

            # 3️⃣ DETECCION
            cursor.execute("""
                INSERT INTO deteccion (captura_id, num_vehiculos)
                VALUES (%s, %s)
            """, (captura_id, row["num_vehiculos"]))

            # 4️⃣ TRAFICO
            cursor.execute("""
                INSERT INTO trafico (captura_id, nivel)
                VALUES (%s, %s)
            """, (captura_id, row["nivel_trafico"]))

            # 5️⃣ METEO
            cursor.execute("""
                INSERT INTO meteo (captura_id, temperatura, precipitacion)
                VALUES (%s, %s, %s)
            """, (
                captura_id,
                row["temperatura"],
                row["litros_m2"] if "litros_m2" in row else 0
            ))

            registros += 1

        except Exception as e:
            print(f"[ERROR FILA] {e}")

    cursor.close()
    conn.close()

    print(f"[OK] {registros} registros insertados en BD")
    return registros
