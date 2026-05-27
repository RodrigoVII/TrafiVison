# -*- coding: utf-8 -*-
"""
Pipeline ETL completo TrafiVision
---------------------------------

Este script hace todo el proceso final:

1. Ejecuta YOLO sobre las imágenes scrapeadas.
2. Genera csv/yolo_final.csv.
3. Ejecuta el merge de cámaras + YOLO + clima.
4. Genera csv/dataset_final.csv.
5. Inserta los datos finales en MariaDB.

Lo llama FastAPI cuando pulso "Procesar Datos".
"""

from pathlib import Path
import importlib.util
import pandas as pd
import pymysql


BASE_DIR = Path(__file__).resolve().parent
CSV_DIR = BASE_DIR / "csv"
DATASET_FINAL = CSV_DIR / "dataset_final.csv"


DB_CONFIG = {
    "host": "host.docker.internal",
    "port": 3306,
    "user": "root",
    "password": "",
    "database": "trafivision",
    "autocommit": True,
}


def cargar_modulo(nombre_modulo: str, ruta_archivo: Path):
    """Cargo un archivo Python externo como módulo."""
    if not ruta_archivo.exists():
        raise FileNotFoundError(f"No existe el archivo: {ruta_archivo}")

    spec = importlib.util.spec_from_file_location(nombre_modulo, ruta_archivo)
    modulo = importlib.util.module_from_spec(spec)

    if spec.loader is None:
        raise RuntimeError(f"No se pudo cargar el módulo: {ruta_archivo}")

    spec.loader.exec_module(modulo)
    return modulo


def conectar_bd():
    """Abro conexión con MariaDB."""
    return pymysql.connect(**DB_CONFIG)


def preparar_valor(valor, defecto=None):
    """Evito meter NaN en MariaDB."""
    if pd.isna(valor):
        return defecto
    return valor


def insertar_dataset_en_bd():
    """
    Inserto dataset_final.csv en MariaDB.

    Tablas usadas:
    - camara
    - captura
    - deteccion
    - trafico
    - meteo
    """

    if not DATASET_FINAL.exists():
        raise FileNotFoundError(f"No existe {DATASET_FINAL}")

    df = pd.read_csv(DATASET_FINAL)

    if df.empty:
        raise ValueError("dataset_final.csv está vacío")

    conexion = conectar_bd()
    cursor = conexion.cursor()

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

            laborable = str(row["laborable"]).strip().lower()
            es_laborable = 1 if laborable == "laborable" else 0

            num_vehiculos = int(preparar_valor(row["num_vehiculos"], 0))
            nivel_trafico = str(preparar_valor(row["nivel_trafico"], "bajo")).lower()

            temperatura = float(preparar_valor(row["temperatura"], 0))
            precipitacion = float(preparar_valor(row["litros_m2"], 0))

            # 1. Inserto la cámara si no existe.
            cursor.execute(
                """
                INSERT IGNORE INTO camara (codigo, distrito)
                VALUES (%s, %s);
                """,
                (calle, "Madrid"),
            )

            cursor.execute(
                """
                SELECT id
                FROM camara
                WHERE codigo = %s
                LIMIT 1;
                """,
                (calle,),
            )

            camara = cursor.fetchone()

            if not camara:
                saltados += 1
                continue

            camara_id = camara[0]

            # 2. Evito duplicados por cámara + timestamp.
            cursor.execute(
                """
                SELECT id
                FROM captura
                WHERE camara_id = %s AND timestamp = %s
                LIMIT 1;
                """,
                (camara_id, timestamp),
            )

            captura_existente = cursor.fetchone()

            if captura_existente:
                saltados += 1
                continue

            # 3. Inserto captura.
            cursor.execute(
                """
                INSERT INTO captura (camara_id, timestamp, ruta_imagen, franja_horaria, es_laborable)
                VALUES (%s, %s, %s, %s, %s);
                """,
                (camara_id, timestamp, ruta_imagen, franja, es_laborable),
            )

            captura_id = cursor.lastrowid

            # 4. Inserto detección.
            cursor.execute(
                """
                INSERT INTO deteccion (captura_id, num_vehiculos)
                VALUES (%s, %s);
                """,
                (captura_id, num_vehiculos),
            )

            # 5. Inserto tráfico.
            cursor.execute(
                """
                INSERT INTO trafico (captura_id, nivel)
                VALUES (%s, %s);
                """,
                (captura_id, nivel_trafico),
            )

            # 6. Inserto meteo.
            cursor.execute(
                """
                INSERT INTO meteo (captura_id, temperatura, precipitacion)
                VALUES (%s, %s, %s);
                """,
                (captura_id, temperatura, precipitacion),
            )

            insertados += 1

        except Exception as e:
            print(f"[ERROR FILA BD] {e}")
            saltados += 1

    cursor.close()
    conexion.close()

    return {
        "insertados": insertados,
        "saltados": saltados,
        "total_csv": len(df),
    }


def ejecutar_pipeline():
    """
    Ejecuto todo el pipeline completo.
    """

    # 1. Ejecutar YOLO.
    print("[PIPELINE] Ejecutando YOLO...")
    yolo_path = BASE_DIR / "yolo_final.py"
    yolo = cargar_modulo("yolo_final", yolo_path)

    if not hasattr(yolo, "procesar_carpeta"):
        raise AttributeError("yolo_final.py no tiene la función procesar_carpeta()")

    yolo.procesar_carpeta()

    # 2. Ejecutar merge.
    print("[PIPELINE] Ejecutando merge...")
    merge_path = BASE_DIR / "merge_datasets_final.py"

    # Tu script de merge se ejecuta al importarse.
    cargar_modulo("merge_datasets_final", merge_path)

    # 3. Insertar en MariaDB.
    print("[PIPELINE] Insertando en MariaDB...")
    resultado_bd = insertar_dataset_en_bd()

    print("[PIPELINE] Proceso terminado")

    return {
        "mensaje": "Pipeline completo ejecutado correctamente",
        "registros_insertados": resultado_bd["insertados"],
        "registros_saltados": resultado_bd["saltados"],
        "total_csv": resultado_bd["total_csv"],
    }


if __name__ == "__main__":
    print(ejecutar_pipeline())
