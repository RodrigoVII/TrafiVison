"""
============================================================
TrafiVision - Carga de CSV a MariaDB (adaptado a tu CSV)
============================================================

CSV (12 columnas):
['ciudad', 'calle', 'fecha', 'hora', 'ruta_imagen', 'num_vehiculos',
 'nivel_trafico', 'temperatura', 'lluvia', 'litros_m2',
 'laborable', 'franja_horaria']

Mapeo a BD:
- camara.codigo       -> "ciudad - calle"
- captura.timestamp   -> "fecha hora"
- captura.franja      -> franja_horaria
- captura.laborable   -> laborable (texto -> 1/0)
- meteo.temperatura   -> temperatura
- meteo.precipitacion -> litros_m2 (si existe) si no lluvia
- deteccion.num_veh   -> num_vehiculos
- trafico.nivel       -> nivel_trafico (bajo/medio/alto)


"""

import os
import pandas as pd
import pymysql


# -----------------------------
# CONFIGURACIÓN BD
# -----------------------------
DB_HOST = "127.0.0.1"
DB_PORT = 3306
DB_USER = "root"
DB_PASSWORD = "trafivision"
DB_NAME = "trafivision"

CSV_PATH = "dataset_final_limpio.csv"


def connect_db():
    """Abre conexión a MariaDB."""
    return pymysql.connect(
        host=DB_HOST,
        port=DB_PORT,
        user=DB_USER,
        password=DB_PASSWORD,
        database=DB_NAME,
        connect_timeout=10,
        read_timeout=30,
        write_timeout=30,
        autocommit=False,
    )


def normalize_level(value):
    """Normaliza nivel_trafico al ENUM: bajo/medio/alto."""
    if pd.isna(value):
        return None
    v = str(value).strip().lower()
    if v in ["bajo", "low"]:
        return "bajo"
    if v in ["medio", "medium"]:
        return "medio"
    if v in ["alto", "high"]:
        return "alto"
    if v in ["bajo", "medio", "alto"]:
        return v
    return None


def build_camera_code(ciudad, calle):
    """Crea un identificador estable para la 'cámara' (en tu caso: calle)."""
    c1 = "" if pd.isna(ciudad) else str(ciudad).strip()
    c2 = "" if pd.isna(calle) else str(calle).strip()
    if c1 and c2:
        return f"{c1} - {c2}"
    return c2 or c1 or None


def build_timestamp(fecha, hora):
    """
    Construye un DATETIME a partir de columnas separadas.
    En tu CSV parece venir con formato YYYY-MM-DD y HH:MM
    Ej: 2024-02-01 y 08:30
    """
    if pd.isna(fecha) or pd.isna(hora):
        return None

    raw = f"{str(fecha).strip()} {str(hora).strip()}"

    # Probamos formatos típicos para evitar warnings y errores
    for fmt in ("%Y-%m-%d %H:%M", "%Y-%m-%d %H:%M:%S", "%d/%m/%Y %H:%M", "%d/%m/%Y %H:%M:%S"):
        try:
            dt = pd.to_datetime(raw, format=fmt, errors="raise")
            return dt.strftime("%Y-%m-%d %H:%M:%S")
        except Exception:
            pass

    # Último intento genérico
    try:
        dt = pd.to_datetime(raw, errors="raise")
        return dt.strftime("%Y-%m-%d %H:%M:%S")
    except Exception:
        return None


def parse_laborable(value):
    """
    Convierte la columna 'laborable' del CSV a 1/0 (boolean).
    Soporta:
    - "Laborable" / "No laborable"
    - "Sí" / "No"
    - 1/0
    - True/False
    """
    if pd.isna(value):
        return None

    # Si ya viene numérico
    if isinstance(value, (int, float)) and not pd.isna(value):
        return 1 if int(value) != 0 else 0

    v = str(value).strip().lower()

    if v in ["laborable", "si", "sí", "true", "t", "1", "yes", "y"]:
        return 1
    if v in ["no laborable", "no", "false", "f", "0", "not", "n"]:
        return 0

    # Si viene algo raro, lo dejamos NULL para no romper
    return None


def main():
    print("🟡 Cargando CSV:", CSV_PATH)

    if not os.path.exists(CSV_PATH):
        print("❌ No existe el archivo CSV:", CSV_PATH)
        return

    df = pd.read_csv(CSV_PATH)
    print(f"✅ CSV cargado: {len(df)} filas, {len(df.columns)} columnas")

    # Comprobación mínima
    missing = [c for c in ["calle", "fecha", "hora"] if c not in df.columns]
    if missing:
        print("❌ Faltan columnas obligatorias:", missing)
        print("Columnas del CSV:", list(df.columns))
        return

    conn = connect_db()
    inserted = 0
    skipped = 0

    try:
        with conn.cursor() as cur:
            for _, row in df.iterrows():

                # --------- CAMARA ---------
                codigo = build_camera_code(row.get("ciudad"), row.get("calle"))
                if not codigo:
                    skipped += 1
                    continue

                # --------- TIMESTAMP ---------
                ts = build_timestamp(row.get("fecha"), row.get("hora"))
                if ts is None:
                    skipped += 1
                    continue

                # No tienes lat/lon/distrito en el CSV: NULL
                lat, lon, distrito = None, None, None

                # 1) UPSERT CAMARA
                cur.execute(
                    """
                    INSERT INTO camara (codigo, latitud, longitud, distrito)
                    VALUES (%s, %s, %s, %s)
                    ON DUPLICATE KEY UPDATE
                        latitud = VALUES(latitud),
                        longitud = VALUES(longitud),
                        distrito = VALUES(distrito)
                    """,
                    (codigo, lat, lon, distrito),
                )

                # Recuperar camara_id
                cur.execute("SELECT id FROM camara WHERE codigo=%s", (codigo,))
                camara_id = cur.fetchone()[0]

                # 2) UPSERT CAPTURA
                dia_semana = None  # opcional calcular más adelante
                franja = row.get("franja_horaria", None)
                laborable = parse_laborable(row.get("laborable", None))

                cur.execute(
                    """
                    INSERT INTO captura (camara_id, `timestamp`, dia_semana, franja_horaria, es_laborable)
                    VALUES (%s, %s, %s, %s, %s)
                    ON DUPLICATE KEY UPDATE
                        dia_semana = VALUES(dia_semana),
                        franja_horaria = VALUES(franja_horaria),
                        es_laborable = VALUES(es_laborable)
                    """,
                    (camara_id, ts, dia_semana, franja, laborable),
                )

                # Recuperar captura_id
                cur.execute(
                    "SELECT id FROM captura WHERE camara_id=%s AND `timestamp`=%s",
                    (camara_id, ts),
                )
                captura_id = cur.fetchone()[0]

                # 3) UPSERT METEO
                temp = row.get("temperatura", None)

                precip = row.get("litros_m2", None)
                if pd.isna(precip) or precip is None:
                    precip = row.get("lluvia", None)

                humedad = None  # no existe en tu CSV

                cur.execute(
                    """
                    INSERT INTO meteo (captura_id, temperatura, precipitacion, humedad)
                    VALUES (%s, %s, %s, %s)
                    ON DUPLICATE KEY UPDATE
                        temperatura = VALUES(temperatura),
                        precipitacion = VALUES(precipitacion),
                        humedad = VALUES(humedad)
                    """,
                    (captura_id, temp, precip, humedad),
                )

                # 4) UPSERT DETECCION
                num_veh = row.get("num_vehiculos", None)
                cur.execute(
                    """
                    INSERT INTO deteccion (captura_id, num_vehiculos)
                    VALUES (%s, %s)
                    ON DUPLICATE KEY UPDATE
                        num_vehiculos = VALUES(num_vehiculos)
                    """,
                    (captura_id, num_veh),
                )

                # 5) UPSERT TRAFICO
                nivel = normalize_level(row.get("nivel_trafico", None))
                if nivel is None:
                    nivel = "medio"

                cur.execute(
                    """
                    INSERT INTO trafico (captura_id, nivel)
                    VALUES (%s, %s)
                    ON DUPLICATE KEY UPDATE
                        nivel = VALUES(nivel)
                    """,
                    (captura_id, nivel),
                )

                inserted += 1
                if inserted % 500 == 0:
                    print(f"Procesadas {inserted} filas...")

        conn.commit()
        print("✅ Carga finalizada")
        print("Filas procesadas:", inserted)
        print("Filas saltadas:", skipped)

    except Exception as e:
        conn.rollback()
        print("❌ Error durante la carga. ROLLBACK ejecutado.")
        print(repr(e))

    finally:
        conn.close()


if __name__ == "__main__":
    main()