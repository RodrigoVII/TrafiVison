"""
============================================================
TrafiVision - API REST básica con FastAPI
============================================================

Este archivo define una API sencilla para consultar la base
de datos MariaDB del proyecto TrafiVision.

Incluye:
- Endpoint raíz para comprobar funcionamiento
- Endpoint /stats para obtener estadísticas generales

Tecnologías:
- FastAPI
- PyMySQL
- MariaDB

Autor: Proyecto PCII
"""

from fastapi import FastAPI
import pymysql


# ============================================================
# CONFIGURACIÓN DE LA BASE DE DATOS
# ============================================================

DB_CONFIG = {
    "host": "127.0.0.1",
    "port": 3306,
    "user": "root",
    "password": "trafivision",
    "database": "trafivision",
    "autocommit": True
}


# ============================================================
# CREACIÓN DE LA API
# ============================================================

app = FastAPI(
    title="TrafiVision API",
    description="API básica para consultar los datos de TrafiVision almacenados en MariaDB.",
    version="1.0.0"
)


# ============================================================
# FUNCIÓN AUXILIAR DE CONEXIÓN
# ============================================================

def get_connection():
    """
    Crea y devuelve una conexión a la base de datos MariaDB.
    """
    return pymysql.connect(**DB_CONFIG)


# ============================================================
# ENDPOINT RAÍZ
# ============================================================

@app.get("/")
def home():
    """
    Endpoint de prueba para comprobar que la API está funcionando.
    """
    return {
        "mensaje": "TrafiVision API funcionando correctamente"
    }


# ============================================================
# ENDPOINT /stats
# ============================================================

@app.get("/stats")
def get_stats():
    """
    Devuelve estadísticas generales de la base de datos:
    - número de cámaras
    - número de capturas
    - número de registros meteo
    - número de detecciones
    - número de registros de tráfico
    """

    connection = None
    cursor = None

    try:
        connection = get_connection()
        cursor = connection.cursor()

        # Consulta número de cámaras
        cursor.execute("SELECT COUNT(*) FROM camara;")
        camaras = cursor.fetchone()[0]

        # Consulta número de capturas
        cursor.execute("SELECT COUNT(*) FROM captura;")
        capturas = cursor.fetchone()[0]

        # Consulta número de registros meteo
        cursor.execute("SELECT COUNT(*) FROM meteo;")
        meteo = cursor.fetchone()[0]

        # Consulta número de detecciones
        cursor.execute("SELECT COUNT(*) FROM deteccion;")
        detecciones = cursor.fetchone()[0]

        # Consulta número de registros de tráfico
        cursor.execute("SELECT COUNT(*) FROM trafico;")
        trafico = cursor.fetchone()[0]

        return {
            "camaras": camaras,
            "capturas": capturas,
            "meteo": meteo,
            "detecciones": detecciones,
            "trafico": trafico
        }

    except Exception as e:
        return {
            "error": "No se pudieron obtener las estadísticas",
            "detalle": str(e)
        }

    finally:
        if cursor:
            cursor.close()
        if connection:
            connection.close()
            # ============================================================
# ENDPOINT /camaras
# ============================================================

@app.get("/camaras")
def get_camaras():
    """
    Devuelve la lista de cámaras registradas en la base de datos.
    """

    connection = None
    cursor = None

    try:
        connection = get_connection()
        cursor = connection.cursor()

        cursor.execute("""
            SELECT id, codigo
            FROM camara
            ORDER BY codigo;
        """)

        rows = cursor.fetchall()

        camaras = []

        for row in rows:
            camaras.append({
                "id": row[0],
                "codigo": row[1]
            })

        return camaras

    except Exception as e:
        return {
            "error": "No se pudieron obtener las cámaras",
            "detalle": str(e)
        }

    finally:
        if cursor:
            cursor.close()
        if connection:
            connection.close()
            # ============================================================
# ENDPOINT /capturas
# ============================================================

@app.get("/capturas")
def get_capturas(limit: int = 100):
    """
    Devuelve una lista de capturas con información asociada:
    - cámara
    - fecha y hora
    - franja horaria
    - si es laborable
    - número de vehículos
    - nivel de tráfico

    El parámetro 'limit' permite limitar cuántas capturas devolver.
    Por defecto devuelve 100.
    """

    connection = None
    cursor = None

    try:
        connection = get_connection()
        cursor = connection.cursor()

        query = """
            SELECT
                cp.id,
                c.codigo,
                cp.timestamp,
                cp.franja_horaria,
                cp.es_laborable,
                d.num_vehiculos,
                t.nivel
            FROM captura cp
            JOIN camara c ON cp.camara_id = c.id
            LEFT JOIN deteccion d ON d.captura_id = cp.id
            LEFT JOIN trafico t ON t.captura_id = cp.id
            ORDER BY cp.timestamp DESC
            LIMIT %s;
        """

        cursor.execute(query, (limit,))
        rows = cursor.fetchall()

        capturas = []

        for row in rows:
            capturas.append({
                "id": row[0],
                "camara": row[1],
                "timestamp": str(row[2]),
                "franja_horaria": row[3],
                "es_laborable": row[4],
                "num_vehiculos": row[5],
                "nivel_trafico": row[6]
            })

        return capturas

    except Exception as e:
        return {
            "error": "No se pudieron obtener las capturas",
            "detalle": str(e)
        }

    finally:
        if cursor:
            cursor.close()
        if connection:
            connection.close()
            # ============================================================
# ENDPOINT /camaras/{camara_id}/capturas
# ============================================================

@app.get("/camaras/{camara_id}/capturas")
def get_capturas_por_camara(camara_id: int, limit: int = 100):
    """
    Devuelve capturas filtradas por una cámara concreta.
    """

    connection = None
    cursor = None

    try:
        connection = get_connection()
        cursor = connection.cursor()

        query = """
            SELECT
                cp.id,
                c.codigo,
                cp.timestamp,
                cp.franja_horaria,
                cp.es_laborable,
                d.num_vehiculos,
                t.nivel
            FROM captura cp
            JOIN camara c ON cp.camara_id = c.id
            LEFT JOIN deteccion d ON d.captura_id = cp.id
            LEFT JOIN trafico t ON t.captura_id = cp.id
            WHERE cp.camara_id = %s
            ORDER BY cp.timestamp DESC
            LIMIT %s;
        """

        cursor.execute(query, (camara_id, limit))
        rows = cursor.fetchall()

        capturas = []

        for row in rows:
            capturas.append({
                "id": row[0],
                "camara": row[1],
                "timestamp": str(row[2]),
                "franja_horaria": row[3],
                "es_laborable": row[4],
                "num_vehiculos": row[5],
                "nivel_trafico": row[6]
            })

        return capturas

    except Exception as e:
        return {
            "error": "No se pudieron obtener las capturas de la cámara",
            "detalle": str(e)
        }

    finally:
        if cursor:
            cursor.close()
        if connection:
            connection.close()
            # ============================================================
# ENDPOINT /camaras/{camara_id}/capturas
# ============================================================

@app.get("/camaras/{camara_id}/capturas")
def get_capturas_por_camara(camara_id: int, limit: int = 100):
    """
    Devuelve las capturas asociadas a una cámara concreta.
    Permite limitar el número de resultados con el parámetro 'limit'.
    """

    connection = None
    cursor = None

    try:
        connection = get_connection()
        cursor = connection.cursor()

        query = """
            SELECT
                cp.id,
                c.codigo,
                cp.timestamp,
                cp.franja_horaria,
                cp.es_laborable,
                d.num_vehiculos,
                t.nivel
            FROM captura cp
            JOIN camara c ON cp.camara_id = c.id
            LEFT JOIN deteccion d ON d.captura_id = cp.id
            LEFT JOIN trafico t ON t.captura_id = cp.id
            WHERE cp.camara_id = %s
            ORDER BY cp.timestamp DESC
            LIMIT %s;
        """

        cursor.execute(query, (camara_id, limit))
        rows = cursor.fetchall()

        capturas = []

        for row in rows:
            capturas.append({
                "id": row[0],
                "camara": row[1],
                "timestamp": str(row[2]),
                "franja_horaria": row[3],
                "es_laborable": row[4],
                "num_vehiculos": row[5],
                "nivel_trafico": row[6]
            })

        return capturas

    except Exception as e:
        return {
            "error": "No se pudieron obtener las capturas de la cámara",
            "detalle": str(e)
        }

    finally:
        if cursor:
            cursor.close()
        if connection:
            connection.close()


# ============================================================
# ENDPOINT /trafico
# ============================================================

@app.get("/trafico")
def get_trafico(limit: int = 100):
    """
    Devuelve información de tráfico reciente:
    - cámara
    - timestamp
    - número de vehículos
    - nivel de tráfico
    """

    connection = None
    cursor = None

    try:
        connection = get_connection()
        cursor = connection.cursor()

        query = """
            SELECT
                cp.id,
                c.codigo,
                cp.timestamp,
                d.num_vehiculos,
                t.nivel
            FROM captura cp
            JOIN camara c ON cp.camara_id = c.id
            LEFT JOIN deteccion d ON d.captura_id = cp.id
            LEFT JOIN trafico t ON t.captura_id = cp.id
            ORDER BY cp.timestamp DESC
            LIMIT %s;
        """

        cursor.execute(query, (limit,))
        rows = cursor.fetchall()

        trafico_data = []

        for row in rows:
            trafico_data.append({
                "captura_id": row[0],
                "camara": row[1],
                "timestamp": str(row[2]),
                "num_vehiculos": row[3],
                "nivel_trafico": row[4]
            })

        return trafico_data

    except Exception as e:
        return {
            "error": "No se pudieron obtener los datos de tráfico",
            "detalle": str(e)
        }

    finally:
        if cursor:
            cursor.close()
        if connection:
            connection.close()


# ============================================================
# ENDPOINT /stats/camara/{camara_id}
# ============================================================

@app.get("/stats/camara/{camara_id}")
def get_stats_por_camara(camara_id: int):
    """
    Devuelve estadísticas de una cámara concreta:
    - nombre/código
    - número de capturas
    - media de vehículos detectados
    - máximo de vehículos detectados
    - mínimo de vehículos detectados
    """

    connection = None
    cursor = None

    try:
        connection = get_connection()
        cursor = connection.cursor()

        query = """
            SELECT
                c.id,
                c.codigo,
                COUNT(cp.id) AS total_capturas,
                AVG(d.num_vehiculos) AS media_vehiculos,
                MAX(d.num_vehiculos) AS max_vehiculos,
                MIN(d.num_vehiculos) AS min_vehiculos
            FROM camara c
            LEFT JOIN captura cp ON cp.camara_id = c.id
            LEFT JOIN deteccion d ON d.captura_id = cp.id
            WHERE c.id = %s
            GROUP BY c.id, c.codigo;
        """

        cursor.execute(query, (camara_id,))
        row = cursor.fetchone()

        if row is None:
            return {
                "error": "No existe ninguna cámara con ese id"
            }

        return {
            "camara_id": row[0],
            "camara": row[1],
            "total_capturas": row[2],
            "media_vehiculos": float(row[3]) if row[3] is not None else None,
            "max_vehiculos": row[4],
            "min_vehiculos": row[5]
        }

    except Exception as e:
        return {
            "error": "No se pudieron obtener las estadísticas de la cámara",
            "detalle": str(e)
        }

    finally:
        if cursor:
            cursor.close()
        if connection:
            connection.close()