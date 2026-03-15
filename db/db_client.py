"""
========================================================
TrafiVision - Cliente de acceso a base de datos
========================================================

Este módulo actúa como intermediario entre la aplicación
de escritorio y la base de datos MariaDB.

Su objetivo es centralizar todas las consultas SQL para que
el resto de la aplicación NO tenga que preocuparse por
cómo se accede a la base de datos.

Funciones principales:
    - get_connection()
    - get_stats()
    - get_camaras()
    - get_capturas_dataframe()
    - get_training_dataframe()

Autor: Proyecto TrafiVision
"""

import pymysql
import pandas as pd


# ======================================================
# CONFIGURACIÓN DE LA BASE DE DATOS
# ======================================================

DB_CONFIG = {
    "host": "localhost",
    "port": 3306,
    "user": "root",
    "password": "trafivision",
    "database": "trafivision",
    "cursorclass": pymysql.cursors.Cursor
}


# ======================================================
# CONEXIÓN A LA BASE DE DATOS
# ======================================================

def get_connection():
    """
    Crea y devuelve una conexión a la base de datos.
    """
    connection = pymysql.connect(**DB_CONFIG)
    return connection


# ======================================================
# ESTADÍSTICAS GENERALES
# ======================================================

def get_stats():
    """
    Obtiene estadísticas generales de la base de datos.

    Returns
    -------
    dict
        Diccionario con el número de registros por tabla.
    """
    connection = get_connection()
    cursor = connection.cursor()

    stats = {}

    try:
        cursor.execute("SELECT COUNT(*) FROM camara")
        stats["camaras"] = cursor.fetchone()[0]

        cursor.execute("SELECT COUNT(*) FROM captura")
        stats["capturas"] = cursor.fetchone()[0]

        cursor.execute("SELECT COUNT(*) FROM meteo")
        stats["meteo"] = cursor.fetchone()[0]

        cursor.execute("SELECT COUNT(*) FROM deteccion")
        stats["detecciones"] = cursor.fetchone()[0]

        cursor.execute("SELECT COUNT(*) FROM trafico")
        stats["trafico"] = cursor.fetchone()[0]

    finally:
        cursor.close()
        connection.close()

    return stats


# ======================================================
# OBTENER LISTA DE CÁMARAS
# ======================================================

def get_camaras():
    """
    Devuelve todas las cámaras registradas en la base de datos.

    Returns
    -------
    list
        Lista de diccionarios con id y nombre de cámara.
    """
    connection = get_connection()
    cursor = connection.cursor()

    camaras = []

    try:
        query = """
        SELECT id, codigo
        FROM camara
        ORDER BY codigo;
        """

        cursor.execute(query)
        rows = cursor.fetchall()

        for row in rows:
            camaras.append({
                "id": row[0],
                "codigo": row[1]
            })

    finally:
        cursor.close()
        connection.close()

    return camaras


# ======================================================
# DATAFRAME DE CAPTURAS (PARA LA APP Y ML)
# ======================================================

def get_capturas_dataframe():
    """
    Obtiene todas las capturas en formato DataFrame.

    Este método sustituye al antiguo uso de CSV.

    Returns
    -------
    pandas.DataFrame
        DataFrame con todos los datos necesarios para
        visualización y entrenamiento del modelo.
    """
    connection = get_connection()

    query = """
    SELECT
        c.codigo AS calle,
        DATE(cp.timestamp) AS fecha,
        TIME(cp.timestamp) AS hora,
        d.num_vehiculos,
        t.nivel AS nivel_trafico,
        m.temperatura,
        m.precipitacion AS lluvia,
        m.humedad,
        cp.es_laborable AS laborable,
        cp.franja_horaria
    FROM captura cp
    JOIN camara c ON cp.camara_id = c.id
    LEFT JOIN deteccion d ON d.captura_id = cp.id
    LEFT JOIN trafico t ON t.captura_id = cp.id
    LEFT JOIN meteo m ON m.captura_id = cp.id
    ORDER BY cp.timestamp;
    """

    try:
        df = pd.read_sql(query, connection)
    finally:
        connection.close()

    # Formatear hora para que no salga "0 days ..."
    if "hora" in df.columns:
        df["hora"] = df["hora"].astype(str).str.replace("0 days ", "", regex=False)

    return df


# ======================================================
# DATAFRAME PARA ENTRENAMIENTO DEL MODELO
# ======================================================

def get_training_dataframe():
    """
    Devuelve un DataFrame preparado para el entrenamiento
    de los modelos de Machine Learning.

    Solo elimina filas con nulos en columnas realmente
    necesarias para entrenar.

    Returns
    -------
    pandas.DataFrame
    """
    df = get_capturas_dataframe()

    required_for_training = [
        "calle",
        "franja_horaria",
        "laborable",
        "temperatura",
        "nivel_trafico"
    ]

    # lluvia puede faltar; luego train_models la convierte a lluvia_cat
    existing_required = [c for c in required_for_training if c in df.columns]

    df = df.dropna(subset=existing_required)

    return df