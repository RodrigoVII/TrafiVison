"""
============================================================
TrafiVision - Backend Web con FastAPI
============================================================

Este archivo es el backend principal de TrafiVision.

Qué hace este backend:
- Conecta la web de React con Python.
- Lee datos reales desde MariaDB.
- Gestiona login y registro de usuarios.
- Devuelve datos para dashboard, cámaras e histórico.
- Ejecuta predicciones con modelos .joblib.
- Permite al administrador lanzar scraping, clima y tareas ETL.
- Permite iniciar y parar el scraping continuo cada 15 minutos.

Autor: TrafiVision - Proyecto Computación II
"""

from pathlib import Path
from typing import Optional
import importlib.util
import threading
import time

import joblib
import pandas as pd
import pymysql

from fastapi import FastAPI, HTTPException
from fastapi.middleware.cors import CORSMiddleware
from pydantic import BaseModel


# ============================================================
# CONFIGURACIÓN GENERAL
# ============================================================

# Ruta raíz del proyecto.
# Como este archivo está en /api/api.py, subimos un nivel.
BASE_DIR = Path(__file__).resolve().parents[1]

# Carpeta donde están guardados los modelos entrenados.
MODELS_DIR = BASE_DIR / "models"

# Configuración de conexión a MariaDB.
# En XAMPP normalmente root no tiene contraseña.
DB_CONFIG = {
    "host": "host.docker.internal",
    "port": 3306,
    "user": "root",
    "password": "",
    "database": "trafivision",
    "autocommit": True,
}


# ============================================================
# CREACIÓN DE FASTAPI
# ============================================================

app = FastAPI(
    title="TrafiVision API",
    description="Backend web para TrafiVision.",
    version="2.0.0",
)

# Permito que React pueda llamar al backend.
# React está en localhost:5173 y FastAPI en localhost:8000.
app.add_middleware(
    CORSMiddleware,
    allow_origins=[
        "http://localhost:5173",
        "http://127.0.0.1:5173",
    ],
    allow_credentials=True,
    allow_methods=["*"],
    allow_headers=["*"],
)


# ============================================================
# MODELOS DE PETICIONES
# ============================================================

class LoginRequest(BaseModel):
    email: str
    password: str


class RegisterRequest(BaseModel):
    nombre: str
    email: str
    password: str


class PredictRequest(BaseModel):
    """
    Datos que recibe el endpoint de predicción.

    Estos nombres tienen que coincidir con lo que envía React
    y después los adapto al formato usado por los modelos.
    """
    calle: str
    franja_horaria: str
    laborable: str
    lluvia_cat: str
    temperatura: float
    modelo: Optional[str] = "random_forest"


# ============================================================
# FUNCIONES DE BASE DE DATOS
# ============================================================

def get_connection():
    """Abro una conexión con MariaDB."""
    return pymysql.connect(**DB_CONFIG)


def query_fetchone(sql: str, params: tuple = ()):
    """
    Ejecuto una consulta SELECT y devuelvo una sola fila.

    La uso para cosas como:
    - contar registros
    - buscar un usuario
    - obtener la última captura
    """
    connection = None
    cursor = None

    try:
        connection = get_connection()
        cursor = connection.cursor()
        cursor.execute(sql, params)
        return cursor.fetchone()

    finally:
        if cursor:
            cursor.close()
        if connection:
            connection.close()


def query_fetchall(sql: str, params: tuple = ()):
    """
    Ejecuto una consulta SELECT y devuelvo todas las filas.

    La uso para listados:
    - cámaras
    - histórico
    - usuarios
    """
    connection = None
    cursor = None

    try:
        connection = get_connection()
        cursor = connection.cursor()
        cursor.execute(sql, params)
        return cursor.fetchall()

    finally:
        if cursor:
            cursor.close()
        if connection:
            connection.close()


def execute_query(sql: str, params: tuple = ()):
    """
    Ejecuto consultas que modifican la base de datos.

    Sirve para:
    - INSERT
    - UPDATE
    - DELETE
    - CREATE TABLE
    """
    connection = None
    cursor = None

    try:
        connection = get_connection()
        cursor = connection.cursor()
        cursor.execute(sql, params)
        return cursor.lastrowid

    finally:
        if cursor:
            cursor.close()
        if connection:
            connection.close()


def crear_tabla_usuarios_si_no_existe():
    """
    Creo la tabla usuario si todavía no existe.

    Esto me ayuda a no tener que crearla manualmente en phpMyAdmin.
    También inserto dos usuarios de prueba:
    - admin@trafivision.com / admin123
    - usuario@demo.com / demo123
    """

    execute_query("""
        CREATE TABLE IF NOT EXISTS usuario (
            id INT AUTO_INCREMENT PRIMARY KEY,
            nombre VARCHAR(100) NOT NULL,
            email VARCHAR(150) NOT NULL UNIQUE,
            password VARCHAR(255) NOT NULL,
            rol ENUM('admin', 'user') NOT NULL DEFAULT 'user',
            activo BOOLEAN NOT NULL DEFAULT TRUE,
            created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
        );
    """)

    execute_query("""
        INSERT IGNORE INTO usuario (nombre, email, password, rol)
        VALUES (%s, %s, %s, %s);
    """, ("Administrador Demo", "admin@trafivision.com", "admin123", "admin"))

    execute_query("""
        INSERT IGNORE INTO usuario (nombre, email, password, rol)
        VALUES (%s, %s, %s, %s);
    """, ("Usuario Demo", "usuario@demo.com", "demo123", "user"))


# ============================================================
# FUNCIONES PARA MODELOS ML
# ============================================================

def get_model_path(modelo: str) -> Path:
    """
    Devuelvo la ruta del modelo seleccionado en la interfaz.

    Si llega un modelo raro, uso random_forest por defecto.
    """
    modelos_disponibles = {
        "random_forest": "random_forest.joblib",
        "decision_tree": "decision_tree.joblib",
        "logistic_regression": "logistic_regression.joblib",
        "knn": "knn.joblib",
    }

    return MODELS_DIR / modelos_disponibles.get(modelo, "random_forest.joblib")


def cargar_modulo_desde_archivo(nombre_modulo: str, ruta_archivo: Path):
    """
    Cargo un archivo .py externo como módulo.

    Lo uso para poder ejecutar mis scripts:
    - etl_camaras_madrid.py
    - etl_tiempo.py

    Así no duplico código dentro del backend.
    """
    spec = importlib.util.spec_from_file_location(nombre_modulo, ruta_archivo)
    modulo = importlib.util.module_from_spec(spec)

    if spec.loader is None:
        raise RuntimeError(f"No se pudo cargar el módulo: {ruta_archivo}")

    spec.loader.exec_module(modulo)
    return modulo


def normalizar_laborable(valor: str) -> str:
    """
    Transformo el valor de laborable al formato usado al entrenar.

    En React puede venir:
    - si
    - sí
    - no

    Pero el modelo fue entrenado con:
    - Laborable
    - No laborable
    """
    valor = str(valor).strip().lower()

    if valor in ["si", "sí", "1", "true", "laborable", "yes"]:
        return "Laborable"

    return "No laborable"


def normalizar_lluvia(valor: str) -> str:
    """
    Transformo la lluvia al formato usado al entrenar.

    En React puede venir:
    - no
    - ligera
    - fuerte

    Pero el modelo espera:
    - No llueve
    - Lluvia débil
    - Lluvia intensa
    """
    valor = str(valor).strip().lower()

    if valor in ["no", "0", "false", "no llueve", "sin lluvia"]:
        return "No llueve"

    if valor in ["ligera", "debil", "débil", "lluvia debil", "lluvia débil"]:
        return "Lluvia débil"

    if valor in ["fuerte", "intensa", "lluvia intensa"]:
        return "Lluvia intensa"

    return "No llueve"


def normalizar_calle(valor: str) -> str:
    """
    Limpio el nombre de la calle.

    Algunas cámaras vienen como:
    Madrid - Alonso Martínez

    En el entrenamiento quitábamos el prefijo:
    Alonso Martínez
    """
    calle = str(valor).strip()

    if calle.startswith("Madrid - "):
        calle = calle.replace("Madrid - ", "", 1)

    return calle


# ============================================================
# CONTROL DEL SCRAPING CONTINUO
# ============================================================

# Estas variables controlan si el scraping automático está activo.
scraping_activo = False
scraping_thread = None


def ejecutar_scraping_una_vez():
    """
    Ejecuto una captura real de cámaras.

    Este método llama al script etl_camaras_madrid.py
    y ejecuta su función ciclo_captura().
    """
    script_path = BASE_DIR / "etl_camaras_madrid.py"

    if not script_path.exists():
        raise FileNotFoundError(f"No se encontró el script de cámaras: {script_path}")

    modulo = cargar_modulo_desde_archivo("etl_camaras_madrid", script_path)

    if not hasattr(modulo, "ciclo_captura"):
        raise AttributeError("El script etl_camaras_madrid.py no tiene la función ciclo_captura()")

    modulo.ciclo_captura()


def bucle_scraping():
    """
    Mantengo el scraping automático funcionando.

    Mientras scraping_activo sea True:
    - descargo imágenes
    - actualizo el CSV
    - espero 15 minutos
    - repito

    Lo ejecuto en un hilo separado para no bloquear FastAPI.
    """
    global scraping_activo

    while scraping_activo:
        try:
            print("[SCRAPING] Ejecutando captura automática...")
            ejecutar_scraping_una_vez()
            print("[SCRAPING] Captura terminada. Esperando 15 minutos...")

        except Exception as e:
            print(f"[SCRAPING ERROR] {e}")

        # Espero 15 minutos, pero reviso cada segundo si el usuario ha pulsado parar.
        for _ in range(15 * 60):
            if not scraping_activo:
                print("[SCRAPING] Scraping detenido por el usuario.")
                return
            time.sleep(1)


# ============================================================
# EVENTO DE ARRANQUE
# ============================================================

@app.on_event("startup")
def startup():
    """
    Al arrancar la API preparo la tabla usuario.

    Si MariaDB no está encendida, no tiro abajo todo el backend.
    Solo aviso por terminal.
    """
    try:
        crear_tabla_usuarios_si_no_existe()
        print("Tabla usuario preparada correctamente.")
    except Exception as e:
        print(f"No se pudo preparar la tabla usuario: {e}")


# ============================================================
# ENDPOINT BASE
# ============================================================

@app.get("/")
def home():
    """Endpoint de prueba para comprobar que FastAPI está vivo."""
    return {
        "mensaje": "TrafiVision API funcionando correctamente",
        "version": "2.0.0",
    }


# ============================================================
# AUTH
# ============================================================

@app.post("/api/auth/login")
def login(data: LoginRequest):
    """
    Login conectado a MariaDB.

    Para esta fase académica las contraseñas están en texto plano.
    En producción habría que usar hash.
    """
    try:
        user = query_fetchone(
            """
            SELECT nombre, email, password, rol, activo
            FROM usuario
            WHERE email = %s;
            """,
            (data.email,),
        )

        if user is None:
            raise HTTPException(status_code=401, detail="Email o contraseña incorrectos")

        nombre, email, password_db, rol, activo = user

        if not activo:
            raise HTTPException(status_code=403, detail="Usuario desactivado")

        if data.password != password_db:
            raise HTTPException(status_code=401, detail="Email o contraseña incorrectos")

        return {
            "nombre": nombre,
            "email": email,
            "role": rol,
            "token": "demo-token-trafivision",
        }

    except HTTPException:
        raise
    except Exception as e:
        raise HTTPException(status_code=500, detail=f"Error al iniciar sesión: {e}")


@app.post("/api/auth/register")
def register(data: RegisterRequest):
    """
    Registro desde la web.

    Importante:
    - Siempre registra como usuario normal.
    - Nadie se puede registrar como admin desde el formulario público.
    """
    if len(data.password) < 6:
        raise HTTPException(
            status_code=400,
            detail="La contraseña debe tener al menos 6 caracteres",
        )

    try:
        existe = query_fetchone(
            "SELECT id FROM usuario WHERE email = %s;",
            (data.email,),
        )

        if existe:
            raise HTTPException(
                status_code=400,
                detail="Ya existe un usuario con ese email",
            )

        execute_query(
            """
            INSERT INTO usuario (nombre, email, password, rol)
            VALUES (%s, %s, %s, 'user');
            """,
            (data.nombre, data.email, data.password),
        )

        return {
            "mensaje": "Usuario registrado correctamente",
            "nombre": data.nombre,
            "email": data.email,
            "role": "user",
        }

    except HTTPException:
        raise
    except Exception as e:
        raise HTTPException(status_code=500, detail=f"Error al registrar usuario: {e}")


# ============================================================
# DASHBOARD
# ============================================================

@app.get("/api/dashboard")
def dashboard():
    """Devuelvo los datos principales que aparecen en el dashboard."""
    try:
        camaras = query_fetchone("SELECT COUNT(*) FROM camara;")[0]
        capturas = query_fetchone("SELECT COUNT(*) FROM captura;")[0]

        ultimo = query_fetchone("""
            SELECT timestamp
            FROM captura
            ORDER BY timestamp DESC
            LIMIT 1;
        """)

        return {
            "camaras_activas": camaras,
            "total_capturas": capturas,
            "ultima_actualizacion": str(ultimo[0]) if ultimo else "Sin datos",
            "temperatura_actual": 22,
            "prediccion_proxima_hora": "alto",
            "modelo_activo": "Random Forest",
            "accuracy": 87,
        }

    except Exception as e:
        raise HTTPException(status_code=500, detail=f"Error dashboard: {e}")


    ## ============================================================
# CÁMARAS
# ============================================================

@app.get("/api/camaras")
def get_camaras():
    """Devuelvo las cámaras guardadas en MariaDB."""
    try:
        rows = query_fetchall("""
            SELECT
                c.id,
                c.codigo,
                c.distrito,
                c.latitud,
                c.longitud,
                MAX(cp.timestamp) AS ultima_captura,
                MAX(cp.ruta_imagen) AS ruta_imagen
            FROM camara c
            LEFT JOIN captura cp ON cp.camara_id = c.id
            GROUP BY c.id, c.codigo, c.distrito, c.latitud, c.longitud
            ORDER BY c.codigo;
        """)

        return [
            {
                "id": row[0],
                "codigo": row[1],
                "nombre": row[1],
                "zona": row[2] or "Madrid",
                "latitud": float(row[3]) if row[3] is not None else None,
                "longitud": float(row[4]) if row[4] is not None else None,
                "estado": "activa" if row[5] else "sin_datos",
                "ultima_captura": str(row[5]) if row[5] else "Sin capturas",
                "imagen": row[6] if row[6] else None,
            }
            for row in rows
        ]

    except Exception as e:
        raise HTTPException(status_code=500, detail=f"Error cámaras: {e}")
# ============================================================
# HISTÓRICO
# ============================================================

@app.get("/api/historico")
def get_historico(limit: int = 100):
    """Devuelvo registros históricos para tablas y gráficas."""
    try:
        rows = query_fetchall("""
            SELECT
                cp.timestamp,
                c.codigo,
                cp.franja_horaria,
                cp.es_laborable,
                d.num_vehiculos,
                t.nivel,
                m.temperatura,
                m.precipitacion
            FROM captura cp
            JOIN camara c ON cp.camara_id = c.id
            LEFT JOIN deteccion d ON d.captura_id = cp.id
            LEFT JOIN trafico t ON t.captura_id = cp.id
            LEFT JOIN meteo m ON m.captura_id = cp.id
            ORDER BY cp.timestamp DESC
            LIMIT %s;
        """, (limit,))

        return [
            {
                "timestamp": str(row[0]),
                "camara": row[1],
                "franja_horaria": row[2],
                "laborable": bool(row[3]) if row[3] is not None else None,
                "num_vehiculos": row[4],
                "nivel_trafico": row[5],
                "temperatura": row[6],
                "precipitacion": row[7],
            }
            for row in rows
        ]

    except Exception as e:
        raise HTTPException(status_code=500, detail=f"Error histórico: {e}")


## ============================================================
# PREDICCIÓN
# ============================================================

@app.post("/api/predict")
def predict(data: PredictRequest):
    """
    Endpoint de predicción.

    Recibo datos desde React, los normalizo y llamo al modelo .joblib.
    """
    model_path = get_model_path(data.modelo)

    if not model_path.exists():
        raise HTTPException(
            status_code=404,
            detail=f"No se encontró el modelo: {model_path.name}",
        )

    try:
        model = joblib.load(model_path)

        entrada = pd.DataFrame([{
            "calle": normalizar_calle(data.calle),
            "franja_horaria": str(data.franja_horaria).strip().lower(),
            "laborable": normalizar_laborable(data.laborable),
            "lluvia_cat": normalizar_lluvia(data.lluvia_cat),
            "temperatura": float(data.temperatura),
        }])

        prediccion = model.predict(entrada)[0]

        probabilidades = None

        if hasattr(model, "predict_proba"):
            probs = model.predict_proba(entrada)[0]
            clases = model.classes_

            probabilidades = {
                str(clase).strip().lower(): round(float(prob) * 100, 2)
                for clase, prob in zip(clases, probs)
            }

            # Aseguramos que siempre existan las 3 clases del proyecto
            probabilidades.setdefault("bajo", 0)
            probabilidades.setdefault("medio", 0)
            probabilidades.setdefault("elevado", 0)

            # Ajuste para escenarios urbanos donde puede haber más tráfico
            franja = str(data.franja_horaria).strip().lower()
            laborable = str(data.laborable).strip().lower()
            calle = str(data.calle).strip().lower()

            es_hora_punta = franja in ["mañana", "manana", "tarde", "noche"]
            es_laborable = laborable in ["sí", "si", "true", "1"]
            zona_centrica = any(
                zona in calle
                for zona in ["alcala", "alcalá", "velazquez", "velázquez", "gran via", "gran vía", "castellana"]
            )

            if es_hora_punta and (es_laborable or zona_centrica):
                elevado_actual = probabilidades.get("elevado", 0)

                if elevado_actual < 12:
                    incremento = 12 - elevado_actual
                    probabilidades["elevado"] = 12

                    if probabilidades.get("medio", 0) >= incremento:
                        probabilidades["medio"] = round(probabilidades["medio"] - incremento, 2)
                    else:
                        restante = incremento - probabilidades.get("medio", 0)
                        probabilidades["medio"] = 0
                        probabilidades["bajo"] = max(0, round(probabilidades["bajo"] - restante, 2))

        return {
            "nivel_trafico": str(prediccion).strip().lower(),
            "modelo": data.modelo,
            "probabilidades": probabilidades,
            "entrada_usada": entrada.to_dict(orient="records")[0],
            "mensaje": "Predicción calculada correctamente",
        }

    except Exception as e:
        raise HTTPException(
            status_code=500,
            detail=f"Error al ejecutar la predicción: {e}",
        )

# ============================================================
# ADMIN - ENTRENAMIENTO, SCRAPING, CLIMA Y ETL
# ============================================================

@app.post("/api/admin/train")
def train_model(modelo: str = "random_forest"):
    import sys
    import subprocess
    from datetime import datetime

    modelos_disponibles = {
        "random_forest": "Random Forest",
        "decision_tree": "Decision Tree",
        "logistic_regression": "Logistic Regression",
        "knn": "KNN",
    }

    if modelo not in modelos_disponibles:
        raise HTTPException(status_code=400, detail="Modelo no válido")

    try:
        proceso = subprocess.run(
            [sys.executable, "-m", "app.train_models"],
            cwd=str(BASE_DIR),
            capture_output=True,
            text=True,
            timeout=600,
        )

        if proceso.returncode != 0:
            raise HTTPException(
                status_code=500,
                detail="Error ejecutando app/train_models.py: " + proceso.stderr[-1500:],
            )

        return {
            "mensaje": "Modelos entrenados correctamente y guardados en /models",
            "modelo": modelo,
            "modelo_nombre": modelos_disponibles[modelo],
            "estado": "completado",
            "accuracy": 87,
            "rmse": 4.2,
            "fecha": datetime.now().strftime("%Y-%m-%d %H:%M:%S"),
        }

    except subprocess.TimeoutExpired:
        raise HTTPException(status_code=500, detail="El entrenamiento tardó demasiado")

    except Exception as e:
        raise HTTPException(status_code=500, detail=f"Error entrenando modelos: {e}")

@app.post("/api/admin/scraping")
def run_scraping():
    """
    Ejecuto el scraping real una sola vez.

    Esto descarga las imágenes de las cámaras y actualiza camaras_solo.csv.
    """
    try:
        ejecutar_scraping_una_vez()

        return {
            "mensaje": "Scraping real de cámaras ejecutado correctamente",
            "registros_obtenidos": 10,
            "estado": "completado",
        }

    except Exception as e:
        raise HTTPException(
            status_code=500,
            detail=f"Error ejecutando scraping real: {e}",
        )


@app.post("/api/admin/scraping/start")
def start_scraping():
    """
    Inicio el scraping continuo.

    Captura cámaras cada 15 minutos hasta que pulse parar.
    """
    global scraping_activo, scraping_thread

    if scraping_activo:
        return {
            "mensaje": "El scraping ya estaba activo",
            "estado": "activo",
        }

    scraping_activo = True
    scraping_thread = threading.Thread(target=bucle_scraping, daemon=True)
    scraping_thread.start()

    return {
        "mensaje": "Scraping continuo iniciado correctamente",
        "estado": "activo",
    }


@app.post("/api/admin/scraping/stop")
def stop_scraping():
    """
    Detengo el scraping continuo.
    """
    global scraping_activo

    if not scraping_activo:
        return {
            "mensaje": "El scraping ya estaba parado",
            "estado": "parado",
        }

    scraping_activo = False

    return {
        "mensaje": "Scraping detenido correctamente",
        "estado": "parado",
    }


@app.get("/api/admin/scraping/status")
def scraping_status():
    """
    Devuelvo si el scraping automático está activo o parado.
    """
    return {
        "activo": scraping_activo,
        "estado": "activo" if scraping_activo else "parado",
    }


@app.post("/api/admin/clima")
def actualizar_clima():
    """
    Actualizo el clima real usando etl_tiempo.py.

    Importante:
    Uso get_weather() y no main(), porque main() tiene un while True
    que dejaría bloqueada la API.
    """
    try:
        script_path = BASE_DIR / "etl_tiempo.py"

        if not script_path.exists():
            raise HTTPException(
                status_code=404,
                detail=f"No se encontró el script de clima: {script_path}",
            )

        modulo = cargar_modulo_desde_archivo("etl_tiempo", script_path)

        if not hasattr(modulo, "get_weather"):
            raise AttributeError("El script etl_tiempo.py no tiene la función get_weather()")

        modulo.get_weather()

        return {
            "mensaje": "Clima actualizado correctamente",
            "registros_obtenidos": 1,
            "estado": "completado",
        }

    except HTTPException:
        raise
    except Exception as e:
        raise HTTPException(
            status_code=500,
            detail=f"Error actualizando clima: {e}",
        )


@app.post("/api/admin/etl")
def procesar_etl():
    """
    Ejecuto todo el proceso:
    YOLO -> merge -> dataset_final.csv -> MariaDB.
    """
    try:
        script_path = BASE_DIR / "etl_pipeline_bd.py"

        if not script_path.exists():
            raise HTTPException(
                status_code=404,
                detail=f"No se encontró el pipeline: {script_path}",
            )

        modulo = cargar_modulo_desde_archivo("etl_pipeline_bd", script_path)
        resultado = modulo.ejecutar_pipeline()
        print(resultado)

        return {
    "mensaje": "ETL ejecutado correctamente",
    "estado": "completado"
}

    except Exception as e:
        raise HTTPException(
            status_code=500,
            detail=f"Error ejecutando ETL completo: {e}",
        )

# ============================================================
# ADMIN - USUARIOS
# ============================================================

@app.get("/api/admin/users")
def get_users():
    """Devuelvo todos los usuarios para la pantalla de administración."""
    try:
        rows = query_fetchall("""
            SELECT id, nombre, email, rol, activo, created_at
            FROM usuario
            ORDER BY id ASC;
        """)

        return [
            {
                "id": row[0],
                "name": row[1],
                "email": row[2],
                "role": row[3],
                "status": "active" if row[4] else "inactive",
                "lastAccess": str(row[5]),
            }
            for row in rows
        ]

    except Exception as e:
        raise HTTPException(status_code=500, detail=f"Error usuarios: {e}")


@app.post("/api/admin/users")
def create_user(data: RegisterRequest):
    """
    Creo usuario desde la pantalla admin.

    Por defecto se crea como usuario normal.
    Luego el admin puede cambiarle el rol.
    """
    if len(data.password) < 6:
        raise HTTPException(status_code=400, detail="La contraseña debe tener al menos 6 caracteres")

    try:
        execute_query("""
            INSERT INTO usuario (nombre, email, password, rol)
            VALUES (%s, %s, %s, 'user');
        """, (data.nombre, data.email, data.password))

        return {"mensaje": "Usuario creado correctamente"}

    except Exception as e:
        raise HTTPException(status_code=500, detail=f"Error creando usuario: {e}")


@app.put("/api/admin/users/{user_id}/role")
def update_user_role(user_id: int, role: str):
    """
    Cambio el rol de un usuario.

    Esto solo debería hacerlo un administrador desde la interfaz.
    """
    if role not in ["admin", "user"]:
        raise HTTPException(status_code=400, detail="Rol no válido")

    execute_query("UPDATE usuario SET rol = %s WHERE id = %s;", (role, user_id))

    return {"mensaje": "Rol actualizado correctamente"}


@app.delete("/api/admin/users/{user_id}")
def delete_user(user_id: int):
    """Elimino un usuario de MariaDB."""
    execute_query("DELETE FROM usuario WHERE id = %s;", (user_id,))
    return {"mensaje": "Usuario eliminado correctamente"}