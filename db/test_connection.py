"""
============================================================
TrafiVision - Test de conexión a MariaDB (PyMySQL)
============================================================

Usamos PyMySQL porque en algunos Windows mysql-connector
puede quedarse colgado en el handshake.
"""

import socket
import pymysql

DB_HOST = "127.0.0.1"
DB_PORT = 3306
DB_USER = "root"
DB_PASSWORD = "trafivision"
DB_NAME = "trafivision"


def check_port(host: str, port: int, timeout_sec: int = 2) -> bool:
    try:
        with socket.create_connection((host, port), timeout=timeout_sec):
            return True
    except OSError:
        return False


def main():
    print("🟡 Ejecutando test_connection.py (PyMySQL)...")
    print(f"🔎 Comprobando puerto {DB_HOST}:{DB_PORT} ...")

    if not check_port(DB_HOST, DB_PORT, timeout_sec=2):
        print("❌ No hay nada escuchando en 3306. Arranca el servicio MariaDB.")
        return

    print("✅ Puerto abierto. Conectando...")

    try:
        conn = pymysql.connect(
            host=DB_HOST,
            port=DB_PORT,
            user=DB_USER,
            password=DB_PASSWORD,
            database=DB_NAME,
            connect_timeout=5,
            read_timeout=5,
            write_timeout=5,
            autocommit=True,
        )

        with conn.cursor() as cur:
            cur.execute("SELECT VERSION();")
            version = cur.fetchone()[0]

            cur.execute("SELECT DATABASE();")
            db = cur.fetchone()[0]

        conn.close()

        print("✅ Conexión exitosa")
        print("Servidor:", version)
        print("Base de datos:", db)
        print("🏁 Fin del test.")

    except Exception as e:
        print("❌ Error conectando con PyMySQL")
        print(repr(e))


if __name__ == "__main__":
    main()