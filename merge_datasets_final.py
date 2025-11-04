
# -*- coding: utf-8 -*-
"""
Fusión de datasets (PC1 - TrafiVision)
--------------------------------------
Une:
 - camaras_solo.csv  -> (fecha, hora, calle, ruta_imagen)
 - yolo_final.csv    -> (foto, num_vehiculos, nivel_trafico)
 - tiempo_madrid.csv -> (fecha, hora, temperatura, lluvia, litros_m2)

Características:
 - Solo conserva imágenes que estén en YOLO (evita faltantes).
 - Empareja cámaras+clima por fecha y hora con tolerancia ±2 min.
 - Limpia 'hora' a formato HH:MM (sin fechas extra).
 - Añade:
    * laborable: 'Laborable' (L-V) / 'No laborable' (S, D, o festivo Madrid 2025)
    * franja_horaria: madrugada(05-07), mañana(08-12), mediodía(13-16), tarde(17-20), noche(21-04)

Salida:
 - dataset_final.csv
"""

import os
import re
import pandas as pd
from datetime import datetime, timedelta

# ----- RUTAS -----
BASE = r"C:\Users\ditas\OneDrive\Escritorio\UE\2025-26 UE\Primer Cuatri\Proyecto De Computacion I\TrafiVison\csv"
CAM_PATH  = os.path.join(BASE, "camaras_solo.csv")
YOLO_PATH = os.path.join(BASE, "yolo_final.csv")
MET_PATH  = os.path.join(BASE, "tiempo_madrid.csv")
OUT_PATH  = os.path.join(BASE, "dataset_final.csv")

# ----- AUXILIARES -----
def basename(p):
    return os.path.basename(str(p)).strip()

def limpiar_hhmm(texto):
    """Extrae HH:MM de cualquier cadena (admite 'YYYY-mm-dd HH:MM:SS', 'HH:MM:SS', 'HH:MM', etc.)."""
    if pd.isna(texto):
        return None
    s = str(texto).replace("\n", " ").replace("\r", " ")
    s = re.sub(r"\s+", " ", s).strip()
    m = re.search(r"\b([01]?\d|2[0-3]):([0-5]\d)\b", s)
    if m:
        hh = int(m.group(1)); mm = m.group(2)
        return f"{hh:02d}:{mm}"
    try:
        dt = pd.to_datetime(s, errors="coerce")
        if pd.notna(dt):
            return dt.strftime("%H:%M")
    except Exception:
        pass
    return None

def combinar_fecha_hora(fecha_str, hhmm_str):
    """Devuelve datetime a partir de 'YYYY-mm-dd' y 'HH:MM' (o lo que venga)."""
    hhmm = limpiar_hhmm(hhmm_str)
    if not hhmm:
        return pd.NaT
    try:
        return pd.to_datetime(f"{fecha_str} {hhmm}", format="%Y-%m-%d %H:%M", errors="coerce")
    except Exception:
        return pd.NaT

def es_festivo_madrid_2025(dtobj):
    festivos = {
        "2025-01-01","2025-01-06","2025-03-19","2025-03-20","2025-05-01",
        "2025-05-02","2025-07-25","2025-08-15","2025-10-12",
        "2025-11-01","2025-12-06","2025-12-08","2025-12-25"
    }
    return dtobj.strftime("%Y-%m-%d") in festivos

def etiqueta_laborable(dtobj):
    # L=0 ... D=6
    if dtobj.weekday() >= 5 or es_festivo_madrid_2025(dtobj):
        return "No laborable"
    return "Laborable"

def franja_horaria(dtobj):
    h = dtobj.hour
    if 5 <= h <= 7:
        return "madrugada"
    if 8 <= h <= 12:
        return "mañana"
    if 13 <= h <= 16:
        return "mediodía"
    if 17 <= h <= 20:
        return "tarde"
    return "noche"  # 21-23 y 00-04

def merge_con_tolerancia(izq_df, der_df, key_fecha="fecha", key_dt_izq="dt", key_dt_der="dt", tolerancia_min=2):
    """
    Une izq_df con der_df por fecha y 'hora' más cercana dentro de ±tolerancia_min.
    Retorna izq_df con columnas de der_df anexadas donde hay match.
    """
    out_rows = []
    # index por fecha para acelerar
    der_g = {f: g.copy() for f, g in der_df.groupby(key_fecha)}  # dict fecha -> DF
    for _, row in izq_df.iterrows():
        f = row[key_fecha]
        dt_izq = row[key_dt_izq]
        anexo = {"temperatura": pd.NA, "lluvia": pd.NA, "litros_m2": pd.NA}
        if f in der_g and pd.notna(dt_izq):
            sub = der_g[f]
            # diferencia absoluta en minutos
            dif = (sub[key_dt_der] - dt_izq).abs().dt.total_seconds() / 60.0
            sub2 = sub.loc[dif <= tolerancia_min].copy()
            if not sub2.empty:
                # fila más cercana
                j = dif.loc[sub2.index].idxmin()
                anexo["temperatura"] = sub.loc[j, "temperatura"]
                anexo["lluvia"]      = sub.loc[j, "lluvia"]
                anexo["litros_m2"]   = sub.loc[j, "litros_m2"]
        merged_row = {**row.to_dict(), **anexo}
        out_rows.append(merged_row)
    return pd.DataFrame(out_rows)

# ----- CARGA -----
cam = pd.read_csv(CAM_PATH)
yolo = pd.read_csv(YOLO_PATH)
met = pd.read_csv(MET_PATH)

# ----- NORMALIZAR NOMBRES PARA UNIR CÁMARAS + YOLO -----
cam["archivo"] = cam["ruta_imagen"].apply(basename)
if "foto" in yolo.columns:
    yolo["archivo"] = yolo["foto"].apply(basename)
else:
    # por si el csv de yolo ya trae 'archivo' directamente
    yolo["archivo"] = yolo["archivo"].apply(basename)

# SOLO imágenes que estén en YOLO
cam_yolo = cam.merge(
    yolo[["archivo", "num_vehiculos", "nivel_trafico"]],
    on="archivo",
    how="inner"
)

# ----- LIMPIAR HORAS Y CREAR DATETIME -----
cam_yolo["hora"]   = cam_yolo["hora"].apply(limpiar_hhmm)
met["hora"]        = met["hora"].apply(limpiar_hhmm)

cam_yolo["dt"] = cam_yolo.apply(lambda r: combinar_fecha_hora(r["fecha"], r["hora"]), axis=1)
met["dt"]      = met.apply(     lambda r: combinar_fecha_hora(r["fecha"], r["hora"]), axis=1)

# Asegurar tipos datetime
cam_yolo["dt"] = pd.to_datetime(cam_yolo["dt"], errors="coerce")
met["dt"]      = pd.to_datetime(met["dt"], errors="coerce")

# ----- MERGE CON TOLERANCIA ±2 MIN -----
final = merge_con_tolerancia(
    izq_df=cam_yolo,
    der_df=met,
    key_fecha="fecha",
    key_dt_izq="dt",
    key_dt_der="dt",
    tolerancia_min=2
)

# ----- COLUMNAS DERIVADAS: laborable, franja -----
final["fecha_dt"] = pd.to_datetime(final["fecha"], format="%Y-%m-%d", errors="coerce")
final["laborable"] = final["fecha_dt"].apply(etiqueta_laborable)
final["franja_horaria"] = final["dt"].apply(franja_horaria)

# ----- HORA LIMPIA (HH:MM) -----
final["hora"] = final["dt"].dt.strftime("%H:%M")

# ----- ORDEN Y GUARDADO -----
cols = [
    # si tienes 'ciudad' en camaras_solo.csv, la conservamos; si no, la omitimos automáticamente
    *(["ciudad"] if "ciudad" in final.columns else []),
    "calle", "fecha", "hora", "ruta_imagen",
    "num_vehiculos", "nivel_trafico",
    "temperatura", "lluvia", "litros_m2",
    "laborable", "franja_horaria"
]
# filtra solo columnas existentes por si falta 'ciudad'
cols = [c for c in cols if c in final.columns]

final = final[cols].copy()
final.to_csv(OUT_PATH, index=False, encoding="utf-8")

print("\n✅ Dataset final generado.")
print(f"📁 {OUT_PATH}")
print(f"📊 Filas: {len(final)}")
#prueba git