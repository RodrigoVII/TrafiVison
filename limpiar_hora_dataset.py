# -*- coding: utf-8 -*-
"""
Limpieza de columna 'hora' en dataset_final(A).csv
--------------------------------------------------
Extrae únicamente la hora (HH:MM) sin fecha ni segundos.
"""

import os
import re
import pandas as pd

# Ruta del CSV original y de salida
CSV_IN = r"C:\Users\ditas\OneDrive\Escritorio\UE\2025-26 UE\Primer Cuatri\Proyecto De Computacion I\dataset_final(A).csv"
CSV_OUT = r"C:\Users\ditas\OneDrive\Escritorio\UE\2025-26 UE\Primer Cuatri\Proyecto De Computacion I\dataset_final_limpio.csv"

def limpiar_hhmm(valor):
    """Devuelve HH:MM a partir de cualquier texto de hora o datetime."""
    if pd.isna(valor):
        return None
    s = str(valor).strip().replace("\n", " ").replace("\r", " ")
    # Buscar HH:MM
    m = re.search(r"\b([01]?\d|2[0-3]):([0-5]\d)\b", s)
    if m:
        hh = int(m.group(1))
        mm = m.group(2)
        return f"{hh:02d}:{mm}"
    try:
        dt = pd.to_datetime(s, errors="coerce")
        if pd.notna(dt):
            return dt.strftime("%H:%M")
    except Exception:
        pass
    return None

# Leer CSV
df = pd.read_csv(CSV_IN, dtype=str)

# Limpiar la columna 'hora'
if "hora" not in df.columns:
    raise ValueError("No existe la columna 'hora' en el CSV.")
df["hora"] = df["hora"].apply(limpiar_hhmm)

# Guardar el CSV limpio
df.to_csv(CSV_OUT, index=False, encoding="utf-8")

print("\n✅ Columna 'hora' limpiada correctamente.")
print(f"📁 Archivo guardado en:\n{CSV_OUT}")
