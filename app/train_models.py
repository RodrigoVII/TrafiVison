# app/train_models.py
"""
Entrenamiento de modelos para TrafiVision (con guardado en /models).

NOVEDAD PCII:
- Ya no carga dataset_final_limpio.csv
- Ahora obtiene los datos desde la BASE DE DATOS MariaDB
- Usa db/db_client.py como capa de acceso a datos

Qué hace:
- Carga los datos desde MariaDB
- Normaliza columnas clave para que el entrenamiento y la app usen los mismos nombres
- Crea (si hace falta) la columna 'lluvia_cat'
- Entrena y guarda varios modelos:
    - Decision Tree
    - Logistic Regression
    - KNN
    - Random Forest

Cómo ejecutar:
    Desde la carpeta raíz del proyecto:
        python -m app.train_models

Salida:
    Carpeta /models en la raíz del proyecto con los .joblib
"""

from pathlib import Path
import pandas as pd
import joblib

from sklearn.pipeline import Pipeline
from sklearn.compose import ColumnTransformer
from sklearn.preprocessing import OneHotEncoder, StandardScaler
from sklearn.impute import SimpleImputer

from sklearn.tree import DecisionTreeClassifier
from sklearn.linear_model import LogisticRegression
from sklearn.neighbors import KNeighborsClassifier
from sklearn.ensemble import RandomForestClassifier

from db.db_client import get_training_dataframe


def _find_first_existing_column(df: pd.DataFrame, candidates):
    """
    Devuelve el primer nombre de columna que exista en df,
    o None si no existe ninguno.
    """
    for c in candidates:
        if c in df.columns:
            return c
    return None


def _build_lluvia_cat(df: pd.DataFrame) -> pd.DataFrame:
    """
    Garantiza que existe df['lluvia_cat'].

    Casos que cubre:
    - Si ya existe lluvia_cat: no hace nada.
    - Si existe lluvia (numérica o categórica): la transforma a categorías coherentes.
    - Si no hay nada: crea 'No llueve' por defecto.
    """
    if "lluvia_cat" in df.columns:
        return df

    if "lluvia" in df.columns:
        # Intentamos convertir a numérico
        lluvia_num = pd.to_numeric(df["lluvia"], errors="coerce")

        if not lluvia_num.isna().all():
            def map_lluvia_num(v):
                if pd.isna(v) or v <= 0:
                    return "No llueve"
                if v <= 1.5:
                    return "Lluvia débil"
                return "Lluvia intensa"

            df["lluvia_cat"] = lluvia_num.apply(map_lluvia_num)
            return df

        # Si no era numérica, tratamos como texto
        s = df["lluvia"].astype(str).str.strip().str.lower()

        def map_lluvia_text(x: str) -> str:
            if x in ["no", "0", "false", "nan", "none", "no llueve"]:
                return "No llueve"
            if "debil" in x or "débil" in x:
                return "Lluvia débil"
            if "intens" in x or "fuerte" in x:
                return "Lluvia intensa"
            if x in ["si", "sí", "1", "true"] or "llueve" in x:
                return "Lluvia débil"
            return "No llueve"

        df["lluvia_cat"] = s.apply(map_lluvia_text)
        return df

    df["lluvia_cat"] = "No llueve"
    return df


def _normalizar_laborable(df: pd.DataFrame) -> pd.DataFrame:
    """
    Convierte la columna laborable a los textos que usa la interfaz:
    - 1 -> Laborable
    - 0 -> No laborable
    """
    if "laborable" not in df.columns:
        return df

    def map_laborable(v):
        if pd.isna(v):
            return "No laborable"
        if isinstance(v, str):
            v_clean = v.strip().lower()
            if v_clean in ["laborable", "1", "true", "sí", "si", "yes"]:
                return "Laborable"
            return "No laborable"
        try:
            return "Laborable" if int(v) == 1 else "No laborable"
        except Exception:
            return "No laborable"

    df["laborable"] = df["laborable"].apply(map_laborable)
    return df


def _limpiar_nombre_calle(df: pd.DataFrame) -> pd.DataFrame:
    """
    Limpia el nombre de la calle para que coincida con lo que usa la app.
    Ejemplo:
        'Madrid - Alcalá - Velázquez' -> 'Alcalá - Velázquez'
    """
    if "calle" not in df.columns:
        return df

    def clean_name(name):
        if pd.isna(name):
            return name
        name = str(name).strip()
        if name.startswith("Madrid - "):
            name = name.replace("Madrid - ", "", 1)
        return name

    df["calle"] = df["calle"].apply(clean_name)
    return df


def main():
    # -----------------------------
    # Ruta de salida modelos
    # -----------------------------
    root = Path(__file__).resolve().parents[1]   # .../TrafiVison
    models_dir = root / "models"
    models_dir.mkdir(exist_ok=True)

    # -----------------------------
    # Cargar datos desde la base de datos
    # -----------------------------
    print("Cargando datos desde MariaDB...")
    df = get_training_dataframe()

    if df.empty:
        raise ValueError("No se han encontrado datos en la base de datos para entrenar.")

    # -----------------------------
    # Normalizaciones necesarias
    # -----------------------------
    df = _limpiar_nombre_calle(df)
    df = _normalizar_laborable(df)
    df = _build_lluvia_cat(df)

    # -----------------------------
    # Detectar columna objetivo
    # -----------------------------
    target_col = _find_first_existing_column(df, ["trafico", "nivel_trafico", "nivel", "label"])
    if target_col is None:
        raise KeyError(
            "No encuentro la columna objetivo. "
            "Busqué: trafico / nivel_trafico / nivel / label.\n"
            f"Columnas disponibles: {list(df.columns)}"
        )

    # -----------------------------
    # Comprobar columnas necesarias
    # -----------------------------
    required = ["calle", "franja_horaria", "laborable", "temperatura", "lluvia_cat"]
    missing = [c for c in required if c not in df.columns]
    if missing:
        raise KeyError(
            f"Faltan columnas necesarias para entrenar: {missing}\n"
            f"Columnas disponibles: {list(df.columns)}"
        )

    # -----------------------------
    # X / y
    # -----------------------------
    FEATURES = ["calle", "franja_horaria", "laborable", "lluvia_cat", "temperatura"]
    X = df[FEATURES].copy()
    y = df[target_col].copy()

    # -----------------------------
    # Limpiar posibles nulos en target
    # -----------------------------
    valid_mask = ~y.isna()
    X = X.loc[valid_mask].copy()
    y = y.loc[valid_mask].copy()

    # -----------------------------
    # Preprocesado
    # -----------------------------
    cat_features = ["calle", "franja_horaria", "laborable", "lluvia_cat"]
    num_features = ["temperatura"]

    cat_transformer = Pipeline(steps=[
        ("imputer", SimpleImputer(strategy="most_frequent")),
        ("onehot", OneHotEncoder(handle_unknown="ignore"))
    ])

    num_transformer = Pipeline(steps=[
        ("imputer", SimpleImputer(strategy="median")),
        ("scaler", StandardScaler())
    ])

    preprocessor = ColumnTransformer(
        transformers=[
            ("cat", cat_transformer, cat_features),
            ("num", num_transformer, num_features),
        ]
    )

    # -----------------------------
    # Modelos
    # -----------------------------
    models = {
        "decision_tree.joblib": DecisionTreeClassifier(
            random_state=42,
            class_weight="balanced"
        ),
        "logistic_regression.joblib": LogisticRegression(
            max_iter=2000,
            class_weight="balanced",
            n_jobs=None
        ),
        "knn.joblib": KNeighborsClassifier(
            n_neighbors=15
        ),
        "random_forest.joblib": RandomForestClassifier(
            n_estimators=400,
            random_state=42,
            class_weight="balanced",
            n_jobs=-1
        )
    }

    # -----------------------------
    # Entrenar y guardar
    # -----------------------------
    print(f"Entrenando con {len(X)} registros...")
    for filename, model in models.items():
        pipe = Pipeline(steps=[
            ("prep", preprocessor),
            ("model", model)
        ])

        pipe.fit(X, y)

        out_path = models_dir / filename
        joblib.dump(pipe, out_path)
        print(f"OK -> guardado {filename} en {out_path}")

    print("\nEntrenamiento completado. Modelos listos en /models.")


if __name__ == "__main__":
    main()