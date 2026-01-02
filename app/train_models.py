# app/train_models.py
"""
Entrenamiento de modelos para TrafiVision (con guardado en /models).

Qué hace:
- Carga el CSV final limpio desde la raíz del proyecto: dataset_final_limpio.csv
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


def _find_first_existing_column(df: pd.DataFrame, candidates):
    """Devuelve el primer nombre de columna que exista en df, o None si no existe ninguno."""
    for c in candidates:
        if c in df.columns:
            return c
    return None


def _build_lluvia_cat(df: pd.DataFrame) -> pd.DataFrame:
    """
    Garantiza que existe df['lluvia_cat'].

    Casos típicos que cubre:
    - Si ya existe lluvia_cat: no hace nada.
    - Si existe lluvia (valores como "No", "Sí", "No llueve", "Lluvia débil"...): lo convierte a categorías coherentes.
    - Si existe litros_m2: deriva lluvia_cat por umbrales.
    - Si no hay nada: crea "No llueve" por defecto.
    """
    if "lluvia_cat" in df.columns:
        return df

    # 1) Si hay columna 'lluvia'
    if "lluvia" in df.columns:
        s = df["lluvia"].astype(str).str.strip().str.lower()

        # Normalización simple y robusta
        def map_lluvia(x: str) -> str:
            if x in ["no", "0", "false", "nan", "none"] or "no" == x or "no llueve" in x:
                return "No llueve"
            # Si ya viene con palabras tipo "débil/intensa"
            if "debil" in x or "débil" in x:
                return "Lluvia débil"
            if "intens" in x or "fuerte" in x:
                return "Lluvia intensa"
            # Si viene "si", "sí", "1", etc. asumimos débil
            if x in ["si", "sí", "1", "true"] or "llueve" in x:
                return "Lluvia débil"
            # fallback
            return "No llueve"

        df["lluvia_cat"] = s.apply(map_lluvia)
        return df

    # 2) Si no hay lluvia, pero hay litros_m2
    if "litros_m2" in df.columns:
        # Convertimos a numérico de forma segura
        litros = pd.to_numeric(df["litros_m2"], errors="coerce").fillna(0.0)

        def map_litros(v: float) -> str:
            if v <= 0:
                return "No llueve"
            if v <= 1.5:
                return "Lluvia débil"
            return "Lluvia intensa"

        df["lluvia_cat"] = litros.apply(map_litros)
        return df

    # 3) Si no hay nada, por defecto
    df["lluvia_cat"] = "No llueve"
    return df


def main():
    # -----------------------------
    # Rutas
    # -----------------------------
    root = Path(__file__).resolve().parents[1]          # .../TrafiVison
    csv_path = root / "dataset_final_limpio.csv"
    models_dir = root / "models"
    models_dir.mkdir(exist_ok=True)

    if not csv_path.exists():
        raise FileNotFoundError(f"No se encuentra el CSV en: {csv_path}")

    # -----------------------------
    # Cargar datos
    # -----------------------------
    df = pd.read_csv(csv_path)

    # -----------------------------
    # Detectar columna objetivo (target)
    # -----------------------------
    # Ajusta aquí si tu target se llama diferente
    target_col = _find_first_existing_column(df, ["trafico", "nivel_trafico", "nivel", "label"])
    if target_col is None:
        raise KeyError(
            "No encuentro la columna objetivo. "
            "Busqué: trafico / nivel_trafico / nivel / label.\n"
            f"Columnas disponibles: {list(df.columns)}"
        )

    # -----------------------------
    # Asegurar lluvia_cat
    # -----------------------------
    df = _build_lluvia_cat(df)

    # -----------------------------
    # Comprobar que existen columnas clave (features)
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
    # Preprocesado
    # - categóricas: OneHotEncoder(handle_unknown="ignore") para permitir combinaciones nuevas
    # - numéricas: imputación + escalado (para KNN y LogisticRegression viene bien)
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
