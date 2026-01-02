# app/train_models.py
"""
Entrenamiento de modelos para TrafiVision.

Objetivo:
- Leer el dataset_final_limpio.csv desde la raíz del proyecto (no desde /app)
- Entrenar 3 modelos:
    1) Decision Tree
    2) Logistic Regression
    3) KNN
- Guardarlos en /models como .joblib
- Usar SOLO estas variables (sin precipitación):
    - calle (categórica)
    - franja_horaria (categórica)
    - lluvia (categórica)
    - laborable (categórica)
    - temperatura (numérica)

Esto permite que la app sea coherente: si ya tenemos lluvia en categorías,
no forzamos al usuario a introducir litros/m² (evita incoherencias).

Ejecución (desde la raíz del proyecto):
    python -m app.train_models
"""

from pathlib import Path
import pandas as pd
import joblib

from sklearn.model_selection import train_test_split
from sklearn.pipeline import Pipeline
from sklearn.compose import ColumnTransformer
from sklearn.preprocessing import OneHotEncoder, StandardScaler
from sklearn.metrics import accuracy_score, classification_report

from sklearn.tree import DecisionTreeClassifier
from sklearn.linear_model import LogisticRegression
from sklearn.neighbors import KNeighborsClassifier


def main():
    # -------------------------------------------------
    # 1) Rutas del proyecto
    # -------------------------------------------------
    project_root = Path(__file__).resolve().parents[1]  # raíz del proyecto
    csv_path = project_root / "dataset_final_limpio.csv"
    models_dir = project_root / "models"
    models_dir.mkdir(exist_ok=True)

    if not csv_path.exists():
        raise FileNotFoundError(f"No se encuentra el CSV: {csv_path}")

    # -------------------------------------------------
    # 2) Cargar dataset
    # -------------------------------------------------
    df = pd.read_csv(csv_path)

    # -------------------------------------------------
    # 3) Selección de variables
    # -------------------------------------------------
    # Variables de entrada (sin 'litros_m2')
    features = ["calle", "hora", "lluvia", "laborab", "temperatura"]
    # Nota: en tu CSV real he visto columnas tipo:
    # - calle, hora, lluvia, laborab, temperatura, nivel_trafico...
    # Si tus nombres son diferentes, ajústalos aquí.

    # Intento de normalización de nombres (por si el CSV usa otras etiquetas)
    # Ajusta SOLO si tu CSV no coincide.
    rename_map = {}
    if "franja_horaria" in df.columns and "hora" not in df.columns:
        rename_map["franja_horaria"] = "hora"
    if "laborable" in df.columns and "laborab" not in df.columns:
        rename_map["laborable"] = "laborab"
    if rename_map:
        df = df.rename(columns=rename_map)

    # Recalcular features ya con nombres corregidos
    features = ["calle", "hora", "lluvia", "laborab", "temperatura"]

    target = "nivel_trafico"
    if target not in df.columns:
        raise ValueError(
            f"No existe la columna objetivo '{target}' en el CSV. "
            f"Columnas disponibles: {list(df.columns)}"
        )

    for col in features:
        if col not in df.columns:
            raise ValueError(
                f"No existe la columna '{col}' en el CSV. "
                f"Columnas disponibles: {list(df.columns)}"
            )

    X = df[features].copy()
    y = df[target].copy()

    # -------------------------------------------------
    # 4) Split train/test
    # -------------------------------------------------
    X_train, X_test, y_train, y_test = train_test_split(
        X, y, test_size=0.2, random_state=42, stratify=y
    )

    # -------------------------------------------------
    # 5) Preprocesado
    # -------------------------------------------------
    cat_cols = ["calle", "hora", "lluvia", "laborab"]
    num_cols = ["temperatura"]

    preprocessor = ColumnTransformer(
        transformers=[
            ("cat", OneHotEncoder(handle_unknown="ignore"), cat_cols),
            ("num", StandardScaler(), num_cols),
        ],
        remainder="drop"
    )

    # -------------------------------------------------
    # 6) Definición de modelos
    # -------------------------------------------------
    models = {
        "decision_tree": DecisionTreeClassifier(
            random_state=42,
            max_depth=6  # límite para que sea “mostrable” en el árbol
        ),
        "logistic_regression": LogisticRegression(
            max_iter=2000
        ),
        "knn": KNeighborsClassifier(
            n_neighbors=7
        ),
    }

    # -------------------------------------------------
    # 7) Entrenar y guardar
    # -------------------------------------------------
    for name, model in models.items():
        pipe = Pipeline(steps=[
            ("prep", preprocessor),
            ("model", model)
        ])

        pipe.fit(X_train, y_train)

        y_pred = pipe.predict(X_test)
        acc = accuracy_score(y_test, y_pred)

        print("\n" + "=" * 60)
        print(f"Modelo: {name}")
        print(f"Accuracy: {acc:.3f}")
        print(classification_report(y_test, y_pred))

        out_path = models_dir / f"{name}.joblib"
        joblib.dump(pipe, out_path)
        print(f"Guardado: {out_path}")

    print("\n✅ Modelos generados correctamente en /models")


if __name__ == "__main__":
    main()
