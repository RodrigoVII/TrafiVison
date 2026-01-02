from pathlib import Path
import pandas as pd
import joblib

root = Path(__file__).resolve().parent
pipe = joblib.load(root / "models" / "random_forest.joblib")

X = pd.DataFrame([{
    "calle": "Cibeles",
    "franja_horaria": "Mañana",
    "laborable": "Laborable",
    "lluvia_cat": "No llueve",
    "temperatura": 20.0
}])

print("Pred:", pipe.predict(X))
if hasattr(pipe, "predict_proba"):
    print("Proba:", pipe.predict_proba(X))
    print("Classes:", pipe.classes_)
