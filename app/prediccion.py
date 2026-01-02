# app/prediccion.py
"""
Ventana de predicción de tráfico (modelo real).

- Se abre maximizada
- Oculta la ventana principal mientras está abierta
- Permite escoger modelo (Decision Tree / Logistic Regression / KNN)
- Calcula una predicción y muestra probabilidades (%)
- Botón "Ver árbol": muestra el árbol dentro de la app (solo si el modelo es Decision Tree)
- Botón "Volver al inicio": cierra esta ventana y reabre la principal maximizada

ARREGLOS IMPORTANTES DE ESTA VERSIÓN:
1) Evita el error: ufunc 'isnan' not supported
   -> Forzamos tipos numéricos (float) y categóricos (str) según el pipeline REAL.
2) Evita el error: ['laborab'] not in index
   -> Si el modelo espera 'laborab', la creamos.
3) Quitamos el input de "precipitación" (litros_m2) del formulario,
   pero SI el modelo lo necesita, lo calculamos automáticamente desde "Lluvia".
"""

from pathlib import Path
import customtkinter as ctk
from tkinter import messagebox

import joblib
import pandas as pd

# Para mostrar árbol dentro de la app
import matplotlib.pyplot as plt
from sklearn.tree import plot_tree


def abrir_ventana_prediccion(ventana_principal: ctk.CTk):
    """
    Abre la ventana de predicción y oculta la ventana principal.
    """
    ventana_principal.withdraw()

    win = ctk.CTkToplevel()
    win.title("Predicción TrafiVision")

    # Maximizada (Windows)
    try:
        win.state("zoomed")
    except Exception:
        win.geometry("1200x700")

    # -----------------------------
    # Colores (mismo estilo TrafiVision)
    # -----------------------------
    BG = "#F7F7F7"
    RED = "#E30613"
    RED_DARK = "#B0040F"
    BLACK = "#111111"
    BLACK_HOVER = "#2A2A2A"
    TEXT = "#111111"
    SUBTEXT = "#444444"
    CARD = "#FFFFFF"
    BORDER = "#EDEDED"
    SOFT = "#F6F6F6"
    SOFT_HOVER = "#EEEEEE"

    win.configure(fg_color=BG)
    win.grid_rowconfigure(2, weight=1)
    win.grid_columnconfigure(0, weight=1)

    # -----------------------------
    # Rutas del proyecto
    # -----------------------------
    project_root = Path(__file__).resolve().parents[1]  # raíz del repo/proyecto
    models_dir = project_root / "models"
    csv_path = project_root / "dataset_final_limpio.csv"

    # -----------------------------
    # Modelos (texto UI -> nombre fichero)
    # -----------------------------
    model_options = {
        "Decision Tree (recomendado)": "decision_tree.joblib",
        "Logistic Regression": "logistic_regression.joblib",
        "KNN": "knn.joblib"
    }

    # -----------------------------
    # Cargar CSV base (solo para defaults)
    # -----------------------------
    df_base = None
    if csv_path.exists():
        try:
            df_base = pd.read_csv(csv_path)
        except Exception:
            df_base = None

    # -----------------------------
    # Header
    # -----------------------------
    header = ctk.CTkFrame(win, fg_color=BG)
    header.grid(row=0, column=0, sticky="ew", padx=24, pady=(18, 8))
    header.grid_columnconfigure(0, weight=1)

    title = ctk.CTkLabel(
        header,
        text="Predicción de tráfico",
        text_color=TEXT,
        font=("Segoe UI", 30, "bold")
    )
    title.grid(row=0, column=0, sticky="w")

    subtitle = ctk.CTkLabel(
        header,
        text="Selecciona condiciones y obtén el nivel de tráfico con porcentajes.",
        text_color=SUBTEXT,
        font=("Segoe UI", 14)
    )
    subtitle.grid(row=1, column=0, sticky="w", pady=(4, 0))

    dataset_info_text = f"Archivo: {csv_path.name} (no encontrado)"
    if df_base is not None:
        dataset_info_text = (
            f"Archivo: {csv_path.name}   |   Filas: {len(df_base)}   |   Columnas: {len(df_base.columns)}"
        )

    dataset_info = ctk.CTkLabel(
        header,
        text=dataset_info_text,
        text_color="#666666",
        font=("Segoe UI", 12)
    )
    dataset_info.grid(row=2, column=0, sticky="w", pady=(6, 0))

    sep = ctk.CTkFrame(win, fg_color="#EAEAEA", height=2)
    sep.grid(row=1, column=0, sticky="ew", padx=24, pady=(0, 12))

    # -----------------------------
    # Card principal
    # -----------------------------
    card = ctk.CTkFrame(
        win,
        fg_color=CARD,
        corner_radius=22,
        border_width=1,
        border_color=BORDER
    )
    card.grid(row=2, column=0, sticky="n", padx=24, pady=(0, 18))
    card.grid_columnconfigure(0, weight=1)

    content = ctk.CTkFrame(card, fg_color=CARD)
    content.grid(row=0, column=0, padx=36, pady=28, sticky="nsew")
    content.grid_columnconfigure(1, weight=1)

    # -----------------------------
    # Helper estilo OptionMenu (sin azul)
    # -----------------------------
    def option_style(menu: ctk.CTkOptionMenu):
        menu.configure(
            fg_color=SOFT,
            button_color=RED,
            button_hover_color=RED_DARK,
            dropdown_fg_color=CARD,
            dropdown_hover_color=SOFT_HOVER,
            text_color=TEXT
        )

    # -----------------------------
    # Inputs (MISMO DISEÑO)
    # -----------------------------
    ctk.CTkLabel(content, text="Modelo", text_color=TEXT, font=("Segoe UI", 16, "bold")).grid(
        row=0, column=0, sticky="w", pady=(0, 10)
    )
    model_menu = ctk.CTkOptionMenu(content, values=list(model_options.keys()), width=420)
    model_menu.set("Decision Tree (recomendado)")
    option_style(model_menu)
    model_menu.grid(row=0, column=1, sticky="w", pady=(0, 10))

    ctk.CTkLabel(content, text="Calle", text_color=TEXT, font=("Segoe UI", 16, "bold")).grid(
        row=1, column=0, sticky="w", pady=10
    )
    calle_menu = ctk.CTkOptionMenu(
        content,
        values=[
            "Calle Princesa – Serrano Jover",
            "Alonso Martínez",
            "Cibeles",
            "Calle Alcalá – Velázquez",
            "Paseo de la Castellana – Santiago Delgado",
            "Avenida de América – Francisco Silvela",
            "Callao – Gran Vía",
            "Cuatro Caminos",
            "Paseo del Prado – Huertas",
            "Plaza Castilla (Norte)"
        ],
        width=420
    )
    calle_menu.set("Cibeles")
    option_style(calle_menu)
    calle_menu.grid(row=1, column=1, sticky="w", pady=10)

    ctk.CTkLabel(content, text="Franja horaria", text_color=TEXT, font=("Segoe UI", 16, "bold")).grid(
        row=2, column=0, sticky="w", pady=10
    )
    franja_menu = ctk.CTkOptionMenu(content, values=["Madrugada", "Mañana", "Mediodía", "Tarde", "Noche"], width=420)
    franja_menu.set("Tarde")
    option_style(franja_menu)
    franja_menu.grid(row=2, column=1, sticky="w", pady=10)

    ctk.CTkLabel(content, text="Tipo de día", text_color=TEXT, font=("Segoe UI", 16, "bold")).grid(
        row=3, column=0, sticky="w", pady=10
    )
    dia_menu = ctk.CTkOptionMenu(content, values=["Laborable", "No laborable"], width=420)
    dia_menu.set("Laborable")
    option_style(dia_menu)
    dia_menu.grid(row=3, column=1, sticky="w", pady=10)

    ctk.CTkLabel(content, text="Lluvia", text_color=TEXT, font=("Segoe UI", 16, "bold")).grid(
        row=4, column=0, sticky="w", pady=10
    )
    lluvia_menu = ctk.CTkOptionMenu(content, values=["No llueve", "Lluvia débil", "Lluvia intensa"], width=420)
    lluvia_menu.set("No llueve")
    option_style(lluvia_menu)
    lluvia_menu.grid(row=4, column=1, sticky="w", pady=10)

    ctk.CTkLabel(content, text="Temperatura (°C)", text_color=TEXT, font=("Segoe UI", 16, "bold")).grid(
        row=5, column=0, sticky="w", pady=10
    )
    temp_entry = ctk.CTkEntry(content, width=420)
    temp_entry.insert(0, "20")
    temp_entry.grid(row=5, column=1, sticky="w", pady=10)

    # -----------------------------
    # Resultado
    # -----------------------------
    result_box = ctk.CTkFrame(content, fg_color="#FFF5F6", corner_radius=14)
    result_box.grid(row=6, column=0, columnspan=2, sticky="ew", pady=(18, 10))
    result_box.grid_columnconfigure(0, weight=1)

    result_label = ctk.CTkLabel(
        result_box,
        text="Resultado: (aún no calculado)",
        text_color=TEXT,
        font=("Segoe UI", 18, "bold")
    )
    result_label.grid(row=0, column=0, sticky="w", padx=14, pady=(10, 4))

    prob_label = ctk.CTkLabel(
        result_box,
        text="Probabilidades: -",
        text_color=SUBTEXT,
        font=("Segoe UI", 14)
    )
    prob_label.grid(row=1, column=0, sticky="w", padx=14, pady=(0, 10))

    # ============================================================
    #  LÓGICA DE PREDICCIÓN (ARREGLO DE TIPOS + COLUMNAS ESPERADAS)
    # ============================================================

    def cargar_modelo_seleccionado():
        """
        Carga el pipeline entrenado (joblib) según el modelo escogido.
        """
        modelo_name = model_menu.get()
        fname = model_options[modelo_name]
        path = models_dir / fname

        if not path.exists():
            messagebox.showerror(
                "Modelo no encontrado",
                "No se encontró el archivo del modelo:\n"
                f"{path}\n\n"
                "Solución:\n"
                "1) Ejecuta:  python -m app.train_models\n"
                "2) Debe aparecer la carpeta /models con los .joblib"
            )
            return None

        return joblib.load(path)

    def _safe_float(x, default=0.0) -> float:
        """
        Convierte a float de forma robusta.
        Si llega texto raro o vacío, devuelve default.
        """
        try:
            if x is None:
                return default
            s = str(x).strip().replace(",", ".")
            if s == "":
                return default
            return float(s)
        except Exception:
            return default

    def _franja_a_hora_num(franja: str) -> int:
        """
        Convierte franja horaria a una hora aproximada (0-23),
        por si el modelo entrenó con columna 'hora'.
        """
        mapa = {
            "Madrugada": 3,
            "Mañana": 9,
            "Mediodía": 14,
            "Tarde": 18,
            "Noche": 22
        }
        return mapa.get(franja, 12)

    def _lluvia_a_litros(lluvia: str) -> float:
        """
        Sustituto de "precipitación" (litros_m2) ahora que quitamos el campo.
        - No llueve   -> 0.0
        - Lluvia débil -> 0.5
        - Lluvia intensa -> 2.0
        """
        mapa = {
            "No llueve": 0.0,
            "Lluvia débil": 0.5,
            "Lluvia intensa": 2.0
        }
        return mapa.get(lluvia, 0.0)

    def _obtener_columnas_prep(pipe):
        """
        Saca del ColumnTransformer del pipeline cuáles son columnas numéricas y categóricas.
        Esto es CLAVE para evitar el error de isnan (tipos mezclados).
        """
        num_cols = []
        cat_cols = []

        try:
            prep = pipe.named_steps["prep"]
        except Exception:
            return num_cols, cat_cols

        try:
            for name, transformer, cols in prep.transformers_:
                # Ojo: a veces hay un 'remainder'
                if name == "num":
                    num_cols = list(cols)
                elif name == "cat":
                    cat_cols = list(cols)
        except Exception:
            pass

        return num_cols, cat_cols

    def _crear_input_modelo(pipe) -> pd.DataFrame:
        """
        Construye un DataFrame de 1 fila con EXACTAMENTE las columnas que el modelo espera,
        y con tipos correctos (numéricas como float, categóricas como str).
        """
        if df_base is None or len(df_base) == 0:
            raise FileNotFoundError(
                "No se ha podido cargar dataset_final_limpio.csv. "
                "Es necesario para construir el input correctamente."
            )

        # Columnas esperadas por el modelo (las del entrenamiento)
        try:
            expected_cols = list(pipe.feature_names_in_)
        except Exception:
            # fallback: columnas del csv
            expected_cols = list(df_base.columns)

        # Qué columnas son num y cuáles cat según el pipeline
        num_cols, cat_cols = _obtener_columnas_prep(pipe)

        # Fila base: valores neutros por defecto
        base_row = df_base.iloc[0]

        # Valores desde la UI
        calle_val = calle_menu.get()
        franja_val = franja_menu.get()
        lluvia_val = lluvia_menu.get()
        dia_val = dia_menu.get()
        temp_val = _safe_float(temp_entry.get(), default=20.0)

        # Derivados
        laborab_val = 1 if dia_val == "Laborable" else 0
        hora_val = _franja_a_hora_num(franja_val)
        litros_val = _lluvia_a_litros(lluvia_val)

        data = {}
        for col in expected_cols:
            # default desde CSV si existe esa columna
            default_value = base_row[col] if col in base_row.index else None

            # -----------------------------
            # MAPEOS IMPORTANTES
            # -----------------------------
            if col == "calle":
                data[col] = str(calle_val)

            elif col == "franja_horaria":
                data[col] = str(franja_val)

            elif col == "lluvia":
                data[col] = str(lluvia_val)

            elif col == "laborab":
                # Algunos entrenamientos usan laborab como 0/1
                data[col] = float(laborab_val)

            elif col == "laborable":
                # Otros entrenamientos usan laborable como 0/1
                data[col] = float(laborab_val)

            elif col == "hora":
                # Si el modelo espera una hora numérica
                data[col] = float(hora_val)

            elif col == "temperatura":
                data[col] = float(temp_val)

            elif col == "litros_m2":
                # Si el modelo lo espera, lo calculamos desde "Lluvia"
                data[col] = float(litros_val)

            else:
                # -----------------------------
                # RESTO DE COLUMNAS: forzamos tipo según pipeline
                # -----------------------------
                if col in num_cols:
                    data[col] = _safe_float(default_value, default=0.0)

                elif col in cat_cols:
                    # Para categóricas, siempre string (esto evita isnan con objetos raros)
                    data[col] = "" if default_value is None else str(default_value)

                else:
                    # Si no está clasificada, elegimos según dtype del CSV si existe
                    if col in df_base.columns:
                        if pd.api.types.is_numeric_dtype(df_base[col]):
                            data[col] = _safe_float(default_value, default=0.0)
                        else:
                            data[col] = "" if default_value is None else str(default_value)
                    else:
                        # Último fallback
                        data[col] = _safe_float(default_value, default=0.0)

        # Construimos el DataFrame final en el orden exacto
        X_df = pd.DataFrame([data], columns=expected_cols)

        # Extra seguridad: asegura numéricas en columnas que el pipeline diga numéricas
        for col in num_cols:
            if col in X_df.columns:
                X_df[col] = pd.to_numeric(X_df[col], errors="coerce").fillna(0.0)

        # Extra seguridad: categóricas como str
        for col in cat_cols:
            if col in X_df.columns:
                X_df[col] = X_df[col].astype(str)

        return X_df

    def predecir():
        """
        Ejecuta la predicción del pipeline y actualiza el resultado en pantalla.
        """
        pipe = cargar_modelo_seleccionado()
        if pipe is None:
            return

        try:
            X_df = _crear_input_modelo(pipe)
            pred = pipe.predict(X_df)[0]

            # Probabilidades si el modelo las soporta
            if hasattr(pipe, "predict_proba"):
                probs = pipe.predict_proba(X_df)[0]
                classes = pipe.classes_
                pairs = list(zip(classes, probs))
                pairs.sort(key=lambda x: x[1], reverse=True)
                prob_text = " · ".join([f"{c} {p*100:.0f}%" for c, p in pairs])
            else:
                prob_text = "Este modelo no devuelve probabilidades."

            result_label.configure(text=f"El tráfico en {calle_menu.get()} es: {pred}")
            prob_label.configure(text=f"Probabilidades: {prob_text}")

        except Exception as e:
            messagebox.showerror(
                "Error al predecir",
                "Ha ocurrido un error al ejecutar el modelo.\n\n"
                f"Detalle:\n{e}\n\n"
                "Recomendación:\n"
                "1) Ejecuta: python -m app.train_models\n"
                "2) Vuelve a abrir la app"
            )

    def ver_arbol():
        """
        Muestra el árbol en una ventana interna (solo para Decision Tree).
        """
        if model_menu.get() != "Decision Tree (recomendado)":
            messagebox.showinfo(
                "Árbol no disponible",
                "El árbol solo se puede mostrar si el modelo seleccionado es Decision Tree."
            )
            return

        pipe = cargar_modelo_seleccionado()
        if pipe is None:
            return

        # Accedemos al árbol interno del pipeline
        try:
            tree_model = pipe.named_steps["model"]
        except Exception:
            messagebox.showerror("Error", "No se pudo acceder al modelo interno.")
            return

        # Nombres tras OneHot (si se puede)
        try:
            prep = pipe.named_steps["prep"]
            feature_names = prep.get_feature_names_out()
        except Exception:
            feature_names = None

        tree_win = ctk.CTkToplevel(win)
        tree_win.title("Árbol de decisión - TrafiVision")
        try:
            tree_win.state("zoomed")
        except Exception:
            tree_win.geometry("1200x700")

        tree_win.configure(fg_color=BG)
        tree_win.grid_rowconfigure(1, weight=1)
        tree_win.grid_columnconfigure(0, weight=1)

        top = ctk.CTkFrame(tree_win, fg_color=BG)
        top.grid(row=0, column=0, sticky="ew", padx=24, pady=(18, 10))
        top.grid_columnconfigure(0, weight=1)

        ctk.CTkLabel(
            top,
            text="Árbol de decisión (modelo seleccionado)",
            text_color=TEXT,
            font=("Segoe UI", 24, "bold")
        ).grid(row=0, column=0, sticky="w")

        scroll = ctk.CTkScrollableFrame(
            tree_win,
            fg_color=CARD,
            corner_radius=18,
            border_width=1,
            border_color=BORDER
        )
        scroll.grid(row=1, column=0, sticky="nsew", padx=24, pady=(0, 18))
        scroll.grid_columnconfigure(0, weight=1)

        # Render del árbol con matplotlib -> imagen
        import io
        from PIL import Image

        fig = plt.figure(figsize=(24, 12), dpi=120)
        ax = fig.add_subplot(111)

        plot_tree(
            tree_model,
            feature_names=feature_names,
            class_names=list(pipe.classes_),
            filled=True,
            rounded=True,
            fontsize=7,
            ax=ax
        )
        fig.tight_layout()

        buf = io.BytesIO()
        fig.savefig(buf, format="png")
        plt.close(fig)
        buf.seek(0)

        pil_img = Image.open(buf).convert("RGBA")
        ctk_img = ctk.CTkImage(light_image=pil_img, dark_image=pil_img, size=(pil_img.width, pil_img.height))

        tree_label = ctk.CTkLabel(scroll, text="", image=ctk_img)
        tree_label.grid(row=0, column=0, sticky="n", padx=12, pady=12)
        tree_label.image = ctk_img

    # -----------------------------
    # Botonera
    # -----------------------------
    btns = ctk.CTkFrame(content, fg_color=CARD)
    btns.grid(row=7, column=0, columnspan=2, sticky="ew", pady=(10, 6))
    btns.grid_columnconfigure((0, 1, 2), weight=1)

    btn_predecir = ctk.CTkButton(
        btns,
        text="Predecir",
        height=50,
        corner_radius=16,
        fg_color=RED,
        hover_color=RED_DARK,
        font=("Segoe UI", 16, "bold"),
        command=predecir
    )
    btn_predecir.grid(row=0, column=0, sticky="ew", padx=(0, 10))

    btn_arbol = ctk.CTkButton(
        btns,
        text="Ver árbol",
        height=50,
        corner_radius=16,
        fg_color=BLACK,
        hover_color=BLACK_HOVER,
        font=("Segoe UI", 16, "bold"),
        command=ver_arbol
    )
    btn_arbol.grid(row=0, column=1, sticky="ew", padx=(10, 10))

    def volver():
        """
        Cierra esta ventana y vuelve a la principal (maximizada).
        """
        try:
            win.destroy()
        finally:
            ventana_principal.deiconify()
            try:
                ventana_principal.state("zoomed")
            except Exception:
                pass

    btn_volver = ctk.CTkButton(
        btns,
        text="Volver al inicio",
        height=50,
        corner_radius=16,
        fg_color=CARD,
        hover_color=SOFT_HOVER,
        border_width=2,
        border_color=RED,
        text_color=RED,
        font=("Segoe UI", 16, "bold"),
        command=volver
    )
    btn_volver.grid(row=0, column=2, sticky="ew", padx=(10, 0))
