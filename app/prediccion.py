# app/prediccion.py
"""
Ventana de predicción de tráfico (modelo ML real con Random Forest).

Qué hace:
- Se abre maximizada
- Oculta la ventana principal mientras está abierta
- Permite escoger modelo (Random Forest / Decision Tree / Logistic Regression / KNN)
- Predice y muestra probabilidades (%)
- "Ver árbol": muestra el árbol dentro de la app (solo si el modelo es Decision Tree)
- "Volver al inicio": cierra esta ventana y reabre la principal maximizada

Requisito:
Antes hay que entrenar modelos:
    python -m app.train_models
Esto crea /models/*.joblib en la raíz del proyecto.
"""

from pathlib import Path
import customtkinter as ctk
import pandas as pd
import joblib
from tkinter import messagebox

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
    # Rutas proyecto
    # -----------------------------
    project_root = Path(__file__).resolve().parents[1]
    models_dir = project_root / "models"
    csv_path = project_root / "dataset_final_limpio.csv"

    # -----------------------------
    # Modelos disponibles
    # -----------------------------
    model_options = {
        "Random Forest (recomendado)": "random_forest.joblib",
        "Decision Tree": "decision_tree.joblib",
        "Logistic Regression": "logistic_regression.joblib",
        "KNN": "knn.joblib",
    }

    # -----------------------------
    # Cargar calles reales desde el CSV (para que no falten y no haya combinaciones inválidas)
    # -----------------------------
    calles = [
        "Cibeles",
        "Callao – Gran Vía",
        "Cuatro Caminos",
        "Plaza Castilla (Norte)"
    ]
    filas = 0
    columnas = 0
    archivo_nombre = csv_path.name

    if csv_path.exists():
        try:
            df = pd.read_csv(csv_path)
            filas = len(df)
            columnas = len(df.columns)

            if "calle" in df.columns:
                # Quitamos NaNs, duplicados, ordenamos
                calles = sorted(df["calle"].dropna().astype(str).unique().tolist())
        except Exception:
            # Si el CSV falla, no rompemos la interfaz: usamos fallback
            pass

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

    info = ctk.CTkLabel(
        header,
        text=f"Archivo: {archivo_nombre}   |   Filas: {filas}   |   Columnas: {columnas}",
        text_color="#777777",
        font=("Segoe UI", 12)
    )
    info.grid(row=2, column=0, sticky="w", pady=(10, 0))

    sep = ctk.CTkFrame(win, fg_color="#EAEAEA", height=2)
    sep.grid(row=1, column=0, sticky="ew", padx=24, pady=(10, 12))

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
    # Helper: estilo OptionMenu (evitar azul y mantener rojo)
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
    # Inputs
    # -----------------------------
    ctk.CTkLabel(content, text="Modelo", text_color=TEXT, font=("Segoe UI", 16, "bold")).grid(
        row=0, column=0, sticky="w", pady=(0, 10)
    )
    model_menu = ctk.CTkOptionMenu(content, values=list(model_options.keys()), width=420)
    model_menu.set("Random Forest (recomendado)")
    option_style(model_menu)
    model_menu.grid(row=0, column=1, sticky="w", pady=(0, 10))

    ctk.CTkLabel(content, text="Calle", text_color=TEXT, font=("Segoe UI", 16, "bold")).grid(
        row=1, column=0, sticky="w", pady=10
    )
    calle_menu = ctk.CTkOptionMenu(content, values=calles, width=420)
    calle_menu.set(calles[0] if len(calles) > 0 else "Cibeles")
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

    # -----------------------------
    # Helpers: cargar modelo / predecir / ver árbol
    # -----------------------------
    def cargar_modelo_seleccionado():
        modelo_name = model_menu.get()
        fname = model_options[modelo_name]
        path = models_dir / fname

        if not path.exists():
            messagebox.showerror(
                "Modelo no encontrado",
                "No se encontró el archivo del modelo:\n"
                f"{path}\n\n"
                "Solución:\n"
                "1) Ejecuta en terminal:  python -m app.train_models\n"
                "2) Debe aparecer una carpeta /models con los .joblib\n"
            )
            return None

        return joblib.load(path)

    def predecir():
        pipe = cargar_modelo_seleccionado()
        if pipe is None:
            return

        try:
            temperatura = float(temp_entry.get().strip().replace(",", "."))
        except Exception:
            messagebox.showerror("Error", "Temperatura debe ser un número.")
            return

        # IMPORTANTE:
        # Estas columnas deben coincidir con train_models.py
        X_input = pd.DataFrame([{
            "calle": calle_menu.get(),
            "franja_horaria": franja_menu.get(),
            "laborable": dia_menu.get(),
            "lluvia_cat": lluvia_menu.get(),
            "temperatura": temperatura,
        }])

        try:
            pred = pipe.predict(X_input)[0]
        except Exception as e:
            messagebox.showerror(
                "Error al predecir",
                "Ha ocurrido un error al ejecutar el modelo.\n\n"
                f"Detalle:\n{e}\n\n"
                "Solución recomendada:\n"
                "1) Borra la carpeta /models\n"
                "2) Ejecuta: python -m app.train_models\n"
                "3) Vuelve a abrir la app\n"
            )
            return

        # Probabilidades
        prob_text = "Este modelo no devuelve probabilidades."
        if hasattr(pipe, "predict_proba"):
            try:
                probs = pipe.predict_proba(X_input)[0]
                classes = pipe.classes_
                pairs = list(zip(classes, probs))
                pairs.sort(key=lambda x: x[1], reverse=True)

                # Porcentajes redondeados, ajustando visualmente si suma 101 por redondeo
                perc = [int(round(p * 100)) for _, p in pairs]
                diff = sum(perc) - 100
                if diff != 0 and len(perc) > 0:
                    perc[0] -= diff  # corregimos el primero para que visualmente sume 100

                prob_text = " · ".join([f"{c} {p}%" for (c, _), p in zip(pairs, perc)])
            except Exception:
                prob_text = "Probabilidades no disponibles."

        result_label.configure(text=f"El tráfico en {calle_menu.get()} es: {pred}")
        prob_label.configure(text=f"Probabilidades: {prob_text}")

    def ver_arbol():
        if model_menu.get() != "Decision Tree":
            messagebox.showinfo(
                "Árbol no disponible",
                "El árbol solo se puede mostrar si el modelo seleccionado es Decision Tree."
            )
            return

        pipe = cargar_modelo_seleccionado()
        if pipe is None:
            return

        # Acceder al árbol interno del pipeline
        try:
            tree_model = pipe.named_steps["model"]
        except Exception:
            messagebox.showerror("Error", "No se pudo acceder al modelo interno.")
            return

        # Feature names tras OneHot (si se puede)
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
