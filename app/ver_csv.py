"""
app/ver_csv.py

Ventana de visualización del dataset CSV en formato tabla (tipo Excel).
- Uso de ttk.Treeview para mostrar filas y columnas.
- Solo lectura.
- Al abrir esta ventana se OCULTA la principal.
- Botón "Volver al inicio" para regresar a la ventana principal.
- Estilo alterno tipo "Universidad Europea": filas blancas y rojas (texto blanco en rojo).
- Muestra TODAS las filas del CSV (no solo un preview).
- La ventana se abre MAXIMIZADA.
"""

from pathlib import Path
import customtkinter as ctk
import pandas as pd
from tkinter import messagebox
from tkinter import ttk


def abrir_visor_csv(app_principal):
    """
    Abre la ventana del visor CSV, oculta la ventana principal
    y muestra el CSV completo en formato tabla.
    """

    # -----------------------------
    # 1) Localizar el CSV (desde la raíz del proyecto)
    # -----------------------------
    project_root = Path(__file__).resolve().parents[1]
    csv_path = project_root / "dataset_final_limpio.csv"

    # -----------------------------
    # 2) Cargar CSV
    # -----------------------------
    try:
        df = pd.read_csv(csv_path)
    except Exception as e:
        messagebox.showerror(
            "Error",
            f"No se pudo cargar el CSV:\n{csv_path}\n\nDetalle:\n{e}"
        )
        return

    # -----------------------------
    # 3) Ocultar ventana principal
    # -----------------------------
    app_principal.withdraw()

    # -----------------------------
    # 4) Crear ventana visor
    # -----------------------------
    visor = ctk.CTkToplevel(app_principal)
    visor.title("Dataset TrafiVision")
    visor.geometry("1200x650")
    visor.minsize(1000, 550)
    visor.configure(fg_color="#F7F7F7")

    # Abrir maximizada (Windows)
    try:
        visor.state("zoomed")
    except Exception:
        pass

    visor.lift()
    visor.focus_force()

    # -----------------------------
    # 5) Función volver al inicio
    # -----------------------------
    def volver_al_inicio():
        visor.destroy()
        app_principal.deiconify()

        # Por si acaso, la volvemos a maximizar también
        try:
            app_principal.state("zoomed")
        except Exception:
            pass

        app_principal.lift()
        app_principal.focus_force()

    # Si cierran con la X, también vuelve al inicio
    visor.protocol("WM_DELETE_WINDOW", volver_al_inicio)

    # -----------------------------
    # 6) Contenedor principal (estética)
    # -----------------------------
    container = ctk.CTkFrame(
        visor,
        fg_color="#FFFFFF",
        corner_radius=16
    )
    container.pack(fill="both", expand=True, padx=18, pady=18)

    # -----------------------------
    # 7) Título e info
    # -----------------------------
    titulo = ctk.CTkLabel(
        container,
        text="Vista previa del dataset",
        font=("Segoe UI", 22, "bold"),
        text_color="#111111"
    )
    titulo.pack(pady=(18, 6))

    info = ctk.CTkLabel(
        container,
        text=f"Archivo: {csv_path.name}   |   Filas: {len(df)}   |   Columnas: {len(df.columns)}",
        font=("Segoe UI", 13),
        text_color="#444444"
    )
    info.pack(pady=(0, 12))

    # -----------------------------
    # 8) Frame para la tabla
    # -----------------------------
    table_frame = ctk.CTkFrame(container, fg_color="#FFFFFF")
    table_frame.pack(fill="both", expand=True, padx=12, pady=(0, 12))

    # -----------------------------
    # 9) Estilos (tabla tipo Excel)
    # -----------------------------
    style = ttk.Style()
    style.theme_use("default")

    style.configure(
        "Treeview",
        background="#FFFFFF",
        foreground="#111111",
        rowheight=26,
        fieldbackground="#FFFFFF",
        font=("Segoe UI", 11)
    )

    style.configure(
        "Treeview.Heading",
        background="#F0F0F0",
        foreground="#111111",
        font=("Segoe UI", 11, "bold")
    )

    # Selección cuando pinchas una fila
    style.map(
        "Treeview",
        background=[("selected", "#111111")],
        foreground=[("selected", "#FFFFFF")]
    )

    # -----------------------------
    # 10) Crear Treeview
    # -----------------------------
    tree = ttk.Treeview(
        table_frame,
        columns=list(df.columns),
        show="headings"
    )

    # Scroll vertical
    scroll_y = ttk.Scrollbar(
        table_frame,
        orient="vertical",
        command=tree.yview
    )

    # Scroll horizontal
    scroll_x = ttk.Scrollbar(
        table_frame,
        orient="horizontal",
        command=tree.xview
    )

    tree.configure(
        yscrollcommand=scroll_y.set,
        xscrollcommand=scroll_x.set
    )

    tree.grid(row=0, column=0, sticky="nsew")
    scroll_y.grid(row=0, column=1, sticky="ns")
    scroll_x.grid(row=1, column=0, sticky="ew")

    table_frame.grid_rowconfigure(0, weight=1)
    table_frame.grid_columnconfigure(0, weight=1)

    # -----------------------------
    # 11) Configurar columnas
    # -----------------------------
    for col in df.columns:
        tree.heading(col, text=col)
        tree.column(col, anchor="w", width=140, stretch=True)

    # -----------------------------
    # 12) Tags para filas alternas (blanco/rojo UE)
    # -----------------------------
    UE_RED = "#E30613"

    tree.tag_configure("row_white", background="#FFFFFF", foreground="#111111")
    tree.tag_configure("row_red", background=UE_RED, foreground="#FFFFFF")

    # -----------------------------
    # 13) Insertar TODAS las filas (2950)
    # -----------------------------
    for i, (_, row) in enumerate(df.iterrows()):
        tag = "row_red" if i % 2 == 1 else "row_white"
        tree.insert("", "end", values=list(row), tags=(tag,))

    # -----------------------------
    # 14) Botón volver
    # -----------------------------
    footer = ctk.CTkFrame(container, fg_color="transparent")
    footer.pack(fill="x", padx=12, pady=(0, 16))

    btn_volver = ctk.CTkButton(
        footer,
        text="Volver al inicio",
        height=44,
        corner_radius=14,
        fg_color=UE_RED,
        hover_color="#B0040F",
        text_color="white",
        font=("Segoe UI", 14, "bold"),
        command=volver_al_inicio
    )
    btn_volver.pack(side="right")
