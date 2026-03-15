# app/estadisticas.py
"""
Ventana de estadísticas generales de TrafiVision.

Qué hace:
- Se abre maximizada
- Oculta la ventana principal mientras está abierta
- Consulta estadísticas desde la base de datos MariaDB
- Muestra tarjetas con métricas principales
- Permite volver al inicio

Fuente de datos:
- db/db_client.py -> get_stats()
"""

import customtkinter as ctk
from tkinter import messagebox

from db.db_client import get_stats


def abrir_ventana_estadisticas(ventana_principal: ctk.CTk):
    """
    Abre la ventana de estadísticas y oculta la ventana principal.
    """

    # ---------------------------------------------------------
    # Cargar estadísticas desde la base de datos
    # ---------------------------------------------------------
    try:
        stats = get_stats()
    except Exception as e:
        messagebox.showerror(
            "Error",
            f"No se pudieron cargar las estadísticas desde la base de datos.\n\nDetalle:\n{e}"
        )
        return

    # Ocultamos la ventana principal
    ventana_principal.withdraw()

    # ---------------------------------------------------------
    # Colores
    # ---------------------------------------------------------
    BG = "#F7F7F7"
    CARD = "#FFFFFF"
    TEXT = "#111111"
    SUBTEXT = "#444444"
    RED = "#E30613"
    RED_DARK = "#B0040F"
    BORDER = "#EDEDED"
    BLACK = "#111111"
    BLACK_HOVER = "#2A2A2A"

    # ---------------------------------------------------------
    # Crear ventana
    # ---------------------------------------------------------
    win = ctk.CTkToplevel(ventana_principal)
    win.title("Estadísticas TrafiVision")
    win.configure(fg_color=BG)

    try:
        win.state("zoomed")
    except Exception:
        win.geometry("1200x700")

    win.grid_rowconfigure(2, weight=1)
    win.grid_columnconfigure(0, weight=1)

    # ---------------------------------------------------------
    # Volver al inicio
    # ---------------------------------------------------------
    def volver():
        try:
            win.destroy()
        finally:
            ventana_principal.deiconify()
            try:
                ventana_principal.state("zoomed")
            except Exception:
                pass
            ventana_principal.lift()
            ventana_principal.focus_force()

    win.protocol("WM_DELETE_WINDOW", volver)

    # ---------------------------------------------------------
    # Header
    # ---------------------------------------------------------
    header = ctk.CTkFrame(win, fg_color=BG)
    header.grid(row=0, column=0, sticky="ew", padx=24, pady=(18, 8))
    header.grid_columnconfigure(0, weight=1)

    title = ctk.CTkLabel(
        header,
        text="Estadísticas generales",
        text_color=TEXT,
        font=("Segoe UI", 30, "bold")
    )
    title.grid(row=0, column=0, sticky="w")

    subtitle = ctk.CTkLabel(
        header,
        text="Resumen del estado actual de la base de datos TrafiVision.",
        text_color=SUBTEXT,
        font=("Segoe UI", 14)
    )
    subtitle.grid(row=1, column=0, sticky="w", pady=(4, 0))

    sep = ctk.CTkFrame(win, fg_color="#EAEAEA", height=2)
    sep.grid(row=1, column=0, sticky="ew", padx=24, pady=(10, 12))

    # ---------------------------------------------------------
    # Contenedor principal
    # ---------------------------------------------------------
    container = ctk.CTkFrame(
        win,
        fg_color=BG
    )
    container.grid(row=2, column=0, sticky="nsew", padx=24, pady=(0, 18))
    container.grid_columnconfigure((0, 1), weight=1)
    container.grid_rowconfigure((0, 1, 2), weight=1)

    # ---------------------------------------------------------
    # Helper para crear tarjetas
    # ---------------------------------------------------------
    def crear_tarjeta(parent, row, col, titulo, valor, color_acento=RED):
        card = ctk.CTkFrame(
            parent,
            fg_color=CARD,
            corner_radius=20,
            border_width=1,
            border_color=BORDER
        )
        card.grid(row=row, column=col, padx=12, pady=12, sticky="nsew")

        card.grid_columnconfigure(0, weight=1)

        accent = ctk.CTkFrame(card, fg_color=color_acento, height=6, corner_radius=12)
        accent.grid(row=0, column=0, sticky="ew", padx=18, pady=(18, 14))

        label_titulo = ctk.CTkLabel(
            card,
            text=titulo,
            text_color=SUBTEXT,
            font=("Segoe UI", 16, "bold")
        )
        label_titulo.grid(row=1, column=0, sticky="w", padx=18, pady=(0, 8))

        label_valor = ctk.CTkLabel(
            card,
            text=str(valor),
            text_color=TEXT,
            font=("Segoe UI", 34, "bold")
        )
        label_valor.grid(row=2, column=0, sticky="w", padx=18, pady=(0, 18))

        return card

    # ---------------------------------------------------------
    # Tarjetas estadísticas
    # ---------------------------------------------------------
    crear_tarjeta(container, 0, 0, "Cámaras", stats.get("camaras", 0), RED)
    crear_tarjeta(container, 0, 1, "Capturas", stats.get("capturas", 0), BLACK)
    crear_tarjeta(container, 1, 0, "Registros meteo", stats.get("meteo", 0), RED)
    crear_tarjeta(container, 1, 1, "Detecciones", stats.get("detecciones", 0), BLACK)
    crear_tarjeta(container, 2, 0, "Registros de tráfico", stats.get("trafico", 0), RED)

    # Tarjeta resumen / mensaje
    resumen = ctk.CTkFrame(
        container,
        fg_color=CARD,
        corner_radius=20,
        border_width=1,
        border_color=BORDER
    )
    resumen.grid(row=2, column=1, padx=12, pady=12, sticky="nsew")

    resumen.grid_columnconfigure(0, weight=1)

    accent2 = ctk.CTkFrame(resumen, fg_color=BLACK, height=6, corner_radius=12)
    accent2.grid(row=0, column=0, sticky="ew", padx=18, pady=(18, 14))

    resumen_title = ctk.CTkLabel(
        resumen,
        text="Resumen",
        text_color=SUBTEXT,
        font=("Segoe UI", 16, "bold")
    )
    resumen_title.grid(row=1, column=0, sticky="w", padx=18, pady=(0, 8))

    resumen_text = ctk.CTkLabel(
        resumen,
        text=(
            "La base de datos TrafiVision contiene la información histórica "
            "del sistema y sirve como fuente para la API, la visualización "
            "de datos y los modelos de predicción."
        ),
        text_color=TEXT,
        font=("Segoe UI", 15),
        justify="left",
        wraplength=420
    )
    resumen_text.grid(row=2, column=0, sticky="w", padx=18, pady=(0, 18))

    # ---------------------------------------------------------
    # Footer con botón volver
    # ---------------------------------------------------------
    footer = ctk.CTkFrame(win, fg_color="transparent")
    footer.grid(row=3, column=0, sticky="ew", padx=24, pady=(0, 18))

    btn_volver = ctk.CTkButton(
        footer,
        text="Volver al inicio",
        height=48,
        corner_radius=16,
        fg_color=RED,
        hover_color=RED_DARK,
        text_color="white",
        font=("Segoe UI", 15, "bold"),
        command=volver
    )
    btn_volver.pack(side="right")