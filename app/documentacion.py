# app/documentacion.py
"""
Visor de documentación (PDF) DENTRO de la aplicación.

- Renderiza el PDF página a página como imágenes usando PyMuPDF (fitz).
- Se abre en una ventana maximizada.
- Incluye botón "Volver al inicio" que cierra esta ventana y vuelve a la principal.
"""

from pathlib import Path
import customtkinter as ctk

# PIL para manejar imágenes
from PIL import Image

# PyMuPDF para renderizar PDF
import fitz  # pip install pymupdf


def abrir_visor_documentacion(ventana_principal: ctk.CTk, pdf_path: Path):
    """
    Abre una ventana nueva con el PDF renderizado dentro de la app.
    Oculta la ventana principal mientras se visualiza la documentación.
    """

    # 1) Ocultamos la ventana principal (así luego vuelve sin reconstruirse)
    ventana_principal.withdraw()

    # 2) Creamos la ventana de documentación
    win = ctk.CTkToplevel()
    win.title("Documentación TrafiVision")

    # Ventana siempre maximizada (Windows)
    try:
        win.state("zoomed")
    except Exception:
        win.geometry("1200x700")

    # Paleta de colores (mismo estilo)
    BG = "#F7F7F7"
    RED = "#E30613"
    RED_DARK = "#B0040F"
    TEXT = "#111111"
    SUBTEXT = "#444444"
    BORDER = "#EDEDED"

    win.configure(fg_color=BG)
    win.grid_rowconfigure(2, weight=1)
    win.grid_columnconfigure(0, weight=1)

    # --- Header ---
    header = ctk.CTkFrame(win, fg_color=BG)
    header.grid(row=0, column=0, sticky="ew", padx=24, pady=(18, 8))
    header.grid_columnconfigure(0, weight=1)

    title = ctk.CTkLabel(
        header,
        text="Documentación del proyecto",
        text_color=TEXT,
        font=("Segoe UI", 28, "bold")
    )
    title.grid(row=0, column=0, sticky="w")

    subtitle = ctk.CTkLabel(
        header,
        text=f"Archivo: {pdf_path.name}",
        text_color=SUBTEXT,
        font=("Segoe UI", 14)
    )
    subtitle.grid(row=1, column=0, sticky="w", pady=(2, 0))

    sep = ctk.CTkFrame(win, fg_color="#EAEAEA", height=2)
    sep.grid(row=1, column=0, sticky="ew", padx=24, pady=(0, 12))

    # --- Contenedor scroll (para ver todas las páginas) ---
    # CTkScrollableFrame es lo más cómodo para "todas las páginas"
    scroll = ctk.CTkScrollableFrame(
        win,
        fg_color="#FFFFFF",
        corner_radius=18,
        border_width=1,
        border_color=BORDER
    )
    scroll.grid(row=2, column=0, sticky="nsew", padx=24, pady=(0, 18))
    scroll.grid_columnconfigure(0, weight=1)

    # Si el PDF no existe, mostramos mensaje y botón de volver
    if not pdf_path.exists():
        error_lbl = ctk.CTkLabel(
            scroll,
            text=f"No se encontró el PDF:\n{pdf_path}",
            text_color=TEXT,
            font=("Segoe UI", 16, "bold")
        )
        error_lbl.grid(row=0, column=0, sticky="w", padx=12, pady=12)

    else:
        # Renderizamos el PDF página por página
        doc = fitz.open(str(pdf_path))

        # Guardamos referencias de imágenes para que no se "pierdan" (muy importante en Tkinter)
        win._page_images = []

        for i in range(len(doc)):
            page = doc[i]

            # Escala: más alto => más nítido pero consume más
            zoom = 1.5
            mat = fitz.Matrix(zoom, zoom)
            pix = page.get_pixmap(matrix=mat, alpha=False)

            # Convertimos a PIL Image
            img = Image.frombytes("RGB", [pix.width, pix.height], pix.samples)

            # Pasamos a CTkImage
            ctk_img = ctk.CTkImage(light_image=img, dark_image=img, size=(pix.width, pix.height))
            win._page_images.append(ctk_img)

            page_label = ctk.CTkLabel(scroll, text="", image=ctk_img)
            page_label.grid(row=i, column=0, sticky="n", padx=10, pady=(8, 16))

        doc.close()

    # --- Footer con botón volver ---
    footer = ctk.CTkFrame(win, fg_color=BG)
    footer.grid(row=3, column=0, sticky="ew", padx=24, pady=(0, 18))
    footer.grid_columnconfigure(0, weight=1)

    def volver_al_inicio():
        """
        Cierra esta ventana y vuelve a mostrar la ventana principal maximizada.
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
        footer,
        text="Volver al inicio",
        height=54,
        corner_radius=18,
        fg_color=RED,
        hover_color=RED_DARK,
        text_color="white",
        font=("Segoe UI", 16, "bold"),
        command=volver_al_inicio
    )
    btn_volver.grid(row=0, column=0, sticky="e")
