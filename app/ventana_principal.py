# app/ventana_principal.py
from pathlib import Path
import customtkinter as ctk

# Importo las ventanas secundarias (CSV y Documentación)
# (Así la lógica queda modular y el main solo llama a funciones)
from app.ver_csv import abrir_visor_csv
from app.documentacion import abrir_visor_documentacion

from app.prediccion import abrir_ventana_prediccion

try:
    from PIL import Image
    PIL_OK = True
except Exception:
    PIL_OK = False


class VentanaPrincipal(ctk.CTk):
    def __init__(self):
        super().__init__()

        # -----------------------------
        # Ventana (tamaños base)
        # -----------------------------
        self.BASE_W, self.BASE_H = 1200, 700
        self.MIN_W, self.MIN_H = 980, 600

        self.title("TrafiVision")
        self.geometry(f"{self.BASE_W}x{self.BASE_H}")
        self.minsize(self.MIN_W, self.MIN_H)

        # Arrancar maximizada (Windows)
        try:
            self.state("zoomed")
        except Exception:
            pass

        # Fondo
        self.BG = "#F7F7F7"
        self.configure(fg_color=self.BG)

        # Colores (paleta UE)
        self.COLOR_RED = "#E30613"
        self.COLOR_RED_DARK = "#B0040F"
        self.COLOR_TEXT = "#111111"
        self.COLOR_SUBTEXT = "#444444"
        self.COLOR_BLACK = "#111111"
        self.COLOR_BLACK_HOVER = "#2A2A2A"
        self.COLOR_SOFT = "#F6F6F6"
        self.COLOR_SOFT_HOVER = "#EEEEEE"
        self.BORDER_SOFT = "#EDEDED"
        self.SEPARATOR = "#EAEAEA"

        # -----------------------------
        # Fuentes
        # -----------------------------
        self.TITLE_FONTS = [
            "Franklin Gothic Demi",
            "Agency FB",
            "Bahnschrift SemiBold",
            "Segoe UI Semibold"
        ]
        self.FONT_BODY = "Segoe UI"

        # -----------------------------
        # Paths (assets y doc)
        # -----------------------------
        self.app_dir = Path(__file__).resolve().parent
        self.project_root = self.app_dir.parent

        # Logo UE en app/logo.png
        self.logo_path = self.app_dir / "logo.png"

        # Semáforo opcional
        self.semaforo_path = self.app_dir / "semaforo.png"

        # PDF documentación (ahora el 80%, luego lo cambias a la completa)
        self.doc_pdf = self.project_root / "MemoriaTrafiVision 80%.docx.pdf"

        # Estado responsive
        self._resize_job = None
        self._lock_resize = False

        # -----------------------------
        # Layout principal
        # -----------------------------
        self.grid_rowconfigure(0, weight=1)
        self.grid_columnconfigure(0, weight=1)

        self.root = ctk.CTkFrame(self, fg_color=self.BG)
        self.root.grid(row=0, column=0, sticky="nsew")
        self.root.grid_rowconfigure(2, weight=1)
        self.root.grid_columnconfigure(0, weight=1)

        # -----------------------------
        # Topbar (logo UE arriba izquierda)
        # -----------------------------
        self.topbar = ctk.CTkFrame(self.root, fg_color=self.BG)
        self.topbar.grid(row=0, column=0, sticky="ew", padx=18, pady=(14, 0))
        self.topbar.grid_columnconfigure(0, weight=1)

        self.logo_ue_label = ctk.CTkLabel(self.topbar, text="")
        self.logo_ue_label.grid(row=0, column=0, sticky="w")
        self._load_image_once(self.logo_ue_label, self.logo_path, target_height=80)

        # Separador
        self.separator = ctk.CTkFrame(self.root, fg_color=self.SEPARATOR, height=2)
        self.separator.grid(row=1, column=0, sticky="ew", padx=18, pady=(10, 0))

        # -----------------------------
        # Centro (card)
        # -----------------------------
        self.center = ctk.CTkFrame(self.root, fg_color=self.BG)
        self.center.grid(row=2, column=0, sticky="nsew")
        self.center.grid_rowconfigure(0, weight=1)
        self.center.grid_columnconfigure(0, weight=1)

        self.card = ctk.CTkFrame(
            self.center,
            fg_color="#FFFFFF",
            corner_radius=26,
            border_width=1,
            border_color=self.BORDER_SOFT
        )
        # centrado
        self.card.grid(row=0, column=0, sticky="n", padx=40, pady=(55, 0))
        self.card.grid_columnconfigure(0, weight=1)

        self.content = ctk.CTkFrame(self.card, fg_color="#FFFFFF")
        self.content.grid(row=0, column=0, padx=72, pady=56)
        self.content.grid_columnconfigure(0, weight=1)

        # Construyo UI
        self._build_ui()

        # Responsive
        self.bind("<Configure>", self._on_resize)
        self.after(50, self._apply_layout)

    # -----------------------------
    # UI
    # -----------------------------
    def _build_ui(self):
        # Badge superior
        self.badge = ctk.CTkLabel(
            self.content,
            text="MOBILITY AI  ·  SMART CITY  ·  TRAFFIC ANALYTICS",
            text_color=self.COLOR_RED,
            font=(self.FONT_BODY, 14, "bold")
        )
        self.badge.grid(row=0, column=0, sticky="w", pady=(0, 10))

        # Barra roja decorativa
        self.accent = ctk.CTkFrame(self.content, fg_color=self.COLOR_RED, height=6, corner_radius=10)
        self.accent.grid(row=1, column=0, sticky="ew", pady=(0, 22))

        # Header: semáforo + título
        self.header = ctk.CTkFrame(self.content, fg_color="#FFFFFF")
        self.header.grid(row=2, column=0, sticky="ew", pady=(0, 10))
        self.header.grid_columnconfigure(1, weight=1)

        self.semaforo_label = ctk.CTkLabel(self.header, text="")
        self.semaforo_label.grid(row=0, column=0, padx=(0, 16))

        self.title_label = ctk.CTkLabel(
            self.header,
            text="TrafiVision",
            text_color=self.COLOR_TEXT,
            font=(self.TITLE_FONTS[-1], 58)
        )
        self.title_label.grid(row=0, column=1, sticky="w")

        # Subtítulo
        self.subtitle_label = ctk.CTkLabel(
            self.content,
            text="Análisis y predicción del tráfico urbano",
            text_color=self.COLOR_SUBTEXT,
            font=(self.FONT_BODY, 24)
        )
        self.subtitle_label.grid(row=3, column=0, sticky="w", pady=(0, 26))

        # Botonera
        self.btn_frame = ctk.CTkFrame(self.content, fg_color="#FFFFFF")
        self.btn_frame.grid(row=4, column=0, sticky="ew")
        self.btn_frame.grid_columnconfigure(0, weight=1)

        self.btn_csv = ctk.CTkButton(
            self.btn_frame,
            text="Ver CSV",
            height=70,
            corner_radius=20,
            fg_color=self.COLOR_RED,
            hover_color=self.COLOR_RED_DARK,
            text_color="white",
            font=(self.FONT_BODY, 18, "bold"),
            command=self.ver_csv
        )
        self.btn_csv.grid(row=0, column=0, sticky="ew", pady=(0, 16))

        self.btn_pred = ctk.CTkButton(
            self.btn_frame,
            text="Predicción",
            height=70,
            corner_radius=20,
            fg_color=self.COLOR_BLACK,
            hover_color=self.COLOR_BLACK_HOVER,
            text_color="white",
            font=(self.FONT_BODY, 18, "bold"),
            command=self.prediccion
        )
        self.btn_pred.grid(row=1, column=0, sticky="ew", pady=(0, 16))

        self.btn_doc = ctk.CTkButton(
            self.btn_frame,
            text="Documentación",
            height=70,
            corner_radius=20,
            fg_color=self.COLOR_SOFT,
            hover_color=self.COLOR_SOFT_HOVER,
            border_width=2,
            border_color=self.COLOR_RED,
            text_color=self.COLOR_RED,
            font=(self.FONT_BODY, 18, "bold"),
            command=self.abrir_documentacion
        )
        self.btn_doc.grid(row=2, column=0, sticky="ew", pady=(0, 16))

        # ✅ Botón SALIR (nuevo)
        self.btn_salir = ctk.CTkButton(
            self.btn_frame,
            text="Salir",
            height=58,
            corner_radius=18,
            fg_color="#FFFFFF",
            hover_color="#F3F3F3",
            border_width=2,
            border_color="#111111",
            text_color="#111111",
            font=(self.FONT_BODY, 16, "bold"),
            command=self.salir
        )
        self.btn_salir.grid(row=3, column=0, sticky="ew")

        # Footer
        self.footer = ctk.CTkLabel(
            self.content,
            text="Universidad Europea · TrafiVision",
            text_color="#777777",
            font=(self.FONT_BODY, 13)
        )
        self.footer.grid(row=5, column=0, sticky="w", pady=(22, 0))

        # Semáforo: si no hay imagen, uso emoji (queda top)
        sem_ok = self._load_image_once(self.semaforo_label, self.semaforo_path, target_height=66)
        if not sem_ok:
            self.semaforo_label.configure(text="🚦", font=(self.FONT_BODY, 54))

    # -----------------------------
    # Responsive
    # -----------------------------
    def _on_resize(self, _event=None):
        if self._lock_resize:
            return
        if self._resize_job is not None:
            try:
                self.after_cancel(self._resize_job)
            except Exception:
                pass
        self._resize_job = self.after(20, self._apply_layout)

    def _apply_layout(self):
        w = self.winfo_width()
        h = self.winfo_height()
        if w <= 1 or h <= 1:
            return

        # forzar mínimo (para que no se rompa el diseño)
        if w < self.MIN_W or h < self.MIN_H:
            self._lock_resize = True
            self.geometry(f"{max(w, self.MIN_W)}x{max(h, self.MIN_H)}")
            self._lock_resize = False
            w = self.winfo_width()
            h = self.winfo_height()

        # Escala suave según ancho de ventana
        w_min, w_max = self.MIN_W, self.BASE_W
        t = (w - w_min) / (w_max - w_min)
        t = max(0.0, min(1.0, t))

        def lerp(a, b):
            return int(a + (b - a) * t)

        # Card con ancho máximo
        target_card_w = lerp(940, 1120)
        max_card_w = 1180
        card_w = min(max_card_w, target_card_w)

        self.card.configure(width=card_w)
        self.card.grid_configure(padx=32)

        # Padding interno
        content_padx = lerp(48, 72)
        content_pady = lerp(42, 56)
        self.content.grid_configure(padx=content_padx, pady=content_pady)

        # Tipos
        title_size = lerp(44, 58)
        sub_size = lerp(18, 24)
        btn_font = lerp(14, 18)
        badge_font = lerp(11, 14)
        btn_h = lerp(56, 70)

        # Fuente título: elige la mejor instalada
        for fam in self.TITLE_FONTS:
            try:
                self.title_label.configure(font=(fam, title_size))
                break
            except Exception:
                continue

        self.subtitle_label.configure(font=(self.FONT_BODY, sub_size))
        self.badge.configure(font=(self.FONT_BODY, badge_font, "bold"))
        self.footer.configure(font=(self.FONT_BODY, lerp(11, 13)))

        # Botones grandes (como te gustaba)
        self.btn_csv.configure(height=btn_h, font=(self.FONT_BODY, btn_font, "bold"))
        self.btn_pred.configure(height=btn_h, font=(self.FONT_BODY, btn_font, "bold"))
        self.btn_doc.configure(height=btn_h, font=(self.FONT_BODY, btn_font, "bold"))

        self.btn_csv.grid_configure(pady=(0, lerp(10, 16)))
        self.btn_pred.grid_configure(pady=(0, lerp(10, 16)))
        self.btn_doc.grid_configure(pady=(0, lerp(10, 16)))

        self.footer.grid_configure(pady=(lerp(14, 22), 0))
        self.subtitle_label.grid_configure(pady=(0, lerp(18, 26)))

        # Semáforo escalado
        if PIL_OK and self.semaforo_path.exists():
            self._load_image_once(self.semaforo_label, self.semaforo_path, target_height=lerp(52, 66))
        else:
            self.semaforo_label.configure(font=(self.FONT_BODY, lerp(42, 54)))

    # -----------------------------
    # Carga imagen (helper)
    # -----------------------------
    def _load_image_once(self, label: ctk.CTkLabel, path: Path, target_height: int = 34) -> bool:
        """
        Cargo una imagen y la inserto en un CTkLabel.
        Devuelve True si la pudo cargar, False si no.
        """
        if (not PIL_OK) or (not path.exists()):
            label.configure(text="")
            return False

        try:
            img = Image.open(path).convert("RGBA")
            w, h = img.size
            new_h = target_height
            new_w = int((w / h) * new_h)
            img = img.resize((new_w, new_h))

            ctk_img = ctk.CTkImage(light_image=img, dark_image=img, size=(new_w, new_h))
            label.configure(image=ctk_img, text="")
            label.image = ctk_img  # mantener referencia (si no, desaparece)
            return True
        except Exception:
            label.configure(text="")
            return False

    # -----------------------------
    # Acciones (botones)
    # -----------------------------
    def ver_csv(self):
        """
        Abre la ventana de visor de CSV.
        La ventana principal se oculta y vuelve al pulsar 'Volver al inicio' en el visor.
        """
        abrir_visor_csv(self)

    def prediccion(self):
        abrir_ventana_prediccion(self)
        """
        (De momento) placeholder: aquí luego conectaremos tu módulo real de predicción.
        """
        print("Botón PREDICCIÓN pulsado")

    def abrir_documentacion(self):
        """
        Abre la documentación dentro de la app (NO navegador),
        en una ventana maximizada y con botón 'Volver al inicio'.
        """
        abrir_visor_documentacion(self, self.doc_pdf)

    def salir(self):
        """
        Cierra toda la aplicación.
        """
        self.destroy()
