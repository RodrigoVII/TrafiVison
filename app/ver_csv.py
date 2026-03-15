"""
app/ver_csv.py

Ventana de exploración y gestión de datos de TrafiVision.

Ahora esta ventana:
- Lee los datos desde la BASE DE DATOS MariaDB
- Permite buscar por calle
- Permite filtrar por nivel de tráfico, franja horaria y tipo de día
- Permite refrescar los datos
- Permite eliminar un registro seleccionado
- Mantiene estilo visual tipo Universidad Europea
- Se abre maximizada

NOTA:
La función sigue llamándose abrir_visor_csv() para no romper
el resto de la aplicación, aunque ya no trabaja con CSV.
"""

import customtkinter as ctk
from tkinter import messagebox
from tkinter import ttk

from db.db_client import get_capturas_dataframe, get_connection


def abrir_visor_csv(app_principal):
    """
    Abre la ventana de visualización y gestión de datos,
    oculta la ventana principal y muestra los registros obtenidos
    desde la base de datos MariaDB.
    """

    # ---------------------------------------------------------
    # Colores corporativos
    # ---------------------------------------------------------
    BG = "#F7F7F7"
    CARD = "#FFFFFF"
    TEXT = "#111111"
    SUBTEXT = "#444444"
    UE_RED = "#E30613"
    UE_RED_DARK = "#B0040F"
    SOFT = "#F5F5F5"

    # ---------------------------------------------------------
    # Función para cargar datos desde la BD
    # ---------------------------------------------------------
    def cargar_dataframe():
        try:
            df_local = get_capturas_dataframe().copy()
        except Exception as e:
            messagebox.showerror(
                "Error",
                f"No se pudieron cargar los datos desde la base de datos.\n\nDetalle:\n{e}"
            )
            return None

        # Renombrado visual por claridad
        column_rename_map = {
            "calle": "calle",
            "fecha": "fecha",
            "hora": "hora",
            "num_vehiculos": "num_vehiculos",
            "nivel_trafico": "nivel_trafico",
            "temperatura": "temperatura",
            "lluvia": "lluvia",
            "humedad": "humedad",
            "laborable": "laborable",
            "franja_horaria": "franja_horaria"
        }

        df_local = df_local.rename(columns=column_rename_map)

        # Formatear laborable
        if "laborable" in df_local.columns:
            def format_laborable(value):
                if value == 1:
                    return "Laborable"
                if value == 0:
                    return "No laborable"
                return value

            df_local["laborable"] = df_local["laborable"].apply(format_laborable)

        # Formatear hora
        if "hora" in df_local.columns:
            df_local["hora"] = df_local["hora"].astype(str).str.replace("0 days ", "", regex=False)

        return df_local

    # ---------------------------------------------------------
    # Eliminar un registro de la base de datos
    # ---------------------------------------------------------
    def eliminar_registro_bd(calle, fecha, hora):
        """
        Elimina una captura concreta a partir de:
        - calle
        - fecha
        - hora

        Como en la BD existe una restricción UNIQUE(camara_id, timestamp),
        esa combinación identifica una captura de forma única.
        """
        connection = None
        cursor = None

        try:
            connection = get_connection()
            cursor = connection.cursor()

            timestamp = f"{fecha} {hora}"

            query = """
                DELETE cp
                FROM captura cp
                JOIN camara c ON cp.camara_id = c.id
                WHERE c.codigo = %s
                  AND cp.timestamp = %s
            """

            cursor.execute(query, (calle, timestamp))
            connection.commit()

            return cursor.rowcount

        finally:
            if cursor:
                cursor.close()
            if connection:
                connection.close()

    # ---------------------------------------------------------
    # Cargar datos iniciales
    # ---------------------------------------------------------
    df_original = cargar_dataframe()
    if df_original is None:
        return

    df_actual = df_original.copy()

    # ---------------------------------------------------------
    # Ocultar ventana principal
    # ---------------------------------------------------------
    app_principal.withdraw()

    # ---------------------------------------------------------
    # Crear ventana visor
    # ---------------------------------------------------------
    visor = ctk.CTkToplevel(app_principal)
    visor.title("Explorador de datos TrafiVision")
    visor.geometry("1300x700")
    visor.minsize(1100, 650)
    visor.configure(fg_color=BG)

    try:
        visor.state("zoomed")
    except Exception:
        pass

    visor.lift()
    visor.focus_force()

    # ---------------------------------------------------------
    # Volver al inicio
    # ---------------------------------------------------------
    def volver_al_inicio():
        visor.destroy()
        app_principal.deiconify()

        try:
            app_principal.state("zoomed")
        except Exception:
            pass

        app_principal.lift()
        app_principal.focus_force()

    visor.protocol("WM_DELETE_WINDOW", volver_al_inicio)

    # ---------------------------------------------------------
    # Contenedor principal
    # ---------------------------------------------------------
    container = ctk.CTkFrame(visor, fg_color=CARD, corner_radius=16)
    container.pack(fill="both", expand=True, padx=18, pady=18)

    # ---------------------------------------------------------
    # Título
    # ---------------------------------------------------------
    titulo = ctk.CTkLabel(
        container,
        text="Explorador de datos",
        font=("Segoe UI", 24, "bold"),
        text_color=TEXT
    )
    titulo.pack(pady=(18, 6))

    info_label = ctk.CTkLabel(
        container,
        text=f"Origen: Base de datos MariaDB   |   Filas visibles: {len(df_actual)}   |   Columnas: {len(df_actual.columns)}",
        font=("Segoe UI", 13),
        text_color=SUBTEXT
    )
    info_label.pack(pady=(0, 12))

    # ---------------------------------------------------------
    # Panel de filtros
    # ---------------------------------------------------------
    filtros_frame = ctk.CTkFrame(container, fg_color=SOFT, corner_radius=14)
    filtros_frame.pack(fill="x", padx=12, pady=(0, 12))

    filtros_frame.grid_columnconfigure((0, 1, 2, 3), weight=1)

    ctk.CTkLabel(
        filtros_frame,
        text="Buscar calle",
        font=("Segoe UI", 13, "bold"),
        text_color=TEXT
    ).grid(row=0, column=0, padx=12, pady=(12, 4), sticky="w")

    search_entry = ctk.CTkEntry(filtros_frame, placeholder_text="Escribe parte del nombre de la calle...")
    search_entry.grid(row=1, column=0, padx=12, pady=(0, 12), sticky="ew")

    ctk.CTkLabel(
        filtros_frame,
        text="Nivel tráfico",
        font=("Segoe UI", 13, "bold"),
        text_color=TEXT
    ).grid(row=0, column=1, padx=12, pady=(12, 4), sticky="w")

    nivel_menu = ctk.CTkOptionMenu(
        filtros_frame,
        values=["Todos", "bajo", "medio", "alto"]
    )
    nivel_menu.set("Todos")
    nivel_menu.grid(row=1, column=1, padx=12, pady=(0, 12), sticky="ew")

    ctk.CTkLabel(
        filtros_frame,
        text="Franja horaria",
        font=("Segoe UI", 13, "bold"),
        text_color=TEXT
    ).grid(row=0, column=2, padx=12, pady=(12, 4), sticky="w")

    franja_menu = ctk.CTkOptionMenu(
        filtros_frame,
        values=["Todas", "Madrugada", "Mañana", "Mediodía", "Tarde", "Noche"]
    )
    franja_menu.set("Todas")
    franja_menu.grid(row=1, column=2, padx=12, pady=(0, 12), sticky="ew")

    ctk.CTkLabel(
        filtros_frame,
        text="Tipo de día",
        font=("Segoe UI", 13, "bold"),
        text_color=TEXT
    ).grid(row=0, column=3, padx=12, pady=(12, 4), sticky="w")

    laborable_menu = ctk.CTkOptionMenu(
        filtros_frame,
        values=["Todos", "Laborable", "No laborable"]
    )
    laborable_menu.set("Todos")
    laborable_menu.grid(row=1, column=3, padx=12, pady=(0, 12), sticky="ew")

    # ---------------------------------------------------------
    # Frame de tabla
    # ---------------------------------------------------------
    table_frame = ctk.CTkFrame(container, fg_color=CARD)
    table_frame.pack(fill="both", expand=True, padx=12, pady=(0, 12))

    # ---------------------------------------------------------
    # Estilos tabla
    # ---------------------------------------------------------
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

    style.map(
        "Treeview",
        background=[("selected", "#111111")],
        foreground=[("selected", "#FFFFFF")]
    )

    # ---------------------------------------------------------
    # Columnas visibles
    # ---------------------------------------------------------
    columnas_visibles = [
        "calle",
        "fecha",
        "hora",
        "num_vehiculos",
        "nivel_trafico",
        "temperatura",
        "lluvia",
        "laborable",
        "franja_horaria"
    ]

    columnas_visibles = [c for c in columnas_visibles if c in df_actual.columns]

    tree = ttk.Treeview(
        table_frame,
        columns=columnas_visibles,
        show="headings",
        selectmode="browse"
    )

    scroll_y = ttk.Scrollbar(table_frame, orient="vertical", command=tree.yview)
    scroll_x = ttk.Scrollbar(table_frame, orient="horizontal", command=tree.xview)

    tree.configure(
        yscrollcommand=scroll_y.set,
        xscrollcommand=scroll_x.set
    )

    tree.grid(row=0, column=0, sticky="nsew")
    scroll_y.grid(row=0, column=1, sticky="ns")
    scroll_x.grid(row=1, column=0, sticky="ew")

    table_frame.grid_rowconfigure(0, weight=1)
    table_frame.grid_columnconfigure(0, weight=1)

    for col in columnas_visibles:
        tree.heading(col, text=col)
        tree.column(col, anchor="w", width=150, stretch=True)

    tree.tag_configure("row_white", background="#FFFFFF", foreground="#111111")
    tree.tag_configure("row_red", background=UE_RED, foreground="#FFFFFF")

    # ---------------------------------------------------------
    # Función para rellenar tabla
    # ---------------------------------------------------------
    def poblar_tabla(df_mostrar):
        tree.delete(*tree.get_children())

        for i, (_, row) in enumerate(df_mostrar.iterrows()):
            tag = "row_red" if i % 2 == 1 else "row_white"
            valores = [row[col] if col in row else "" for col in columnas_visibles]
            tree.insert("", "end", values=valores, tags=(tag,))

        info_label.configure(
            text=f"Origen: Base de datos MariaDB   |   Filas visibles: {len(df_mostrar)}   |   Columnas: {len(columnas_visibles)}"
        )

    poblar_tabla(df_actual)

    # ---------------------------------------------------------
    # Aplicar filtros
    # ---------------------------------------------------------
    def aplicar_filtros():
        nonlocal df_actual

        df_filtrado = df_original.copy()

        texto_busqueda = search_entry.get().strip().lower()
        if texto_busqueda and "calle" in df_filtrado.columns:
            df_filtrado = df_filtrado[
                df_filtrado["calle"].astype(str).str.lower().str.contains(texto_busqueda, na=False)
            ]

        nivel = nivel_menu.get()
        if nivel != "Todos" and "nivel_trafico" in df_filtrado.columns:
            df_filtrado = df_filtrado[
                df_filtrado["nivel_trafico"].astype(str).str.lower() == nivel.lower()
            ]

        franja = franja_menu.get()
        if franja != "Todas" and "franja_horaria" in df_filtrado.columns:
            df_filtrado = df_filtrado[
                df_filtrado["franja_horaria"].astype(str).str.lower() == franja.lower()
            ]

        laborable = laborable_menu.get()
        if laborable != "Todos" and "laborable" in df_filtrado.columns:
            df_filtrado = df_filtrado[
                df_filtrado["laborable"].astype(str).str.lower() == laborable.lower()
            ]

        df_actual = df_filtrado
        poblar_tabla(df_actual)

    # ---------------------------------------------------------
    # Limpiar filtros
    # ---------------------------------------------------------
    def limpiar_filtros():
        nonlocal df_actual

        search_entry.delete(0, "end")
        nivel_menu.set("Todos")
        franja_menu.set("Todas")
        laborable_menu.set("Todos")

        df_actual = df_original.copy()
        poblar_tabla(df_actual)

    # ---------------------------------------------------------
    # Refrescar datos desde la BD
    # ---------------------------------------------------------
    def refrescar_datos():
        nonlocal df_original, df_actual

        nuevo_df = cargar_dataframe()
        if nuevo_df is None:
            return

        df_original = nuevo_df
        df_actual = df_original.copy()

        limpiar_filtros()
        poblar_tabla(df_actual)

    # ---------------------------------------------------------
    # Eliminar fila seleccionada
    # ---------------------------------------------------------
    def eliminar_seleccionado():
        selected = tree.selection()

        if not selected:
            messagebox.showwarning(
                "Sin selección",
                "Debes seleccionar una fila para eliminarla."
            )
            return

        values = tree.item(selected[0], "values")

        if len(values) < 3:
            messagebox.showerror(
                "Error",
                "No se pudo identificar correctamente el registro seleccionado."
            )
            return

        calle = values[0]
        fecha = values[1]
        hora = values[2]

        confirmar = messagebox.askyesno(
            "Confirmar eliminación",
            f"¿Seguro que quieres eliminar este registro?\n\n"
            f"Calle: {calle}\n"
            f"Fecha: {fecha}\n"
            f"Hora: {hora}"
        )

        if not confirmar:
            return

        try:
            borrados = eliminar_registro_bd(calle, fecha, hora)

            if borrados > 0:
                messagebox.showinfo(
                    "Registro eliminado",
                    "El registro se eliminó correctamente."
                )
                refrescar_datos()
            else:
                messagebox.showwarning(
                    "No eliminado",
                    "No se encontró ningún registro para eliminar."
                )

        except Exception as e:
            messagebox.showerror(
                "Error al eliminar",
                f"No se pudo eliminar el registro.\n\nDetalle:\n{e}"
            )

    # ---------------------------------------------------------
    # Botonera superior de filtros
    # ---------------------------------------------------------
    acciones_frame = ctk.CTkFrame(container, fg_color="transparent")
    acciones_frame.pack(fill="x", padx=12, pady=(0, 8))

    btn_aplicar = ctk.CTkButton(
        acciones_frame,
        text="Aplicar filtros",
        height=40,
        corner_radius=12,
        fg_color=UE_RED,
        hover_color=UE_RED_DARK,
        text_color="white",
        font=("Segoe UI", 13, "bold"),
        command=aplicar_filtros
    )
    btn_aplicar.pack(side="left", padx=(0, 10))

    btn_limpiar = ctk.CTkButton(
        acciones_frame,
        text="Limpiar filtros",
        height=40,
        corner_radius=12,
        fg_color="#FFFFFF",
        hover_color="#F0F0F0",
        border_width=1,
        border_color="#CCCCCC",
        text_color=TEXT,
        font=("Segoe UI", 13, "bold"),
        command=limpiar_filtros
    )
    btn_limpiar.pack(side="left", padx=(0, 10))

    btn_refrescar = ctk.CTkButton(
        acciones_frame,
        text="Refrescar",
        height=40,
        corner_radius=12,
        fg_color="#111111",
        hover_color="#2A2A2A",
        text_color="white",
        font=("Segoe UI", 13, "bold"),
        command=refrescar_datos
    )
    btn_refrescar.pack(side="left", padx=(0, 10))

    btn_eliminar = ctk.CTkButton(
        acciones_frame,
        text="Eliminar seleccionado",
        height=40,
        corner_radius=12,
        fg_color="#FFFFFF",
        hover_color="#FFF0F0",
        border_width=1,
        border_color=UE_RED,
        text_color=UE_RED,
        font=("Segoe UI", 13, "bold"),
        command=eliminar_seleccionado
    )
    btn_eliminar.pack(side="right")

    # ---------------------------------------------------------
    # Footer
    # ---------------------------------------------------------
    footer = ctk.CTkFrame(container, fg_color="transparent")
    footer.pack(fill="x", padx=12, pady=(0, 16))

    btn_volver = ctk.CTkButton(
        footer,
        text="Volver al inicio",
        height=44,
        corner_radius=14,
        fg_color=UE_RED,
        hover_color=UE_RED_DARK,
        text_color="white",
        font=("Segoe UI", 14, "bold"),
        command=volver_al_inicio
    )
    btn_volver.pack(side="right")