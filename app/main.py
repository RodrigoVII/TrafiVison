import customtkinter as ctk
from app.ventana_principal import VentanaPrincipal

def main():
    # Estilo general
    ctk.set_appearance_mode("light")   # Fondo blanco limpio
    ctk.set_default_color_theme("blue")  # No importa mucho si defines colores manuales

    app = VentanaPrincipal()
    app.mainloop()

if __name__ == "__main__":
    main()
