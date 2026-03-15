import customtkinter as ctk
from app.ventana_principal import VentanaPrincipal

def main():
    ctk.set_appearance_mode("light")
    app = VentanaPrincipal()
    app.mainloop()

if __name__ == "__main__":
    main()
