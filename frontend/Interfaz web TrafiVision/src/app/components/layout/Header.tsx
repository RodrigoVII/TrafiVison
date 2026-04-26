import { useState } from 'react';
import { Bell, HelpCircle, X, CheckCircle2, AlertCircle } from 'lucide-react';
import { Badge } from '../ui/Badge';
import { Tooltip } from '../ui/Tooltip';

interface HeaderProps {
  title: string;
  subtitle?: string;
}

export function Header({ title, subtitle }: HeaderProps) {
  const [showHelp, setShowHelp] = useState(false);
  const [showNotifications, setShowNotifications] = useState(false);

  const [notifications, setNotifications] = useState([
    {
      id: 1,
      text: 'Dashboard conectado a MariaDB',
      type: 'success',
      read: false,
    },
    {
      id: 2,
      text: 'Modelo predictivo disponible',
      type: 'info',
      read: false,
    },
    {
      id: 3,
      text: 'Revisa el estado de las cámaras',
      type: 'warning',
      read: false,
    },
  ]);

  const unreadCount = notifications.filter((notification) => !notification.read).length;

  const markAsRead = (id: number) => {
    setNotifications((prev) =>
      prev.map((notification) =>
        notification.id === id ? { ...notification, read: true } : notification
      )
    );
  };

  const markAllAsRead = () => {
    setNotifications((prev) =>
      prev.map((notification) => ({ ...notification, read: true }))
    );
  };

  const helpContent: Record<string, string> = {
    'Dashboard': 'Vista general del estado del tráfico en tiempo real. Consulta las métricas principales y alertas recientes.',
    'Predicción de Tráfico': 'Selecciona fecha, hora y cámara para obtener una predicción del nivel de tráfico esperado.',
    'Histórico de Tráfico': 'Analiza datos históricos usando los filtros disponibles. Visualiza tendencias y patrones de tráfico.',
    'Cámaras de Tráfico': 'Monitorea el estado de todas las cámaras en tiempo real. Verde=activa, Rojo=error.',
    'Administración': 'Panel para ejecutar tareas administrativas: scraping, actualización de clima y entrenamiento de modelos.',
    'Entrenamiento ML': 'Gestiona y entrena modelos de Machine Learning. Selecciona el algoritmo y ejecuta el entrenamiento.',
    'Actualización de Datos': 'Ejecuta procesos de scraping y actualización de datos. Consulta el historial de ejecuciones.',
    'Gestión de Usuarios': 'Administra usuarios del sistema: crear, modificar roles y eliminar usuarios.',
    'Mi Perfil': 'Configura tu información personal, cambia tu contraseña y ajusta preferencias.',
  };

  return (
    <header className="border-b border-border bg-card">
      <div className="h-16 px-6 flex items-center justify-between">
        <div>
          <h2 className="text-xl">{title}</h2>
          {subtitle && <p className="text-sm text-muted-foreground">{subtitle}</p>}
        </div>

        <div className="flex items-center gap-2">
          <Tooltip content="Ayuda sobre esta página">
            <button
              onClick={() => setShowHelp(!showHelp)}
              className="p-2 hover:bg-accent rounded-lg transition-colors"
            >
              <HelpCircle className="h-5 w-5" />
            </button>
          </Tooltip>

          <div className="relative">
            <Tooltip content="Notificaciones del sistema">
              <button
                onClick={() => setShowNotifications(!showNotifications)}
                className="relative p-2 hover:bg-accent rounded-lg transition-colors"
              >
                <Bell className="h-5 w-5" />

                {unreadCount > 0 && (
                  <Badge
                    variant="destructive"
                    className="absolute -top-1 -right-1 h-5 w-5 p-0 flex items-center justify-center"
                  >
                    {unreadCount}
                  </Badge>
                )}
              </button>
            </Tooltip>

            {showNotifications && (
              <div className="absolute right-0 mt-2 w-80 bg-card border border-border rounded-lg shadow-lg z-50">
                <div className="p-3 border-b border-border flex items-center justify-between">
                  <p className="text-sm">Notificaciones</p>

                  {unreadCount > 0 && (
                    <button
                      onClick={markAllAsRead}
                      className="text-xs text-primary hover:underline"
                    >
                      Marcar todas
                    </button>
                  )}
                </div>

                <div className="max-h-72 overflow-y-auto">
                  {notifications.map((notification) => (
                    <button
                      key={notification.id}
                      onClick={() => markAsRead(notification.id)}
                      className={`w-full text-left p-3 text-sm flex items-start gap-2 hover:bg-accent transition-colors ${
                        !notification.read ? 'bg-primary/5' : ''
                      }`}
                    >
                      {notification.type === 'success' && (
                        <CheckCircle2 className="h-4 w-4 text-success mt-0.5 flex-shrink-0" />
                      )}

                      {notification.type === 'warning' && (
                        <AlertCircle className="h-4 w-4 text-warning mt-0.5 flex-shrink-0" />
                      )}

                      {notification.type === 'info' && (
                        <Bell className="h-4 w-4 text-primary mt-0.5 flex-shrink-0" />
                      )}

                      <div>
                        <p>{notification.text}</p>
                        <p className="text-xs text-muted-foreground mt-1">
                          {notification.read ? 'Leída' : 'Nueva'}
                        </p>
                      </div>
                    </button>
                  ))}

                  {notifications.length === 0 && (
                    <div className="p-3 text-sm text-muted-foreground">
                      No hay notificaciones
                    </div>
                  )}
                </div>
              </div>
            )}
          </div>
        </div>
      </div>

      {showHelp && helpContent[title] && (
        <div className="px-6 py-4 bg-primary/5 border-t border-primary/20">
          <div className="flex items-start justify-between gap-4">
            <div className="flex items-start gap-3">
              <HelpCircle className="h-5 w-5 text-primary mt-0.5 flex-shrink-0" />

              <div>
                <p className="text-sm font-medium mb-1">Ayuda: {title}</p>
                <p className="text-sm text-muted-foreground">{helpContent[title]}</p>
              </div>
            </div>

            <button
              onClick={() => setShowHelp(false)}
              className="p-1 hover:bg-accent rounded transition-colors"
            >
              <X className="h-4 w-4" />
            </button>
          </div>
        </div>
      )}
    </header>
  );
}