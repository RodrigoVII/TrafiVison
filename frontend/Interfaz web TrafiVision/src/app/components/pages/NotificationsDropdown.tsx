import { useState } from 'react';
import { Bell, CheckCircle2, AlertCircle } from 'lucide-react';

export function NotificationsDropdown() {
  const [open, setOpen] = useState(false);

  const [notifications, setNotifications] = useState([
    {
      id: 1,
      text: 'Modelo entrenado correctamente',
      type: 'success',
      read: false,
    },
    {
      id: 2,
      text: 'Web scraping completado',
      type: 'info',
      read: false,
    },
    {
      id: 3,
      text: 'Cámara C004 sin conexión',
      type: 'warning',
      read: true,
    },
  ]);

  const unreadCount = notifications.filter(n => !n.read).length;

  const markAsRead = (id: number) => {
    setNotifications(prev =>
      prev.map(n => n.id === id ? { ...n, read: true } : n)
    );
  };

  return (
    <div className="relative">
      {/* Botón campana */}
      <button
        onClick={() => setOpen(!open)}
        className="relative p-2 rounded-lg hover:bg-muted"
      >
        <Bell className="h-5 w-5" />

        {unreadCount > 0 && (
          <span className="absolute -top-1 -right-1 bg-red-500 text-white text-xs w-5 h-5 flex items-center justify-center rounded-full">
            {unreadCount}
          </span>
        )}
      </button>

      {/* Dropdown */}
      {open && (
        <div className="absolute right-0 mt-2 w-72 bg-white border rounded-lg shadow-lg z-50">
          <div className="p-3 border-b">
            <p className="text-sm">Notificaciones</p>
          </div>

          <div className="max-h-60 overflow-y-auto">
            {notifications.map(n => (
              <div
                key={n.id}
                onClick={() => markAsRead(n.id)}
                className={`p-3 text-sm cursor-pointer flex items-start gap-2 ${
                  !n.read ? 'bg-blue-50' : ''
                }`}
              >
                {n.type === 'success' && <CheckCircle2 className="h-4 w-4 text-green-500 mt-0.5" />}
                {n.type === 'warning' && <AlertCircle className="h-4 w-4 text-yellow-500 mt-0.5" />}
                {n.type === 'info' && <Bell className="h-4 w-4 text-blue-500 mt-0.5" />}

                <span>{n.text}</span>
              </div>
            ))}
          </div>

          {notifications.length === 0 && (
            <div className="p-3 text-sm text-muted-foreground">
              No hay notificaciones
            </div>
          )}
        </div>
      )}
    </div>
  );
}